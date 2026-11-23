package mcpproxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
)

// StdioProxy 包装本地 Stdio MCP 工具子进程，对按行切分的 JSON-RPC 报文进行拦截录制、Mock 或混沌注入。
type StdioProxy struct {
	interceptor *Interceptor
	command     string
	args        []string
	traceparent string
}

// NewStdioProxy 创建 Stdio 通道的 MCP 代理；严格离线 Mock 模式下 command 可为空。
func NewStdioProxy(interceptor *Interceptor, command string, args []string, traceparent string) *StdioProxy {
	return &StdioProxy{
		interceptor: interceptor,
		command:     command,
		args:        append([]string(nil), args...),
		traceparent: traceparent,
	}
}

// ServeStreams 从 in 读取客户端逐行 JSON-RPC 报文，将结果写回 out；必要时拉起并管理下游子进程。
func (p *StdioProxy) ServeStreams(ctx context.Context, in io.Reader, out io.Writer) error {
	var (
		cmd       *exec.Cmd
		upStdin   io.WriteCloser
		upReader  *bufio.Reader
		upMu      sync.Mutex
		childDone chan error
	)

	if p.command != "" {
		cmd = exec.CommandContext(ctx, p.command, p.args...)
		var err error
		upStdin, err = cmd.StdinPipe()
		if err != nil {
			return fmt.Errorf("create upstream stdin pipe: %w", err)
		}
		upStdout, err := cmd.StdoutPipe()
		if err != nil {
			return fmt.Errorf("create upstream stdout pipe: %w", err)
		}
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start upstream mcp command %q: %w", p.command, err)
		}
		upReader = bufio.NewReaderSize(upStdout, 64*1024)
		childDone = make(chan error, 1)
		go func() {
			childDone <- cmd.Wait()
		}()
		defer func() {
			_ = upStdin.Close()
			if childDone != nil {
				<-childDone
			}
		}()
	}

	forward := func(_ context.Context, reqBytes []byte) ([]byte, error) {
		if upStdin == nil || upReader == nil {
			return nil, errors.New("upstream stdio process not started")
		}
		upMu.Lock()
		defer upMu.Unlock()

		line := append([]byte(nil), bytes.TrimSpace(reqBytes)...)
		line = append(line, '\n')
		if _, err := upStdin.Write(line); err != nil {
			return nil, fmt.Errorf("write upstream mcp stdin: %w", err)
		}
		for {
			respLine, err := upReader.ReadBytes('\n')
			trimmed := bytes.TrimSpace(respLine)
			if len(trimmed) > 0 {
				return trimmed, nil
			}
			if err != nil {
				return nil, fmt.Errorf("read upstream mcp stdout: %w", err)
			}
		}
	}

	baseCtx := ctx
	if p.traceparent != "" {
		baseCtx = WithTraceparent(baseCtx, p.traceparent)
	}

	inReader := bufio.NewReaderSize(in, 64*1024)
	var outMu sync.Mutex
	writeLine := func(b []byte) error {
		trimmed := bytes.TrimSpace(b)
		if len(trimmed) == 0 {
			return nil
		}
		outMu.Lock()
		defer outMu.Unlock()
		if _, err := out.Write(trimmed); err != nil {
			return err
		}
		_, err := out.Write([]byte{'\n'})
		return err
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rawLine, readErr := inReader.ReadBytes('\n')
		trimmed := bytes.TrimSpace(rawLine)
		if len(trimmed) > 0 {
			var msg JSONRPCMessage
			if uErr := json.Unmarshal(trimmed, &msg); uErr != nil {
				errResp, _ := BuildErrorResponse(nil, -32700, "invalid json-rpc request")
				if wErr := writeLine(errResp); wErr != nil {
					return wErr
				}
			} else if len(bytes.TrimSpace(msg.ID)) == 0 {
				// JSON-RPC 通知报文（无 id）：仅转发给上游不等待响应。
				if upStdin != nil {
					upMu.Lock()
					_, _ = upStdin.Write(append(append([]byte(nil), trimmed...), '\n'))
					upMu.Unlock()
				}
			} else if _, isTool := ParseToolCall(msg); isTool {
				respBytes, _, err := p.interceptor.HandleToolCall(baseCtx, msg, trimmed, forward)
				if err != nil {
					errResp, _ := BuildErrorResponse(msg.ID, -32603, err.Error())
					if wErr := writeLine(errResp); wErr != nil {
						return wErr
					}
				} else if wErr := writeLine(respBytes); wErr != nil {
					return wErr
				}
			} else {
				// 非 tools/call 的请求（如 initialize、tools/list）直接透传。
				if upStdin != nil {
					respBytes, fErr := forward(baseCtx, trimmed)
					if fErr != nil {
						errResp, _ := BuildErrorResponse(msg.ID, -32603, fErr.Error())
						if wErr := writeLine(errResp); wErr != nil {
							return wErr
						}
					} else if wErr := writeLine(respBytes); wErr != nil {
						return wErr
					}
				} else {
					errResp, _ := BuildErrorResponse(msg.ID, -32004, "offline stdio mock only supports tools/call")
					if wErr := writeLine(errResp); wErr != nil {
						return wErr
					}
				}
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}
