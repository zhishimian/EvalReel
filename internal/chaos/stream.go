package chaos

import (
	"errors"
	"io"

	"evalreel/internal/config"
)

// ErrChaosStreamCut 表示流式输出到达配置块数后被混沌测试主动切断。
var ErrChaosStreamCut = errors.New("chaos: forced stream cut")

// WrapStream 在流式响应体读取途中按规则切断数据流或注入畸形 JSON。
func WrapStream(orig io.ReadCloser, rule *config.ChaosRule) io.ReadCloser {
	if rule == nil {
		return orig
	}
	if rule.Action != "cut_stream" && rule.Action != "malformed_json" {
		return orig
	}
	limit := rule.CutAfterChunks
	if limit <= 0 {
		limit = 1
	}
	return &chaosStreamReader{
		orig:       orig,
		action:     rule.Action,
		chunkLimit: limit,
	}
}

type chaosStreamReader struct {
	orig         io.ReadCloser
	action       string
	chunkLimit   int
	chunksSeen   int
	prevLineEnd  bool // 记录上一个字节是否刚结束了一行（用于精确识别跨 buffer 的 \n\n）
	pendingCR    bool
	triggered    bool
	malformedBuf []byte
}

func (c *chaosStreamReader) Read(p []byte) (int, error) {
	if c.triggered {
		if c.action == "malformed_json" && len(c.malformedBuf) > 0 {
			n := copy(p, c.malformedBuf)
			c.malformedBuf = c.malformedBuf[n:]
			if len(c.malformedBuf) == 0 {
				return n, io.EOF
			}
			return n, nil
		}
		if c.action == "malformed_json" {
			return 0, io.EOF
		}
		return 0, ErrChaosStreamCut
	}

	n, err := c.orig.Read(p)
	if n == 0 {
		return 0, err
	}

	cutIdx := -1
	for i := 0; i < n; i++ {
		b := p[i]
		if c.pendingCR {
			c.pendingCR = false
			if b == '\n' {
				if c.recordNewline() {
					cutIdx = i + 1
					break
				}
				continue
			}
			// 孤立的 \r 后跟其他字符，前一行也已算作换行结束
			c.prevLineEnd = false
		}

		if b == '\r' {
			c.pendingCR = true
			continue
		}
		if b == '\n' {
			if c.recordNewline() {
				cutIdx = i + 1
				break
			}
			continue
		}
		c.prevLineEnd = false
	}

	if cutIdx >= 0 {
		c.triggered = true
		if c.action == "malformed_json" {
			c.malformedBuf = []byte("data: {\"choices\":[{\"delta\":{\"content\":\n\n")
		}
		return cutIdx, nil
	}

	return n, err
}

func (c *chaosStreamReader) recordNewline() bool {
	if c.prevLineEnd {
		c.chunksSeen++
		c.prevLineEnd = false
		return c.chunksSeen >= c.chunkLimit
	}
	c.prevLineEnd = true
	return false
}

func (c *chaosStreamReader) Close() error {
	return c.orig.Close()
}
