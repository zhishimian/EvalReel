package sse

import (
	"context"
	"errors"
	"io"
	"sync"

	"evalreel/internal/model"
)

// TapReadCloser 包装上游响应流，在零延迟透传给客户端的同时旁路解析 SSE 事件。
type TapReadCloser struct {
	ctx        context.Context
	orig       io.ReadCloser
	pw         *io.PipeWriter
	pr         *io.PipeReader
	doneCh     chan struct{}
	closeOnce  sync.Once
	result     Result
	onComplete func(Result)
}

// NewTapReadCloser 创建一个流式旁路解析包装器并启动后台解析协程。
func NewTapReadCloser(ctx context.Context, orig io.ReadCloser, opts ParseOptions, onComplete func(Result)) *TapReadCloser {
	pr, pw := io.Pipe()
	t := &TapReadCloser{
		ctx:        ctx,
		orig:       orig,
		pw:         pw,
		pr:         pr,
		doneCh:     make(chan struct{}),
		onComplete: onComplete,
	}

	go func() {
		defer func() {
			// 解析提前退出时必须排空管道，避免后续 Read 写入 pw 时阻塞死锁。
			_, _ = io.Copy(io.Discard, pr)
			_ = pr.Close()
			close(t.doneCh)
		}()
		t.result = ParseStream(pr, opts)
	}()

	return t
}

// Read 从原始响应读取数据，复制一份给旁路管道后立刻返回给代理转发；遇到异常读错误时透传给解析器。
func (t *TapReadCloser) Read(p []byte) (int, error) {
	n, err := t.orig.Read(p)
	if n > 0 {
		_, _ = t.pw.Write(p[:n])
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			_ = t.pw.Close()
		} else {
			_ = t.pw.CloseWithError(err)
		}
	}
	return n, err
}

// Close 按顺序关闭原始流与旁路管道，等待解析协程彻底退出后再触发完成回调。
func (t *TapReadCloser) Close() error {
	var origErr error
	t.closeOnce.Do(func() {
		origErr = t.orig.Close()
		_ = t.pw.Close()
		<-t.doneCh

		res := t.result
		// 若客户端已完整收到 [DONE] 或 finish_reason 后才立刻关闭连接（如 Python openai SDK 退出循环时），仍记为正常完成。
		if t.ctx != nil && t.ctx.Err() != nil && !res.Completed {
			res.Status = model.StatusAborted
			if res.RawError == "" {
				res.RawError = t.ctx.Err().Error()
			}
		}
		if t.onComplete != nil {
			t.onComplete(res)
		}
	})
	return origErr
}
