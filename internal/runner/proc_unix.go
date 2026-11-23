//go:build !windows

package runner

import (
	"os/exec"
	"syscall"
)

func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
}

func killProcessTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	// 传入负 PID 杀掉整个进程组内的全部子孙进程。
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
