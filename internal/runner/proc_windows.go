//go:build windows

package runner

import (
	"os/exec"
	"strconv"
	"syscall"
)

func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
}

func killProcessTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	pid := strconv.Itoa(cmd.Process.Pid)
	// Windows 下通过 taskkill /T 连带子进程和孙进程一并强杀，防止残留后台进程。
	_ = exec.Command("taskkill", "/T", "/F", "/PID", pid).Run()
	return cmd.Process.Kill()
}
