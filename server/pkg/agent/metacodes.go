package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type metacodesBackend struct{ cfg Config }

var metacodesBlockedArgs = map[string]blockedArgMode{
	"-":                   blockedStandalone,
	"-p":                  blockedWithValue,
	"--print":             blockedWithValue,
	"--json":              blockedStandalone,
	"--stream-json":       blockedStandalone,
	"--permission":        blockedWithValue,
	"--permission-mode":   blockedWithValue,
	"--model":             blockedWithValue,
	"--resume":            blockedWithValue,
	"--resume-response":   blockedWithValue,
	"--session":           blockedWithValue,
	"--suspendable":       blockedStandalone,
	"--web":               blockedStandalone,
	"--dump-prompt":       blockedStandalone,
	"--dump-plugins":      blockedStandalone,
	"--version":           blockedStandalone,
	"--help":              blockedStandalone,
	"--teammate":          blockedWithValue,
	"--parent-session-id": blockedWithValue,
	"--teammate-lease-id": blockedWithValue,
	"--answers-file":      blockedWithValue,
	"--response-format":   blockedWithValue,
}

func buildMetacodesArgs(opts ExecOptions, cfg Config) ([]string, error) {
	args := []string{"--stream-json", "--permission-mode", "bypassPermissions"}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	for _, custom := range [][]string{opts.ExtraArgs, opts.CustomArgs} {
		for _, arg := range filterCustomArgs(custom, metacodesBlockedArgs, cfg.Logger) {
			if arg == "serve" || arg == "login" || arg == "logout" || arg == "update" {
				return nil, fmt.Errorf("metacodes custom arguments cannot select the %q subcommand", arg)
			}
			args = append(args, arg)
		}
	}
	return append(args, "-"), nil
}

func (b *metacodesBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	if hasManagedMcpConfig(opts.McpConfig) {
		return nil, fmt.Errorf("metacodes does not support Multica-managed MCP configuration; configure MCP in metacodes itself")
	}
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "metacodes"
	}
	if _, err := exec.LookPath(execPath); err != nil {
		return nil, fmt.Errorf("metacodes executable not found at %q; install metacodes or set MULTICA_METACODES_PATH: %w", execPath, err)
	}
	args, err := buildMetacodesArgs(opts, b.cfg)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := runContext(ctx, opts.Timeout)
	runtimeCmd := b.cfg.commandAt(execPath)
	runtimeCmd.Prefix = FilterLaunchPrefix("metacodes", runtimeCmd.Prefix, b.cfg.Logger)
	cmd := runtimeCmd.exec(runCtx, args...)
	hideAgentWindow(cmd)
	cmd.Dir = opts.Cwd
	cmd.Env = buildEnv(b.cfg.Env)
	b.cfg.logAgentCommand(cmd, newAgentCommandLogArgs(args, trustAgentCommandPositional(len(args)-1, "-")))
	stderr := newStderrTail(io.Discard, agentStderrTailBytes)
	stdout, outW, err := os.Pipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("metacodes stdout pipe: %w", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		cancel()
		_ = stdout.Close()
		_ = outW.Close()
		return nil, fmt.Errorf("metacodes stderr pipe: %w", err)
	}
	cmd.Stdout, cmd.Stderr = outW, errW
	inR, stdin, err := os.Pipe()
	if err != nil {
		cancel()
		_ = stdout.Close()
		_ = outW.Close()
		_ = errR.Close()
		_ = errW.Close()
		return nil, fmt.Errorf("metacodes stdin pipe: %w", err)
	}
	cmd.Stdin = inR
	if err := startOwnedProcessTree(cmd, b.cfg.Logger); err != nil {
		cancel()
		_ = stdout.Close()
		_ = outW.Close()
		_ = errR.Close()
		_ = errW.Close()
		_ = inR.Close()
		_ = stdin.Close()
		return nil, fmt.Errorf("start metacodes: %w", err)
	}
	_ = outW.Close()
	_ = errW.Close()
	_ = inR.Close()
	if opts.MaxTurns > 0 {
		b.cfg.Logger.Warn("metacodes does not support max_turns; use the task timeout to bound execution")
	}
	msgCh := make(chan Message, 256)
	resCh := make(chan Result, 1)
	var terminalObserved atomic.Bool
	waitCh := make(chan error, 1)
	go func(done chan<- error) { done <- cmd.Wait() }(waitCh)
	inputCh := make(chan error, 1)
	go func(done chan<- error) {
		_, err := io.WriteString(stdin, prompt)
		_ = stdin.Close()
		done <- err
	}(inputCh)
	stderrDone := make(chan struct{})
	go func(done chan<- struct{}) {
		_, _ = io.Copy(stderr, errR)
		close(done)
	}(stderrDone)
	reads := make(chan metacodesStreamRead, 16)
	stopReads := make(chan struct{})
	go readMetacodesStream(stdout, reads, stopReads)
	go func() {
		defer cancel()
		defer close(msgCh)
		defer close(resCh)
		started := time.Now()
		state := metacodesStreamState{model: opts.Model}
		var scanErr, exitErr, inputErr, runErr error
		ctxDone := runCtx.Done()
		var drain *time.Timer
		var drainC <-chan time.Time
		defer func() {
			if drain != nil {
				drain.Stop()
			}
		}()
		setDrainDeadline := func() {
			if drain == nil {
				drain = time.NewTimer(metacodesStreamDrainGrace)
			} else {
				drain.Reset(metacodesStreamDrainGrace)
			}
			drainC = drain.C
		}
		cleaning := false
		beginCleanup := func() {
			if cleaning {
				return
			}
			cleaning = true
			runErr = runCtx.Err()
			ctxDone = nil
			if waitCh == nil && state.sawResult && runErr == nil {
				terminalObserved.Store(true)
			}
			signalProcessGroup(cmd, syscall.SIGKILL)
			_ = stdin.Close()
			setDrainDeadline()
		}
		for waitCh != nil || reads != nil || stderrDone != nil || inputCh != nil {
			select {
			case item := <-reads:
				if item.done {
					reads, scanErr = nil, item.err
					if scanErr != nil {
						beginCleanup()
						cancel()
					}
				} else {
					handleMetacodesEvent(item.event, msgCh, &state)
				}
			case exitErr = <-waitCh:
				waitCh = nil
				if !cleaning {
					setDrainDeadline()
				}
			case inputErr = <-inputCh:
				inputCh = nil
			case <-stderrDone:
				stderrDone = nil
			case <-ctxDone:
				beginCleanup()
			case <-drainC:
				if cleaning {
					b.cfg.Logger.Warn("metacodes cleanup did not settle before drain deadline", "pid", cmd.Process.Pid)
					if waitCh != nil {
						exitErr = fmt.Errorf("process did not exit during cleanup")
					}
					waitCh, reads, stderrDone, inputCh = nil, nil, nil, nil
				} else {
					beginCleanup()
				}
			}
			if waitCh == nil && (state.sawResult || reads == nil) {
				beginCleanup()
			}
		}
		close(stopReads)
		_ = stdout.Close()
		_ = errR.Close()
		_ = stdin.Close()
		releaseProcessGroup(cmd)
		if state.sawResult {
			inputErr = nil
		}
		terminal := streamTerminalState{
			sawResult: state.sawResult, finalResultText: state.finalText,
			resultIsError: state.failed, scanErr: scanErr,
		}
		if state.failed {
			terminal.finalResultText = state.errorText
		}
		status, output, errText := finalizeStreamResult("metacodes", opts.Timeout, runErr, inputErr, exitErr, "", terminal, "")
		if errText != "" {
			errText = sanitizeAgentDiagnostic(withAgentStderr(errText, "metacodes", stderr.Tail()))
		}
		var usage map[string]TokenUsage
		if state.usage != nil {
			model := state.model
			if model == "" {
				model = "unknown"
			}
			usage = map[string]TokenUsage{model: *state.usage}
		}
		b.cfg.Logger.Info("metacodes finished", "pid", cmd.Process.Pid, "status", status)
		resCh <- Result{
			Status: status, Output: output, Error: errText,
			DurationMs: time.Since(started).Milliseconds(), Usage: usage,
		}
	}()
	return &Session{Messages: msgCh, Result: resCh, TerminalObserved: terminalObserved.Load}, nil
}

const metacodesStreamDrainGrace = 2 * time.Second

type metacodesStreamRead struct {
	event metacodesStreamEvent
	done  bool
	err   error
}

func readMetacodesStream(stdout io.Reader, reads chan<- metacodesStreamRead, stop <-chan struct{}) {
	scanner := newAgentStreamScanner(stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		var event metacodesStreamEvent
		if line == "" || json.Unmarshal([]byte(line), &event) != nil {
			continue
		}
		select {
		case reads <- metacodesStreamRead{event: event}:
		case <-stop:
			return
		}
	}
	select {
	case reads <- metacodesStreamRead{done: true, err: scanner.Err()}:
	case <-stop:
	}
}

type metacodesStreamEvent struct {
	Type             string `json:"type"`
	Text             string `json:"text"`
	StopReason       string `json:"stop_reason"`
	ID               string `json:"id"`
	Name             string `json:"name"`
	Input            string `json:"input"`
	Content          string `json:"content"`
	IsError          bool   `json:"is_error"`
	InputTokens      int64  `json:"input_tokens"`
	OutputTokens     int64  `json:"output_tokens"`
	CacheReadTokens  int64  `json:"cache_read_input_tokens"`
	CacheWriteTokens int64  `json:"cache_creation_input_tokens"`
	UsageStatus      string `json:"usage_status"`
}

type metacodesStreamState struct {
	model, finalText, errorText string
	started, sawResult, failed  bool
	usage                       *TokenUsage
}

func handleMetacodesEvent(e metacodesStreamEvent, ch chan<- Message, s *metacodesStreamState) {
	switch e.Type {
	case "turn_begin":
		if !s.started {
			s.started = true
			trySend(ch, Message{Type: MessageStatus, Status: "running"})
		}
	case "text":
		trySend(ch, Message{Type: MessageText, Content: e.Text})
	case "thinking":
		trySend(ch, Message{Type: MessageThinking, Content: e.Text})
	case "tool_start":
		var input map[string]any
		if json.Unmarshal([]byte(e.Input), &input) != nil && e.Input != "" {
			input = map[string]any{"input": e.Input}
		}
		trySend(ch, Message{Type: MessageToolUse, Tool: e.Name, CallID: e.ID, Input: input})
	case "tool_result":
		trySend(ch, Message{Type: MessageToolResult, Tool: e.Name, CallID: e.ID, Output: e.Content})
	case "result":
		s.sawResult = true
		s.finalText = e.Text
		if e.UsageStatus == "known" {
			s.usage = &TokenUsage{
				InputTokens: e.InputTokens, OutputTokens: e.OutputTokens,
				CacheReadTokens: e.CacheReadTokens, CacheWriteTokens: e.CacheWriteTokens,
			}
		}
		if e.StopReason != "end_turn" {
			s.failed = true
			s.errorText = "metacodes stopped: " + e.StopReason
			if detail := sanitizeAgentDiagnostic(e.Text); detail != "" {
				s.errorText += ": " + detail
			}
		}
	}
}
