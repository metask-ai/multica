package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestMetacodesHelperProcess(t *testing.T) {
	if os.Getenv("METACODES_HELPER_PROCESS") != "1" {
		return
	}
	input, _ := io.ReadAll(os.Stdin)
	if capture := os.Getenv("METACODES_CAPTURE"); capture != "" {
		cwd, _ := os.Getwd()
		data, _ := json.Marshal(map[string]any{"args": os.Args[1:], "prompt": string(input), "cwd": cwd})
		_ = os.WriteFile(capture, data, 0o600)
	}
	stream := os.Getenv("METACODES_STREAM")
	if !slices.Contains(os.Args, "--json") {
		var plain []string
		for _, line := range strings.SplitAfter(stream, "\n") {
			var event struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if json.Unmarshal([]byte(line), &event) == nil && event.Type == "result" {
				line = event.Text + "\n"
			}
			plain = append(plain, line)
		}
		stream = strings.Join(plain, "")
	}
	fmt.Print(stream)
	fmt.Fprint(os.Stderr, os.Getenv("METACODES_STDERR"))
	if os.Getenv("METACODES_FAIL") == "1" {
		os.Exit(1)
	}
	os.Exit(0)
}

func newFakeMetacodes(t *testing.T, env map[string]string) Backend {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	env["METACODES_HELPER_PROCESS"] = "1"
	backend, err := New("metacodes", Config{
		ExecutablePath: executable, LaunchPrefix: []string{"-test.run=^TestMetacodesHelperProcess$", "--"},
		Env: env, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func runFakeMetacodes(t *testing.T, env map[string]string, opts ExecOptions) ([]Message, Result) {
	t.Helper()
	session, err := newFakeMetacodes(t, env).Execute(context.Background(), "do the task", opts)
	if err != nil {
		t.Fatal(err)
	}
	var messages []Message
	timeout := time.After(time.Minute)
	for msg := range session.Messages {
		messages = append(messages, msg)
	}
	select {
	case result := <-session.Result:
		return messages, result
	case <-timeout:
		t.Fatal("metacodes result not delivered")
	}
	return nil, Result{}
}

const metacodesStreamFixture = `{"type":"turn_begin","turn":1}
{"type":"thinking","text":"plan"}
{"type":"text","text":"Running "}
{"type":"text","text":"tests."}
{"type":"tool_start","id":"t1","name":"Bash","input":"{\"command\":\"go test\"}","input_bytes":20}
{"type":"tool_result","id":"t1","name":"Bash","is_error":false,"elapsed_ms":5,"content_bytes":2,"content":"ok"}
{"type":"turn_end","turn":1,"tool_calls":1}
{"type":"result","stop_reason":"end_turn","turns":2,"tool_calls":1,"continuations":0,"input_tokens":120,"output_tokens":30,"cache_read_input_tokens":7,"cache_creation_input_tokens":3,"cost_usd":0.0,"usage_status":"known","text":"All tests pass.","text_kind":"final"}
`

func TestMetacodesArgumentsProtectProtocol(t *testing.T) {
	args, err := buildMetacodesArgs(ExecOptions{
		Model:      "glm-5.2",
		ExtraArgs:  []string{"--reasoning-effort", "high", "--stream-json"},
		CustomArgs: []string{"--model", "other", "--json", "-p", "hijack", "--session=abc", "-", "--max-tokens", "4096"},
	}, Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--stream-json", "--json", "--permission-mode", "bypassPermissions", "--model", "glm-5.2",
		"--reasoning-effort", "high", "--max-tokens", "4096", "-"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %q, want %q", args, want)
	}
	if _, err := buildMetacodesArgs(ExecOptions{CustomArgs: []string{"serve"}}, Config{Logger: slog.Default()}); err == nil {
		t.Fatal("a custom subcommand must be rejected")
	}
}

func TestMetacodesExecuteStreamAndPrompt(t *testing.T) {
	capture := filepath.Join(t.TempDir(), "capture.json")
	cwd := t.TempDir()
	messages, result := runFakeMetacodes(t, map[string]string{
		"METACODES_STREAM": metacodesStreamFixture, "METACODES_CAPTURE": capture,
	}, ExecOptions{Cwd: cwd, Model: "glm-5.2"})

	if result.Status != "completed" || result.Output != "All tests pass." || result.Error != "" {
		t.Fatalf("result = %+v", result)
	}
	if got := result.Usage["glm-5.2"]; got != (TokenUsage{InputTokens: 120, OutputTokens: 30, CacheReadTokens: 7, CacheWriteTokens: 3}) {
		t.Fatalf("usage = %+v", result.Usage)
	}
	var text strings.Builder
	var sawThinking, sawTool, sawToolResult bool
	for _, msg := range messages {
		switch msg.Type {
		case MessageText:
			text.WriteString(msg.Content)
		case MessageThinking:
			sawThinking = msg.Content == "plan"
		case MessageToolUse:
			sawTool = msg.Tool == "Bash" && msg.CallID == "t1" && msg.Input["command"] == "go test"
		case MessageToolResult:
			sawToolResult = msg.CallID == "t1" && msg.Output == "ok"
		}
	}
	if text.String() != "Running tests." || !sawThinking || !sawTool || !sawToolResult {
		t.Fatalf("messages = %+v", messages)
	}

	var captured struct {
		Args   []string `json:"args"`
		Prompt string   `json:"prompt"`
		Cwd    string   `json:"cwd"`
	}
	data, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &captured); err != nil {
		t.Fatal(err)
	}
	if captured.Prompt != "do the task" || captured.Args[len(captured.Args)-1] != "-" {
		t.Fatalf("captured = %+v", captured)
	}
	resolvedCwd, _ := filepath.EvalSymlinks(cwd)
	if gotCwd, _ := filepath.EvalSymlinks(captured.Cwd); gotCwd != resolvedCwd {
		t.Fatalf("cwd = %q, want %q", captured.Cwd, cwd)
	}
}

func TestMetacodesFailurePaths(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		wantError string
	}{
		{
			name: "non end_turn stop reason",
			env: map[string]string{
				"METACODES_STREAM": `{"type":"result","stop_reason":"api_error","usage_status":"unknown","text":"rate limited","text_kind":"none"}` + "\n",
				"METACODES_FAIL":   "1",
			},
			wantError: "metacodes stopped: api_error: rate limited",
		},
		{
			name:      "no terminal result",
			env:       map[string]string{"METACODES_STREAM": `{"type":"text","text":"partial"}` + "\n", "METACODES_STDERR": "Authentication required."},
			wantError: "metacodes stream ended without terminal result",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, result := runFakeMetacodes(t, tc.env, ExecOptions{Cwd: t.TempDir()})
			if result.Status != "failed" || result.Output != "" || !strings.Contains(result.Error, tc.wantError) {
				t.Fatalf("result = %+v, want error containing %q", result, tc.wantError)
			}
		})
	}
}

func TestMetacodesRejectsManagedMCPAndMissingExecutable(t *testing.T) {
	backend, err := New("metacodes", Config{ExecutablePath: filepath.Join(t.TempDir(), "missing-metacodes"), Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Execute(context.Background(), "x", ExecOptions{}); err == nil || !strings.Contains(err.Error(), "MULTICA_METACODES_PATH") {
		t.Fatalf("missing executable error = %v", err)
	}
	mcp := json.RawMessage(`{"mcpServers":{"x":{"command":"y"}}}`)
	if _, err := backend.Execute(context.Background(), "x", ExecOptions{McpConfig: mcp}); err == nil || !strings.Contains(err.Error(), "MCP") {
		t.Fatalf("managed MCP error = %v", err)
	}
}
