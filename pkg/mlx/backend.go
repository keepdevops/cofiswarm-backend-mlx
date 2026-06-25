// Package mlx implements the InferenceBackend contract against an mlx_lm.server
// instance (OpenAI-compatible, streaming). Go port of cofiswarm_backend_mlx.MlxBackend;
// coexists with the Python backend. It is the first Go consumer of cofiswarm-backend-sdk.
package mlx

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/keepdevops/cofiswarm-backend-sdk/pkg/backend"
)

// Per-port inflight counters + serialization (one request per port at a time),
// mirroring the Python module-level _inflight / _port_semaphores.
var (
	mu       sync.Mutex
	inflight = map[int]int{}
	sems     = map[int]chan struct{}{}
)

func semFor(port int) chan struct{} {
	mu.Lock()
	defer mu.Unlock()
	s, ok := sems[port]
	if !ok {
		s = make(chan struct{}, 1)
		sems[port] = s
	}
	return s
}

func inc(port int) { mu.Lock(); inflight[port]++; mu.Unlock() }
func dec(port int) {
	mu.Lock()
	if inflight[port] > 0 {
		inflight[port]--
	}
	mu.Unlock()
}

// Pressure returns a snapshot of inflight requests per port (ports get_pressure).
func Pressure() map[int]int {
	mu.Lock()
	defer mu.Unlock()
	out := make(map[int]int, len(inflight))
	for k, v := range inflight {
		out[k] = v
	}
	return out
}

// Backend streams from one mlx_lm.server instance.
type Backend struct {
	Port         int
	AgentID      string
	SystemPrompt string
	MaxTokens    int
	Temperature  float64
	baseURL      string
	client       *http.Client
}

var _ backend.InferenceBackend = (*Backend)(nil)

// New constructs a Backend (defaults: max_tokens 512, temperature 0.2 — matching MlxBackend).
func New(port int, agentID, systemPrompt string, maxTokens int, temperature float64) *Backend {
	if maxTokens <= 0 {
		maxTokens = backend.DefaultMaxTokens
	}
	if temperature == 0 {
		temperature = 0.2
	}
	return &Backend{
		Port: port, AgentID: agentID, SystemPrompt: systemPrompt,
		MaxTokens: maxTokens, Temperature: temperature,
		baseURL: fmt.Sprintf("http://127.0.0.1:%d/v1", port),
		client:  &http.Client{Timeout: 300 * time.Second},
	}
}

// buildContent merges system_prompt + prompt into a single user message (MLX convention).
func (b *Backend) buildContent(prompt string) string {
	if b.SystemPrompt != "" {
		return b.SystemPrompt + "\n\n" + prompt
	}
	return prompt
}

// GenerateStream POSTs an SSE chat completion, calling emit for each content delta and a
// final Done chunk. Unlike the Python (which yields error-text chunks), connection/HTTP
// failures return a Go error so callers can mark the agent unavailable.
func (b *Backend) GenerateStream(ctx context.Context, req backend.GenerateRequest, emit func(backend.TokenChunk) error) error {
	maxTok := req.MaxTokens
	if maxTok == 0 {
		maxTok = b.MaxTokens
	}
	temp := req.Temperature
	if temp == 0 {
		temp = b.Temperature
	}
	payload := map[string]any{
		"messages":    []map[string]string{{"role": "user", "content": b.buildContent(req.Prompt)}},
		"max_tokens":  maxTok,
		"temperature": temp,
		"stream":      true,
	}
	if len(req.Stop) > 0 {
		payload["stop"] = req.Stop
	}
	body, _ := json.Marshal(payload)

	sem := semFor(b.Port)
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	inc(b.Port)
	defer dec(b.Port)

	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL+"/chat/completions", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("mlx-backend %s: connection error: %w", b.AgentID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("mlx-backend %s: HTTP %d: %s", b.AgentID, resp.StatusCode, truncate(string(raw), 200))
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(line[len("data:"):])
		if data == "[DONE]" {
			break
		}
		var ev struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			continue // skip malformed SSE frame (logged-loudly equivalent omitted for brevity)
		}
		if len(ev.Choices) > 0 && ev.Choices[0].Delta.Content != "" {
			if err := emit(backend.TokenChunk{Text: ev.Choices[0].Delta.Content}); err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("mlx-backend %s: stream read: %w", b.AgentID, err)
	}
	return emit(backend.TokenChunk{Done: true})
}

// Embed is not supported by the MLX coordinator (ports embed's NotImplementedError).
func (b *Backend) Embed(context.Context, []string) ([][]float32, error) {
	return nil, fmt.Errorf("mlx-backend %s: embed() not supported — use the RAG sidecar", b.AgentID)
}

// Health probes GET /v1/models (mlx_lm.server exposes this, not /health).
func (b *Backend) Health(ctx context.Context) backend.HealthStatus {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/models", nil)
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return backend.HealthStatus{OK: false, Detail: err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return backend.HealthStatus{OK: true, Detail: fmt.Sprintf("port %d ok", b.Port)}
	}
	return backend.HealthStatus{OK: false, Detail: fmt.Sprintf("port %d HTTP %d", b.Port, resp.StatusCode)}
}

// Close is a no-op (http.Client needs no teardown).
func (b *Backend) Close() error { return nil }

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
