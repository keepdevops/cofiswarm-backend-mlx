package mlx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/keepdevops/cofiswarm-backend-sdk/pkg/backend"
)

func newOn(t *testing.T, h http.HandlerFunc) (*Backend, func()) {
	t.Helper()
	srv := httptest.NewServer(h)
	_, portStr, _ := strings.Cut(strings.TrimPrefix(srv.URL, "http://"), ":")
	port, _ := strconv.Atoi(portStr)
	b := New(port, "scout", "You are Scout.", 64, 0)
	b.baseURL = srv.URL + "/v1"
	return b, srv.Close
}

func TestGenerateStreamAndHealth(t *testing.T) {
	var gotContent string
	b, stop := newOn(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.WriteHeader(200)
		case "/v1/chat/completions":
			var body struct {
				Messages []struct {
					Content string `json:"content"`
				} `json:"messages"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.Messages) == 1 {
				gotContent = body.Messages[0].Content
			}
			w.Header().Set("Content-Type", "text/event-stream")
			for _, tok := range []string{"RE", "VIEW"} {
				fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", tok)
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
		}
	})
	defer stop()

	var out []string
	var last backend.TokenChunk
	err := b.GenerateStream(context.Background(), backend.GenerateRequest{Prompt: "rate it"}, func(c backend.TokenChunk) error {
		if c.Text != "" {
			out = append(out, c.Text)
		}
		last = c
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if strings.Join(out, "") != "REVIEW" {
		t.Errorf("streamed=%q", strings.Join(out, ""))
	}
	if !last.Done {
		t.Error("final chunk must be Done")
	}
	if !strings.HasPrefix(gotContent, "You are Scout.\n\n") || !strings.HasSuffix(gotContent, "rate it") {
		t.Errorf("merged content = %q", gotContent)
	}
	if h := b.Health(context.Background()); !h.OK {
		t.Errorf("health = %+v", h)
	}
	if _, err := b.Embed(context.Background(), []string{"x"}); err == nil {
		t.Error("embed should be unsupported")
	}
}

func TestGenerateStreamHTTPError(t *testing.T) {
	b, stop := newOn(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("boom"))
	})
	defer stop()
	err := b.GenerateStream(context.Background(), backend.GenerateRequest{Prompt: "x"}, func(backend.TokenChunk) error { return nil })
	if err == nil {
		t.Error("non-200 should return an error")
	}
}
