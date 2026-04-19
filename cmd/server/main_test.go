package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	appconfig "github.com/langchain-ai/ls-go-run-handler/internal/config"
)

// helper to construct a server instance and router using test env settings
func newTestRouter(tb testing.TB) (*chi.Mux, *Server) {
	tb.Helper()

	// assert RUN_HANDLER_ENV=test
	env := os.Getenv("RUN_HANDLER_ENV")
	if env != "test" {
		tb.Fatalf("expected RUN_HANDLER_ENV=test, got %s", env)
	}

	// Provide sensible defaults if .env.test isn't present
	cfg := appconfig.Load()

	// Build S3 client matching main.go
	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, "")),
	)
	if err != nil {
		tb.Fatalf("failed to load AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(cfg.S3Endpoint)
	})

	dsn := "postgres://" + cfg.DBUser + ":" + cfg.DBPassword + "@" + cfg.DBHost + ":" + cfg.DBPort + "/" + cfg.DBName
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		tb.Fatalf("failed to create pool: %v", err)
	}
	tb.Cleanup(func() { pool.Close() })
	srv := &Server{cfg: cfg, pool: pool, s3: s3Client}

	r := chi.NewRouter()
	r.Post("/runs", srv.createRunsHandler)
	r.Get("/runs/{id}", srv.getRunHandler)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	return r, srv
}

func TestCreateAndGetRun(t *testing.T) {
	r, _ := newTestRouter(t)
	ts := httptest.NewServer(r)
	defer ts.Close()

	// Prepare runs
	runs := []map[string]any{
		{
			"trace_id": uuid.New().String(),
			"name":     "Test Run 1",
			"inputs":   map[string]any{"prompt": "What is the capital of France?"},
			"outputs":  map[string]any{"answer": "Paris"},
			"metadata": map[string]any{"model": "gpt-4", "temperature": 0.7},
		},
		{
			"trace_id": uuid.New().String(),
			"name":     "Test Run 2",
			"inputs":   map[string]any{"prompt": "Tell me about machine learning"},
			"outputs":  map[string]any{"answer": "Machine learning is a branch of AI..."},
			"metadata": map[string]any{"model": "gpt-3.5-turbo", "temperature": 0.5},
		},
		{
			"trace_id": uuid.New().String(),
			"name":     "Test Run 3",
			"inputs":   map[string]any{"prompt": "Python code example"},
			"outputs":  map[string]any{"code": "print('Hello, World!')"},
			"metadata": map[string]any{"model": "codex", "temperature": 0.2},
		},
	}

	// POST /runs
	body, _ := json.Marshal(runs)
	resp, err := http.Post(ts.URL+"/runs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /runs failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", resp.StatusCode)
	}

	var created struct {
		Status string   `json:"status"`
		RunIDs []string `json:"run_ids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}
	if created.Status != "created" {
		t.Fatalf("unexpected status: %s", created.Status)
	}
	if len(created.RunIDs) != 3 {
		t.Fatalf("expected 3 run_ids, got %d", len(created.RunIDs))
	}

	// GET each run and verify
	for i, id := range created.RunIDs {
		rresp, err := http.Get(ts.URL + "/runs/" + id)
		if err != nil {
			t.Fatalf("GET /runs/%s failed: %v", id, err)
		}
		if rresp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 for %s, got %d", id, rresp.StatusCode)
		}
		var got map[string]any
		if err := json.NewDecoder(rresp.Body).Decode(&got); err != nil {
			t.Fatalf("decode get response: %v", err)
		}
		_ = rresp.Body.Close()

		if got["id"].(string) != id {
			t.Fatalf("expected id %s, got %v", id, got["id"])
		}
		if got["name"].(string) != runs[i]["name"].(string) {
			t.Fatalf("name mismatch: want %v, got %v", runs[i]["name"], got["name"])
		}

		// Compare maps: inputs, outputs, metadata
		for _, field := range []string{"inputs", "outputs", "metadata"} {
			want := runs[i][field]
			gotField := got[field]
			if !reflect.DeepEqual(normalizeJSON(want), normalizeJSON(gotField)) {
				t.Fatalf("%s mismatch: want %#v, got %#v", field, want, gotField)
			}
		}
	}
}

// normalizeJSON ensures numbers are float64 and maps are comparable
func normalizeJSON(v any) any {
	// Marshal and unmarshal through JSON to normalize types
	b, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(b, &out)
	return out
}
