package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	appconfig "github.com/langchain-ai/ls-go-run-handler/internal/config"
)

const (
	kb        = 1024
	chunkSize = 100
)

// generateLargeString returns a string of approximately sizeKB kilobytes.
func generateLargeString(sizeKB int) string {
	n := sizeKB * kb
	if n <= 0 {
		return ""
	}
	return strings.Repeat("X", n)
}

// generateLargePayload splits the string into 100-byte chunks and assigns them
// to keys key_0, key_1, ...
func generateLargePayload(sizeKB int) map[string]string {
	s := generateLargeString(sizeKB)
	m := make(map[string]string, (len(s)+chunkSize-1)/chunkSize)
	for i := 0; i < len(s); i += chunkSize {
		key := fmt.Sprintf("key_%d", i/chunkSize)
		end := i + chunkSize
		if end > len(s) {
			end = len(s)
		}
		m[key] = s[i:end]
	}
	return m
}

// makeRunsBody builds a JSON array body for POST /runs with the given batch size and
// sizeKB for inputs/outputs/metadata payloads.
func makeRunsBody(batch, sizeKB int) []byte {
	runs := make([]map[string]any, 0, batch)
	for i := 0; i < batch; i++ {
		runs = append(runs, map[string]any{
			"trace_id": uuid.New().String(),
			"name":     fmt.Sprintf("Benchmark Run %s", uuid.New().String()),
			"inputs":   generateLargePayload(sizeKB),
			"outputs":  generateLargePayload(sizeKB),
			"metadata": generateLargePayload(sizeKB),
		})
	}
	b, _ := json.Marshal(runs)
	return b
}

// BenchmarkDBConnection compares the cost of acquiring a DB connection two ways:
//   - direct: pgx.Connect() — full TCP handshake + Postgres auth on every call
//   - pooled: pgxpool.Acquire() — returns an already-open connection in microseconds
//
// Run with -benchtime=5s and -cpu=8 to stress concurrent acquisition.
func BenchmarkDBConnection(b *testing.B) {
	cfg := appconfig.Load()
	dsn := "postgres://" + cfg.DBUser + ":" + cfg.DBPassword + "@" + cfg.DBHost + ":" + cfg.DBPort + "/" + cfg.DBName

	b.Run("direct_pgx_Connect", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				conn, err := pgx.Connect(context.Background(), dsn)
				if err != nil {
					b.Errorf("connect failed: %v", err)
					return
				}
				var n int
				_ = conn.QueryRow(context.Background(), "SELECT 1").Scan(&n)
				conn.Close(context.Background())
			}
		})
	})

	b.Run("pooled_pgxpool_Acquire", func(b *testing.B) {
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			b.Fatalf("failed to create pool: %v", err)
		}
		defer pool.Close()

		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				conn, err := pool.Acquire(context.Background())
				if err != nil {
					b.Errorf("acquire failed: %v", err)
					return
				}
				var n int
				_ = conn.QueryRow(context.Background(), "SELECT 1").Scan(&n)
				conn.Release()
			}
		})
	})
}

// BenchmarkGetRun measures GET /runs/{id} — fetches a run from DB and resolves
// all three S3 byte-range refs (inputs, outputs, metadata) back into a response.
func BenchmarkGetRun(b *testing.B) {
	r, _ := newTestRouter(b)
	ts := httptest.NewServer(r)
	defer ts.Close()
	client := &http.Client{}

	cases := []struct {
		name      string
		fieldSize int
	}{
		{name: "100KB_fields", fieldSize: 100},
		{name: "1000KB_fields", fieldSize: 1000},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			// Seed one run to fetch — outside the timed loop.
			body := makeRunsBody(1, tc.fieldSize)
			resp, err := client.Post(ts.URL+"/runs", "application/json", bytes.NewReader(body))
			if err != nil || resp.StatusCode != http.StatusCreated {
				b.Fatalf("seed POST failed: status=%d err=%v", resp.StatusCode, err)
			}
			var created struct {
				RunIDs []string `json:"run_ids"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&created)
			resp.Body.Close()
			if len(created.RunIDs) == 0 {
				b.Fatal("no run_ids in POST response")
			}
			getURL := ts.URL + "/runs/" + created.RunIDs[0]

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := client.Get(getURL)
				if err != nil {
					b.Fatalf("GET failed: %v", err)
				}
				if resp.StatusCode != http.StatusOK {
					b.Fatalf("expected 200, got %d", resp.StatusCode)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		})
	}
}

func BenchmarkCreateRuns(b *testing.B) {
	r, _ := newTestRouter(b)
	ts := httptest.NewServer(r)
	defer ts.Close()

	cases := []struct {
		name      string
		batch     int
		fieldSize int
	}{
		{name: "batch500_100KB", batch: 500, fieldSize: 100},
		{name: "batch50_1000KB", batch: 50, fieldSize: 1000},
	}

	client := &http.Client{}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			body := makeRunsBody(tc.batch, tc.fieldSize)
			url := ts.URL + "/runs"

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := client.Post(url, "application/json", bytes.NewReader(body))
				if err != nil {
					b.Fatalf("POST /runs failed: %v", err)
				}
				if resp.StatusCode != http.StatusCreated {
					// Drain body to allow reuse of connection
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					b.Fatalf("expected status 201, got %d", resp.StatusCode)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		})
	}
}
