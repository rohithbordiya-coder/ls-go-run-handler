package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	appconfig "github.com/langchain-ai/ls-go-run-handler/internal/config"
)

// RunIn represents input payload for a run.
// Inputs/Outputs/Metadata are json.RawMessage so the decoder captures their bytes verbatim —
// no map[string]any allocation, no re-serialization on the write path.
type RunIn struct {
	ID       *string         `json:"id,omitempty"`
	TraceID  string          `json:"trace_id"`
	Name     string          `json:"name"`
	Inputs   json.RawMessage `json:"inputs"`
	Outputs  json.RawMessage `json:"outputs"`
	Metadata json.RawMessage `json:"metadata"`
}

type Server struct {
	cfg  appconfig.Settings
	pool *pgxpool.Pool
	s3   *s3.Client
}

func main() {
	ctx := context.Background()

	// Load settings
	settings := appconfig.Load()

	// Build connection pool — shared across all requests, eliminates per-request TCP handshakes.
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", settings.DBUser, settings.DBPassword, settings.DBHost, settings.DBPort, settings.DBName)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Init S3 client (communicate to MinIO locally)
	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(settings.S3Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(settings.S3AccessKey, settings.S3SecretKey, "")),
	)
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(settings.S3Endpoint)
	})

	srv := &Server{cfg: settings, pool: pool, s3: s3Client}

	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	r.Post("/runs", srv.createRunsHandler)
	r.Get("/runs/{id}", srv.getRunHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	addr := ":" + port
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// createRunsHandler accepts a payload of runs, gzip-compresses and uploads the batch JSON to S3,
// and stores run-level S3 refs (s3://bucket/key#runID) in Postgres.
func (s *Server) createRunsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := r.Context()

	// Parse runs. NOTE: feel free to change the format of the payload
	var runs []RunIn
	if err := json.NewDecoder(r.Body).Decode(&runs); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body, expected an array of runs"})
		return
	}
	if len(runs) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "No runs provided"})
		return
	}

	batchID := uuid.New().String()
	// .json.gz signals gzip encoding; GET path decompresses before JSON decoding.
	objectKey := fmt.Sprintf("batches/%s.json.gz", batchID)

	type runRecord struct {
		id      uuid.UUID
		traceID uuid.UUID
		name    string
		ref     string // same ref stored for inputs/outputs/metadata; run is looked up by id inside the batch
	}

	// Build JSON array into buf, then gzip-compress into compressed.
	// All large fields stay as raw bytes — no intermediate map[string]any.
	buf := &bytes.Buffer{}
	buf.WriteByte('[')
	recs := make([]runRecord, 0, len(runs))

	for i, in := range runs {
		var id uuid.UUID
		if in.ID != nil && *in.ID != "" {
			var err error
			id, err = uuid.Parse(*in.ID)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("invalid id at index %d", i)})
				return
			}
		} else {
			id = uuid.New()
		}
		traceID, err := uuid.Parse(in.TraceID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("invalid trace_id at index %d", i)})
			return
		}

		idJSON, _ := json.Marshal(id.String())
		traceJSON, _ := json.Marshal(traceID.String())
		nameJSON, _ := json.Marshal(in.Name)

		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(`{"id":`)
		buf.Write(idJSON)
		buf.WriteString(`,"trace_id":`)
		buf.Write(traceJSON)
		buf.WriteString(`,"name":`)
		buf.Write(nameJSON)
		buf.WriteString(`,"inputs":`)
		buf.Write(in.Inputs)
		buf.WriteString(`,"outputs":`)
		buf.Write(in.Outputs)
		buf.WriteString(`,"metadata":`)
		buf.Write(in.Metadata)
		buf.WriteByte('}')

		ref := fmt.Sprintf("s3://%s/%s#%s", s.cfg.S3BucketName, objectKey, id.String())
		recs = append(recs, runRecord{id: id, traceID: traceID, name: in.Name, ref: ref})
	}
	buf.WriteByte(']')

	// Gzip-compress the JSON batch before upload.
	// Repetitive field names and string values compress ~10-20× — dramatically cuts S3 upload time.
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(buf.Bytes()); err != nil {
		log.Printf("gzip write error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to compress batch"})
		return
	}
	if err := gz.Close(); err != nil {
		log.Printf("gzip close error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to compress batch"})
		return
	}

	_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.S3BucketName),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(compressed.Bytes()),
		ContentType: aws.String("application/gzip"),
	})
	if err != nil {
		log.Printf("s3 PutObject error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to upload batch to object storage"})
		return
	}

	// Insert references into Postgres
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		log.Printf("db acquire error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to connect to database"})
		return
	}
	defer conn.Release()

	// Queue all inserts into a single batch — one round-trip to Postgres instead of N.
	batch := &pgx.Batch{}
	for _, rec := range recs {
		batch.Queue(
			`INSERT INTO runs (id, trace_id, name, inputs, outputs, metadata)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING id`,
			rec.id, rec.traceID, rec.name, rec.ref, rec.ref, rec.ref,
		)
	}
	br := conn.SendBatch(ctx, batch)
	defer br.Close()

	runIDs := make([]string, 0, len(recs))
	for range recs {
		var outID uuid.UUID
		if err := br.QueryRow().Scan(&outID); err != nil {
			log.Printf("db insert error: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to insert runs"})
			return
		}
		runIDs = append(runIDs, outID.String())
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "created", "run_ids": runIDs})
}

// getRunHandler fetches a run by ID, downloads and decompresses its batch from S3,
// and returns the run's inputs/outputs/metadata.
func (s *Server) getRunHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := r.Context()

	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "id must be a valid UUID"})
		return
	}

	var (
		outID   uuid.UUID
		traceID uuid.UUID
		name    string
		ref     string // inputs/outputs/metadata all point to the same gzip batch
	)
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to connect to database"})
		return
	}
	defer conn.Release()
	err = conn.QueryRow(ctx,
		`SELECT id, trace_id, name, COALESCE(inputs, '') FROM runs WHERE id = $1`, id,
	).Scan(&outID, &traceID, &name, &ref)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Run with ID %s not found", idStr)})
		return
	}

	// Single S3 download decompresses the batch and returns all three fields at once.
	// Replaces 3 parallel byte-range fetches; the gzip savings make each fetch much cheaper anyway.
	inputs, outputs, metadata, err := s.fetchRunFromBatch(ctx, ref)
	if err != nil {
		log.Printf("s3 fetch error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to fetch run data from storage"})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":       outID.String(),
		"trace_id": traceID.String(),
		"name":     name,
		"inputs":   inputs,
		"outputs":  outputs,
		"metadata": metadata,
	})
}

// parseGzipRef parses refs like s3://bucket/key#runID
func parseGzipRef(ref string) (bucket, key string, runID uuid.UUID, ok bool) {
	if ref == "" || !strings.HasPrefix(ref, "s3://") {
		return
	}
	rest := strings.TrimPrefix(ref, "s3://")
	slash := strings.IndexByte(rest, '/')
	if slash == -1 {
		return
	}
	bucket = rest[:slash]
	keyAndFrag := rest[slash+1:]
	hash := strings.IndexByte(keyAndFrag, '#')
	if hash == -1 {
		return
	}
	key = keyAndFrag[:hash]
	var err error
	runID, err = uuid.Parse(keyAndFrag[hash+1:])
	if err != nil {
		return
	}
	ok = true
	return
}

// fetchRunFromBatch downloads the gzip batch from S3, decompresses it, and locates the run by ID.
// Returns inputs/outputs/metadata as json.RawMessage — no unmarshal/remarshal of the large fields.
func (s *Server) fetchRunFromBatch(ctx context.Context, ref string) (inputs, outputs, metadata json.RawMessage, err error) {
	bucket, key, runID, ok := parseGzipRef(ref)
	if !ok || bucket == "" || key == "" {
		return json.RawMessage(`{}`), json.RawMessage(`{}`), json.RawMessage(`{}`), nil
	}

	out, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("s3 GetObject: %w", err)
	}
	defer out.Body.Close()

	gr, err := gzip.NewReader(out.Body)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	// Stream-decode the JSON array one element at a time.
	// Early return stops decompression mid-stream — avoids loading the full batch into memory.
	dec := json.NewDecoder(gr)
	if _, err := dec.Token(); err != nil { // consume opening '['
		return nil, nil, nil, fmt.Errorf("decode batch open: %w", err)
	}

	var run struct {
		ID       uuid.UUID       `json:"id"`
		Inputs   json.RawMessage `json:"inputs"`
		Outputs  json.RawMessage `json:"outputs"`
		Metadata json.RawMessage `json:"metadata"`
	}
	for dec.More() {
		if err := dec.Decode(&run); err != nil {
			return nil, nil, nil, fmt.Errorf("decode run: %w", err)
		}
		if run.ID == runID {
			return run.Inputs, run.Outputs, run.Metadata, nil
		}
	}
	return nil, nil, nil, fmt.Errorf("run %s not found in batch", runID)
}
