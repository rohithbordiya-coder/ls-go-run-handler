package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

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

// runOffsets holds the parsed IDs and S3 byte-range refs for one run after the build phase.
type runOffsets struct {
	id          uuid.UUID
	traceID     uuid.UUID
	name        string
	inputsRef   string
	outputsRef  string
	metadataRef string
}

type Server struct {
	cfg  appconfig.Settings
	pool *pgxpool.Pool
	s3   *s3.Client
}

func main() {
	ctx := context.Background()

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

// writeJSONError writes a JSON error response.
// Centralises the repeated WriteHeader + json.Encode pattern across handlers.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// createRunsHandler accepts a payload of runs, uploads a batch JSON to S3 for large fields, and stores S3 refs in Postgres.
func (s *Server) createRunsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := r.Context()

	var runs []RunIn
	if err := json.NewDecoder(r.Body).Decode(&runs); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body, expected an array of runs")
		return
	}
	if len(runs) == 0 {
		writeJSONError(w, http.StatusBadRequest, "No runs provided")
		return
	}

	batchID := uuid.New().String()
	objectKey := fmt.Sprintf("batches/%s.json", batchID)

	offs, batchBytes, err := buildBatch(runs, s.cfg.S3BucketName, objectKey)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	_, err = s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.S3BucketName),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(batchBytes),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		log.Printf("s3 PutObject error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to upload batch to object storage")
		return
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		log.Printf("db acquire error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to connect to database")
		return
	}
	defer conn.Release()

	// Queue all inserts into a single batch — one round-trip to Postgres instead of N.
	batch := &pgx.Batch{}
	for _, ro := range offs {
		batch.Queue(
			`INSERT INTO runs (id, trace_id, name, inputs, outputs, metadata)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING id`,
			ro.id, ro.traceID, ro.name, ro.inputsRef, ro.outputsRef, ro.metadataRef,
		)
	}
	br := conn.SendBatch(ctx, batch)
	defer br.Close()

	runIDs := make([]string, 0, len(offs))
	for range offs {
		var outID uuid.UUID
		if err := br.QueryRow().Scan(&outID); err != nil {
			log.Printf("db insert error: %v", err)
			writeJSONError(w, http.StatusInternalServerError, "failed to insert runs")
			return
		}
		runIDs = append(runIDs, outID.String())
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "created", "run_ids": runIDs})
}

// buildBatch validates runs and serialises them into a JSON array, tracking byte offsets
// for each large field so GET can use byte-range reads.
//
// Eliminates: 3× json.Marshal for large fields, 1× json.Marshal for the whole struct,
// and 3× bytes.Index searches through the marshaled bytes.
// Separated from the handler so it can be tested independently and to keep the handler
// focused on HTTP and I/O concerns.
func buildBatch(runs []RunIn, bucket, objectKey string) ([]runOffsets, []byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte('[')
	offs := make([]runOffsets, 0, len(runs))

	for i, in := range runs {
		var id uuid.UUID
		if in.ID != nil && *in.ID != "" {
			var err error
			id, err = uuid.Parse(*in.ID)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid id at index %d", i)
			}
		} else {
			id = uuid.New()
		}
		traceID, err := uuid.Parse(in.TraceID)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid trace_id at index %d", i)
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
		inputsStart := buf.Len()
		buf.Write(in.Inputs)
		inputsEnd := buf.Len()
		buf.WriteString(`,"outputs":`)
		outputsStart := buf.Len()
		buf.Write(in.Outputs)
		outputsEnd := buf.Len()
		buf.WriteString(`,"metadata":`)
		metadataStart := buf.Len()
		buf.Write(in.Metadata)
		metadataEnd := buf.Len()
		buf.WriteByte('}')

		mkRef := func(field string, start, end int) string {
			return fmt.Sprintf("s3://%s/%s#%d:%d/%s", bucket, objectKey, start, end, field)
		}
		offs = append(offs, runOffsets{
			id:          id,
			traceID:     traceID,
			name:        in.Name,
			inputsRef:   mkRef("inputs", inputsStart, inputsEnd),
			outputsRef:  mkRef("outputs", outputsStart, outputsEnd),
			metadataRef: mkRef("metadata", metadataStart, metadataEnd),
		})
	}
	buf.WriteByte(']')
	return offs, buf.Bytes(), nil
}

// getRunHandler fetches a run by ID and resolves S3 byte-range refs for inputs/outputs/metadata.
func (s *Server) getRunHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := r.Context()

	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}

	var (
		outID       uuid.UUID
		traceID     uuid.UUID
		name        string
		inputsRef   string
		outputsRef  string
		metadataRef string
	)
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to connect to database")
		return
	}
	defer conn.Release()
	err = conn.QueryRow(ctx,
		`SELECT id, trace_id, name, COALESCE(inputs, ''), COALESCE(outputs, ''), COALESCE(metadata, '')
         FROM runs WHERE id = $1`, id,
	).Scan(&outID, &traceID, &name, &inputsRef, &outputsRef, &metadataRef)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("Run with ID %s not found", idStr))
		return
	}

	// Fetch all three fields in parallel.
	// errgroup.WithContext cancels gctx if any fetch errors, stopping the other two mid-flight.
	var inputs, outputs, metadata json.RawMessage
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { var err error; inputs, err = s.fetchFromS3(gctx, inputsRef); return err })
	g.Go(func() error { var err error; outputs, err = s.fetchFromS3(gctx, outputsRef); return err })
	g.Go(func() error { var err error; metadata, err = s.fetchFromS3(gctx, metadataRef); return err })
	if err := g.Wait(); err != nil {
		log.Printf("s3 fetch error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to fetch run data from storage")
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

// parseS3Ref parses refs like s3://bucket/key#start:end/field
func parseS3Ref(ref string) (bucket, key string, start, end int, ok bool) {
	if ref == "" || !strings.HasPrefix(ref, "s3://") {
		return "", "", 0, 0, false
	}
	rest := strings.TrimPrefix(ref, "s3://")
	slash := strings.IndexByte(rest, '/')
	if slash == -1 {
		return "", "", 0, 0, false
	}
	bucket = rest[:slash]
	keyAndFrag := rest[slash+1:]
	key = keyAndFrag
	if i := strings.IndexByte(keyAndFrag, '#'); i != -1 {
		key = keyAndFrag[:i]
		frag := keyAndFrag[i+1:]
		// frag is like start:end/field
		if j := strings.IndexByte(frag, '/'); j != -1 {
			offsets := frag[:j]
			parts := strings.Split(offsets, ":")
			if len(parts) == 2 {
				st, err1 := strconv.Atoi(parts[0])
				en, err2 := strconv.Atoi(parts[1])
				if err1 == nil && err2 == nil {
					start, end, ok = st, en, true
				}
			}
		}
	}
	return
}

// fetchFromS3 retrieves the JSON fragment using byte range.
// Returns json.RawMessage so the caller can embed the bytes directly into the response
// without an extra unmarshal+remarshal cycle through map[string]any.
func (s *Server) fetchFromS3(ctx context.Context, ref string) (json.RawMessage, error) {
	bucket, key, start, end, ok := parseS3Ref(ref)
	if !ok || bucket == "" || key == "" || end <= start {
		return json.RawMessage(`{}`), nil
	}
	rng := fmt.Sprintf("bytes=%d-%d", start, end-1)
	out, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rng),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject: %w", err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3 body: %w", err)
	}
	return json.RawMessage(b), nil
}
