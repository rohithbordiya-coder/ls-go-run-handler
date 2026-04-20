# Performance Optimization

This document walks through the performance work done on `ls-go-run-handler` — a Go service that ingests batches of ML/LLM runs, stores large payloads in S3-compatible object storage, and keeps references in Postgres.

The methodology was: measure a baseline, identify one bottleneck at a time, fix it, re-measure, move on. Each step has a captured benchmark artefact so improvements are verifiable rather than assumed.

> **Environment note:** MinIO on Docker for Mac has known connectivity/performance issues. All numbers below are from Apple M2, darwin/arm64. Absolute times will differ on your machine; the *relative* improvements should reproduce.

---

## TL;DR

| Path | Metric | Baseline | After | Change |
|------|--------|----------|-------|--------|
| `POST /runs` (batch500_100KB) | Latency | 4,124ms | 2,325ms | **44% faster** |
| `POST /runs` (batch500_100KB) | Memory | 1,845MB | 1,228MB | **33% less** |
| `POST /runs` (batch500_100KB) | Allocations | 9,297,588 | 48,491 | **99.5% fewer** |
| `GET /runs/{id}` (100KB fields) | Latency | 8,584µs | 4,521µs | **47% faster** |
| `GET /runs/{id}` (100KB fields) | Memory | 2.9MB | 1.7MB | **41% less** |
| `GET /runs/{id}` (100KB fields) | Allocations | 20,834 | 2,321 | **99% fewer** |
| DB connection acquire (concurrent) | Latency | 1,890µs | 53µs | **36× faster** |

---

## Baseline

```
BenchmarkCreateRuns/batch500_100KB-8   1   4124196834 ns/op   1845680336 B/op   9297588 allocs/op
BenchmarkCreateRuns/batch50_1000KB-8   1   3706818667 ns/op   1874244136 B/op   9234108 allocs/op
```

Both scenarios move ~150MB of payload and allocate ~9.3M objects. That allocation count is almost identical between "500 small runs" and "50 large runs" — which points at per-field `map[string]any` allocation being the dominant cost, not total bytes.

---

## Optimizations

### 1. `json.RawMessage` on decode

Large fields (`inputs`, `outputs`, `metadata`) arrived as JSON bytes and needed to be written back out as JSON bytes. The baseline decoded them into `map[string]any` and re-serialized — pure waste. Every key-value pair became a separate allocation: ~1000 allocs per 100KB field.

**Fix:** change `RunIn.Inputs/Outputs/Metadata` from `map[string]any` to `json.RawMessage`. The JSON decoder then copies the raw bytes verbatim — one allocation regardless of size.

| Metric | batch500_100KB | batch50_1000KB |
|--------|----------------|----------------|
| Allocs | 9,297,588 → 48,554 (**99.5% fewer**) | 9,234,108 → 5,974 (**99.9% fewer**) |
| Memory | 1,845MB → 1,445MB (**22% less**) | 1,874MB → 1,517MB (**19% less**) |
| Time | 4,124ms → 3,807ms (**8% faster**) | 3,706ms → 3,691ms (~flat) |

> Benchmark: `/tmp/bench_problem1.txt`

### 2. Eliminate `bytes.Index` scanning

After marshaling a whole run struct, the old code called `bytes.Index(runBytes, fieldBytes)` three times to find each field's byte offset. For 100KB fields, each search scanned ~300KB. Across batch500 that's **450MB of scanning per request**.

**Fix:** build the run JSON directly into the batch buffer and record `buf.Len()` before and after each field. No search — offsets are known at write time.

| Metric | batch500_100KB | batch50_1000KB |
|--------|----------------|----------------|
| Time | 3,807ms → 2,514ms (**34% faster**) | 3,691ms → 2,331ms (**37% faster**) |
| Memory | 1,445MB → 1,226MB (**15% less**) | 1,517MB → 1,311MB (**14% less**) |

> Benchmark: `/tmp/bench_problem2.txt`

### 3. Connection pooling

The baseline called `pgx.Connect()` on every request — a full TCP connection + TLS handshake + Postgres auth, per request.

**Fix:** `pgxpool.Pool` initialized once at startup; handlers call `pool.Acquire()`.

The single-request benchmark barely moved (requests ran serially so pool vs connect was comparable). The real proof is under concurrency — captured in a dedicated `BenchmarkDBConnection`:

| | 1 goroutine | 4 goroutines |
|--------|-------------|--------------|
| Direct `pgx.Connect` | 4,446µs, 297 allocs | 1,890µs, 297 allocs |
| Pooled `pgxpool.Acquire` | 116µs, 7 allocs | 53µs, 7 allocs |
| **Improvement** | **38× faster** | **36× faster** |

At 8 goroutines, `pgx.Connect` exhausted Postgres's connection limit entirely (`dial tcp: can't assign requested address`) while the pool handled the same load without issue. Connection pooling isn't just an optimization — it's a correctness requirement at scale.

> Benchmarks: `/tmp/bench_problem3.txt`, `/tmp/bench_dbconnection.txt`

### 4. Batch DB inserts

500 individual `conn.QueryRow(INSERT)` calls meant 500 sequential network round-trips.

**Fix:** `pgx.Batch{}` queues all N inserts and `conn.SendBatch()` sends them in one round-trip, pipelined.

The gain is small in the single-iteration benchmark because the 150MB S3 upload dominates total time. The batch benefit shows most when DB latency is the bottleneck (small payloads, or high DB network latency).

> Benchmark: `/tmp/bench_problem4.txt`

### 5. Parallel S3 fetches on GET

`GET /runs/{id}` fetched inputs, outputs, metadata sequentially — 3× the network latency of one fetch.

**Fix:** `errgroup.WithContext` runs all three byte-range reads concurrently.

Why `errgroup` over `sync.WaitGroup`:
- `WaitGroup` silently swallows errors; `Wait()` completes even if fetches failed
- `WaitGroup` has no cancellation; if one fetch errors, the others keep going
- `errgroup.WithContext` cancels `gctx` on first error, aborting the in-flight S3 calls
- Errors surface to the caller rather than getting buried

| Metric | 100KB fields | 1000KB fields |
|--------|--------------|---------------|
| Latency | 8,584µs → 5,214µs (**39% faster**) | 52,212µs → 40,164µs (**23% faster**) |

> Benchmark: `/tmp/bench_problem5.txt`

### 6. `json.RawMessage` on GET response

`fetchFromS3` had been decoding S3 bytes into `map[string]any`, then the encoder re-serialized that back to JSON. Same waste as Problem 1, on the read side.

**Fix:** `fetchFromS3` returns `json.RawMessage`. The raw bytes from S3 are embedded directly into the response body via `MarshalJSON()`.

| Metric | 100KB fields | 1000KB fields |
|--------|--------------|---------------|
| Latency | 5,214µs → 4,521µs (**13% faster**) | 40,164µs → 33,501µs (**17% faster**) |
| Memory | 2.9MB → 1.7MB (**41% less**) | 31MB → 15MB (**49% less**) |
| Allocs | 20,857 → 2,321 (**99% fewer**) | 186,944 → 2,341 (**99% fewer**) |

Allocations are now flat ~2,300 regardless of payload size — previously they scaled with field content.

> Benchmark: `/tmp/bench_problem6.txt`

---

## Explored but not shipped

Areas I researched but judged out of scope for this change:

1. **Gzip compression of the batch upload** — compresses structured JSON 10–20×, cuts S3 upload ~3× on POST. Reviewed how Datadog, Langfuse, and Splunk handle compression. Trade-off: gzip isn't seekable, so GET can no longer use byte-range reads — must download + decompress the batch and scan by run ID. Acceptable for write-heavy ingest, but changes the data model significantly.
2. **NDJSON + S3 Select** — store batches as newline-delimited JSON so S3 Select can filter server-side by run ID. Preserves GET latency independently of batch position. Requires the object store to support S3 Select (MinIO does).
3. **Protobuf + multipart upload** — lower serialization overhead and parallel upload parts. Heavier integration cost; most valuable at much larger payload sizes than this service currently handles.
4. **Streaming via NATS JetStream** — decouple ingest from storage:
   ```
   NATS JetStream (stream: TRACES)
       ├── Consumer 1: batch → zstd compress → multipart S3 PUT
       └── Consumer 2: extract metadata → write to Postgres/DynamoDB
   ```
   Best fit for high-throughput fan-out scenarios where different downstream systems need different views of the data.

---

## How to reproduce

```bash
make db-up           # Postgres + MinIO
make db-migrate      # schema
make bench           # run benchmarks
```

Unit tests for `buildBatch` run without any infrastructure:
```bash
go test ./cmd/server/ -run TestBuildBatch
```
