# Optimization Thought Process

## Baseline (before any changes)

Machine: Apple M2, darwin/arm64

```
BenchmarkCreateRuns/batch500_100KB-8    1    4124196834 ns/op    1845680336 B/op    9297588 allocs/op
BenchmarkCreateRuns/batch50_1000KB-8    1    3706818667 ns/op    1874244136 B/op    9234108 allocs/op
```
##  NOTE: MinIO on Docker for Mac has known connectivity/performance issues.  

Interpretation:
- batch500_100KB: 500 runs × 100KB per field (inputs/outputs/metadata) = ~150MB payload → 4.1s, 1.76GB allocated, 9.3M allocs
- batch50_1000KB: 50 runs × 1000KB per field = ~150MB payload → 3.7s, 1.79GB allocated, 9.2M allocs

Both cases move roughly the same total bytes (~150MB) and show nearly identical allocation counts (~9.3M). This tells us the bottleneck is proportional to **number of field values** (500×3=1500 vs 50×3=150... wait, alloc counts are nearly equal), which actually points at the per-run overhead being dominated by the map allocation, not the data size.

---

### Problem 1: Serialization Round-Trip (Biggest Win)

**Current:**
```
JSON bytes (in HTTP body)
  → decoded into map[string]any   (parse every key, every value, allocate map)
  → re-encoded back to JSON bytes (walk the map, write JSON)
```
 
The large fields arrive as JSON bytes and need to be stored as JSON bytes in S3. We never inspect the contents — we just need to store them. So deserializing into `map[string]any` and re-serializing costs:
- ~1000 allocations per 100KB field (one per key-value pair in the map)
- A full re-serialization pass

**Fix:** Use `json.RawMessage` for `Inputs`, `Outputs`, `Metadata` in `RunIn`.  
`json.RawMessage` is a `[]byte`. The JSON decoder, when it sees a `json.RawMessage` field, copies the raw token bytes verbatim — no parsing, no map allocation, 1 allocation regardless of content size.

**Benchmark file:** `/tmp/bench_problem1.txt` (on local machine)

```
                     BASELINE                                AFTER PROBLEM 1
batch500_100KB   4,124ms  1,845MB  9,297,588 allocs  →   3,807ms  1,445MB     48,554 allocs
batch50_1000KB   3,706ms  1,874MB  9,234,108 allocs  →   3,691ms  1,517MB      5,974 allocs
```

| Metric | batch500_100KB | batch50_1000KB |
|--------|---------------|----------------|
| Allocs reduction | 9,297,588 → 48,554 (**99.5% fewer**) | 9,234,108 → 5,974 (**99.9% fewer**) |
| Memory reduction | 1,845MB → 1,445MB (**22% less**) | 1,874MB → 1,517MB (**19% less**) |
| Time reduction | 4,124ms → 3,807ms (**8% faster**) | 3,706ms → 3,691ms (**~flat**) |

### Problem 2: bytes.Index Scanning

**Current:** After marshaling the whole `runJSON` struct into `runBytes`, the code calls `bytes.Index(runBytes, fieldBytes)` 3× per run to find where each field sits. For 100KB fields, `runBytes` is ~300KB — so each search scans through 300KB of data.

**Fix:** Build the run JSON directly into the batch buffer using manual writes. Record `buf.Len()` before and after writing each field. No search needed — the offsets are known at write time.

**Benchmark file:** `/tmp/bench_problem2.txt`

```
                     AFTER PROBLEM 1                         AFTER PROBLEM 2
batch500_100KB   3,807ms  1,445MB    48,554 allocs  →   2,514ms  1,226MB    46,010 allocs
batch50_1000KB   3,691ms  1,517MB     5,974 allocs  →   2,331ms  1,311MB     5,679 allocs
```

| Metric | batch500_100KB | batch50_1000KB |
|--------|---------------|----------------|
| Time reduction | 3,807ms → 2,514ms (**34% faster**) | 3,691ms → 2,331ms (**37% faster**) |
| Memory reduction | 1,445MB → 1,226MB (**15% less**) | 1,517MB → 1,311MB (**14% less**) |
| Allocs | ~flat (46K → 46K) | ~flat (6K → 6K) |

Allocs barely moved because the alloc savings from eliminating marshal calls are small compared to what Problem 1 already removed. The time and memory improvement here is from eliminating the 450MB of scanning and the intermediate buffer allocations for `runBytes`.

**Cumulative vs baseline:**
```
batch500_100KB:  4,124ms → 2,514ms  (39% faster),  1,845MB → 1,226MB  (34% less memory),  9.3M → 46K allocs (99.5% fewer)
batch50_1000KB:  3,706ms → 2,331ms  (37% faster),  1,874MB → 1,311MB  (30% less memory),  9.2M →  6K allocs (99.9% fewer)
```

### Problem 3: Connection Pooling

**Change:** Replace `pgx.Connect()` (new TCP connection per request) with `pgxpool.Pool` initialized once at startup. Handlers call `pool.Acquire()` which returns a ready connection in microseconds.
**Benchmark file:** `/tmp/bench_problem3.txt`

```
                     AFTER PROBLEM 2                         AFTER PROBLEM 3
batch500_100KB   2,514ms  1,226MB    46,010 allocs  →   2,386ms  1,226MB    46,465 allocs
batch50_1000KB   2,331ms  1,311MB     5,679 allocs  →   2,369ms  1,311MB     5,416 allocs
```

| Metric | batch500_100KB | batch50_1000KB |
|--------|---------------|----------------|
| Time reduction | 2,514ms → 2,386ms (**5% faster**) | ~flat (single-threaded benchmark) |
| Memory/Allocs | unchanged | unchanged |

**Note:** The single-request benchmark shows minimal improvement because it runs serially. The real cost of per-request connections appears under concurrent load. A dedicated concurrent benchmark (`BenchmarkDBConnection`) proves the point:

**Benchmark file:** `/tmp/bench_dbconnection.txt`

```
                                        1 goroutine          4 goroutines
direct  pgx.Connect + close            4,446µs/op  297 allocs    1,890µs/op  297 allocs
pooled  pgxpool.Acquire + Release        116µs/op    7 allocs       53µs/op    7 allocs
```

| Metric | 1 goroutine | 4 goroutines |
|--------|-------------|--------------|
| Latency improvement | 4,446µs → 116µs (**38× faster**) | 1,890µs → 53µs (**36× faster**) |
| Allocs improvement | 297 → 7 (**97% fewer**) | 297 → 7 (**97% fewer**) |

### Problem 4: Batch DB Inserts

**Change:** Replace N individual `conn.QueryRow(INSERT)` calls in a loop with a single `pgx.Batch` sent via `conn.SendBatch()` — one Postgres round-trip instead of N.

**Benchmark file:** `/tmp/bench_problem4.txt`

```
                     AFTER PROBLEM 3                         AFTER PROBLEM 4
batch500_100KB   2,386ms  1,226MB    46,465 allocs  →   2,325ms  1,228MB    48,491 allocs
batch50_1000KB   2,369ms  1,311MB     5,416 allocs  →   2,363ms  1,311MB     5,627 allocs
```

| Metric | batch500_100KB | batch50_1000KB |
|--------|---------------|----------------|
| Time | 2,386ms → 2,325ms (**3% faster**) | ~flat |
| Memory/Allocs | unchanged | unchanged |

## Problem 5: Parallel S3 Fetches on GET

**Change:** Replace 3 sequential `fetchFromS3` calls with `errgroup.WithContext` — all three byte-range reads fire concurrently.

**Why errgroup over sync.WaitGroup:**
- `WaitGroup` silently swallows errors — `wg.Wait()` completes regardless, leaving nil/zero-value data
- `WaitGroup` has no cancellation — if one fetch fails, the other two keep running uselessly
- `errgroup.WithContext` cancels `gctx` the moment any goroutine returns an error, signalling the other S3 GetObject calls to abort mid-flight
- Error is surfaced to the caller rather than buried

**Also changed:** `fetchFromS3` signature from `map[string]any` to `(map[string]any, error)` — errors were previously logged and silently converted to empty objects.

**Benchmark file:** `/tmp/bench_problem5.txt`

```
                     GET BASELINE                            AFTER PROBLEM 5
100KB_fields     8,584µs  2.9MB   20,834 allocs  →   5,214µs  2.9MB   20,857 allocs
1000KB_fields   52,212µs  31MB   186,927 allocs  →  40,164µs   31MB  186,944 allocs
```

| Metric | 100KB fields | 1000KB fields |
|--------|-------------|---------------|
| Latency | 8,584µs → 5,214µs (**39% faster**) | 52,212µs → 40,164µs (**23% faster**) |
| Memory/Allocs | unchanged | unchanged |

Latency drops because the three S3 round-trips now overlap. Memory and allocs are unchanged — that's Problem 6 (eliminating the `map[string]any` unmarshal+remarshal on the response path).

---

## Problem 6: json.RawMessage Response (GET path)

**Change:** `fetchFromS3` now returns `json.RawMessage` instead of `map[string]any`. The raw bytes from S3 are embedded directly into the response — `json.Encoder` calls `MarshalJSON()` on the value which returns the bytes as-is.

**Why it works:** The previous path was:
```
S3 bytes → json.Unmarshal into map[string]any  (allocates map + all keys + all values)
         → json.Encoder re-serializes map       (walks map, writes JSON bytes)
```
With `json.RawMessage`, both steps collapse to a single byte copy. A 1000KB field with ~10000 key-value pairs no longer allocates ~20000 objects — just one `[]byte`.

**Benchmark file:** `/tmp/bench_problem6.txt`

```
                     AFTER PROBLEM 5                         AFTER PROBLEM 6
100KB_fields      5,214µs  2.9MB   20,857 allocs  →   4,521µs  1.7MB    2,321 allocs
1000KB_fields    40,164µs   31MB  186,944 allocs  →  33,501µs   15MB    2,341 allocs
```

| Metric | 100KB fields | 1000KB fields |
|--------|-------------|---------------|
| Latency | 5,214µs → 4,521µs (**13% faster**) | 40,164µs → 33,501µs (**17% faster**) |
| Memory | 2.9MB → 1.7MB (**41% less**) | 31MB → 15MB (**49% less**) |
| Allocs | 20,857 → 2,321 (**99% fewer**) | 186,944 → 2,341 (**99% fewer**) |

Allocations drop 99% — from scaling with field content size to a flat ~2300 regardless of payload size. Memory halves. The remaining latency is S3 network I/O.
