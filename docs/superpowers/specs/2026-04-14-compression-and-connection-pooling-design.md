# Design: Compression + Connection Pooling

**Date:** 2026-04-14  
**Status:** Approved  
**Scope:** `parquet_transform/processor.py`, `parquet_transform/storage.py`, `gui/workers.py`

---

## Context

The pipeline uploads ~960 Parquet files (~24 MB each, 22.8 GB total) to Azure Blob Storage.
Available upload bandwidth is ~4.5 MB/s (network-bound). Recent runs complete in ~2h 44min.

Two independent improvements have been identified that reduce bytes on the wire and eliminate
redundant connection overhead — without touching the autoscaler logic or adding architectural
complexity.

---

## Option A — zstd Compression

### Problem

`pq.write_table(table, out_buf)` is called with no compression argument. PyArrow's default
behaviour varies by version; it does not guarantee optimal compression for upload pipelines.

### Change

Pass explicit compression settings to every `write_table` call:

```python
pq.write_table(table, out_buf, compression='zstd', compression_level=3)
```

### Rationale

| Codec | Compression ratio | Encode speed | Notes |
|-------|-------------------|--------------|-------|
| snappy (PyArrow default) | ~70% of original | very fast | poor ratio |
| zstd level 1 | ~45–55% | fast | good ratio, near-zero CPU overhead |
| zstd level 3 | ~40–50% | fast | sweet spot for I/O-bound pipelines |
| zstd level 6+ | ~35–45% | 3–5× slower | not worth it here |

At level 3, zstd typically reduces Parquet files with UUID/timestamp/numeric columns by
40–55%. For 24 MB average files this means ~11–14 MB on the wire — effectively 1.7–2×
the current throughput at identical bandwidth.

### Compatibility

Parquet is self-describing. The compression codec is stored in the file footer. Any
compliant Parquet reader (Azure Synapse, Spark, pandas, DuckDB, etc.) decompresses
transparently without any reader-side change.

### Affected file

- `parquet_transform/processor.py` — the single `pq.write_table()` call in
  `_process_blob_once()`

---

## Option B — BlobServiceClient Pooling per Thread

### Problem

In `worker_loop()` inside `run_pass()`, a new `BlobStorageClient` is instantiated for
every file:

```python
# current (simplified)
def worker_loop():
    while True:
        blob_name = task_queue.get()
        worker_client = BlobStorageClient(...)  # TCP handshake + TLS every time
        _process_blob_once(blob_name, worker_client)
```

Each instantiation triggers a full TCP + TLS handshake to Azure. This overhead is
especially visible during autoscaler scale-up events when multiple new threads start
simultaneously and all connect at once.

### Explicitly excluded: max_concurrency

`upload_blob()` supports a `max_concurrency` parameter that splits a single file into
parallel 4 MB chunks. This was evaluated and rejected:

- With N workers × max_concurrency=4, up to 4N simultaneous HTTP requests compete for
  the same ~4.5 MB/s link.
- The autoscaler measures per-worker throughput to estimate headroom and fit the USL
  model. Multiplying hidden connections per worker corrupts these measurements.
- Azure already saturates at ~4.5 MB/s with a single stream per connection on this
  endpoint, so parallel chunks add overhead without bandwidth gain.

### Change

Move client creation to **thread start** — one client per thread, reused for all files
that thread processes during the pass:

```python
# after fix (simplified)
def worker_loop():
    worker_client = BlobStorageClient(...)  # once per thread
    while True:
        blob_name = task_queue.get()
        if blob_name is sentinel:
            break
        _process_blob_once(blob_name, worker_client)
```

Clients are **not shared between threads** — the Azure SDK BlobServiceClient is not
documented as thread-safe for concurrent operations on the same instance.

### Dynamically spawned threads

The `_spawn_workers()` function spawns additional threads mid-pass on scale-up. These
threads must also receive a freshly created client at spawn time — the same pattern
applies.

### Affected files

- `gui/workers.py` — `worker_loop()` closure inside `run_pass()`, and `_spawn_workers()`

---

## What is NOT changing

- Autoscaler logic (Phase B plateau guard, Phase C USL) — untouched
- Retry/backoff strategy — untouched
- Thread pool model — remains OS threads, not asyncio
  - PyArrow's `read_table`/`write_table` are synchronous C extensions that block the
    event loop; wrapping them in `run_in_executor` would reintroduce threads with added
    complexity and no throughput benefit since bandwidth is the bottleneck, not the
    scheduler
- Download path — compression applies only to the output (uploaded) file; the input
  file decompresses automatically via PyArrow

---

## Expected outcome

| Metric | Before | After (estimated) |
|--------|--------|-------------------|
| Average upload size | ~24 MB | ~11–14 MB |
| Total data transferred | ~22.8 GB | ~11–14 GB |
| Run time (bandwidth-bound) | ~2h 44min | ~80–100 min |
| Connection setup per file | 1 TCP+TLS | 0 (reused) |
| Scale-up connection burst | N new handshakes | 0 extra handshakes |

---

## Testing

1. **Compression ratio verification** — after a run, compare `len(upload_data)` logged
   output against pre-change baseline for the same source files.
2. **Read-back validation** — existing unit tests that round-trip Parquet
   (write → read → compare schema/values) must pass unchanged.
3. **Connection pooling** — no new unit test needed; correct behaviour is verified by
   the existing worker scaling integration tests passing and by absence of regression
   in error rates during a real run.
4. **End-to-end** — run the pipeline against a representative prefix and confirm:
   - All files succeed in pass 1 (or at least pass 2)
   - Upload durations drop proportionally to the compression ratio achieved
   - No new Azure SDK errors introduced by client reuse
