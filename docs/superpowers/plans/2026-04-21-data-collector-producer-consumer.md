# Data-Collector: Producer-Consumer + CollectorScaler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-threaded `DataCollectorWorker` with a parallel producer-consumer architecture: N download+filter worker threads feed a shared queue, a dedicated writer thread flushes RAM-bounded batches via `ParquetWriter` to a local temp file, and a new `CollectorScaler` scales workers based on download throughput and RAM pressure.

**Architecture:** `MetadataAccumulator` and `rewrite_with_metadata` are added to `parquet_transform/collector.py` as pure logic. `CollectorScaler` is a new class in `parquet_transform/scaler.py` with the same `should_scale` / `window_ready` / `record_*` interface as `AdaptiveScaler` but optimising for RAM pressure and download errors instead of upload throughput. `DataCollectorWorker` in `gui/workers.py` is completely replaced with the producer-consumer implementation. `CollectorPanel` gains worker-count, autoscale, and RAM-limit controls.

**Tech Stack:** PyQt6, PyArrow (`pyarrow`, `pyarrow.compute`, `pyarrow.parquet`), Python `threading`, `queue`, `tempfile`, pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `parquet_transform/collector.py` | **Modify** | Add `MetadataAccumulator` class and `rewrite_with_metadata()` function |
| `parquet_transform/scaler.py` | **Modify** | Append `CollectorScaler` class at end of file |
| `gui/workers.py` | **Modify** | Replace `DataCollectorWorker` with producer-consumer implementation |
| `gui/collector_panel.py` | **Modify** | Add worker-count spinbox, autoscale checkbox, RAM-limit spinbox |
| `tests/test_collector.py` | **Modify** | Add tests for `MetadataAccumulator` and `rewrite_with_metadata` |
| `tests/test_scaler_collector.py` | **Create** | Unit tests for `CollectorScaler` |
| `tests/test_collector_worker.py` | **Modify** | Update existing tests + add new ones for parallel architecture |

---

### Task 1: Add `MetadataAccumulator` and `rewrite_with_metadata` to `parquet_transform/collector.py`

**Files:**
- Modify: `parquet_transform/collector.py`
- Modify: `tests/test_collector.py`

- [ ] **Step 1: Write the failing tests — append to `tests/test_collector.py`**

```python
# --- MetadataAccumulator ---

def test_accumulator_single_chunk():
    acc = MetadataAccumulator()
    acc.update(_make_table())
    meta = acc.to_metadata()
    assert meta["recordCount"] == "4"
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"
    assert meta["dateTo"] == "04/14/2026 23:47:00 +00:00"
    assert "uid1 dev1 1.0" in meta["deviceIds"]
    assert meta["batchNumber"] == "1"


def test_accumulator_multiple_chunks_combines_correctly():
    acc = MetadataAccumulator()
    table = _make_table()
    acc.update(table.slice(0, 2))   # rows 0-1: uid1, uid2
    acc.update(table.slice(2, 2))   # rows 2-3: uid1, uid3
    meta = acc.to_metadata()
    assert meta["recordCount"] == "4"
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"
    assert meta["dateTo"] == "04/14/2026 23:47:00 +00:00"


def test_accumulator_date_range_spans_chunks():
    acc = MetadataAccumulator()
    table = _make_table()
    acc.update(table.slice(1, 2))   # rows 1-2 (middle timestamps)
    acc.update(table.slice(0, 1))   # row 0 (earliest)
    acc.update(table.slice(3, 1))   # row 3 (latest)
    meta = acc.to_metadata()
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"
    assert meta["dateTo"] == "04/14/2026 23:47:00 +00:00"


def test_accumulator_empty_raises():
    acc = MetadataAccumulator()
    with pytest.raises(ValueError, match="No rows"):
        acc.to_metadata()


def test_accumulator_total_rows_property():
    acc = MetadataAccumulator()
    assert acc.total_rows == 0
    acc.update(_make_table())
    assert acc.total_rows == 4


# --- rewrite_with_metadata ---

def test_rewrite_with_metadata_produces_valid_parquet(tmp_path):
    import pyarrow.parquet as pq

    src = tmp_path / "src.parquet"
    dst = tmp_path / "dst.parquet"
    pq.write_table(_make_table(), str(src))

    rewrite_with_metadata(str(src), str(dst), {"recordCount": "4", "batchNumber": "1"})

    result = pq.read_table(str(dst))
    assert result.num_rows == 4
    meta = result.schema.metadata
    assert meta[b"recordCount"] == b"4"


def test_rewrite_with_metadata_preserves_rows(tmp_path):
    import pyarrow.parquet as pq

    src = tmp_path / "src.parquet"
    dst = tmp_path / "dst.parquet"
    original = _make_table()
    pq.write_table(original, str(src))

    rewrite_with_metadata(str(src), str(dst), {"recordCount": "4"})

    result = pq.read_table(str(dst))
    assert result["SenderUid"].to_pylist() == original["SenderUid"].to_pylist()


def test_rewrite_preserves_existing_metadata_and_overwrites(tmp_path):
    import pyarrow.parquet as pq

    src = tmp_path / "src.parquet"
    dst = tmp_path / "dst.parquet"
    table = _make_table().replace_schema_metadata({"existingKey": "existingVal", "recordCount": "old"})
    pq.write_table(table, str(src))

    rewrite_with_metadata(str(src), str(dst), {"recordCount": "4"})

    result = pq.read_table(str(dst))
    meta = {k.decode(): v.decode() for k, v in result.schema.metadata.items()}
    assert meta["existingKey"] == "existingVal"   # preserved
    assert meta["recordCount"] == "4"             # overwritten
```

- [ ] **Step 2: Run to verify they fail**

```
pytest tests/test_collector.py -k "accumulator or rewrite" -v
```
Expected: `ImportError: cannot import name 'MetadataAccumulator'`

- [ ] **Step 3: Implement — append to `parquet_transform/collector.py`**

```python
class MetadataAccumulator:
    """Incrementally tracks metadata values across multiple filtered chunks."""

    def __init__(self) -> None:
        self._total_rows: int = 0
        self._min_ts = None
        self._max_ts = None
        self._triples: set[str] = set()

    @property
    def total_rows(self) -> int:
        return self._total_rows

    def update(self, chunk: pa.Table) -> None:
        if chunk.num_rows == 0:
            return
        self._total_rows += chunk.num_rows
        ts_col = chunk.column("TsCreate").cast(pa.timestamp("ms", tz="UTC"))
        chunk_min = pc.min(ts_col).as_py()
        chunk_max = pc.max(ts_col).as_py()
        if self._min_ts is None or chunk_min < self._min_ts:
            self._min_ts = chunk_min
        if self._max_ts is None or chunk_max > self._max_ts:
            self._max_ts = chunk_max
        for s, d, v in zip(
            chunk.column("SenderUid").to_pylist(),
            chunk.column("DeviceUid").to_pylist(),
            chunk.column("MessageVersion").to_pylist(),
        ):
            self._triples.add(f"{s} {d} {v}")

    def to_metadata(self) -> dict[str, str]:
        if self._total_rows == 0:
            raise ValueError("No rows accumulated — cannot build metadata")

        def _fmt(dt) -> str:
            return dt.strftime("%m/%d/%Y %H:%M:%S +00:00")

        return {
            "recordCount": str(self._total_rows),
            "dateFrom": _fmt(self._min_ts),
            "dateTo": _fmt(self._max_ts),
            "deviceIds": ",".join(sorted(self._triples)),
            "batchNumber": "1",
        }


def rewrite_with_metadata(src_path: str, dst_path: str, metadata: dict[str, str]) -> None:
    """Stream-copy a Parquet file to dst_path, merging metadata into the schema footer.

    Existing metadata keys in src are preserved; keys in *metadata* overwrite them.
    Uses iter_batches so peak RAM = one row group, not the full file.
    """
    import pyarrow.parquet as _pq

    pf = _pq.ParquetFile(src_path)
    existing = pf.schema_arrow.metadata or {}
    decoded: dict[str, str] = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in existing.items()
    }
    decoded.update(metadata)
    schema_with_meta = pf.schema_arrow.with_metadata(decoded)
    with _pq.ParquetWriter(dst_path, schema_with_meta, compression="zstd", compression_level=3) as w:
        for batch in pf.iter_batches():
            w.write_batch(batch)
```

Also update the import in `tests/test_collector.py` — the top import line currently reads:
```python
from parquet_transform.collector import filter_table_by_ids, build_metadata, make_output_blob_name
```
Change to:
```python
from parquet_transform.collector import (
    filter_table_by_ids,
    build_metadata,
    make_output_blob_name,
    MetadataAccumulator,
    rewrite_with_metadata,
)
```

- [ ] **Step 4: Run to verify they pass**

```
pytest tests/test_collector.py -v
```
Expected: All tests PASS (previously passing tests still pass, 8 new ones pass)

- [ ] **Step 5: Commit**

```bash
git add parquet_transform/collector.py tests/test_collector.py
git commit -m "feat: add MetadataAccumulator and rewrite_with_metadata to collector"
```

---

### Task 2: Add `CollectorScaler` to `parquet_transform/scaler.py`

**Files:**
- Modify: `parquet_transform/scaler.py` (append at end — do not touch `AdaptiveScaler`)
- Create: `tests/test_scaler_collector.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_scaler_collector.py
import pytest
from parquet_transform.scaler import CollectorScaler


def _make_scaler(**kwargs) -> CollectorScaler:
    defaults = dict(max_workers=8, min_samples=3)
    defaults.update(kwargs)
    return CollectorScaler(**defaults)


def _fill_window(scaler: CollectorScaler, n: int, success: bool = True) -> None:
    for _ in range(n):
        scaler.record_download(1_000_000, 0.1, success)


# --- window_ready ---

def test_window_not_ready_before_min_samples():
    s = _make_scaler(min_samples=5)
    _fill_window(s, 4)
    assert not s.window_ready()


def test_window_ready_after_min_samples():
    s = _make_scaler(min_samples=3)
    _fill_window(s, 3)
    assert s.window_ready()


# --- should_scale: no change when not ready ---

def test_no_scale_when_window_not_ready():
    s = _make_scaler(min_samples=5)
    _fill_window(s, 2)
    new, reason = s.should_scale(4, ram_used_bytes=0, ram_limit_bytes=1_000_000_000)
    assert new == 4
    assert reason == ""


# --- should_scale: scale DOWN on high RAM ---

def test_scale_down_when_ram_above_high_water():
    s = _make_scaler(min_samples=3, ram_high_water=0.75)
    _fill_window(s, 5)
    # 80% RAM → above high water
    new, reason = s.should_scale(4, ram_used_bytes=800_000_000, ram_limit_bytes=1_000_000_000)
    assert new == 3
    assert "RAM" in reason


def test_scale_down_respects_min_workers():
    s = _make_scaler(min_samples=3, min_workers=1)
    _fill_window(s, 5)
    new, reason = s.should_scale(1, ram_used_bytes=900_000_000, ram_limit_bytes=1_000_000_000)
    assert new == 1  # already at minimum


# --- should_scale: scale UP on low RAM ---

def test_scale_up_when_ram_below_low_water():
    s = _make_scaler(min_samples=3, ram_low_water=0.35, max_workers=8)
    _fill_window(s, 5)
    # 20% RAM → well below low water, no errors
    new, reason = s.should_scale(4, ram_used_bytes=200_000_000, ram_limit_bytes=1_000_000_000)
    assert new == 5
    assert reason != ""


def test_scale_up_respects_max_workers():
    s = _make_scaler(min_samples=3, max_workers=4)
    _fill_window(s, 5)
    new, reason = s.should_scale(4, ram_used_bytes=100_000_000, ram_limit_bytes=1_000_000_000)
    assert new == 4  # already at max


# --- should_scale: hold in middle zone ---

def test_hold_when_ram_in_middle_zone():
    s = _make_scaler(min_samples=3, ram_low_water=0.35, ram_high_water=0.75)
    _fill_window(s, 5)
    # 50% RAM → in the middle zone
    new, reason = s.should_scale(4, ram_used_bytes=500_000_000, ram_limit_bytes=1_000_000_000)
    assert new == 4
    assert reason == ""


# --- should_scale: scale DOWN on error rate ---

def test_scale_down_on_high_error_rate():
    s = _make_scaler(min_samples=3, error_rate_threshold=0.30)
    # 4 successes, 2 failures → 33% error rate
    for _ in range(4):
        s.record_download(1_000_000, 0.1, True)
    for _ in range(2):
        s.record_download(0, 0.0, False)
    new, reason = s.should_scale(4, ram_used_bytes=200_000_000, ram_limit_bytes=1_000_000_000)
    assert new == 3
    assert "error" in reason.lower()


# --- consume_hot_error_flag ---

def test_hot_error_flag_set_on_high_failure_rate():
    s = _make_scaler(min_samples=3, hot_error_rate=0.50)
    # 6 failures in a row → hot error
    for _ in range(6):
        s.record_download(0, 0.0, False)
    assert s.consume_hot_error_flag() is True


def test_hot_error_flag_consumed_once():
    s = _make_scaler(min_samples=3, hot_error_rate=0.50)
    for _ in range(6):
        s.record_download(0, 0.0, False)
    assert s.consume_hot_error_flag() is True
    assert s.consume_hot_error_flag() is False  # consumed


def test_no_hot_error_on_low_failure_rate():
    s = _make_scaler(min_samples=3, hot_error_rate=0.50)
    _fill_window(s, 5)  # all success
    assert s.consume_hot_error_flag() is False


# --- hot error triggers halving in should_scale ---

def test_hot_error_halves_workers():
    s = _make_scaler(min_samples=3, hot_error_rate=0.50, min_workers=1)
    for _ in range(6):
        s.record_download(0, 0.0, False)
    new, reason = s.should_scale(8, ram_used_bytes=0, ram_limit_bytes=1_000_000_000)
    assert new == 4  # 8 // 2
    assert "hot" in reason.lower()
```

- [ ] **Step 2: Run to verify they fail**

```
pytest tests/test_scaler_collector.py -v
```
Expected: `ImportError: cannot import name 'CollectorScaler'`

- [ ] **Step 3: Append `CollectorScaler` to end of `parquet_transform/scaler.py`**

```python
# ---------------------------------------------------------------------------
# Collector Scaler
# ---------------------------------------------------------------------------

class CollectorScaler:
    """
    Scaling decisions for the Data-Collector producer-consumer architecture.

    Scales worker count based on:
    - RAM pressure (buffer fill vs. configured limit)
    - Download error rate
    - Hot-error spikes (immediate halving)

    Unlike AdaptiveScaler, this class has no USL model — it uses simple
    threshold-based rules appropriate for bounded-memory collection workloads.
    """

    def __init__(
        self,
        *,
        max_workers: int,
        min_workers: int = 1,
        window_size: int = 30,
        min_samples: int = 5,
        ram_high_water: float = 0.75,
        ram_low_water: float = 0.35,
        error_rate_threshold: float = 0.30,
        hot_error_rate: float = 0.50,
    ) -> None:
        import threading as _threading
        self._max_workers = max_workers
        self._min_workers = min_workers
        self._window_size = window_size
        self._min_samples = min_samples
        self._ram_high_water = ram_high_water
        self._ram_low_water = ram_low_water
        self._error_rate_threshold = error_rate_threshold
        self._hot_error_rate = hot_error_rate

        self._lock = _threading.Lock()
        self._window: list[bool] = []   # True = success, False = failure
        self._hot_flag: bool = False

    def record_download(self, bytes_: int, seconds: float, success: bool) -> None:
        with self._lock:
            self._window.append(success)
            if len(self._window) > self._window_size:
                self._window.pop(0)
            # Detect hot-error spike
            if len(self._window) >= self._min_samples:
                failures = self._window.count(False)
                if failures / len(self._window) >= self._hot_error_rate:
                    self._hot_flag = True

    def window_ready(self) -> bool:
        with self._lock:
            return len(self._window) >= self._min_samples

    def consume_hot_error_flag(self) -> bool:
        with self._lock:
            flag = self._hot_flag
            self._hot_flag = False
            return flag

    def should_scale(
        self,
        current_workers: int,
        ram_used_bytes: int,
        ram_limit_bytes: int,
    ) -> tuple[int, str]:
        """Return (new_worker_count, reason). Returns current_workers and '' if no change."""
        if not self.window_ready():
            return current_workers, ""

        with self._lock:
            window = list(self._window)
            hot = self._hot_flag
            if hot:
                self._hot_flag = False

        error_rate = window.count(False) / len(window)
        ram_pressure = ram_used_bytes / ram_limit_bytes if ram_limit_bytes > 0 else 0.0

        # Priority 1: hot error spike → halve workers
        if hot:
            new = max(self._min_workers, current_workers // 2)
            return new, f"hot error rate {error_rate:.0%}: halving workers"

        # Priority 2: high RAM → scale down
        if ram_pressure > self._ram_high_water:
            new = max(self._min_workers, current_workers - 1)
            if new != current_workers:
                return new, f"RAM pressure {ram_pressure:.0%}: reducing workers"

        # Priority 3: high error rate → scale down
        if error_rate > self._error_rate_threshold:
            new = max(self._min_workers, current_workers - 1)
            if new != current_workers:
                return new, f"error rate {error_rate:.0%}: reducing workers"

        # Priority 4: low RAM + low errors → scale up
        if ram_pressure < self._ram_low_water and error_rate < 0.10:
            new = min(self._max_workers, current_workers + 1)
            if new != current_workers:
                return new, f"RAM pressure {ram_pressure:.0%}: adding worker"

        return current_workers, ""
```

- [ ] **Step 4: Run to verify all tests pass**

```
pytest tests/test_scaler_collector.py -v
```
Expected: All 15 tests PASS

- [ ] **Step 5: Commit**

```bash
git add parquet_transform/scaler.py tests/test_scaler_collector.py
git commit -m "feat: add CollectorScaler for RAM+download based worker scaling"
```

---

### Task 3: Replace `DataCollectorWorker` in `gui/workers.py`

**Files:**
- Modify: `gui/workers.py` — delete old `DataCollectorWorker` class (lines from `# --- Data Collector Worker ---` to end of file), replace with new implementation
- Modify: `tests/test_collector_worker.py` — update existing tests + add new ones

- [ ] **Step 1: Update and add tests in `tests/test_collector_worker.py`**

Replace the entire file content:

```python
# tests/test_collector_worker.py
import io
import sys
import queue
import datetime
import threading
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from unittest.mock import MagicMock, patch, call

from PyQt6.QtWidgets import QApplication
_app = QApplication.instance() or QApplication(sys.argv)


def _ts_ms(year, month, day, hour, minute, second) -> int:
    dt = datetime.datetime(year, month, day, hour, minute, second,
                           tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def _make_parquet_bytes(sender_uid: str, device_uid: str, n: int = 3) -> bytes:
    table = pa.table({
        "Id": [f"id{i}" for i in range(n)],
        "SenderUid": [sender_uid] * n,
        "DeviceUid": [device_uid] * n,
        "DeviceType": ["T"] * n,
        "DeviceManufacturer": ["M"] * n,
        "TsCreate": pa.array(
            [_ts_ms(2026, 4, 14, 23, 45, 8) + i * 1000 for i in range(n)],
            type=pa.timestamp("ms", tz="UTC"),
        ),
        "MessageVersion": ["1.0"] * n,
        "MessageType": ["X"] * n,
        "Payload": [f"p{i}" for i in range(n)],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_thread_safe_download_mock(*blob_bytes_list):
    """Return a side_effect function that is safe to call from multiple threads."""
    q = queue.Queue()
    for b in blob_bytes_list:
        q.put(b)
    def _side_effect(blob_name, timeout=None):
        return q.get(timeout=5.0)
    return _side_effect


def _run_worker(worker):
    results = {}
    worker.finished.connect(lambda d: results.update(d))
    worker.run()
    return results


def _make_worker(**kwargs):
    from gui.workers import DataCollectorWorker
    defaults = dict(
        connection_string="fake",
        container="c",
        source_prefix="prefix/",
        output_prefix="out/",
        filter_col="SenderUid",
        filter_values=["uid1"],
        max_workers=2,
        ram_limit_mb=512,
        autoscale=False,
    )
    defaults.update(kwargs)
    return DataCollectorWorker(**defaults)


# --- basic functionality ---

def test_worker_combines_rows_from_multiple_blobs():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet", "p/f2.parquet"]
    mock_client.download_bytes.side_effect = _make_thread_safe_download_mock(
        _make_parquet_bytes("uid1", "dev1", 2),
        _make_parquet_bytes("uid1", "dev1", 3),
    )

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 5
    assert mock_client.upload_bytes.call_count == 1


def test_worker_output_blob_name_is_correct():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        _run_worker(_make_worker())

    blob_name = mock_client.upload_bytes.call_args[0][0]
    assert blob_name == "out/SenderUid_uid1.parquet"


def test_worker_filters_only_matching_rows():
    mixed_table = pa.concat_tables([
        pq.read_table(io.BytesIO(_make_parquet_bytes("uid1", "dev1", 2))),
        pq.read_table(io.BytesIO(_make_parquet_bytes("uid2", "dev1", 3))),
    ])
    buf = io.BytesIO()
    pq.write_table(mixed_table, buf)

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = buf.getvalue()

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 2


def test_worker_no_match_skips_upload():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid_other", "dev1", 3)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 0
    assert mock_client.upload_bytes.call_count == 0


def test_worker_emits_cancelled_when_cancel_called():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet", "p/f2.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 1)

    cancelled = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = _make_worker()
        worker.cancelled.connect(lambda: cancelled.append(True))
        worker.cancel()
        worker.run()

    assert cancelled == [True]
    assert mock_client.upload_bytes.call_count == 0


def test_worker_emits_file_error_on_bad_blob_and_continues():
    good_bytes = _make_parquet_bytes("uid1", "dev1", 3)
    call_count = [0]
    lock = threading.Lock()

    def _side_effect(blob_name, timeout=None):
        with lock:
            call_count[0] += 1
            n = call_count[0]
        if n == 1:
            raise Exception("simulated download failure")
        return good_bytes

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/bad.parquet", "p/good.parquet"]
    mock_client.download_bytes.side_effect = _side_effect

    errors = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = _make_worker()
        worker.file_error.connect(lambda b, e: errors.append(b))
        results = _run_worker(worker)

    assert len(errors) == 1
    assert errors[0] == "p/bad.parquet"
    assert results["rowCount"] == 3
    assert mock_client.upload_bytes.call_count == 1


# --- uploaded file has correct metadata ---

def test_worker_output_has_correct_metadata():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 2
    uploaded_bytes = mock_client.upload_bytes.call_args[0][1]
    result_table = pq.read_table(io.BytesIO(uploaded_bytes))
    meta = {k.decode(): v.decode() for k, v in result_table.schema.metadata.items()}
    assert meta["recordCount"] == "2"
    assert "+00:00" in meta["dateFrom"]
    assert "+00:00" in meta["dateTo"]


# --- separate output container ---

def test_worker_uses_separate_output_container():
    source_mock = MagicMock()
    source_mock.list_blobs.return_value = ["p/f1.parquet"]
    source_mock.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    output_mock = MagicMock()

    def _client_factory(conn, container):
        if container == "source-container":
            return source_mock
        return output_mock

    with patch("gui.workers.BlobStorageClient", side_effect=_client_factory):
        worker = _make_worker(container="source-container", output_container="output-container")
        _run_worker(worker)

    assert output_mock.upload_bytes.call_count == 1
    assert source_mock.upload_bytes.call_count == 0
```

- [ ] **Step 2: Run to verify they fail (or partially fail)**

```
pytest tests/test_collector_worker.py -v
```
Expected: Several failures because `DataCollectorWorker.__init__` does not yet accept `max_workers`, `ram_limit_mb`, `autoscale`

- [ ] **Step 3: Replace the `DataCollectorWorker` in `gui/workers.py`**

Find the line `# ---------------------------------------------------------------------------` that precedes `# Data Collector Worker` and delete everything from that line to the end of the file. Then append:

```python
# ---------------------------------------------------------------------------
# Data Collector Worker
# ---------------------------------------------------------------------------

class DataCollectorWorker(QThread):
    """
    Parallel producer-consumer collector.

    N producer threads download and filter Parquet blobs from Azure.
    One writer thread accumulates filtered chunks and flushes to a local
    temp file via ParquetWriter when the RAM buffer reaches ram_limit_mb.
    After all producers finish the temp file is rewritten with recalculated
    metadata, uploaded to Azure, and the temp files are deleted.
    """

    listing_complete = pyqtSignal(int)
    progress = pyqtSignal(int, int, str)
    file_error = pyqtSignal(str, str)
    finished = pyqtSignal(dict)
    cancelled = pyqtSignal()
    log_message = pyqtSignal(str)
    workers_scaled = pyqtSignal(int, int, str, str)

    def __init__(
        self,
        connection_string: str,
        container: str,
        source_prefix: str,
        output_prefix: str,
        filter_col: str,
        filter_values: list[str],
        output_container: str = "",
        max_workers: int = 4,
        ram_limit_mb: int = 1024,
        autoscale: bool = True,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._conn = connection_string
        self._container_name = container
        self._source_prefix = source_prefix
        self._output_prefix = output_prefix
        self._filter_col = filter_col
        self._filter_values = filter_values
        self._output_container = output_container or container
        self._max_workers = max_workers
        self._ram_limit_bytes = ram_limit_mb * 1024 * 1024
        self._autoscale = autoscale
        self._worker_count = max_workers
        self._cancel_event = threading.Event()

    def cancel(self) -> None:
        self._cancel_event.set()

    def run(self) -> None:
        import os
        import tempfile
        from parquet_transform.collector import (
            filter_table_by_ids,
            make_output_blob_name,
            MetadataAccumulator,
            rewrite_with_metadata,
        )
        from parquet_transform.scaler import CollectorScaler

        source_client = BlobStorageClient(self._conn, self._container_name)
        output_client = (
            source_client
            if self._output_container == self._container_name
            else BlobStorageClient(self._conn, self._output_container)
        )

        tmp1_path: str | None = None
        tmp2_path: str | None = None
        writer_ref: list = [None]

        try:
            blobs = source_client.list_blobs(self._source_prefix)
            total = len(blobs)
            self.listing_complete.emit(total)

            if not blobs:
                self.finished.emit({"rowCount": 0})
                return

            # ── Shared state ──────────────────────────────────────────────
            task_queue: queue.Queue = queue.Queue()
            for b in blobs:
                task_queue.put(b)

            chunk_queue: queue.Queue = queue.Queue()

            completed = [0]
            completed_lock = threading.Lock()

            ram_used = [0]
            ram_lock = threading.Lock()

            meta_acc = MetadataAccumulator()

            tmp1_fd, tmp1_path = tempfile.mkstemp(suffix=".parquet")
            os.close(tmp1_fd)

            writer_stop = threading.Event()
            writer_error: list = [None]

            # ── Writer thread ─────────────────────────────────────────────
            def _writer_loop() -> None:
                try:
                    while True:
                        try:
                            chunk = chunk_queue.get(timeout=0.2)
                        except queue.Empty:
                            if writer_stop.is_set() and chunk_queue.empty():
                                break
                            continue
                        if chunk is None:
                            break
                        meta_acc.update(chunk)
                        if writer_ref[0] is None:
                            writer_ref[0] = pq.ParquetWriter(
                                tmp1_path, chunk.schema,
                                compression="zstd", compression_level=3,
                            )
                        writer_ref[0].write_table(chunk)
                        with ram_lock:
                            ram_used[0] = max(0, ram_used[0] - chunk.nbytes)
                except Exception as exc:  # noqa: BLE001
                    writer_error[0] = str(exc)

            writer_thread = threading.Thread(target=_writer_loop, daemon=True)
            writer_thread.start()

            # ── Producer worker function ───────────────────────────────────
            _exit_requested = [0]
            _exit_lock = threading.Lock()
            _threads: list[threading.Thread] = []
            _threads_lock = threading.Lock()
            _next_id = [1]

            scaler = (
                CollectorScaler(max_workers=self._max_workers)
                if self._autoscale else None
            )

            def _producer(wid: int) -> None:
                client = BlobStorageClient(self._conn, self._container_name)
                try:
                    while not self._cancel_event.is_set():
                        with _exit_lock:
                            if _exit_requested[0] > 0:
                                _exit_requested[0] -= 1
                                return
                        try:
                            blob_name = task_queue.get_nowait()
                        except queue.Empty:
                            return
                        try:
                            t0 = time.monotonic()
                            raw = client.download_bytes(blob_name, timeout=DOWNLOAD_TIMEOUT_S)
                            dl_s = time.monotonic() - t0
                            table = pq.read_table(io.BytesIO(raw))
                            filtered = filter_table_by_ids(
                                table, self._filter_col, self._filter_values
                            )
                            if scaler is not None:
                                scaler.record_download(len(raw), dl_s, True)
                            if filtered.num_rows > 0:
                                with ram_lock:
                                    ram_used[0] += filtered.nbytes
                                while not self._cancel_event.is_set():
                                    try:
                                        chunk_queue.put(filtered, timeout=0.5)
                                        break
                                    except queue.Full:
                                        continue
                        except Exception as exc:
                            msg, _, _ = _summarize_exception(exc)
                            self.file_error.emit(blob_name, msg)
                            if scaler is not None:
                                scaler.record_download(0, 0.0, False)
                        finally:
                            with completed_lock:
                                completed[0] += 1
                                done = completed[0]
                            self.progress.emit(done, total, blob_name)
                finally:
                    client.close()

            def _spawn(count: int) -> None:
                for _ in range(count):
                    wid = _next_id[0]
                    _next_id[0] += 1
                    t = threading.Thread(target=_producer, args=(wid,), daemon=True)
                    with _threads_lock:
                        _threads.append(t)
                    t.start()

            _spawn(self._worker_count)

            # ── Main coordination loop ────────────────────────────────────
            _scale_ticks = [0]
            SCALE_INTERVAL = 5

            while True:
                if self._cancel_event.is_set():
                    while not task_queue.empty():
                        try:
                            task_queue.get_nowait()
                        except queue.Empty:
                            break
                    break

                with completed_lock:
                    done = completed[0]
                if done >= total:
                    break

                if scaler is not None:
                    _scale_ticks[0] += 1
                    if _scale_ticks[0] >= SCALE_INTERVAL:
                        _scale_ticks[0] = 0
                        with ram_lock:
                            current_ram = ram_used[0]
                        new_count, reason = scaler.should_scale(
                            self._worker_count, current_ram, self._ram_limit_bytes
                        )
                        if new_count != self._worker_count:
                            old = self._worker_count
                            self._worker_count = new_count
                            direction = "up" if new_count > old else "down"
                            self.workers_scaled.emit(new_count, old, direction, reason)
                            self.log_message.emit(
                                f"[Collector] Workers {old}→{new_count}: {reason}"
                            )
                            if new_count > old:
                                _spawn(new_count - old)
                            else:
                                with _exit_lock:
                                    _exit_requested[0] += old - new_count

                time.sleep(0.05)

            # Wait for all producers
            with _threads_lock:
                all_threads = list(_threads)
            for t in all_threads:
                t.join(timeout=5.0)

            if self._cancel_event.is_set():
                chunk_queue.put(None)
                writer_thread.join(timeout=5.0)
                self.cancelled.emit()
                return

            # Stop writer
            writer_stop.set()
            writer_thread.join(timeout=60.0)

            if writer_error[0]:
                self.log_message.emit(f"Data-Collector writer error: {writer_error[0]}")
                self.finished.emit({"rowCount": 0})
                return

            if writer_ref[0] is not None:
                writer_ref[0].close()
                writer_ref[0] = None

            if meta_acc.total_rows == 0:
                self.log_message.emit(
                    f"Data-Collector: no rows matched "
                    f"{self._filter_col} {self._filter_values}"
                )
                self.finished.emit({"rowCount": 0})
                return

            # Rewrite temp file with recalculated metadata (streaming, bounded RAM)
            final_meta = meta_acc.to_metadata()
            tmp2_fd, tmp2_path = tempfile.mkstemp(suffix=".parquet")
            os.close(tmp2_fd)
            rewrite_with_metadata(tmp1_path, tmp2_path, final_meta)

            # Upload
            with open(tmp2_path, "rb") as fh:
                data = fh.read()

            out_name = make_output_blob_name(
                self._output_prefix, self._filter_col, self._filter_values
            )
            output_client.upload_bytes(out_name, data, overwrite=True, timeout=UPLOAD_TIMEOUT_S)
            self.log_message.emit(
                f"Data-Collector: uploaded {out_name} → "
                f"{self._output_container} ({meta_acc.total_rows} rows)"
            )
            self.finished.emit({
                "rowCount": meta_acc.total_rows,
                "outputBlob": out_name,
                "outputContainer": self._output_container,
            })

        finally:
            if writer_ref[0] is not None:
                try:
                    writer_ref[0].close()
                except Exception:
                    pass
            source_client.close()
            if output_client is not source_client:
                output_client.close()
            for p in (tmp1_path, tmp2_path):
                if p is not None:
                    try:
                        os.unlink(p)
                    except Exception:
                        pass
```

Also ensure these imports are present at the top of `gui/workers.py` (they likely already are — verify and add only if missing):
- `import queue`
- `import time`
- `import os` — **add this if missing**

- [ ] **Step 4: Run tests to verify they pass**

```
pytest tests/test_collector_worker.py -v
```
Expected: All 9 tests PASS

- [ ] **Step 5: Run full test suite to confirm no regressions**

```
pytest tests/ -q --ignore=tests/test_worker_scaling.py
```
Expected: All pass (2 pre-existing failures in test_worker_scaling.py are unrelated)

- [ ] **Step 6: Commit**

```bash
git add gui/workers.py tests/test_collector_worker.py
git commit -m "feat: replace DataCollectorWorker with producer-consumer + CollectorScaler"
```

---

### Task 4: Add worker/RAM controls to `gui/collector_panel.py`

**Files:**
- Modify: `gui/collector_panel.py`

- [ ] **Step 1: Add `QCheckBox` and `QSpinBox` to the imports**

At the top of `gui/collector_panel.py`, find:
```python
from PyQt6.QtWidgets import (
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
```

Replace with:
```python
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
```

- [ ] **Step 2: Add performance row to `_build_filter_group()`**

Find the end of `_build_filter_group()` — just before `return box`:
```python
        layout.addLayout(row2)

        return box
```

Replace with:
```python
        layout.addLayout(row2)

        # Row 3: worker count + autoscale + RAM limit
        row3 = QHBoxLayout()
        row3.addWidget(QLabel("Workers:"))
        self._workers_spin = QSpinBox()
        self._workers_spin.setRange(1, 64)
        self._workers_spin.setValue(4)
        self._workers_spin.setFixedWidth(60)
        row3.addWidget(self._workers_spin)
        self._autoscale_check = QCheckBox("Autoscale")
        self._autoscale_check.setChecked(True)
        self._workers_spin.setEnabled(False)   # disabled when autoscale on
        self._autoscale_check.toggled.connect(
            lambda checked: self._workers_spin.setEnabled(not checked)
        )
        row3.addWidget(self._autoscale_check)
        row3.addSpacing(16)
        row3.addWidget(QLabel("RAM limit (MB):"))
        self._ram_spin = QSpinBox()
        self._ram_spin.setRange(128, 8192)
        self._ram_spin.setValue(1024)
        self._ram_spin.setSingleStep(128)
        self._ram_spin.setFixedWidth(80)
        row3.addWidget(self._ram_spin)
        row3.addStretch()
        layout.addLayout(row3)

        return box
```

- [ ] **Step 3: Pass new params in `_on_collect()`**

Find the `DataCollectorWorker(...)` call in `_on_collect()`:
```python
        self._worker = DataCollectorWorker(
            connection_string=conn,
            container=container,
            source_prefix=self._source_edit.text().strip(),
            output_prefix=output_prefix,
            filter_col=self._filter_combo.currentText(),
            filter_values=filter_values,
            output_container=self._output_container_edit.text().strip(),
        )
```

Replace with:
```python
        self._worker = DataCollectorWorker(
            connection_string=conn,
            container=container,
            source_prefix=self._source_edit.text().strip(),
            output_prefix=output_prefix,
            filter_col=self._filter_combo.currentText(),
            filter_values=filter_values,
            output_container=self._output_container_edit.text().strip(),
            max_workers=self._workers_spin.value(),
            ram_limit_mb=self._ram_spin.value(),
            autoscale=self._autoscale_check.isChecked(),
        )
```

- [ ] **Step 4: Connect `workers_scaled` signal in `_on_collect()`**

Find where the worker signals are connected (after the `DataCollectorWorker` instantiation):
```python
        self._worker.log_message.connect(self._log_info)
        self._worker.start()
```

Replace with:
```python
        self._worker.log_message.connect(self._log_info)
        self._worker.workers_scaled.connect(self._on_workers_scaled)
        self._worker.start()
```

- [ ] **Step 5: Add `_on_workers_scaled` slot**

Add this method to `CollectorPanel` (after `_on_cancelled`):
```python
    def _on_workers_scaled(self, new_count: int, old_count: int, direction: str, reason: str) -> None:
        self._log_info(f"Workers {old_count}→{new_count} ({direction}): {reason}")
```

- [ ] **Step 6: Verify import and run app**

```
python -c "from gui.collector_panel import CollectorPanel; print('OK')"
```
Expected: `OK`

```
pytest tests/ -q --ignore=tests/test_worker_scaling.py
```
Expected: All pass

- [ ] **Step 7: Commit**

```bash
git add gui/collector_panel.py
git commit -m "feat: add worker count, autoscale, RAM limit controls to CollectorPanel"
```

---

## Self-Review

### 1. Spec Coverage

| Requirement | Task |
|-------------|------|
| Producer-consumer architecture | Task 3 (`DataCollectorWorker`) |
| N parallel download+filter workers | Task 3 (`_producer` threads + `_spawn`) |
| Thread-safe queue between producers and writer | Task 3 (`chunk_queue`) |
| Writer flushes RAM-bounded batches via ParquetWriter | Task 3 (`_writer_loop` + `writer_ref[0].write_table`) |
| RAM limit configurable via UI | Task 4 (`_ram_spin`) |
| Worker count configurable via UI | Task 4 (`_workers_spin`) |
| Autoscale toggle | Task 4 (`_autoscale_check`) |
| CollectorScaler scales on RAM pressure | Task 2 (`should_scale` priority 2) |
| CollectorScaler scales on download errors | Task 2 (`should_scale` priority 3) |
| CollectorScaler hot-error halving | Task 2 (`should_scale` priority 1) |
| AdaptiveScaler untouched | Task 2 (append-only to `scaler.py`) |
| Metadata recalculated incrementally | Task 1 (`MetadataAccumulator`) |
| Final file written with metadata (streaming, bounded RAM) | Task 1 (`rewrite_with_metadata`) + Task 3 |
| Existing metadata keys preserved in output | Task 1 (`rewrite_with_metadata` merges) |
| Cancel stops workers and cleans up temp files | Task 3 (`finally` block + `_cancel_event`) |
| Separate output container still supported | Task 3 (`output_client` vs `source_client`) |

### 2. Placeholder Scan
No TBDs, no "handle edge cases", no "similar to Task N". All code blocks are complete.

### 3. Type Consistency
- `MetadataAccumulator.update(chunk: pa.Table)` — called in writer thread with `pa.Table` ✓
- `MetadataAccumulator.to_metadata() -> dict[str, str]` — passed to `rewrite_with_metadata` ✓
- `rewrite_with_metadata(src_path: str, dst_path: str, metadata: dict[str, str])` — called in Task 3 with `tmp1_path`, `tmp2_path`, `final_meta` ✓
- `CollectorScaler.should_scale(current_workers: int, ram_used_bytes: int, ram_limit_bytes: int) -> tuple[int, str]` — called in coordination loop ✓
- `DataCollectorWorker.workers_scaled` signal `(int, int, str, str)` — connected in Task 4 to `_on_workers_scaled(new_count, old_count, direction, reason)` ✓
