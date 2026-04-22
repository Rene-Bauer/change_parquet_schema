# Data-Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a fully independent "Data-Collector" tab to the GUI that downloads Parquet files from Azure Blob Storage, filters rows by SenderUid or DeviceUid (multiple values supported), combines all matching rows into exactly one output Parquet file per run, recalculates schema metadata, and uploads the result back to Azure.

**Architecture:** A `QTabWidget` wraps the existing Schema Transformer UI (unchanged) in Tab 1 and the new Data-Collector in Tab 2. Pure collector logic (filtering, metadata recalculation, output-blob naming) lives in `parquet_transform/collector.py`. A new `DataCollectorWorker` QThread in `gui/workers.py` owns all Azure I/O and emits Qt signals back to a dedicated `CollectorWindow` widget in `gui/collector_panel.py`. `gui/main_window.py` only gains the `QTabWidget` wrapper and tab registration — existing code is not modified.

**Tech Stack:** PyQt6, PyArrow (`pyarrow`, `pyarrow.compute`), Azure Blob Storage SDK (`azure-storage-blob`), pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `parquet_transform/collector.py` | **Create** | `filter_table_by_ids()`, `build_metadata()`, `make_output_blob_name()` — pure logic, no Qt |
| `tests/test_collector.py` | **Create** | Unit tests for all collector.py functions |
| `gui/workers.py` | **Append only** | Add `DataCollectorWorker` QThread at end of file |
| `tests/test_collector_worker.py` | **Create** | Integration tests for DataCollectorWorker (mocked Azure) |
| `gui/collector_panel.py` | **Create** | `CollectorPanel` widget — full Data-Collector UI, self-contained |
| `gui/main_window.py` | **Modify (minimal)** | Wrap existing layout in Tab 1 of a `QTabWidget`; add Tab 2 = `CollectorPanel` |

---

### Task 1: Create `parquet_transform/collector.py` with pure logic

**Files:**
- Create: `parquet_transform/collector.py`
- Test: `tests/test_collector.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_collector.py
import datetime
import pyarrow as pa
import pytest
from parquet_transform.collector import filter_table_by_ids, build_metadata, make_output_blob_name


def _ts_ms(year, month, day, hour, minute, second) -> int:
    dt = datetime.datetime(year, month, day, hour, minute, second,
                           tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def _make_table() -> pa.Table:
    return pa.table({
        "Id": ["a", "b", "c", "d"],
        "SenderUid": ["uid1", "uid2", "uid1", "uid3"],
        "DeviceUid": ["dev1", "dev1", "dev2", "dev2"],
        "DeviceType": ["T", "T", "T", "T"],
        "DeviceManufacturer": ["M", "M", "M", "M"],
        "TsCreate": pa.array([
            _ts_ms(2026, 4, 14, 23, 45,  8),
            _ts_ms(2026, 4, 14, 23, 46,  0),
            _ts_ms(2026, 4, 14, 23, 46, 44),
            _ts_ms(2026, 4, 14, 23, 47,  0),
        ], type=pa.timestamp("ms", tz="UTC")),
        "MessageVersion": ["1.0", "1.0", "1.0", "1.0"],
        "MessageType": ["X", "X", "X", "X"],
        "Payload": ["p1", "p2", "p3", "p4"],
    })


# --- filter_table_by_ids ---

def test_filter_single_sender_uid():
    result = filter_table_by_ids(_make_table(), "SenderUid", ["uid1"])
    assert result.num_rows == 2
    assert set(result["SenderUid"].to_pylist()) == {"uid1"}


def test_filter_multiple_sender_uids():
    result = filter_table_by_ids(_make_table(), "SenderUid", ["uid1", "uid3"])
    assert result.num_rows == 3
    assert set(result["SenderUid"].to_pylist()) == {"uid1", "uid3"}


def test_filter_device_uid():
    result = filter_table_by_ids(_make_table(), "DeviceUid", ["dev2"])
    assert result.num_rows == 2
    assert set(result["DeviceUid"].to_pylist()) == {"dev2"}


def test_filter_no_match_returns_empty():
    result = filter_table_by_ids(_make_table(), "SenderUid", ["no_such"])
    assert result.num_rows == 0


def test_filter_preserves_schema():
    original = _make_table()
    result = filter_table_by_ids(original, "SenderUid", ["uid1"])
    assert result.schema.names == original.schema.names


# --- build_metadata ---

def test_build_metadata_record_count():
    meta = build_metadata(_make_table())
    assert meta["recordCount"] == "4"


def test_build_metadata_date_from_is_minimum():
    meta = build_metadata(_make_table())
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"


def test_build_metadata_date_to_is_maximum():
    meta = build_metadata(_make_table())
    assert meta["dateTo"] == "04/14/2026 23:47:00 +00:00"


def test_build_metadata_device_ids_contains_all_triples():
    meta = build_metadata(_make_table())
    device_ids = meta["deviceIds"]
    assert "uid1 dev1 1.0" in device_ids
    assert "uid1 dev2 1.0" in device_ids
    assert "uid2 dev1 1.0" in device_ids
    assert "uid3 dev2 1.0" in device_ids


def test_build_metadata_batch_number():
    meta = build_metadata(_make_table())
    assert meta["batchNumber"] == "1"


def test_build_metadata_empty_table_raises():
    empty = _make_table().slice(0, 0)
    with pytest.raises(ValueError, match="empty"):
        build_metadata(empty)


# --- make_output_blob_name ---

def test_make_output_blob_name_single_id():
    name = make_output_blob_name("out/collected", "SenderUid", ["uid1"])
    assert name == "out/collected/SenderUid_uid1.parquet"


def test_make_output_blob_name_multiple_ids():
    name = make_output_blob_name("out/collected", "DeviceUid", ["dev1", "dev2"])
    assert name == "out/collected/DeviceUid_dev1_dev2.parquet"


def test_make_output_blob_name_strips_trailing_slash():
    name = make_output_blob_name("out/collected/", "SenderUid", ["uid1"])
    assert name == "out/collected/SenderUid_uid1.parquet"
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest tests/test_collector.py -v
```
Expected: `ModuleNotFoundError: No module named 'parquet_transform.collector'`

- [ ] **Step 3: Implement `parquet_transform/collector.py`**

```python
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def filter_table_by_ids(table: pa.Table, filter_col: str, filter_values: list[str]) -> pa.Table:
    """Return rows where filter_col matches any value in filter_values."""
    value_set = pa.array(filter_values, type=pa.string())
    mask = pc.is_in(table.column(filter_col), value_set=value_set)
    return table.filter(mask)


def build_metadata(table: pa.Table) -> dict[str, str]:
    """Recalculate Parquet file metadata from table contents."""
    if table.num_rows == 0:
        raise ValueError("Cannot build metadata for empty table")

    ts_col = table.column("TsCreate").cast(pa.timestamp("ms", tz="UTC"))
    ts_min = pc.min(ts_col).as_py()
    ts_max = pc.max(ts_col).as_py()

    def _fmt(dt) -> str:
        return dt.strftime("%m/%d/%Y %H:%M:%S +00:00")

    sender_col = table.column("SenderUid").to_pylist()
    device_col = table.column("DeviceUid").to_pylist()
    version_col = table.column("MessageVersion").to_pylist()
    triples = sorted({f"{s} {d} {v}" for s, d, v in zip(sender_col, device_col, version_col)})

    return {
        "recordCount": str(table.num_rows),
        "dateFrom": _fmt(ts_min),
        "dateTo": _fmt(ts_max),
        "deviceIds": ",".join(triples),
        "batchNumber": "1",
    }


def make_output_blob_name(output_prefix: str, filter_col: str, filter_values: list[str]) -> str:
    """Generate output blob path: {output_prefix}/{filter_col}_{id1_id2...}.parquet"""
    prefix = output_prefix.rstrip("/")
    ids_part = "_".join(filter_values)
    return f"{prefix}/{filter_col}_{ids_part}.parquet"
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest tests/test_collector.py -v
```
Expected: All 14 tests PASS

- [ ] **Step 5: Commit**

```bash
git add parquet_transform/collector.py tests/test_collector.py
git commit -m "feat: add collector pure logic (filter, metadata, blob naming)"
```

---

### Task 2: Add `DataCollectorWorker` to `gui/workers.py`

**Files:**
- Modify: `gui/workers.py` (append at end of file only — do not touch existing code)
- Test: `tests/test_collector_worker.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_collector_worker.py
import io
import sys
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from unittest.mock import MagicMock, patch

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


def _run_worker(worker):
    """Run worker synchronously (bypass QThread.start) and collect finished signal."""
    results = {}
    worker.finished.connect(lambda d: results.update(d))
    worker.run()
    return results


def test_worker_combines_rows_from_multiple_blobs():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet", "prefix/f2.parquet"]
    mock_client.download_bytes.side_effect = [
        _make_parquet_bytes("uid1", "dev1", 2),
        _make_parquet_bytes("uid1", "dev1", 3),
    ]

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        results = _run_worker(worker)

    assert results["rowCount"] == 5
    assert mock_client.upload_bytes.call_count == 1


def test_worker_output_blob_name_is_correct():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        _run_worker(worker)

    call_blob_name = mock_client.upload_bytes.call_args[0][0]
    assert call_blob_name == "out/SenderUid_uid1.parquet"


def test_worker_filters_only_matching_rows():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet"]
    # File contains uid1 (2 rows) and uid2 (3 rows)
    table = pa.concat_tables([
        pq.read_table(io.BytesIO(_make_parquet_bytes("uid1", "dev1", 2))),
        pq.read_table(io.BytesIO(_make_parquet_bytes("uid2", "dev1", 3))),
    ])
    buf = io.BytesIO()
    pq.write_table(table, buf)
    mock_client.download_bytes.return_value = buf.getvalue()

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        results = _run_worker(worker)

    assert results["rowCount"] == 2


def test_worker_emits_cancelled_when_cancel_called():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet", "prefix/f2.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 1)

    cancelled = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        worker.cancelled.connect(lambda: cancelled.append(True))
        worker.cancel()
        worker.run()

    assert cancelled == [True]
    assert mock_client.upload_bytes.call_count == 0


def test_worker_no_match_skips_upload():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid2", "dev1", 3)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        results = _run_worker(worker)

    assert results["rowCount"] == 0
    assert mock_client.upload_bytes.call_count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest tests/test_collector_worker.py -v
```
Expected: `ImportError: cannot import name 'DataCollectorWorker' from 'gui.workers'`

- [ ] **Step 3: Append `DataCollectorWorker` to end of `gui/workers.py`**

Open `gui/workers.py`, scroll to the very end, and append the following (after the last class/function):

```python
# ---------------------------------------------------------------------------
# Data Collector Worker
# ---------------------------------------------------------------------------

class DataCollectorWorker(QThread):
    """
    Downloads all Parquet blobs under source_prefix from Azure, filters rows
    by filter_col matching any of filter_values, combines all matches into one
    table, recalculates metadata, and uploads a single output file.
    """

    listing_complete = pyqtSignal(int)       # total blob count found
    progress = pyqtSignal(int, int, str)     # (completed, total, blob_name)
    file_error = pyqtSignal(str, str)        # (blob_name, error_message)
    finished = pyqtSignal(dict)              # {"rowCount": int, "outputBlob": str} or {"rowCount": 0}
    cancelled = pyqtSignal()
    log_message = pyqtSignal(str)

    def __init__(
        self,
        connection_string: str,
        container: str,
        source_prefix: str,
        output_prefix: str,
        filter_col: str,
        filter_values: list[str],
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._conn = connection_string
        self._container_name = container
        self._source_prefix = source_prefix
        self._output_prefix = output_prefix
        self._filter_col = filter_col
        self._filter_values = filter_values
        self._cancel_event = threading.Event()

    def cancel(self) -> None:
        self._cancel_event.set()

    def run(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq
        from parquet_transform.collector import (
            filter_table_by_ids,
            build_metadata,
            make_output_blob_name,
        )

        client = BlobStorageClient(self._conn, self._container_name)
        try:
            blobs = client.list_blobs(self._source_prefix)
            self.listing_complete.emit(len(blobs))

            chunks: list[pa.Table] = []

            for idx, blob_name in enumerate(blobs):
                if self._cancel_event.is_set():
                    self.cancelled.emit()
                    return
                try:
                    raw = client.download_bytes(blob_name, timeout=DOWNLOAD_TIMEOUT_S)
                    table = pq.read_table(io.BytesIO(raw))
                    filtered = filter_table_by_ids(table, self._filter_col, self._filter_values)
                    if filtered.num_rows > 0:
                        chunks.append(filtered)
                except Exception as exc:
                    msg, _, _ = _summarize_exception(exc)
                    self.file_error.emit(blob_name, msg)
                self.progress.emit(idx + 1, len(blobs), blob_name)

            if self._cancel_event.is_set():
                self.cancelled.emit()
                return

            if not chunks:
                self.log_message.emit(
                    f"Data-Collector: no rows matched "
                    f"{self._filter_col} {self._filter_values}"
                )
                self.finished.emit({"rowCount": 0})
                return

            merged = pa.concat_tables(chunks)
            meta = build_metadata(merged)

            # Preserve existing metadata keys, overwrite recalculated ones
            existing = merged.schema.metadata or {}
            decoded = {
                (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                for k, v in existing.items()
            }
            decoded.update(meta)
            merged = merged.replace_schema_metadata(decoded)

            out_buf = io.BytesIO()
            pq.write_table(merged, out_buf, compression="zstd", compression_level=3)

            out_name = make_output_blob_name(
                self._output_prefix, self._filter_col, self._filter_values
            )
            client.upload_bytes(out_name, out_buf.getvalue(), overwrite=True,
                                timeout=UPLOAD_TIMEOUT_S)
            self.log_message.emit(
                f"Data-Collector: uploaded {out_name} ({merged.num_rows} rows)"
            )
            self.finished.emit({"rowCount": merged.num_rows, "outputBlob": out_name})

        finally:
            client.close()
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest tests/test_collector_worker.py -v
```
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add gui/workers.py tests/test_collector_worker.py
git commit -m "feat: add DataCollectorWorker"
```

---

### Task 3: Create `gui/collector_panel.py` — self-contained Data-Collector UI

**Files:**
- Create: `gui/collector_panel.py`

- [ ] **Step 1: Create `gui/collector_panel.py`**

```python
"""Self-contained Data-Collector panel widget."""
from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QTextCharFormat, QTextCursor
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

from gui.workers import DataCollectorWorker


class CollectorPanel(QWidget):
    """Full Data-Collector UI — runs independently of the Schema Transformer tab."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._worker: DataCollectorWorker | None = None

        root = QVBoxLayout(self)
        root.setSpacing(8)
        root.setContentsMargins(12, 12, 12, 12)

        root.addWidget(self._build_connection_group())
        root.addWidget(self._build_filter_group())
        root.addWidget(self._build_action_row())
        root.addWidget(self._build_log_group())

    # ------------------------------------------------------------------
    # Panel builders
    # ------------------------------------------------------------------

    def _build_connection_group(self) -> QGroupBox:
        box = QGroupBox("Azure Connection")
        layout = QHBoxLayout(box)

        layout.addWidget(QLabel("Connection string:"))
        self._conn_edit = QLineEdit()
        self._conn_edit.setPlaceholderText(
            "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=..."
        )
        self._conn_edit.setEchoMode(QLineEdit.EchoMode.Password)
        layout.addWidget(self._conn_edit, stretch=3)

        layout.addWidget(QLabel("Container:"))
        self._container_edit = QLineEdit()
        self._container_edit.setPlaceholderText("my-container")
        layout.addWidget(self._container_edit, stretch=1)

        return box

    def _build_filter_group(self) -> QGroupBox:
        box = QGroupBox("Collection Settings")
        layout = QVBoxLayout(box)

        # Row 1: filter column + IDs
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Filter by:"))
        self._filter_combo = QComboBox()
        self._filter_combo.addItems(["SenderUid", "DeviceUid"])
        row1.addWidget(self._filter_combo)
        row1.addSpacing(16)
        row1.addWidget(QLabel("IDs (one per line):"))
        self._ids_edit = QPlainTextEdit()
        self._ids_edit.setMaximumHeight(80)
        self._ids_edit.setPlaceholderText("uid_001\nuid_002\nuid_003")
        row1.addWidget(self._ids_edit, stretch=1)
        layout.addLayout(row1)

        # Row 2: source + output prefix
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Source prefix:"))
        self._source_edit = QLineEdit()
        self._source_edit.setPlaceholderText("transformed/data/")
        row2.addWidget(self._source_edit, stretch=1)
        row2.addSpacing(16)
        row2.addWidget(QLabel("Output prefix:"))
        self._output_edit = QLineEdit()
        self._output_edit.setPlaceholderText("collected/")
        row2.addWidget(self._output_edit, stretch=1)
        layout.addLayout(row2)

        return box

    def _build_action_row(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        self._collect_btn = QPushButton("Collect")
        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.setEnabled(False)
        self._progress = QProgressBar()
        self._progress.setVisible(False)

        layout.addWidget(self._collect_btn)
        layout.addWidget(self._cancel_btn)
        layout.addWidget(self._progress, stretch=1)

        self._collect_btn.clicked.connect(self._on_collect)
        self._cancel_btn.clicked.connect(self._on_cancel)

        return widget

    def _build_log_group(self) -> QGroupBox:
        box = QGroupBox("Log")
        layout = QVBoxLayout(box)
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setMinimumHeight(140)
        layout.addWidget(self._log)
        return box

    # ------------------------------------------------------------------
    # Slots
    # ------------------------------------------------------------------

    def _on_collect(self) -> None:
        conn = self._conn_edit.text().strip()
        container = self._container_edit.text().strip()
        if not conn or not container:
            self._log_error("Connection string and container are required.")
            return

        filter_values = [
            line.strip()
            for line in self._ids_edit.toPlainText().splitlines()
            if line.strip()
        ]
        if not filter_values:
            self._log_error("Enter at least one ID.")
            return

        output_prefix = self._output_edit.text().strip()
        if not output_prefix:
            self._log_error("Output prefix is required.")
            return

        self._collect_btn.setEnabled(False)
        self._cancel_btn.setEnabled(True)
        self._progress.setValue(0)
        self._progress.setVisible(True)
        self._log_info(
            f"Starting collection: {self._filter_combo.currentText()} "
            f"{filter_values} → {output_prefix}"
        )

        self._worker = DataCollectorWorker(
            connection_string=conn,
            container=container,
            source_prefix=self._source_edit.text().strip(),
            output_prefix=output_prefix,
            filter_col=self._filter_combo.currentText(),
            filter_values=filter_values,
        )
        self._worker.listing_complete.connect(self._on_listing_complete)
        self._worker.progress.connect(self._on_progress)
        self._worker.file_error.connect(self._on_file_error)
        self._worker.finished.connect(self._on_finished)
        self._worker.cancelled.connect(self._on_cancelled)
        self._worker.log_message.connect(self._log_info)
        self._worker.start()

    def _on_cancel(self) -> None:
        if self._worker and self._worker.isRunning():
            self._worker.cancel()

    def _on_listing_complete(self, total: int) -> None:
        self._progress.setMaximum(total)
        self._log_info(f"Found {total} Parquet blobs to scan.")

    def _on_progress(self, completed: int, total: int, blob_name: str) -> None:
        self._progress.setValue(completed)

    def _on_file_error(self, blob_name: str, error: str) -> None:
        self._log_error(f"[{blob_name}]: {error}")

    def _on_finished(self, result: dict) -> None:
        self._collect_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)
        self._progress.setVisible(False)
        row_count = result.get("rowCount", 0)
        if row_count == 0:
            self._log_info("Collection complete: no matching rows found.")
        else:
            out_blob = result.get("outputBlob", "")
            self._log_info(
                f"Collection complete: {row_count} rows → {out_blob}"
            )

    def _on_cancelled(self) -> None:
        self._collect_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)
        self._progress.setVisible(False)
        self._log_info("Collection cancelled.")

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log_info(self, text: str) -> None:
        self._append_log(text, color=None)

    def _log_error(self, text: str) -> None:
        self._append_log(text, color=QColor("#cc0000"))

    def _append_log(self, text: str, color: QColor | None) -> None:
        cursor = self._log.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        fmt = QTextCharFormat()
        if color:
            fmt.setForeground(color)
        cursor.setCharFormat(fmt)
        cursor.insertText(text + "\n")
        self._log.setTextCursor(cursor)
        self._log.ensureCursorVisible()
```

- [ ] **Step 2: Verify the file was created correctly**

```
python -c "from gui.collector_panel import CollectorPanel; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add gui/collector_panel.py
git commit -m "feat: add CollectorPanel widget"
```

---

### Task 4: Wrap existing layout in `QTabWidget` in `gui/main_window.py`

**Files:**
- Modify: `gui/main_window.py`

This task adds a `QTabWidget` so that the Schema Transformer lives in Tab 1 and the Data-Collector lives in Tab 2. The only changes to `main_window.py` are:
1. Import `QTabWidget` and `CollectorPanel`
2. Wrap the existing `root = QVBoxLayout(central)` block in a tab widget

- [ ] **Step 1: Add imports to `gui/main_window.py`**

Find the existing import block (around line 17–36). Add `QTabWidget` to the `QWidgets` import list and add the `CollectorPanel` import.

Current (lines 17–36):
```python
from PyQt6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QStatusBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
```

Replace with:
```python
from PyQt6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QStatusBar,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
```

Also add after the existing `from gui.workers import ...` line:
```python
from gui.collector_panel import CollectorPanel
```

- [ ] **Step 2: Wrap existing layout in Tab 1, add Tab 2**

Find in `MainWindow.__init__` (lines 138–153):
```python
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setSpacing(8)
        root.setContentsMargins(12, 12, 12, 12)

        root.addWidget(self._build_connection_group())
        root.addWidget(self._build_schema_group())
        root.addWidget(self._build_output_group())
        root.addWidget(self._build_action_row())

        # System resources panel — lives between action row and log
        self._resources_panel = ResourcesPanel()
        root.addWidget(self._resources_panel)

        root.addWidget(self._build_log_group())
```

Replace with:
```python
        central = QWidget()
        self.setCentralWidget(central)

        tabs = QTabWidget(central)
        outer = QVBoxLayout(central)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(tabs)

        # --- Tab 1: Schema Transformer (existing layout, completely unchanged) ---
        transformer_widget = QWidget()
        root = QVBoxLayout(transformer_widget)
        root.setSpacing(8)
        root.setContentsMargins(12, 12, 12, 12)

        root.addWidget(self._build_connection_group())
        root.addWidget(self._build_schema_group())
        root.addWidget(self._build_output_group())
        root.addWidget(self._build_action_row())

        # System resources panel — lives between action row and log
        self._resources_panel = ResourcesPanel()
        root.addWidget(self._resources_panel)

        root.addWidget(self._build_log_group())

        tabs.addTab(transformer_widget, "Schema Transformer")

        # --- Tab 2: Data Collector ---
        tabs.addTab(CollectorPanel(), "Data Collector")
```

- [ ] **Step 3: Run the application and verify both tabs work**

```
python main.py
```
Expected:
- App opens with two tabs: "Schema Transformer" and "Data Collector"
- "Schema Transformer" tab looks and behaves exactly as before
- "Data Collector" tab shows its own connection fields, filter settings, Collect/Cancel buttons, and log

- [ ] **Step 4: Run full test suite to confirm no regressions**

```
pytest tests/ -v
```
Expected: All tests PASS (including pre-existing tests for checkpoint, processor, scaler, storage, transforms, worker_scaling)

- [ ] **Step 5: Commit**

```bash
git add gui/main_window.py
git commit -m "feat: add Data Collector tab; wrap existing UI in QTabWidget"
```

---

## Self-Review

### 1. Spec Coverage
| Requirement | Covered by |
|-------------|-----------|
| Filter by SenderUid or DeviceUid | `filter_table_by_ids()` + `_filter_combo` in CollectorPanel |
| Multiple IDs simultaneously | `filter_values: list[str]` throughout |
| Recursive Azure scan | `list_blobs(source_prefix)` — Azure's `name_starts_with` inherently recurses across all "subdirectory" prefixes |
| Exactly 1 output file per run | Single `pa.concat_tables(chunks)` → single `upload_bytes()` call |
| Auto-generated filename from input | `make_output_blob_name()` |
| Metadata recalculated | `build_metadata()` recalculates `recordCount`, `dateFrom`, `dateTo`, `deviceIds`, `batchNumber` |
| Azure Blob only | `BlobStorageClient` used throughout; no local file I/O |
| Strictly separated from Schema Transformer | `CollectorPanel` is a standalone widget; `main_window.py` only adds it as Tab 2 |
| Existing Schema Transformer untouched | Only `root = QVBoxLayout(central)` block is wrapped — zero logic changes |

### 2. Placeholder Scan
- No TBDs, no "similar to Task N", no vague "add error handling"
- All steps include complete code

### 3. Type Consistency
- `filter_table_by_ids(table, filter_col, filter_values: list[str])` — used identically in collector.py, worker, and tests
- `build_metadata(table) -> dict[str, str]` — same signature in implementation and tests
- `make_output_blob_name(output_prefix, filter_col, filter_values: list[str])` — same signature throughout
- `DataCollectorWorker.finished` emits `dict` with keys `"rowCount"` and `"outputBlob"` — matched in `CollectorPanel._on_finished()`
