"""
Background QThread workers for long-running Azure + Parquet operations.

All heavy I/O runs off the main thread so the UI stays responsive.
Workers communicate back to the main thread via Qt signals.
"""
from __future__ import annotations

import io
import queue
import threading
import traceback
from time import perf_counter
from typing import Any

import pyarrow.parquet as pq
from PyQt6.QtCore import QThread, pyqtSignal

from parquet_transform.processor import ColumnConfig, apply_transforms, compute_output_name
from parquet_transform.storage import BlobStorageClient


class SchemaLoaderWorker(QThread):
    """
    Connects to Azure, counts Parquet blobs, and reads the schema from
    the first file (footer only - fast).
    """

    schema_loaded = pyqtSignal(object, int)  # (pa.Schema, file_count)
    error = pyqtSignal(str)

    def __init__(
        self,
        connection_string: str,
        container: str,
        prefix: str,
        parent=None,
    ):
        super().__init__(parent)
        self._connection_string = connection_string
        self._container = container
        self._prefix = prefix

    def run(self) -> None:
        try:
            client = BlobStorageClient(self._connection_string, self._container)
            blob_names = client.list_blobs(self._prefix)
            if not blob_names:
                self.error.emit(
                    f"No .parquet files found under prefix '{self._prefix}'."
                )
                return
            schema = client.read_schema(blob_names[0])
            self.schema_loaded.emit(schema, len(blob_names))
        except Exception:
            self.error.emit(traceback.format_exc())


class TransformWorker(QThread):
    """
    Processes all Parquet blobs: download -> transform -> upload.
    Supports cancellation, dry-run mode, and multi-threaded execution.
    """

    # (completed_files, total_files, blob_name, duration_ms, worker_id)
    progress = pyqtSignal(int, int, str, float, int)
    # (blob_name, error_traceback)
    file_error = pyqtSignal(str, str)
    # (files_processed, files_failed, total_seconds, average_seconds)
    finished = pyqtSignal(int, int, float, float)
    cancelled = pyqtSignal()

    def __init__(
        self,
        connection_string: str,
        container: str,
        prefix: str,
        col_configs: list[ColumnConfig],
        output_prefix: str | None,
        dry_run: bool = False,
        worker_count: int = 1,
        max_attempts: int = 1,
        parent=None,
    ):
        super().__init__(parent)
        self._connection_string = connection_string
        self._container = container
        self._prefix = prefix
        self._col_configs = col_configs
        self._output_prefix = output_prefix
        self._dry_run = dry_run
        self._worker_count = max(1, worker_count)
        self._max_attempts = max(1, max_attempts)
        self._cancel_event = threading.Event()
        self._file_stats: list[dict[str, Any]] = []

    def _log_retry(self, blob_name: str, attempt: int, error: str) -> None:
        self.file_error.emit(
            blob_name,
            (
                f"Attempt {attempt} failed (will retry):\n"
                f"{error.strip()}"
            ),
        )

    def _log_final_failure(self, blob_name: str, attempts: int, error: str) -> None:
        self.file_error.emit(
            blob_name,
            (
                f"Final failure after {attempts} attempt(s):\n"
                f"{error.strip()}"
            ),
        )

    def cancel(self) -> None:
        """Request cancellation after the current file finishes."""
        self._cancel_event.set()

    def run(self) -> None:
        try:
            listing_client = BlobStorageClient(self._connection_string, self._container)
            blob_names = listing_client.list_blobs(self._prefix)
        except Exception:
            self.file_error.emit("(connection)", traceback.format_exc())
            self.finished.emit(0, 1, 0.0, 0.0)
            return

        total = len(blob_names)
        if total == 0:
            self.finished.emit(0, 0, 0.0, 0.0)
            return

        worker_total = min(self._worker_count, total)
        task_queue: queue.Queue[Any] = queue.Queue()
        sentinel = object()
        for blob_name in blob_names:
            task_queue.put(blob_name)
        for _ in range(worker_total):
            task_queue.put(sentinel)

        stats_lock = threading.Lock()
        processed = 0
        failed = 0
        completed = 0
        total_duration_ms = 0.0
        start_time = perf_counter()

        def worker_loop(worker_id: int) -> None:
            nonlocal processed, failed, completed, total_duration_ms
            worker_client = BlobStorageClient(self._connection_string, self._container)
            while True:
                item = task_queue.get()
                if item is sentinel:
                    task_queue.task_done()
                    break

                blob_name = str(item)

                if self._cancel_event.is_set():
                    task_queue.task_done()
                    continue

                attempts = 0
                success = False
                last_error = ""
                file_start = perf_counter()

                while attempts < self._max_attempts and not success:
                    attempts += 1
                    try:
                        raw = worker_client.download_bytes(blob_name)
                        buf = io.BytesIO(raw)
                        table = pq.read_table(buf)
                        table = apply_transforms(table, self._col_configs)

                        output_name = compute_output_name(
                            blob_name, self._prefix, self._output_prefix
                        )

                        if not self._dry_run:
                            out_buf = io.BytesIO()
                            pq.write_table(table, out_buf)
                            worker_client.upload_bytes(
                                output_name, out_buf.getvalue(), overwrite=True
                            )

                        success = True
                    except Exception:
                        last_error = traceback.format_exc()
                        if (
                            attempts < self._max_attempts
                            and not self._cancel_event.is_set()
                        ):
                            self._log_retry(blob_name, attempts, last_error)
                        if self._cancel_event.is_set():
                            break

                duration_ms = (perf_counter() - file_start) * 1000.0

                with stats_lock:
                    if success:
                        processed += 1
                    else:
                        failed += 1
                    completed += 1
                    completed_so_far = completed
                    total_duration_ms += duration_ms
                    self._file_stats.append(
                        {
                            "blob": blob_name,
                            "duration_ms": duration_ms,
                            "worker_id": worker_id,
                            "attempts": attempts,
                            "success": success,
                        }
                    )

                if not success and last_error:
                    self._log_final_failure(blob_name, attempts, last_error)

                self.progress.emit(
                    completed_so_far,
                    total,
                    blob_name,
                    duration_ms,
                    worker_id,
                )

                task_queue.task_done()

        threads = [
            threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
            for i in range(worker_total)
        ]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        if self._cancel_event.is_set():
            self.cancelled.emit()
            return

        total_seconds = perf_counter() - start_time
        avg_seconds = (
            (total_duration_ms / max(completed, 1)) / 1000.0
            if completed
            else 0.0
        )
        self.finished.emit(processed, failed, total_seconds, avg_seconds)
