import sys

import pytest
from PyQt6.QtGui import QCloseEvent
from PyQt6.QtWidgets import QApplication

from gui.collector_panel import CollectorPanel

_app = QApplication.instance() or QApplication(sys.argv)


class _DummyWorker:
    def __init__(self, running: bool = True, wait_result: bool = True) -> None:
        self._running = running
        self._wait_result = wait_result
        self.deleted = False
        self.cancel_calls = 0

    def isRunning(self) -> bool:
        return self._running

    def deleteLater(self) -> None:
        self.deleted = True

    def wait(self, _timeout: int) -> bool:
        return self._wait_result

    def cancel(self) -> None:
        self.cancel_calls += 1

    def stop(self) -> None:
        self._running = False


def test_worker_cleanup_waits_for_thread_to_stop():
    panel = CollectorPanel()
    worker = _DummyWorker(running=True)
    panel._worker = worker  # type: ignore[attr-defined]

    panel._request_worker_cleanup()
    assert panel._worker is worker  # still present while running
    assert not worker.deleted

    worker.stop()
    panel._request_worker_cleanup()

    assert panel._worker is None
    assert worker.deleted


def test_close_event_blocks_when_worker_is_still_running():
    panel = CollectorPanel()
    worker = _DummyWorker(running=True, wait_result=False)
    panel._worker = worker  # type: ignore[attr-defined]

    event = QCloseEvent()
    panel.closeEvent(event)

    assert not event.isAccepted()
    assert worker.cancel_calls == 1
    assert panel._worker is worker
