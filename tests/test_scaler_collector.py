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
