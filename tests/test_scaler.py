import pytest
from parquet_transform.scaler import AdaptiveScaler


def _make_scaler(**overrides) -> AdaptiveScaler:
    defaults = dict(
        window_size=100,
        min_samples=50,
        upload_timeout_s=300,
        down_error_rate=0.15,
        down_throughput_drop=0.30,
        up_min_headroom=2,
        up_confirm_checks=2,
        up_max_step=4,
        configured_max_workers=16,
    )
    defaults.update(overrides)
    return AdaptiveScaler(**defaults)


def test_record_upload_below_min_samples_returns_current():
    scaler = _make_scaler(min_samples=50)
    for _ in range(49):
        scaler.record_upload(1_000_000, 1.0, True)
    assert scaler.should_scale(4, 10_000_000) == (4, "")


def test_record_upload_zero_samples_returns_current():
    scaler = _make_scaler()
    assert scaler.should_scale(4, 10_000_000) == (4, "")


def test_record_upload_zero_p95_returns_current():
    scaler = _make_scaler(min_samples=1)
    scaler.record_upload(1_000_000, 1.0, True)
    assert scaler.should_scale(4, 0) == (4, "")
