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


def _fill_scaler_with_good_uploads(scaler: AdaptiveScaler, count: int, bytes_: int = 5_000_000, seconds: float = 5.0) -> None:
    for _ in range(count):
        scaler.record_upload(bytes_, seconds, True)


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


def test_scale_down_triggers_on_combined_error_and_throughput_drop():
    scaler = _make_scaler(
        min_samples=10,
        window_size=100,
        down_error_rate=0.15,
        down_throughput_drop=0.30,
    )
    # Fill with good uploads first (sets the median baseline)
    _fill_scaler_with_good_uploads(scaler, 35, bytes_=5_000_000, seconds=5.0)
    # Add failing records (16% error rate: 13 out of 81 total)
    for _ in range(13):
        scaler.record_upload(0, 0.0, False)
    # Add slow uploads (throughput < 70% of baseline)
    for _ in range(33):
        scaler.record_upload(1_000_000, 5.0, True)  # 200 KB/s vs baseline 1 MB/s
    new_count, reason = scaler.should_scale(4, 10_000_000)
    assert new_count == 3
    assert "error" in reason.lower() or "throughput" in reason.lower()


def test_scale_down_does_not_trigger_on_errors_alone():
    scaler = _make_scaler(min_samples=10, window_size=100, down_error_rate=0.15, down_throughput_drop=0.30, configured_max_workers=4)
    _fill_scaler_with_good_uploads(scaler, 70, bytes_=5_000_000, seconds=5.0)
    for _ in range(20):
        scaler.record_upload(0, 0.0, False)  # 22% errors but throughput stable
    new_count, _ = scaler.should_scale(4, 10_000_000)
    assert new_count == 4  # no change — throughput drop condition not met


def test_scale_down_does_not_trigger_on_throughput_drop_alone():
    scaler = _make_scaler(min_samples=10, window_size=100, down_error_rate=0.15, down_throughput_drop=0.30, configured_max_workers=4)
    _fill_scaler_with_good_uploads(scaler, 40, bytes_=5_000_000, seconds=5.0)
    # Add slow uploads — but no errors
    for _ in range(60):
        scaler.record_upload(500_000, 5.0, True)  # slow but all succeed
    new_count, _ = scaler.should_scale(4, 10_000_000)
    assert new_count == 4  # no change — error rate condition not met


def test_scale_down_never_goes_below_1():
    scaler = _make_scaler(min_samples=10, window_size=100,
                          down_error_rate=0.15, down_throughput_drop=0.30)
    _fill_scaler_with_good_uploads(scaler, 35, bytes_=5_000_000, seconds=5.0)
    for _ in range(13):                             # 13/81 = 16%, above threshold
        scaler.record_upload(0, 0.0, False)
    for _ in range(33):
        scaler.record_upload(100_000, 5.0, True)    # 20 KB/s — well below 70% of baseline
    new_count, _ = scaler.should_scale(1, 10_000_000)
    assert new_count == 1


def test_first_scale_up_is_proportional():
    # 4 workers, each doing 5 MB in 5s = 1 MB/s per connection = 4 MB/s total.
    # headroom = floor(4_000_000 * 300 / 10_000_000 * 0.7) - 4 = floor(84) - 4 = 80
    # But capped at configured_max_workers=16, so new_count = 16.
    scaler = _make_scaler(min_samples=10, configured_max_workers=16)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    new_count, reason = scaler.should_scale(4, 10_000_000)
    assert new_count == 16  # capped at configured_max_workers
    assert "headroom" in reason.lower() or "kb/s" in reason.lower()


def test_first_scale_up_sets_first_scale_done():
    scaler = _make_scaler(min_samples=10, configured_max_workers=16)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    scaler.should_scale(4, 10_000_000)  # first call
    # Second call: same conditions but should only do incremental now
    _fill_scaler_with_good_uploads(scaler, 10)
    new_count_2, _ = scaler.should_scale(16, 10_000_000)
    # With 16 workers and same bandwidth, headroom is near 0 — no scale up
    assert new_count_2 == 16


def test_subsequent_scale_up_requires_two_stable_checks():
    # 2 workers, 2 MB/s total → headroom = floor(2_000_000*300/5_000_000*0.7)-2 = floor(84)-2 = 82 → up_max_step=4
    scaler = _make_scaler(min_samples=10, up_confirm_checks=2, up_max_step=4, configured_max_workers=32)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    # First check is proportional — consume it
    scaler.should_scale(2, 5_000_000)  # first call, proportional
    _fill_scaler_with_good_uploads(scaler, 10)
    # Now in incremental mode. Check 1 of 2:
    new_count_1, _ = scaler.should_scale(2, 5_000_000)
    assert new_count_1 == 2  # not yet — only 1 of 2 stable checks
    _fill_scaler_with_good_uploads(scaler, 10)
    # Check 2 of 2:
    new_count_2, reason = scaler.should_scale(2, 5_000_000)
    assert new_count_2 == 6  # 2 + min(headroom, 4) = 2 + 4 = 6
    assert "headroom" in reason.lower() or "kb/s" in reason.lower()


def test_scale_up_capped_at_configured_max():
    scaler = _make_scaler(min_samples=10, up_confirm_checks=1, up_max_step=4, configured_max_workers=5)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    # First check is proportional:
    new_count, _ = scaler.should_scale(4, 10_000_000)
    assert new_count == 5  # capped at configured_max_workers


def test_no_scale_up_when_no_headroom():
    # Workers already match bandwidth: 1 worker, 1 MB/s, p95=300MB → headroom≈0
    scaler = _make_scaler(min_samples=10, up_min_headroom=2, configured_max_workers=16)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=1_000_000, seconds=1.0)
    # total_bw = 1_000_000/1.0 * 1 = 1_000_000 B/s
    # headroom = floor(1_000_000 * 300 / 300_000_000 * 0.7) - 1 = floor(0.7) - 1 = 0 - 1 = -1
    new_count, _ = scaler.should_scale(1, 300_000_000)
    assert new_count == 1
