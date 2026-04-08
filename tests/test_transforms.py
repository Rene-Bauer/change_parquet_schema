"""Tests for parquet_transform.transforms — registry and built-in transforms."""
import uuid

import pyarrow as pa
import pytest

import parquet_transform.transforms as tr


# ---------------------------------------------------------------------------
# Registry mechanics
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_list_transforms_returns_name_and_display_name(self):
        transforms = tr.list_transforms()
        assert isinstance(transforms, list)
        for name, display_name in transforms:
            assert isinstance(name, str) and name
            assert isinstance(display_name, str) and display_name

    def test_get_returns_callable(self):
        name, _ = tr.list_transforms()[0]
        fn = tr.get(name)
        assert callable(fn)

    def test_get_unknown_raises_key_error(self):
        with pytest.raises(KeyError, match="unknown_transform_xyz"):
            tr.get("unknown_transform_xyz")

    def test_register_decorator_adds_to_registry(self):
        @tr.register("_test_transform", "Test Transform", applicable_types=["int64"])
        def _dummy(array, params):
            return array

        assert "_test_transform" in dict(tr.list_transforms())
        fn = tr.get("_test_transform")
        assert fn is _dummy

    def test_register_with_no_applicable_types(self):
        @tr.register("_test_no_types", "Test No Types")
        def _dummy2(array, params):
            return array

        assert "_test_no_types" in dict(tr.list_transforms())


# ---------------------------------------------------------------------------
# get_suggested
# ---------------------------------------------------------------------------

class TestGetSuggested:
    def test_suggests_binary16_to_uuid_for_fixed_size_binary_16(self):
        arrow_type = pa.binary(16)
        result = tr.get_suggested(arrow_type)
        assert result == "binary16_to_uuid"

    def test_suggests_timestamp_transform_for_ns_timestamp(self):
        arrow_type = pa.timestamp("ns")
        result = tr.get_suggested(arrow_type)
        assert result == "timestamp_ns_to_ms_utc"

    def test_returns_none_for_unrecognised_type(self):
        result = tr.get_suggested(pa.int32())
        assert result is None

    def test_returns_none_for_utf8_type(self):
        result = tr.get_suggested(pa.utf8())
        assert result is None


# ---------------------------------------------------------------------------
# binary16_to_uuid
# ---------------------------------------------------------------------------

class TestBinary16ToUuid:
    def _make_array(self, values: list) -> pa.Array:
        return pa.array(values, type=pa.binary(16))

    def test_converts_bytes_to_uuid_string(self):
        raw = uuid.UUID("12345678-1234-5678-1234-567812345678").bytes
        result = tr.binary16_to_uuid(self._make_array([raw]), params={})
        assert result[0].as_py() == "12345678-1234-5678-1234-567812345678"

    def test_output_type_is_utf8_string(self):
        raw = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").bytes
        result = tr.binary16_to_uuid(self._make_array([raw]), params={})
        assert result.type == pa.string()

    def test_none_values_are_preserved(self):
        result = tr.binary16_to_uuid(pa.array([None], type=pa.binary(16)), params={})
        assert result[0].as_py() is None

    def test_mixed_none_and_values(self):
        raw = uuid.UUID("00000000-0000-0000-0000-000000000001").bytes
        result = tr.binary16_to_uuid(pa.array([None, raw], type=pa.binary(16)), params={})
        assert result[0].as_py() is None
        assert result[1].as_py() == "00000000-0000-0000-0000-000000000001"

    def test_all_null_array(self):
        result = tr.binary16_to_uuid(pa.array([None, None], type=pa.binary(16)), params={})
        assert all(v.as_py() is None for v in result)

    def test_multiple_values_converted_correctly(self):
        ids = [
            "12345678-1234-5678-1234-567812345678",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
        raw_list = [uuid.UUID(u).bytes for u in ids]
        result = tr.binary16_to_uuid(self._make_array(raw_list), params={})
        assert [v.as_py() for v in result] == ids


# ---------------------------------------------------------------------------
# timestamp_ns_to_ms_utc
# ---------------------------------------------------------------------------

class TestTimestampNsToMsUtc:
    def test_output_type_is_timestamp_ms_utc(self):
        array = pa.array([0], type=pa.timestamp("ns"))
        result = tr.timestamp_ns_to_ms_utc(array, params={})
        assert result.type == pa.timestamp("ms", tz="UTC")

    def test_nanoseconds_are_truncated_to_milliseconds(self):
        # 1_000_500 ns → 1 ms (truncated, not rounded)
        array = pa.array([1_000_500], type=pa.timestamp("ns"))
        result = tr.timestamp_ns_to_ms_utc(array, params={})
        assert result[0].as_py().microsecond == 1000  # 1 ms = 1000 µs

    def test_zero_epoch_stays_zero(self):
        array = pa.array([0], type=pa.timestamp("ns"))
        result = tr.timestamp_ns_to_ms_utc(array, params={})
        assert result[0].as_py().timestamp() == 0.0

    def test_null_values_preserved(self):
        array = pa.array([None], type=pa.timestamp("ns"))
        result = tr.timestamp_ns_to_ms_utc(array, params={})
        assert result[0].as_py() is None
