"""Tests for parquet_transform.processor — apply_transforms and compute_output_name."""
import uuid

import pyarrow as pa
import pytest

from parquet_transform.processor import ColumnConfig, apply_transforms, compute_output_name


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uuid_binary_table(col_name: str = "id") -> pa.Table:
    raw = uuid.UUID("12345678-1234-5678-1234-567812345678").bytes
    return pa.table({col_name: pa.array([raw], type=pa.binary(16))})


def _timestamp_ns_table(col_name: str = "ts") -> pa.Table:
    return pa.table({col_name: pa.array([0], type=pa.timestamp("ns"))})


# ---------------------------------------------------------------------------
# apply_transforms
# ---------------------------------------------------------------------------

class TestApplyTransforms:
    def test_transforms_column_in_place(self):
        table = _uuid_binary_table()
        config = [ColumnConfig(name="id", transform="binary16_to_uuid")]
        result = apply_transforms(table, config)
        assert result.schema.field("id").type == pa.string()

    def test_transformed_value_is_correct(self):
        table = _uuid_binary_table()
        config = [ColumnConfig(name="id", transform="binary16_to_uuid")]
        result = apply_transforms(table, config)
        assert result["id"][0].as_py() == "12345678-1234-5678-1234-567812345678"

    def test_untouched_columns_are_preserved(self):
        table = pa.table({
            "id": pa.array([uuid.UUID("12345678-1234-5678-1234-567812345678").bytes], type=pa.binary(16)),
            "value": pa.array([42], type=pa.int32()),
        })
        config = [ColumnConfig(name="id", transform="binary16_to_uuid")]
        result = apply_transforms(table, config)
        assert result["value"][0].as_py() == 42
        assert result.schema.field("value").type == pa.int32()

    def test_column_order_is_preserved(self):
        table = pa.table({
            "a": pa.array([1], type=pa.int32()),
            "id": pa.array([uuid.UUID("12345678-1234-5678-1234-567812345678").bytes], type=pa.binary(16)),
            "z": pa.array([2], type=pa.int32()),
        })
        config = [ColumnConfig(name="id", transform="binary16_to_uuid")]
        result = apply_transforms(table, config)
        assert result.schema.names == ["a", "id", "z"]

    def test_missing_column_is_skipped(self):
        table = _uuid_binary_table()
        config = [ColumnConfig(name="nonexistent", transform="binary16_to_uuid")]
        result = apply_transforms(table, config)
        # table unchanged
        assert result.schema.names == ["id"]
        assert result.schema.field("id").type == pa.binary(16)

    def test_missing_column_calls_log_fn(self):
        table = _uuid_binary_table()
        config = [ColumnConfig(name="ghost", transform="binary16_to_uuid")]
        log_messages = []
        apply_transforms(table, config, log_fn=log_messages.append)
        assert any("ghost" in msg for msg in log_messages)

    def test_no_log_fn_does_not_raise(self):
        table = _uuid_binary_table()
        config = [ColumnConfig(name="nonexistent", transform="binary16_to_uuid")]
        apply_transforms(table, config, log_fn=None)  # must not raise

    def test_multiple_transforms_applied(self):
        table = pa.table({
            "id": pa.array([uuid.UUID("12345678-1234-5678-1234-567812345678").bytes], type=pa.binary(16)),
            "ts": pa.array([0], type=pa.timestamp("ns")),
        })
        config = [
            ColumnConfig(name="id", transform="binary16_to_uuid"),
            ColumnConfig(name="ts", transform="timestamp_ns_to_ms_utc"),
        ]
        result = apply_transforms(table, config)
        assert result.schema.field("id").type == pa.string()
        assert result.schema.field("ts").type == pa.timestamp("ms", tz="UTC")

    def test_empty_config_list_returns_table_unchanged(self):
        table = _uuid_binary_table()
        result = apply_transforms(table, [])
        assert result.equals(table)

    def test_column_config_params_passed_to_transform(self):
        """Params dict is forwarded; built-in transforms ignore it, but it must not raise."""
        table = _uuid_binary_table()
        config = [ColumnConfig(name="id", transform="binary16_to_uuid", params={"extra": 1})]
        result = apply_transforms(table, config)
        assert result.schema.field("id").type == pa.string()


# ---------------------------------------------------------------------------
# compute_output_name
# ---------------------------------------------------------------------------

class TestComputeOutputName:
    def test_no_output_prefix_returns_blob_name_unchanged(self):
        result = compute_output_name("a/b/c.parquet", "a/", output_prefix=None)
        assert result == "a/b/c.parquet"

    def test_replaces_input_prefix_with_output_prefix(self):
        result = compute_output_name("input/2024/data.parquet", "input/", "output/")
        assert result == "output/2024/data.parquet"

    def test_trailing_slash_on_output_prefix_not_duplicated(self):
        result = compute_output_name("in/file.parquet", "in/", "out/")
        assert result == "out/file.parquet"
        assert "//" not in result

    def test_blob_not_starting_with_prefix_raises_value_error(self):
        with pytest.raises(ValueError, match="does not start with input prefix"):
            compute_output_name("other/file.parquet", "input/", "output/")

    def test_empty_input_prefix_prepends_output_prefix(self):
        result = compute_output_name("dir/file.parquet", "", "out/")
        assert result == "out/dir/file.parquet"

    def test_blob_at_prefix_root(self):
        result = compute_output_name("input/file.parquet", "input/", "output/")
        assert result == "output/file.parquet"

    def test_no_output_prefix_in_place_preserves_nested_path(self):
        blob = "some/deep/nested/path.parquet"
        result = compute_output_name(blob, "some/", output_prefix=None)
        assert result == blob
