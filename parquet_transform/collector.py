from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

_REQUIRED_COLS = {"TsCreate", "SenderUid", "DeviceUid", "MessageVersion"}


def filter_table_by_ids(table: pa.Table, filter_col: str, filter_values: list[str]) -> pa.Table:
    """Return rows where filter_col matches any value in filter_values."""
    if filter_col not in table.schema.names:
        raise ValueError(
            f"filter_col '{filter_col}' not found in table. "
            f"Available columns: {table.schema.names}"
        )
    value_set = pa.array(filter_values, type=pa.string())
    mask = pc.is_in(table.column(filter_col), value_set=value_set)
    return table.filter(mask)


def build_metadata(table: pa.Table) -> dict[str, str]:
    """Recalculate Parquet file metadata from table contents."""
    missing = _REQUIRED_COLS - set(table.schema.names)
    if missing:
        raise ValueError(f"build_metadata: missing required columns: {sorted(missing)}")
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
    if not filter_values:
        raise ValueError("filter_values must not be empty")
    prefix = output_prefix.rstrip("/")
    ids_part = "_".join(filter_values)
    return f"{prefix}/{filter_col}_{ids_part}.parquet"
