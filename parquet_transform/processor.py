"""
Core processing logic: apply column transforms to a PyArrow Table.

This module is UI-agnostic and can be driven by the GUI workers or a plain
script.  The ``process_blob`` function that previously combined download /
transform / upload has been removed — that logic now lives in
``gui/workers.py`` so that progress callbacks, cancellation, and retry are
handled uniformly at the worker level.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pyarrow as pa

from parquet_transform import transforms as tr


@dataclass
class ColumnConfig:
    """Specifies a single column transformation to apply."""
    name: str
    transform: str  # registry key from transforms module
    params: dict = field(default_factory=dict)


def apply_transforms(
    table: pa.Table,
    col_configs: list[ColumnConfig],
    log_fn: Callable[[str], None] | None = None,
) -> pa.Table:
    """
    Apply a list of column transformations to a PyArrow Table.

    Columns not present in the table are skipped with a warning.
    All other columns and their order are preserved unchanged.
    """
    for cfg in col_configs:
        if cfg.name not in table.schema.names:
            if log_fn:
                log_fn(f"  Warning: column '{cfg.name}' not found — skipping")
            continue
        transform_fn = tr.get(cfg.transform)
        col_idx = table.schema.get_field_index(cfg.name)
        new_col = transform_fn(table.column(cfg.name), cfg.params)
        table = table.set_column(col_idx, cfg.name, new_col)
    return table


def compute_output_name(
    blob_name: str,
    input_prefix: str,
    output_prefix: str | None,
) -> str:
    """
    Compute the destination blob name.

    If *output_prefix* is None, returns *blob_name* unchanged (in-place).
    Otherwise, replaces the leading *input_prefix* with *output_prefix*,
    preserving the relative path beneath the prefix.

    Raises:
        ValueError: if *output_prefix* is set but *blob_name* does not start
            with *input_prefix* (prevents silently writing to wrong paths).
    """
    if output_prefix is None:
        return blob_name
    if input_prefix and not blob_name.startswith(input_prefix):
        raise ValueError(
            f"Blob '{blob_name}' does not start with input prefix "
            f"'{input_prefix}'. Cannot compute output path."
        )
    relative = blob_name[len(input_prefix):]
    return output_prefix.rstrip("/") + "/" + relative.lstrip("/")
