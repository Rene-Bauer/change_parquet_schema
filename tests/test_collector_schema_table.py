import sys
import pyarrow as pa
import pytest
from PyQt6.QtWidgets import QApplication

from gui.collector_schema_table import CollectorSchemaTable

_app = QApplication.instance() or QApplication(sys.argv)


def _schema() -> pa.Schema:
    return pa.schema([
        pa.field("Id", pa.string()),
        pa.field("SenderUid", pa.string()),
        pa.field("TsCreate", pa.timestamp("ms", tz="UTC")),
    ])


def test_all_columns_checked_by_default():
    w = CollectorSchemaTable()
    w.load_schema(_schema())
    assert w.get_selected_columns() is None  # None = all selected


def test_deselect_all_returns_empty_list():
    w = CollectorSchemaTable()
    w.load_schema(_schema())
    w._on_deselect_all()
    cols = w.get_selected_columns()
    assert cols == []  # empty → treated as None by caller, but raw value is []


def test_select_all_after_deselect_returns_none():
    w = CollectorSchemaTable()
    w.load_schema(_schema())
    w._on_deselect_all()
    w._on_select_all()
    assert w.get_selected_columns() is None


def test_partial_selection_returns_subset():
    w = CollectorSchemaTable()
    w.load_schema(_schema())
    # Uncheck the second column (SenderUid)
    from PyQt6.QtWidgets import QCheckBox
    check: QCheckBox = w._table.cellWidget(1, 0)
    check.setChecked(False)
    cols = w.get_selected_columns()
    assert cols == ["Id", "TsCreate"]


def test_count_label_updates_on_deselect():
    w = CollectorSchemaTable()
    w.load_schema(_schema())
    w._on_deselect_all()
    assert "0 of 3" in w._count_label.text()


def test_count_label_no_schema():
    w = CollectorSchemaTable()
    assert "No schema loaded" in w._count_label.text()


def test_clear_schema_resets_widget():
    w = CollectorSchemaTable()
    w.load_schema(_schema())
    w.clear_schema()
    assert w._table.rowCount() == 0
    assert "No schema loaded" in w._count_label.text()
    assert w.get_selected_columns() is None  # empty table → None
