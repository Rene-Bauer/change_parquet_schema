"""Column-selection table for the DataCollector tab."""
from __future__ import annotations

import pyarrow as pa
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)


class CollectorSchemaTable(QWidget):
    """Checkbox table listing every column from the loaded schema.

    After the user de-selects unwanted columns, call
    ``get_selected_columns()`` to retrieve the projection list.
    Returns ``None`` when all columns are checked (no projection needed).
    Returns an empty list when no columns are checked.
    """

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        # Header row
        header = QHBoxLayout()
        self._select_all_btn = QPushButton("Select All")
        self._select_all_btn.setFixedWidth(80)
        self._deselect_all_btn = QPushButton("Deselect All")
        self._deselect_all_btn.setFixedWidth(90)
        self._count_label = QLabel("No schema loaded")
        header.addWidget(self._select_all_btn)
        header.addWidget(self._deselect_all_btn)
        header.addStretch()
        header.addWidget(self._count_label)
        layout.addLayout(header)

        # Table
        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Column", "Type"])
        self._table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.ResizeToContents
        )
        self._table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch
        )
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self._table.setMaximumHeight(200)
        layout.addWidget(self._table)

        self._select_all_btn.clicked.connect(self._on_select_all)
        self._deselect_all_btn.clicked.connect(self._on_deselect_all)

    def load_schema(self, schema: pa.Schema) -> None:
        """Populate the table from a PyArrow schema. All columns checked by default."""
        self._table.setRowCount(0)
        for field in schema:
            row = self._table.rowCount()
            self._table.insertRow(row)

            check = QCheckBox(field.name)
            check.setChecked(True)
            check.stateChanged.connect(self._update_count_label)
            self._table.setCellWidget(row, 0, check)

            type_item = QTableWidgetItem(str(field.type))
            type_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
            self._table.setItem(row, 1, type_item)

        self._update_count_label()

    def clear_schema(self) -> None:
        """Remove all rows and reset the count label."""
        self._table.setRowCount(0)
        self._count_label.setText("No schema loaded")

    def get_selected_columns(self) -> list[str] | None:
        """Return checked column names, or None if all columns are checked.

        - None  → all columns checked; caller should not project
        - []    → no columns checked; caller should treat as "keep all"
        - [...]  → subset checked; caller should project to this list
        """
        total = self._table.rowCount()
        if total == 0:
            return None
        selected = [
            self._table.cellWidget(row, 0).text()
            for row in range(total)
            if self._table.cellWidget(row, 0).isChecked()
        ]
        if len(selected) == total:
            return None  # all checked — no projection needed
        return selected  # may be empty list or a subset

    def _on_select_all(self) -> None:
        for row in range(self._table.rowCount()):
            self._table.cellWidget(row, 0).setChecked(True)

    def _on_deselect_all(self) -> None:
        for row in range(self._table.rowCount()):
            self._table.cellWidget(row, 0).setChecked(False)

    def _update_count_label(self) -> None:
        total = self._table.rowCount()
        if total == 0:
            self._count_label.setText("No schema loaded")
            return
        selected_count = sum(
            1 for row in range(total)
            if self._table.cellWidget(row, 0).isChecked()
        )
        self._count_label.setText(f"{selected_count} of {total} columns selected")
