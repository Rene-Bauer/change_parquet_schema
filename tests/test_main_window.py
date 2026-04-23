import sys
from PyQt6.QtWidgets import QApplication

_app = QApplication.instance() or QApplication(sys.argv)


def test_collector_panel_has_resources_panel():
    """MainWindow creates CollectorPanel with a ResourcesPanel attached."""
    from gui.main_window import MainWindow
    from gui.resources_panel import ResourcesPanel

    w = MainWindow()
    assert isinstance(w._collector_panel.resources_panel, ResourcesPanel)
    # Cleanup
    w._sys_monitor.stop()
    w._sys_monitor.wait(2000)
    w.close()
