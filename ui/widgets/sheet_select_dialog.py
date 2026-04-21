"""
UI层 - Sheet选择对话框

F1-05：当名单文件有多个Sheet时，弹出选择对话框。
- Sheet=1：直接读取，无提示
- Sheet>1：弹窗让用户选择，超时5秒自动选择第一个Sheet

使用方式：
    from ui.widgets.sheet_select_dialog import SheetSelectDialog

    dialog = SheetSelectDialog(sheet_names=["Sheet1", "Data", "Report"])
    selected = dialog.exec_()
    if selected:
        print(f"用户选择: {selected}")
"""

import threading
from typing import Optional

from PyQt5.QtCore import QTimer, pyqtSignal
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QLabel,
    QListWidget,
    QVBoxLayout,
)

from infra.log_manager import get_logger

logger = get_logger(__name__)

# 超时默认值（秒）
DEFAULT_TIMEOUT_SECONDS = 5


class SheetSelectDialog(QDialog):
    """
    多Sheet选择对话框。

    Features:
    - 显示所有可用Sheet名称
    - 支持鼠标点击或回车选择
    - 5秒超时自动选择第一个Sheet
    - 超时前3秒显示倒计时提示

    Signals:
        sheet_selected(str): 用户选择了某个Sheet
        timeout_auto_selected(str): 超时自动选择了第一个Sheet
    """

    # 信号：sheet_name (str)
    sheet_selected = pyqtSignal(str)
    timeout_auto_selected = pyqtSignal(str)

    def __init__(
        self,
        sheet_names: list[str],
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        parent=None,
    ):
        """
        Parameters
        ----------
        sheet_names : list[str]
            可选的Sheet名称列表
        timeout_seconds : int
            超时秒数，默认5秒
        parent : QWidget, optional
            父窗口
        """
        super().__init__(parent)
        self.sheet_names = sheet_names
        self.timeout_seconds = timeout_seconds
        self._timer: Optional[QTimer] = None
        self._remaining_seconds = timeout_seconds
        self._is_auto_selected = False

        self.setWindowTitle("选择工作表")
        self.setMinimumSize(400, 280)
        self._setup_ui()
        self._start_timer()

    def _setup_ui(self):
        """构建UI布局"""
        layout = QVBoxLayout(self)

        # 标题
        title_label = QLabel(f"该文件包含 {len(self.sheet_names)} 个 Sheet，请选择一个：")
        title_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(title_label)

        # Sheet列表
        self.sheet_list = QListWidget()
        self.sheet_list.addItems(self.sheet_names)
        self.sheet_list.setSelectionMode(QAbstractItemView.SingleSelection)
        self.sheet_list.setCurrentRow(0)  # 默认选中第一项
        self.sheet_list.doubleClicked.connect(self._on_confirm)
        layout.addWidget(self.sheet_list)

        # 倒计时提示
        self.countdown_label = QLabel(f"{self._remaining_seconds} 秒后将自动选择第一个Sheet")
        self.countdown_label.setStyleSheet("color: gray; font-size: 12px;")
        layout.addWidget(self.countdown_label)

        # 按钮区域
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self._on_confirm)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _start_timer(self):
        """启动超时倒计时"""
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._on_timeout_tick)
        self._timer.start(1000)  # 每秒触发一次

    def _on_timeout_tick(self):
        """每秒更新倒计时"""
        self._remaining_seconds -= 1
        if self._remaining_seconds <= 3:
            self.countdown_label.setText(
                f"⏰ {self._remaining_seconds} 秒后自动选择..."
            )
            self.countdown_label.setStyleSheet("color: orange; font-size: 12px;")

        if self._remaining_seconds <= 0:
            self._auto_select_first()

    def _auto_select_first(self):
        """超时自动选择第一个Sheet"""
        if self._is_auto_selected:
            return
        self._is_auto_selected = True
        self._stop_timer()

        auto_selected = self.sheet_names[0]
        logger.info(f"Sheet选择超时，自动选择: {auto_selected}")
        self.timeout_auto_selected.emit(auto_selected)
        self.accept()

    def _on_confirm(self):
        """确认选择"""
        current_item = self.sheet_list.currentItem()
        if current_item:
            self._stop_timer()
            selected = current_item.text()
            logger.info(f"用户选择Sheet: {selected}")
            self.sheet_selected.emit(selected)
            self.accept()
        else:
            # 未选中任何项，视为取消
            self.reject()

    def _stop_timer(self):
        """停止倒计时"""
        if self._timer is not None and self._timer.isActive():
            self._timer.stop()
            self._timer.deleteLater()
            self._timer = None

    def get_selected_sheet(self) -> Optional[str]:
        """
        获取用户选择的Sheet名称。

        Returns
        -------
        str | None
            选择的Sheet名称，用户取消返回None
        """
        if self.result() == QDialog.Accepted and hasattr(self, "sheet_list"):
            item = self.sheet_list.currentItem()
            if item:
                return item.text()
        return None

    def was_auto_selected(self) -> bool:
        """判断是否超时自动选择"""
        return self._is_auto_selected

    def closeEvent(self, event):
        """窗口关闭时清理资源"""
        self._stop_timer()
        super().closeEvent(event)


def create_sheet_selection_callback(dialog_parent=None):
    """
    创建Sheet选择回调函数（供 F1_loader 使用）。

    Returns
    -------
    callable
        sheet_selection_callback: (sheet_names: list[str]) -> str | None

    示例：
        callback = create_sheet_selection_callback(parent_window)
        df = load_files(ctx, paths, sheet_selection_callback=callback)
    """
    from PyQt5.QtWidgets import QApplication

    def sheet_selection_callback(sheet_names: list[str]) -> str | None:
        """
        显示Sheet选择对话框。

        Parameters
        ----------
        sheet_names : list[str]
            可选的Sheet名称列表

        Returns
        -------
        str | None
            用户选择的Sheet名称，超时返回第一个Sheet，放弃返回None
        """
        if len(sheet_names) <= 1:
            # 单Sheet无需选择
            return sheet_names[0] if sheet_names else None

        # 在主线程中显示对话框
        dialog = SheetSelectDialog(sheet_names, parent=dialog_parent)
        dialog.exec_()

        if dialog.was_auto_selected():
            # 超时自动选择
            return sheet_names[0]
        return dialog.get_selected_sheet()

    return sheet_selection_callback
