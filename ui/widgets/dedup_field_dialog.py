"""
UI层 - 去重字段选择对话框

PRD F1-07 NQ-02 三级去重字段识别策略 第③级：
  弹出对话框让用户从下拉框手动指定。系统将模糊匹配到的近似值预填为默认选项。

使用方式：
    from ui.widgets.dedup_field_dialog import DedupFieldDialog

    dialog = DedupFieldDialog(columns=["email", "phone", "name"], default="email")
    if dialog.exec_() == QDialog.Accepted:
        print(f"用户选择去重字段: {dialog.get_selected_field()}")
"""

from typing import Optional

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QComboBox,
    QCompleter,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QVBoxLayout,
)

from infra.log_manager import get_logger

logger = get_logger(__name__)


class DedupFieldDialog(QDialog):
    """
    去重字段选择对话框。

    Features:
    - 显示所有可用列名供选择
    - 自动预填推荐字段（如 email）
    - 支持用户手动选择
    """

    def __init__(
        self,
        columns: list[str],
        default: Optional[str] = None,
        parent=None,
    ):
        """
        Parameters
        ----------
        columns : list[str]
            可选的列名列表
        default : str, optional
            默认预选的列名
        parent : QWidget, optional
            父窗口
        """
        super().__init__(parent)
        self.columns = columns
        self.selected_field: Optional[str] = None

        self.setWindowTitle("选择去重字段")
        self.setMinimumWidth(400)
        self._setup_ui(default)

    def _setup_ui(self, default: Optional[str]):
        """构建UI布局"""
        layout = QVBoxLayout(self)

        # 说明
        desc_label = QLabel(
            "请选择用于识别重复人员的去重字段：\n"
            "（同一字段值相同的多条记录将被标记为重复）"
        )
        desc_label.setStyleSheet("color: #666; font-size: 12px;")
        layout.addWidget(desc_label)

        # 列名选择下拉框
        field_layout = QHBoxLayout()
        field_layout.addWidget(QLabel("去重字段："))

        self.field_combo = QComboBox()
        self.field_combo.addItems(self.columns)
        self.field_combo.setMinimumWidth(200)
        self.field_combo.setEditable(True)
        self.field_combo.setInsertPolicy(QComboBox.NoInsert)
        self.field_combo.completer().setCompletionMode(QCompleter.PopupCompletion)

        # 预填默认值
        if default and default in self.columns:
            self.field_combo.setCurrentText(default)

        field_layout.addWidget(self.field_combo, stretch=1)
        layout.addLayout(field_layout)

        # 提示
        hint_label = QLabel(
            "提示：常用去重字段包括邮箱、手机号、身份证号等唯一标识字段"
        )
        hint_label.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(hint_label)

        # 按钮区域
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self._on_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _on_accept(self):
        """确认选择"""
        selected = self.field_combo.currentText().strip()
        if selected and selected in self.columns:
            self.selected_field = selected
            logger.info(f"用户选择去重字段: {selected}")
            self.accept()
        else:
            logger.warning(f"选择的去重字段无效: {selected}")

    def get_selected_field(self) -> Optional[str]:
        """
        获取用户选择的去重字段。

        Returns
        -------
        str | None
            选择的列名，用户取消返回 None
        """
        return self.selected_field


def create_dedup_field_dialog_callback(columns: list[str], default: Optional[str] = None):
    """
    创建去重字段选择回调函数（供 UI 层使用）。

    Returns
    -------
    callable
        dedup_field_callback: () -> str | None
    """
    def show_dialog(parent=None) -> Optional[str]:
        dialog = DedupFieldDialog(columns, default, parent)
        if dialog.exec_() == QDialog.Accepted:
            return dialog.get_selected_field()
        return None

    return show_dialog
