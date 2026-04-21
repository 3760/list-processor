"""
UI层 - 字段规范导入对话框

F1-07：用户上传「属性导入模版」Excel → 工具解析预览 → 确认后写入 field_spec.yaml。

使用方式：
    from ui.widgets.spec_import_dialog import SpecImportDialog

    dialog = SpecImportDialog(parent=main_window)
    if dialog.exec_() == QDialog.Accepted:
        print(f"已导入 {dialog.get_imported_count()} 个字段")
"""

import os
from typing import Optional

from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from infra.exceptions import ValidationError, DataQualityError
from infra.log_manager import get_logger
from ui.widgets.spec_parser import parse_spec_excel, write_spec_yaml

logger = get_logger(__name__)

# 默认字段规范输出路径
DEFAULT_OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "config", "field_spec.yaml"
)


class SpecImportDialog(QDialog):
    """
    字段规范导入对话框。

    Features:
    - 选择属性导入模版 Excel 文件
    - 解析并预览所有字段定义
    - 显示解析错误（如有）
    - 确认后写入 field_spec.yaml

    Signals:
        import_completed(int): 导入完成，参数为导入字段数量
        import_cancelled(): 用户取消导入
    """

    # 信号
    import_completed = pyqtSignal(int)
    import_cancelled = pyqtSignal()

    def __init__(
        self,
        output_path: str = DEFAULT_OUTPUT_PATH,
        parent=None,
    ):
        """
        Parameters
        ----------
        output_path : str
            字段规范 YAML 输出路径，默认 config/field_spec.yaml
        parent : QWidget, optional
            父窗口
        """
        super().__init__(parent)
        self.output_path = output_path
        self._parsed_specs: list[dict] = []
        self._excel_path: Optional[str] = None

        self.setWindowTitle("导入字段规范")
        self.setMinimumWidth(700)
        self.setMinimumHeight(500)
        self._setup_ui()

    def _setup_ui(self):
        """构建UI布局"""
        layout = QVBoxLayout(self)

        # === 文件选择区 ===
        file_layout = QHBoxLayout()
        self.file_path_label = QLabel("未选择文件")
        self.file_path_label.setStyleSheet("color: gray;")
        self.file_path_label.setTextInteractionFlags(Qt.TextSelectableByMouse)

        self.select_file_btn = QPushButton("选择属性导入模版...")
        self.select_file_btn.setStyleSheet("""
            QPushButton {
                background-color: #FFFFFF;
                color: #374151;
                border: 1px solid #D1D5DB;
                border-radius: 6px;
                font-size: 13px;
                font-weight: 500;
                min-height: 36px;
                padding: 0 16px;
            }
            QPushButton:hover {
                background-color: #F9FAFB;
                border-color: #9CA3AF;
            }
        """)
        self.select_file_btn.clicked.connect(self._on_select_file)

        file_layout.addWidget(QLabel("文件："))
        file_layout.addWidget(self.file_path_label, stretch=1)
        file_layout.addWidget(self.select_file_btn)
        layout.addLayout(file_layout)

        # === 预览表格 ===
        layout.addWidget(QLabel("字段预览："))

        self.preview_table = QTableWidget()
        self.preview_table.setColumnCount(6)
        self.preview_table.setHorizontalHeaderLabels([
            "序号", "属性编码", "属性名称", "必填", "数据类型", "数据字典"
        ])
        self.preview_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.preview_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.preview_table.setAlternatingRowColors(True)
        layout.addWidget(self.preview_table)

        # === 状态提示 ===
        self.status_label = QLabel("请选择属性导入模版文件")
        self.status_label.setStyleSheet("color: gray; font-size: 12px;")
        layout.addWidget(self.status_label)

        # === 按钮区域 ===
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        self.ok_btn = button_box.button(QDialogButtonBox.Ok)
        self.ok_btn.setText("确认导入")
        self.ok_btn.setEnabled(False)
        self.ok_btn.clicked.connect(self._on_confirm_import)

        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _on_select_file(self):
        """选择文件按钮点击"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择属性导入模版",
            "",
            "Excel 文件 (*.xlsx *.xls);;所有文件 (*)"
        )

        if not file_path:
            return

        self._excel_path = file_path
        self.file_path_label.setText(file_path)
        self.file_path_label.setStyleSheet("")

        self._parse_and_preview(file_path)

    def _parse_and_preview(self, excel_path: str):
        """解析Excel并更新预览表格"""
        self.preview_table.setRowCount(0)
        self._parsed_specs = []

        try:
            # 使用独立解析器（spec_parser.py）
            specs = parse_spec_excel(excel_path)
            self._parsed_specs = specs
            self._populate_table(specs)

            self.status_label.setText(
                f"✅ 解析成功，共 {len(specs)} 个字段"
            )
            self.status_label.setStyleSheet("color: green; font-size: 12px;")
            self.ok_btn.setEnabled(len(specs) > 0)

        except (ValidationError, DataQualityError) as e:
            self.status_label.setText(f"❌ 解析失败：{e}")
            self.status_label.setStyleSheet("color: red; font-size: 12px;")
            self.ok_btn.setEnabled(False)
            logger.error(f"字段规范解析失败: {e}")

    def _populate_table(self, specs: list[dict]):
        """填充预览表格"""
        self.preview_table.setRowCount(len(specs))
        for row_idx, spec in enumerate(specs):
            self.preview_table.setItem(row_idx, 0, QTableWidgetItem(str(row_idx + 1)))
            self.preview_table.setItem(row_idx, 1, QTableWidgetItem(spec["attr_code"]))
            self.preview_table.setItem(row_idx, 2, QTableWidgetItem(spec["attr_name"]))
            self.preview_table.setItem(
                row_idx, 3,
                QTableWidgetItem("是" if spec["required"] else "否")
            )
            self.preview_table.setItem(row_idx, 4, QTableWidgetItem(spec["data_type"]))
            self.preview_table.setItem(
                row_idx, 5,
                QTableWidgetItem(spec.get("dict_id") or "—")
            )

        # 调整列宽
        self.preview_table.resizeColumnsToContents()
        self.preview_table.setColumnWidth(2, 150)  # 属性名称列宽一些

    def _on_confirm_import(self):
        """确认导入"""
        if not self._parsed_specs or not self._excel_path:
            return

        try:
            # 使用独立写入器（spec_parser.py）
            write_spec_yaml(self._parsed_specs, self._excel_path, self.output_path)

            logger.info(f"字段规范已导入: {self.output_path}")
            self.import_completed.emit(len(self._parsed_specs))
            self.accept()

        except Exception as e:
            QMessageBox.critical(
                self,
                "导入失败",
                f"写入字段规范文件失败：\n{e}"
            )
            logger.error(f"字段规范写入失败: {e}")

    def get_imported_count(self) -> int:
        """获取导入的字段数量"""
        return len(self._parsed_specs)

    def get_output_path(self) -> str:
        """获取输出文件路径（YAML文件路径）"""
        return self.output_path

    def get_excel_path(self) -> Optional[str]:
        """[20260420-老谈] 获取原始Excel文件路径（用于显示导入时的文件名）"""
        return self._excel_path


def create_spec_import_callback(dialog_parent=None, output_path: str = DEFAULT_OUTPUT_PATH):
    """
    创建字段规范导入回调函数（供 F1_loader 使用）。

    Returns
    -------
    callable
        spec_import_callback: () -> bool

    示例：
        callback = create_spec_import_callback(parent_window)
        if callback():
            print("字段规范已导入")
    """
    from PyQt5.QtWidgets import QApplication

    def spec_import_callback() -> bool:
        """
        显示字段规范导入对话框。

        Returns
        -------
        bool
            用户确认导入返回 True，取消返回 False
        """
        dialog = SpecImportDialog(output_path=output_path, parent=dialog_parent)
        result = dialog.exec_()
        return result == QDialog.Accepted

    return spec_import_callback
