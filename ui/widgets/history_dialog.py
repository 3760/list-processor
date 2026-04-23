"""
UI层 - 历史记录对话框

展示历史处理记录列表，支持：
- 查看历史处理摘要
- 点击查看详情
- 删除历史记录
- 支持打开输出目录

使用方式：
    from ui.widgets.history_dialog import HistoryDialog

    dialog = HistoryDialog(parent_window)
    dialog.exec_()
"""

import os
import platform
import subprocess
from datetime import datetime
from typing import List, Optional

from PyQt5.QtCore import Qt, QSize
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from db.dao.processing_history import ProcessingHistoryDAO
from infra.log_manager import get_logger
from ui.styles.button_styles import BUTTON_STYLE_SECONDARY, BUTTON_STYLE_DANGER

logger = get_logger(__name__)

# [20260420-老谈] 处理状态映射：英文 → 图标+中文
STATUS_DISPLAY = {
    "completed": "✅ 完成",
    "failed": "❌ 失败",
    "running": "🔄 进行中",
    "warning": "⚠️ 警告",
}


class HistoryDialog(QDialog):
    """
    历史记录查看对话框。

    Features:
    - 显示处理历史列表（时间、文件、数量、状态）
    - 支持查看详情
    - 支持删除记录
    - 支持打开输出目录
    """

    # 列配置 [20260420-老谈] 优化列宽
    COLUMNS = [
        {"name": "序号", "width": 45},
        {"name": "处理时间", "width": 130},
        {"name": "一线名单", "width": 180},
        {"name": "输出目录", "width": 200},
        {"name": "处理状态", "width": 85},
        {"name": "总记录数", "width": 75},
        {"name": "处理时长", "width": 75},
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        # DAO 类所有方法均为静态方法，直接使用类名调用
        self._dao = ProcessingHistoryDAO
        self.selected_record: Optional[dict] = None

        self.setWindowTitle("处理历史记录")
        DIALOG_MIN_WIDTH = 950
        DIALOG_MIN_HEIGHT = 500
        self.setMinimumSize(DIALOG_MIN_WIDTH, DIALOG_MIN_HEIGHT)
        self.setModal(True)

        # P2-08: 应用设计规范样式
        self.setStyleSheet("""
            QDialog {
                background-color: #F9FAFB;
            }
        """)

        self._init_ui()
        self._load_history()

    def _init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)

        # 标题
        title_label = QLabel("📋 历史处理记录")
        # P2-08: 对齐设计规范样式
        title_label.setStyleSheet("""
            font-size: 14px;
            font-weight: 600;
            color: #111827;
            padding-bottom: 8px;
        """)

        # 历史记录表格
        self.table = QTableWidget()
        self.table.setColumnCount(len(self.COLUMNS))
        self.table.setHorizontalHeaderLabels([c["name"] for c in self.COLUMNS])

        # 设置表格属性
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        # P2-08: 对齐设计规范颜色系统
        self.table.setStyleSheet("""
            QTableWidget {
                border: 1px solid #E5E7EB;
                border-radius: 8px;
                background-color: #FFFFFF;
                gridline-color: #F3F4F6;
            }
            QTableWidget::item {
                padding: 6px;
                font-size: 13px;
                color: #374151;
            }
            QTableWidget::item:selected {
                background-color: #DBEAFE;
                color: #1D4ED8;
            }
            QTableWidget::item:alternate {
                background-color: #F9FAFB;
            }
            QHeaderView::section {
                background-color: #F9FAFB;
                padding: 10px;
                font-size: 13px;
                font-weight: 600;
                color: #111827;
                border: none;
                border-bottom: 2px solid #E5E7EB;
            }
        """)

        # 设置列宽
        header = self.table.horizontalHeader()
        for i, col in enumerate(self.COLUMNS):
            self.table.setColumnWidth(i, col["width"])
            header.setSectionResizeMode(i, QHeaderView.Fixed)

        self.table.itemClicked.connect(self._on_row_clicked)
        self.table.itemDoubleClicked.connect(self._on_view_detail)
        layout.addWidget(self.table)

        # 空状态提示
        self.empty_label = QLabel("暂无处理历史记录")
        self.empty_label.setAlignment(Qt.AlignCenter)
        self.empty_label.setStyleSheet("color: #888; font-size: 14px; padding: 50px;")
        self.empty_label.setVisible(False)
        layout.addWidget(self.empty_label)

        # 按钮区域
        button_layout = QHBoxLayout()

        # 查看详情按钮
        self.btn_detail = QPushButton("查看详情")
        self.btn_detail.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_detail.setEnabled(False)
        self.btn_detail.clicked.connect(self._on_view_detail)
        button_layout.addWidget(self.btn_detail)

        # [20260420-老谈] 打开输出目录按钮（修正按钮文字）
        self.btn_open_folder = QPushButton("打开输出目录")
        self.btn_open_folder.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_open_folder.setEnabled(False)
        self.btn_open_folder.clicked.connect(self._on_open_folder)
        button_layout.addWidget(self.btn_open_folder)

        button_layout.addStretch()

        # 删除按钮（危险按钮样式）
        self.btn_delete = QPushButton("删除记录")
        self.btn_delete.setStyleSheet(BUTTON_STYLE_DANGER)
        self.btn_delete.setEnabled(False)
        self.btn_delete.clicked.connect(self._on_delete)
        button_layout.addWidget(self.btn_delete)

        # 关闭按钮
        btn_close = QPushButton("关闭")
        btn_close.setStyleSheet(BUTTON_STYLE_SECONDARY)
        btn_close.clicked.connect(self.accept)
        button_layout.addWidget(btn_close)

        layout.addLayout(button_layout)

    def _load_history(self):
        """加载历史记录"""
        try:
            records = self._dao.get_history(limit=100)

            if not records:
                self.table.setVisible(False)
                self.empty_label.setVisible(True)
                return

            self.table.setVisible(True)
            self.empty_label.setVisible(False)

            self.table.setRowCount(len(records))
            self.history_records = records

            for row, record in enumerate(records):
                # 序号
                self.table.setItem(row, 0, QTableWidgetItem(str(row + 1)))

                # 处理时间
                if record.get("start_time"):
                    try:
                        dt = datetime.fromisoformat(record["start_time"])
                        time_str = dt.strftime("%Y-%m-%d %H:%M")
                    except (ValueError, TypeError):
                        time_str = record["start_time"]
                else:
                    time_str = "-"
                self.table.setItem(row, 1, QTableWidgetItem(time_str))

                # 一线名单文件名
                frontline = record.get("input_yixian", "-")
                if frontline:
                    frontline = os.path.basename(frontline)
                self.table.setItem(row, 2, QTableWidgetItem(frontline))

                # [20260420-老谈] 输出目录：截断显示
                output_dir = record.get("output_dir", "-")
                if output_dir and output_dir != "-":
                    # 截断过长的路径
                    PATH_DISPLAY_MAX_LENGTH = 30
                    PATH_SUFFIX_LENGTH = 27
                    if len(output_dir) > PATH_DISPLAY_MAX_LENGTH:
                        output_dir = "..." + output_dir[-PATH_SUFFIX_LENGTH:]
                self.table.setItem(row, 3, QTableWidgetItem(output_dir or "-"))

                # [20260420-老谈] 处理状态：图标+中文
                status = record.get("status", "-")
                status_text = STATUS_DISPLAY.get(status, status)
                self.table.setItem(row, 4, QTableWidgetItem(status_text))

                # 总记录数
                total = record.get("total_records", "-")
                self.table.setItem(row, 5, QTableWidgetItem(str(total) if total else "-"))

                # 处理时长
                start_time = record.get("start_time")
                end_time = record.get("end_time")
                duration_str = "-"
                if start_time and end_time:
                    try:
                        start_dt = datetime.fromisoformat(start_time)
                        end_dt = datetime.fromisoformat(end_time)
                        duration = (end_dt - start_dt).total_seconds()
                        if duration < 60:
                            duration_str = f"{duration:.1f}秒"
                        else:
                            duration_str = f"{int(duration/60)}分{duration%60:.0f}秒"
                    except (ValueError, TypeError):
                        pass
                self.table.setItem(row, 6, QTableWidgetItem(duration_str))

            logger.info(f"加载历史记录: {len(records)} 条")

        except Exception as e:
            logger.error(f"加载历史记录失败: {e}")
            self.table.setVisible(False)
            self.empty_label.setText("加载历史记录失败")
            self.empty_label.setVisible(True)

    def _on_row_clicked(self, item: QTableWidgetItem):
        """行点击事件"""
        row = item.row()
        self.selected_record = self.history_records[row]
        self.btn_detail.setEnabled(True)
        self.btn_delete.setEnabled(True)
        self.btn_open_folder.setEnabled(True)

    def _on_view_detail(self):
        """[20260420-老谈] 查看详情 - 包含字典和字段规范信息"""
        if not self.selected_record:
            return

        # 显示详情对话框
        from PyQt5.QtWidgets import QMessageBox

        summary = self.selected_record.get("summary", {})
        input_files = self.selected_record.get("input_files", {})
        module_results = summary.get("module_results", {})

        # [20260420-老谈] 显示字典和字段规范文件名（如果有）
        dict_file = input_files.get('dict')
        spec_file = input_files.get('spec')
        dict_name = os.path.basename(dict_file) if dict_file else '-'
        spec_name = os.path.basename(spec_file) if spec_file else '-'

        # [20260420-老谈] 处理状态使用图标+中文
        status = self.selected_record.get('status', '-')
        status_display = STATUS_DISPLAY.get(status, status)

        detail_text = f"""处理时间: {self.selected_record.get('start_time', '-')}

名单文件:
|- 一线名单: {input_files.get('yixian', '-')}
|- 三方名单: {input_files.get('sanfang', '-')}
|- HW名单: {input_files.get('hw', '-')}

配置文件:
|- 数据字典: {dict_name}
|- 字段规范: {spec_name}

处理状态: {status_display}

模块执行结果:"""

        for module, result in module_results.items():
            detail_text += f"\n{module}: 成功={result.get('success', 0)}, 失败={result.get('fail', 0)}"

        QMessageBox.information(self, "处理详情", detail_text)

    def _on_open_folder(self):
        """[20260420-老谈] 打开输出目录"""
        if not self.selected_record:
            return

        # 优先使用 output_dir 字段
        output_dir = self.selected_record.get("output_dir")

        # 兜底：从 summary 中获取
        if not output_dir:
            summary = self.selected_record.get("summary", {})
            output_dir = summary.get("output_dir")

        if not output_dir or not os.path.exists(output_dir):
            from PyQt5.QtWidgets import QMessageBox
            QMessageBox.warning(self, "提示", f"输出目录不存在或已被移动\n路径: {output_dir}")
            return

        # 根据系统打开目录
        system = platform.system()
        if system == "Windows":
            os.startfile(output_dir)
        elif system == "Darwin":  # macOS
            subprocess.run(["open", output_dir])
        else:  # Linux
            subprocess.run(["xdg-open", output_dir])

        logger.info(f"打开输出目录: {output_dir}")

    def _on_delete(self):
        """[20260420-老谈] 删除记录"""
        if not self.selected_record:
            return

        from PyQt5.QtWidgets import QMessageBox

        reply = QMessageBox.question(
            self,
            "确认删除",
            "确定要删除这条历史记录吗？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            try:
                run_id = self.selected_record.get("run_id")
                if run_id:
                    self._dao.delete(run_id)
                    logger.info(f"删除历史记录: {run_id}")
                    self._load_history()
                    self.btn_detail.setEnabled(False)
                    self.btn_delete.setEnabled(False)
                    self.btn_open_folder.setEnabled(False)
            except Exception as e:
                logger.error(f"删除历史记录失败: {e}")
                QMessageBox.warning(self, "错误", f"删除失败: {e}")
