"""
T-30: 结果摘要展示组件

功能：
- 展示处理摘要（各模块处理统计）
- 展示输入文件路径
- 展示配置文件路径

支持两种数据来源：
1. ProcessContext（实时处理结果）
2. 历史记录字典（从数据库读取）

依赖：F7 结果汇总模块
"""

import os
from datetime import datetime
from typing import Dict, Optional, Any

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ui.styles.button_styles import BUTTON_STYLE_SECONDARY

from core.context import ProcessContext


class ResultViewerDialog(QDialog):
    """
    结果查看对话框

    展示内容（合并为单一视图）：
    - 处理状态与基本信息
    - 输入文件路径
    - 配置文件路径
    - 模块处理结果表格
    - 合规性检查错误
    - 字典校验错误
    - 去重统计结果
    """

    # 模块中文名称映射
    MODULE_NAMES_CN = {
        "F1": "文件加载",
        "F2": "字段合规检查",
        "F3": "跨名单去重",
        "F4": "数据字典上码",
        "F5": "字典值校验",
        "F6": "名单内部去重",
        "F7": "结果输出",
    }

    def __init__(self, context: ProcessContext = None, parent=None, history_record: Dict = None):
        """
        初始化对话框

        Parameters
        ----------
        context : ProcessContext, optional
            实时处理上下文（处理完成后的结果）
        parent : QWidget, optional
            父窗口
        history_record : Dict, optional
            历史记录数据（从数据库读取）
            必须包含: summary, input_files
        """
        super().__init__(parent)

        # 支持两种数据来源
        self.context = context
        self.history_record = history_record

        # 确定数据源
        if context is not None:
            self._data_source = "context"
        elif history_record is not None:
            self._data_source = "history"
        else:
            raise ValueError("必须提供 context 或 history_record")

        self.setWindowTitle("处理结果查看")
        self.setMinimumSize(900, 700)
        self._init_ui()

    def _init_ui(self):
        """初始化 UI"""
        # 设置对话框背景白色
        self.setStyleSheet("""
            QDialog {
                background-color: #FFFFFF;
            }
            QScrollArea {
                background-color: #FFFFFF;
                border: none;
            }
            QScrollArea > QWidget > QWidget {
                background-color: #FFFFFF;
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        # 使用滚动区域容纳所有内容
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("background-color: #FFFFFF; border: none;")
        scroll_widget = QWidget()
        scroll_widget.setStyleSheet("background-color: #FFFFFF;")
        scroll_layout = QVBoxLayout(scroll_widget)
        scroll_layout.setSpacing(12)
        scroll_layout.setContentsMargins(0, 0, 0, 0)

        # 添加各个内容区块（紧凑布局）
        self._add_basic_info(scroll_layout)
        self._add_input_files_section(scroll_layout)
        self._add_config_files_section(scroll_layout)
        self._add_module_results_section(scroll_layout)

        scroll_layout.addStretch()
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll, 1)

        # 底部按钮
        self._add_buttons(layout)

    def _get_summary(self) -> Dict[str, Any]:
        """获取处理摘要数据"""
        if self._data_source == "context":
            return self.context.summary if hasattr(self.context, 'summary') and self.context.summary else {}
        else:
            # 历史记录数据
            return self.history_record.get("summary", {}) if self.history_record else {}

    def _get_module_results(self) -> Dict[str, Any]:
        """获取模块结果数据"""
        if self._data_source == "context":
            return self.context.module_results
        else:
            summary = self._get_summary()
            return summary.get("module_results", {})

    def _add_section_title(self, parent_layout, title: str):
        """添加区块标题（紧凑样式）"""
        label = QLabel(f"<b>{title}</b>")
        label.setStyleSheet("font-size: 12px; color: #374151;")
        parent_layout.addWidget(label)

    def _add_basic_info(self, layout):
        """添加基本信息区块（紧凑布局）"""
        info_widget = QWidget()
        info_layout = QGridLayout(info_widget)
        info_layout.setSpacing(4)
        info_layout.setColumnStretch(1, 1)

        row = 0

        # 处理状态
        if self._data_source == "context":
            status = self.context.status
        else:
            status = self.history_record.get("status", "-")

        status_text = {"completed": "✅ 完成", "failed": "❌ 失败", "running": "🔄 进行中"}.get(status, status)
        info_layout.addWidget(QLabel("<b>处理状态：</b>"), row, 0)
        info_layout.addWidget(QLabel(status_text), row, 1)
        row += 1

        # 处理耗时
        if self._data_source == "context":
            start = getattr(self.context, 'start_time', None)
            end = getattr(self.context, 'end_time', None)
        else:
            start = self.history_record.get("start_time")
            end = self.history_record.get("end_time")

        if start and end:
            try:
                if isinstance(start, str):
                    start = datetime.fromisoformat(start)
                if isinstance(end, str):
                    end = datetime.fromisoformat(end)
                elapsed = (end - start).total_seconds()
                if elapsed < 60:
                    elapsed_text = f"{elapsed:.1f} 秒"
                else:
                    elapsed_text = f"{int(elapsed // 60)} 分 {elapsed % 60:.0f} 秒"
                info_layout.addWidget(QLabel("<b>处理耗时：</b>"), row, 0)
                info_layout.addWidget(QLabel(elapsed_text), row, 1)
                row += 1
            except (ValueError, TypeError, AttributeError):
                pass

        # 输出文件（从 summary 获取）
        summary = self._get_summary()
        output_path = summary.get("output_dir") or (self.context.output_path if self._data_source == "context" else None)
        if output_path:
            if os.path.isdir(output_path):
                basename = os.path.basename(output_path.rstrip('/'))
                if basename == 'output':
                    basename = os.path.basename(os.path.dirname(output_path))
                info_layout.addWidget(QLabel("<b>输出目录：</b>"), row, 0)
                info_layout.addWidget(QLabel(basename), row, 1)
            else:
                info_layout.addWidget(QLabel("<b>输出文件：</b>"), row, 0)
                info_layout.addWidget(QLabel(os.path.basename(output_path)), row, 1)
            row += 1

        # 输出数据量
        if self._data_source == "context":
            total_output = 0
            for list_type, df in self.context.dataframes.items():
                if df is not None and hasattr(df, 'height'):
                    total_output += df.height
        else:
            total_output = summary.get("total_output_records", "-")

        info_layout.addWidget(QLabel("<b>输出数据量：</b>"), row, 0)
        info_layout.addWidget(QLabel(str(total_output) if isinstance(total_output, int) else total_output), row, 1)
        row += 1

        # 去重字段
        dedup_field = getattr(self.context, 'dedup_field', None) if self._data_source == "context" else None
        if dedup_field:
            info_layout.addWidget(QLabel("<b>去重字段：</b>"), row, 0)
            info_layout.addWidget(QLabel(dedup_field), row, 1)
            row += 1

        # 字典版本
        dict_version = getattr(self.context, 'dict_version', None) if self._data_source == "context" else None
        if dict_version:
            info_layout.addWidget(QLabel("<b>字典版本：</b>"), row, 0)
            info_layout.addWidget(QLabel(dict_version), row, 1)
            row += 1

        layout.addWidget(info_widget)

    def _add_input_files_section(self, layout):
        """添加输入文件区块（紧凑布局，支持换行）"""
        input_files = {}
        if self._data_source == "context":
            input_files = getattr(self.context, 'input_files', {})
        else:
            input_files = self.history_record.get("input_files", {})

        files_widget = QWidget()
        files_layout = QGridLayout(files_widget)
        files_layout.setSpacing(4)
        files_layout.setColumnStretch(1, 1)

        file_types = [
            ("一线名单", "yixian"),
            ("三方名单", "sanfang"),
            ("HW名单", "hw"),
        ]

        for i, (label_text, key) in enumerate(file_types):
            # 标签
            label = QLabel(f"{label_text}：")
            label.setStyleSheet("color: #6B7280; font-size: 12px;")
            files_layout.addWidget(label, i, 0)

            # 文件路径（支持换行）
            path = input_files.get(key)
            if path:
                path_label = QLabel(path)
                path_label.setStyleSheet("color: #374151; font-size: 12px;")
                path_label.setWordWrap(True)
                files_layout.addWidget(path_label, i, 1)
            else:
                path_label = QLabel("<span style='color:#9CA3AF'>未使用</span>")
                path_label.setStyleSheet("font-size: 12px;")
                files_layout.addWidget(path_label, i, 1)

        layout.addWidget(files_widget)

    def _add_config_files_section(self, layout):
        """添加配置文件区块（紧凑布局，支持换行）"""
        config_widget = QWidget()
        config_layout = QGridLayout(config_widget)
        config_layout.setSpacing(4)
        config_layout.setColumnStretch(1, 1)

        dict_file = None
        spec_file = None

        if self._data_source == "context":
            dict_file = getattr(self.context, 'dict_file_path', None)
            spec_file = getattr(self.context, 'spec_file_path', None)
        else:
            input_files = self.history_record.get("input_files", {})
            dict_file = input_files.get("dict")
            spec_file = input_files.get("spec")

        # 数据字典
        label1 = QLabel("数据字典：")
        label1.setStyleSheet("color: #6B7280; font-size: 12px;")
        config_layout.addWidget(label1, 0, 0)
        if dict_file:
            path_label = QLabel(os.path.basename(dict_file))
            path_label.setStyleSheet("color: #374151; font-size: 12px;")
            path_label.setWordWrap(True)
            config_layout.addWidget(path_label, 0, 1)
        else:
            no_label = QLabel("<span style='color:#9CA3AF'>未使用</span>")
            no_label.setStyleSheet("font-size: 12px;")
            config_layout.addWidget(no_label, 0, 1)

        # 字段规范
        label2 = QLabel("字段规范：")
        label2.setStyleSheet("color: #6B7280; font-size: 12px;")
        config_layout.addWidget(label2, 1, 0)
        if spec_file:
            path_label = QLabel(os.path.basename(spec_file))
            path_label.setStyleSheet("color: #374151; font-size: 12px;")
            path_label.setWordWrap(True)
            config_layout.addWidget(path_label, 1, 1)
        else:
            no_label = QLabel("<span style='color:#9CA3AF'>未使用</span>")
            no_label.setStyleSheet("font-size: 12px;")
            config_layout.addWidget(no_label, 1, 1)

        layout.addWidget(config_widget)

    def _add_module_results_section(self, layout):
        """添加模块结果表格区块"""
        self._add_section_title(layout, "🔧 模块处理结果")

        table = QTableWidget()
        table.setColumnCount(5)
        table.setHorizontalHeaderLabels(["模块", "成功数", "失败数", "跳过数", "说明"])
        table.horizontalHeader().setStretchLastSection(True)
        table.setAlternatingRowColors(True)
        # 禁止双击编辑
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.verticalHeader().setVisible(False)
        table.setStyleSheet("""
            QTableWidget {
                border: 1px solid #E5E7EB;
                border-radius: 6px;
                background-color: #FFFFFF;
                font-size: 12px;
                selection-background-color: #DBEAFE;
                selection-color: #1D4ED8;
            }
            QTableWidget::item {
                padding: 4px 6px;
                color: #374151;
            }
            QTableWidget::item:alternate {
                background-color: #F9FAFB;
            }
            QHeaderView::section {
                background-color: #F3F4F6;
                padding: 6px;
                font-weight: 600;
                border: none;
                border-bottom: 2px solid #E5E7EB;
                font-size: 12px;
            }
        """)

        module_results = self._get_module_results()
        row = 0
        for module, result in module_results.items():
            if module.startswith("F"):
                table.insertRow(row)
                module_name = self.MODULE_NAMES_CN.get(module, module)
                table.setItem(row, 0, QTableWidgetItem(module_name))
                table.setItem(row, 1, QTableWidgetItem(str(result.get("success", 0))))
                table.setItem(row, 2, QTableWidgetItem(str(result.get("fail", 0))))
                table.setItem(row, 3, QTableWidgetItem(str(result.get("skip", 0))))
                table.setItem(row, 4, QTableWidgetItem(str(result.get("message", ""))))
                row += 1

        table.resizeColumnsToContents()
        layout.addWidget(table)

    def _add_buttons(self, layout):
        """添加底部按钮"""
        button_layout = QVBoxLayout()
        button_layout.setSpacing(8)

        # 按钮行
        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self.btn_open_output = QPushButton("📂 打开输出目录")
        self.btn_open_output.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_open_output.clicked.connect(self._open_output_dir)
        btn_row.addWidget(self.btn_open_output)

        self.btn_new_process = QPushButton("🔄 开始新处理")
        self.btn_new_process.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_new_process.clicked.connect(self._start_new_process)
        btn_row.addWidget(self.btn_new_process)

        self.close_btn = QPushButton("关闭")
        self.close_btn.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.close_btn.clicked.connect(self.accept)
        btn_row.addWidget(self.close_btn)

        button_layout.addLayout(btn_row)
        layout.addLayout(button_layout)

    def _open_output_dir(self):
        """打开输出目录"""
        output_path = None

        if self._data_source == "context":
            output_path = getattr(self.context, 'output_path', None)
        else:
            # 从历史记录获取
            summary = self._get_summary()
            output_path = summary.get("output_dir")
            if not output_path:
                output_path = self.history_record.get("output_dir")

        if output_path:
            if os.path.isdir(output_path):
                target = output_path
            else:
                target = os.path.dirname(output_path)

            if target and os.path.exists(target):
                os.system(f'open "{target}"')
                return

        QMessageBox.information(self, "提示", "输出目录不存在或未设置")

    def _start_new_process(self):
        """开始新处理"""
        reply = QMessageBox.question(
            self, "确认", "确定要开始新处理吗？",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            self.accept()
