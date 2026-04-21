"""
T-30: 结果摘要展示组件

功能：
- 展示处理摘要（各模块处理统计）
- 展示合规性检查结果（F2 错误）
- 展示字典校验结果（F5 错误）
- 展示重复名单结果（F3/F6）

依赖：F7 结果汇总模块
"""

import os
from typing import Optional

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.context import ProcessContext


class ResultViewerDialog(QDialog):
    """
    结果查看对话框

    展示内容：
    1. 处理摘要 Tab - 各模块处理统计
    2. 合规性检查结果 Tab - F2 字段合规错误
    3. 字典校验结果 Tab - F5 字典值错误
    4. 重复名单结果 Tab - F3/F6 去重结果
    """

    def __init__(self, context: ProcessContext, parent=None):
        super().__init__(parent)
        self.context = context
        self.setWindowTitle("处理结果查看")
        self.setMinimumSize(800, 600)
        self._init_ui()

    def _init_ui(self):
        """初始化 UI"""
        layout = QVBoxLayout(self)

        # Tab 组件
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)

        # 添加各 Tab
        self._add_summary_tab()
        self._add_compliance_tab()
        self._add_dict_validation_tab()
        self._add_dedup_tab()

        # 底部按钮
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        # 打开输出目录按钮
        self.btn_open_output = QPushButton("📂 打开输出目录")
        self.btn_open_output.clicked.connect(self._open_output_dir)
        button_layout.addWidget(self.btn_open_output)

        # 开始新处理按钮
        self.btn_new_process = QPushButton("🔄 开始新处理")
        self.btn_new_process.clicked.connect(self._start_new_process)
        button_layout.addWidget(self.btn_new_process)

        # 关闭按钮
        self.close_btn = QPushButton("关闭")
        self.close_btn.clicked.connect(self.accept)
        button_layout.addWidget(self.close_btn)

        layout.addLayout(button_layout)

    def _add_summary_tab(self):
        """添加处理摘要 Tab [20260420-老谈] ISSUE-15: 补充详细信息"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 基本信息区
        info_widget = QWidget()
        info_layout = QGridLayout(info_widget)
        info_layout.setSpacing(8)
        
        row = 0
        # 处理状态
        status_label = QLabel(f"<b>处理状态：</b>{self.context.status}")
        status_label.setStyleSheet("font-size: 14pt; padding: 5px;")
        info_layout.addWidget(status_label, row, 0, 1, 2)
        row += 1
        
        # [ISSUE-15] 处理耗时
        if hasattr(self.context, 'start_time') and hasattr(self.context, 'end_time'):
            from datetime import datetime
            start = self.context.start_time
            end = self.context.end_time
            if isinstance(start, str):
                start = datetime.fromisoformat(start)
            if isinstance(end, str):
                end = datetime.fromisoformat(end)
            elapsed = (end - start).total_seconds()
            info_layout.addWidget(QLabel(f"<b>处理耗时：</b>{elapsed:.2f} 秒"), row, 0)
            row += 1
        
        # [ISSUE-15] 输出文件路径
        if hasattr(self.context, 'output_path') and self.context.output_path:
            output_paths = self.context.output_path
            if isinstance(output_paths, dict):
                for key, path in output_paths.items():
                    if path:
                        info_layout.addWidget(QLabel(f"<b>输出文件：</b>{os.path.basename(path)}"), row, 0)
                        row += 1
            elif isinstance(output_paths, str) and output_paths:
                info_layout.addWidget(QLabel(f"<b>输出文件：</b>{os.path.basename(output_paths)}"), row, 0)
                row += 1
        
        # [ISSUE-15] 数据量统计
        total_input = 0
        total_output = 0
        for list_type, df in self.context.dataframes.items():
            if df is not None and hasattr(df, 'height'):
                total_output += df.height
        info_layout.addWidget(QLabel(f"<b>输出数据量：</b>{total_output} 行"), row, 0)
        row += 1
        
        # [ISSUE-15] 去重字段
        if hasattr(self.context, 'dedup_field') and self.context.dedup_field:
            info_layout.addWidget(QLabel(f"<b>去重字段：</b>{self.context.dedup_field}"), row, 0)
            row += 1
        
        # [ISSUE-15] 字典版本
        if hasattr(self.context, 'dict_version') and self.context.dict_version:
            info_layout.addWidget(QLabel(f"<b>字典版本：</b>{self.context.dict_version}"), row, 0)
            row += 1
        
        layout.addWidget(info_widget)
        layout.addSpacing(10)
        
        # 分隔线
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("color: #E5E7EB;")
        layout.addWidget(line)
        
        # 模块结果表格
        table_label = QLabel("<b>模块处理结果</b>")
        table_label.setStyleSheet("font-size: 12pt; padding: 5px;")
        layout.addWidget(table_label)
        
        table = QTableWidget()
        table.setColumnCount(5)
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
        
        table.setHorizontalHeaderLabels(["模块", "成功数", "失败数", "跳过数", "说明"])
        table.horizontalHeader().setStretchLastSection(True)

        module_results = self.context.module_results
        row = 0
        for module, result in module_results.items():
            if module.startswith("F"):
                table.insertRow(row)
                module_name = MODULE_NAMES_CN.get(module, module)
                table.setItem(row, 0, QTableWidgetItem(module_name))
                table.setItem(row, 1, QTableWidgetItem(str(result.get("success", 0))))
                table.setItem(row, 2, QTableWidgetItem(str(result.get("fail", 0))))
                table.setItem(row, 3, QTableWidgetItem(str(result.get("skip", 0))))
                table.setItem(row, 4, QTableWidgetItem(str(result.get("message", ""))))
                row += 1

        table.resizeColumnsToContents()
        layout.addWidget(table)

        self.tab_widget.addTab(tab, "处理摘要")

    def _add_compliance_tab(self):
        """添加合规性检查结果 Tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        error_df = self.context.error_records.get("yixian")

        if error_df is None or len(error_df) == 0:
            layout.addWidget(QLabel("✅ 所有数据均合规"))
        else:
            # 显示错误统计
            stats_label = QLabel(f"<b>合规性错误：</b>{len(error_df)} 条")
            layout.addWidget(stats_label)

            # 错误类型分布
            table = QTableWidget()
            table.setColumnCount(5)
            table.setHorizontalHeaderLabels(["字段名", "行号", "原始值", "问题类型", "说明"])
            table.horizontalHeader().setStretchLastSection(True)

            for i, row_data in enumerate(error_df.iter_rows(named=True)):
                table.insertRow(i)
                table.setItem(i, 0, QTableWidgetItem(str(row_data.get("字段名", ""))))
                table.setItem(i, 1, QTableWidgetItem(str(row_data.get("行号", ""))))
                table.setItem(i, 2, QTableWidgetItem(str(row_data.get("原始值", ""))[:50]))
                table.setItem(i, 3, QTableWidgetItem(str(row_data.get("问题类型", ""))))
                table.setItem(i, 4, QTableWidgetItem(str(row_data.get("说明", ""))))

            table.resizeColumnsToContents()
            layout.addWidget(table)

        self.tab_widget.addTab(tab, "合规性检查结果")

    def _add_dict_validation_tab(self):
        """添加字典校验结果 Tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # F5 的错误记录在 error_records["yixian"] 中（问题类型为 DICT_NOT_FOUND）
        error_df = self.context.error_records.get("yixian")

        if error_df is not None:
            dict_errors = [r for r in error_df.iter_rows(named=True)
                          if r.get("问题类型") == "DICT_NOT_FOUND"]

            if not dict_errors:
                layout.addWidget(QLabel("✅ 所有字典值均有效"))
            else:
                stats_label = QLabel(f"<b>字典校验错误：</b>{len(dict_errors)} 条")
                layout.addWidget(stats_label)

                table = QTableWidget()
                table.setColumnCount(4)
                table.setHorizontalHeaderLabels(["字段名", "行号", "原始值", "说明"])
                table.horizontalHeader().setStretchLastSection(True)

                for i, row_data in enumerate(dict_errors):
                    table.insertRow(i)
                    table.setItem(i, 0, QTableWidgetItem(str(row_data.get("字段名", ""))))
                    table.setItem(i, 1, QTableWidgetItem(str(row_data.get("行号", ""))))
                    table.setItem(i, 2, QTableWidgetItem(str(row_data.get("原始值", ""))[:50]))
                    table.setItem(i, 3, QTableWidgetItem(str(row_data.get("说明", ""))))

                table.resizeColumnsToContents()
                layout.addWidget(table)
        else:
            layout.addWidget(QLabel("✅ 所有字典值均有效"))

        self.tab_widget.addTab(tab, "字典校验结果")

    def _add_dedup_tab(self):
        """添加重复名单结果 Tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # F6 内部去重结果
        yixian_df = self.context.get_dataframe("yixian")

        if yixian_df is not None and "内部去重结果" in yixian_df.columns:
            # 简单统计（polars DataFrame 使用列表推导）
            total = len(yixian_df)
            dup_result = [r for r in yixian_df["内部去重结果"]] if "内部去重结果" in yixian_df.columns else []
            dup_count = dup_result.count("重复") if dup_result else 0
            orig_count = dup_result.count("原始") if dup_result else 0

            stats_label = QLabel(
                f"<b>内部去重结果：</b>原始 {orig_count} 条，重复 {dup_count} 条"
            )
            layout.addWidget(stats_label)
        else:
            layout.addWidget(QLabel("未执行内部去重"))

        # F3 跨名单去重结果
        sanfang_df = self.context.get_dataframe("sanfang")
        hw_df = self.context.get_dataframe("hw")

        if sanfang_df is not None and "是否已在一线名单" in sanfang_df.columns:
            sanfang_result = sanfang_df["是否已在一线名单"].to_list() if "是否已在一线名单" in sanfang_df.columns else []
            sanfang_in_yixian = sanfang_result.count("是") if sanfang_result else 0

            label = QLabel(f"三方名单：{sanfang_in_yixian} 条已在一线名单")
            layout.addWidget(label)

        if hw_df is not None and "是否已在一线名单" in hw_df.columns:
            hw_result = hw_df["是否已在一线名单"].to_list() if "是否已在一线名单" in hw_df.columns else []
            hw_in_yixian = hw_result.count("是") if hw_result else 0

            label = QLabel(f"HW 名单：{hw_in_yixian} 条已在一线名单")
            layout.addWidget(label)

        if sanfang_df is None and hw_df is None:
            layout.addWidget(QLabel("未执行跨名单去重"))

        self.tab_widget.addTab(tab, "重复名单结果")

    def _open_output_dir(self):
        """
        打开输出目录（[20260420-老谈] ISSUE-03 适配：output_path 可能是目录或文件）
        
        新版 F7 输出多个文件，context.output_path 指向主文件，
        context._all_output_paths 包含所有输出文件的映射。
        优先打开目录让用户看到所有输出。
        """
        output_path = getattr(self.context, 'output_path', None)
        all_paths = getattr(self.context, '_all_output_paths', None)
        
        if output_path:
            import os
            # 判断 output_path 是文件还是目录
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
            self.accept()  # 关闭结果查看器，主窗口会重置界面
