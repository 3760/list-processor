"""
T-31: 错误提示与日志查看组件

功能：
- CriticalError 错误弹窗
- 日志文件快速打开
- 错误详情展示

依赖：F7 结果汇总模块、infra/log_manager
"""

import os
from typing import Optional

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class CriticalErrorDialog(QDialog):
    """
    关键错误对话框

    用于展示处理过程中的严重错误：
    - 文件读取失败
    - 内存不足
    - 配置错误等
    """

    def __init__(self, title: str, message: str, details: str = "", parent=None):
        super().__init__(parent)
        self.setWindowTitle(title or "错误")
        self.setMinimumSize(500, 300)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowCloseButtonHint)
        self._init_ui(message, details)

    def _init_ui(self, message: str, details: str):
        """初始化 UI"""
        layout = QVBoxLayout(self)

        # 错误图标 + 消息
        error_label = QLabel(f"<font size='4'><b>❌ {message}</b></font>")
        error_label.setAlignment(Qt.AlignCenter)
        error_label.setStyleSheet("padding: 20px; color: #991B1B;")
        layout.addWidget(error_label)

        # 详细信息（可选）
        if details:
            details_label = QLabel("<b>详细信息：</b>")
            layout.addWidget(details_label)

            details_text = QTextEdit()
            details_text.setReadOnly(True)
            details_text.setPlainText(details)
            details_text.setMaximumHeight(150)
            layout.addWidget(details_text)

        # 按钮
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        self.ok_btn = QPushButton("确定")
        self.ok_btn.clicked.connect(self.accept)
        self.ok_btn.setDefault(True)
        button_layout.addWidget(self.ok_btn)

        layout.addLayout(button_layout)


class ValidationErrorDialog(QDialog):
    """
    校验错误对话框

    用于展示前置检查失败等业务错误：
    - 缺少必要文件
    - 字段不匹配
    - 配置错误等
    """

    def __init__(self, title: str, message: str, suggestions: list = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title or "校验失败")
        self.setMinimumSize(500, 250)
        self._suggestions = suggestions or []
        self._init_ui(message)

    def _init_ui(self, message: str):
        """初始化 UI"""
        layout = QVBoxLayout(self)

        # 警告图标 + 消息
        warn_label = QLabel(f"<font size='4'><b>⚠️ {message}</b></font>")
        warn_label.setAlignment(Qt.AlignCenter)
        warn_label.setStyleSheet("padding: 15px; color: #92400E;")
        layout.addWidget(warn_label)

        # 建议（可选）
        if self._suggestions:
            suggestions_label = QLabel("<b>建议：</b>")
            layout.addWidget(suggestions_label)

            for suggestion in self._suggestions:
                suggestion_widget = QLabel(f"• {suggestion}")
                suggestion_widget.setStyleSheet("padding-left: 20px;")
                layout.addWidget(suggestion_widget)

        # 按钮
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        self.ok_btn = QPushButton("确定")
        self.ok_btn.clicked.connect(self.accept)
        self.ok_btn.setDefault(True)
        button_layout.addWidget(self.ok_btn)

        layout.addLayout(button_layout)


class LogViewerWidget(QWidget):
    """
    日志查看组件

    提供日志文件查看和导出功能：
    - 显示最近日志
    - 打开日志目录
    - 导出日志文件
    """

    def __init__(self, log_dir: str = None, parent=None):
        super().__init__(parent)
        self.log_dir = log_dir
        self._init_ui()

    def _init_ui(self):
        """初始化 UI"""
        layout = QVBoxLayout(self)

        # 标题
        title_label = QLabel("<b>📋 日志查看</b>")
        layout.addWidget(title_label)

        # 日志文本区
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(200)
        layout.addWidget(self.log_text)

        # 按钮行
        button_layout = QHBoxLayout()

        self.refresh_btn = QPushButton("🔄 刷新")
        self.refresh_btn.clicked.connect(self._refresh_log)
        button_layout.addWidget(self.refresh_btn)

        self.open_dir_btn = QPushButton("📂 打开目录")
        self.open_dir_btn.clicked.connect(self._open_log_dir)
        button_layout.addWidget(self.open_dir_btn)

        self.export_btn = QPushButton("💾 导出日志")
        self.export_btn.clicked.connect(self._export_log)
        button_layout.addWidget(self.export_btn)

        layout.addLayout(button_layout)

    def set_log_content(self, content: str):
        """设置日志内容"""
        self.log_text.setPlainText(content)

    def append_log(self, line: str):
        """追加日志行"""
        current = self.log_text.toPlainText()
        lines = current.split("\n") if current else []
        lines.append(line)

        # 保留最近 500 行
        if len(lines) > 500:
            lines = lines[-500:]

        self.log_text.setPlainText("\n".join(lines))
        # 滚动到底部
        self.log_text.verticalScrollBar().setValue(
            self.log_text.verticalScrollBar().maximum()
        )

    def _refresh_log(self):
        """刷新日志"""
        if not self.log_dir:
            return

        try:
            # 读取最近的日志文件
            log_files = [f for f in os.listdir(self.log_dir) if f.endswith(".log")]
            if log_files:
                latest_log = sorted(log_files)[-1]
                log_path = os.path.join(self.log_dir, latest_log)

                with open(log_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    # 保留最后 1000 行
                    lines = content.split("\n")
                    if len(lines) > 1000:
                        lines = lines[-1000:]
                    self.log_text.setPlainText("\n".join(lines))
        except Exception as e:
            self.log_text.setPlainText(f"读取日志失败: {e}")

    def _open_log_dir(self):
        """打开日志目录"""
        if not self.log_dir:
            return

        # 使用 macOS open 命令打开 Finder
        os.system(f'open "{self.log_dir}"')

    def _export_log(self):
        """导出日志"""
        if not self.log_text.toPlainText():
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "导出日志",
            "processing_log.txt",
            "Text Files (*.txt);;All Files (*)"
        )

        if file_path:
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(self.log_text.toPlainText())
            except Exception as e:
                # 静默处理导出错误
                pass


def show_critical_error(parent, title: str, message: str, details: str = ""):
    """显示关键错误对话框"""
    dialog = CriticalErrorDialog(title, message, details, parent)
    dialog.exec_()


def show_validation_error(parent, title: str, message: str, suggestions: list = None):
    """显示校验错误对话框"""
    dialog = ValidationErrorDialog(title, message, suggestions, parent)
    dialog.exec_()
