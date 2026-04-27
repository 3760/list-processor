"""
版本记录对话框

显示版本更新历史记录。

使用方式：
    from ui.widgets.version_dialog import VersionDialog

    dialog = VersionDialog(parent_window)
    dialog.exec_()
"""

import html

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QScrollArea,
)
from PyQt5.QtGui import QFont

from ui.widgets.version_manager import VersionManager
from ui.styles import get_title_qfont
from ui.styles.button_styles import BUTTON_STYLE_SECONDARY


class VersionDialog(QDialog):
    """
    版本记录对话框。
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("版本记录")
        self.setMinimumSize(600, 500)
        self.resize(600, 500)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)

        self.version_manager = VersionManager()
        self._init_ui()

    def _init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        # 标题区域
        header_widget = self._create_header()
        layout.addWidget(header_widget)

        # 版本记录列表
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setObjectName("versionScrollArea")

        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        content_layout.setSpacing(16)

        records = self.version_manager.get_records()
        for record in records:
            record_widget = self._create_record_card(record)
            content_layout.addWidget(record_widget)

        # 添加弹性空间
        content_layout.addStretch()

        scroll.setWidget(content_widget)
        layout.addWidget(scroll, 1)

        # 底部按钮
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        btn_close = QPushButton("关闭")
        btn_close.setStyleSheet(BUTTON_STYLE_SECONDARY)
        btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(btn_close)

        layout.addLayout(btn_layout)

    def _create_header(self) -> QWidget:
        """创建标题区域"""
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 8)

        # 标题
        title = QLabel("📋 版本历史")
        title.setFont(get_title_qfont(16, bold=True))
        layout.addWidget(title)

        # 弹性空间
        layout.addStretch()

        # 版本号标签
        latest = self.version_manager.get_latest_version()
        version_label = QLabel(f"当前版本: v{latest}")
        version_label.setObjectName("versionLabel")
        layout.addWidget(version_label)

        return widget

    def _create_record_card(self, record) -> QWidget:
        """创建版本记录卡片（单 QLabel RichText 减少 widget 数量与布局开销）"""
        widget = QWidget()
        widget.setObjectName("versionCard")

        layout = QVBoxLayout(widget)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(0)

        label = QLabel(self._record_to_html(record))
        label.setTextFormat(Qt.RichText)
        label.setWordWrap(True)
        label.setObjectName("versionCardContent")
        layout.addWidget(label)

        return widget

    def _record_to_html(self, record) -> str:
        """将版本记录转为 HTML"""
        parts = []
        parts.append(
            '<p style="margin:0 0 6px 0;">'
            f'<span style="font-size:15px; font-weight:bold; color:#111827;">v{html.escape(record.version)}</span> '
            f'<span style="color:#6B7280; margin-left:8px;">{html.escape(record.date)}</span> '
            f'<span style="color:#6B7280; margin-left:12px;">👤 {html.escape(record.author)}</span>'
            '</p>'
        )
        parts.append('<hr style="border:none; border-top:1px solid #E5E7EB; margin:0 0 8px 0;">')

        if record.changes:
            parts.append('<p style="margin:4px 0 2px 0; color:#2563EB; font-weight:500; font-size:13px;">【变更】</p>')
            for item in record.changes:
                parts.append(f'<p style="margin:1px 0 1px 12px; color:#374151; font-size:13px;">• {html.escape(item)}</p>')

        if record.bug_fixes:
            parts.append('<p style="margin:4px 0 2px 0; color:#DC2626; font-weight:500; font-size:13px;">【Bug修复】</p>')
            for item in record.bug_fixes:
                parts.append(f'<p style="margin:1px 0 1px 12px; color:#374151; font-size:13px;">• {html.escape(item)}</p>')

        if record.features:
            parts.append('<p style="margin:4px 0 2px 0; color:#059669; font-weight:500; font-size:13px;">【新功能】</p>')
            for item in record.features:
                parts.append(f'<p style="margin:1px 0 1px 12px; color:#374151; font-size:13px;">• {html.escape(item)}</p>')

        return "".join(parts)


class VersionAddDialog(QDialog):
    """添加新版本记录的对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("添加版本记录")
        self.setMinimumSize(500, 400)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)

        self._init_ui()

    def _init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 16)
        layout.setSpacing(12)
        
        lineedit_style = """
            QLineEdit {
                border: 1px solid #E5E7EB;
                border-radius: 6px;
                background-color: #FFFFFF;
                font-size: 13px;
                color: #111827;
                padding: 0 12px;
                min-height: 36px;
            }
            QLineEdit:focus {
                border-color: #2563EB;
            }
            QLineEdit:hover:not(:focus):not(:disabled) {
                border-color: #9CA3AF;
            }
        """

        # 表单布局
        form = QFormLayout()
        form.setSpacing(12)

        # 版本号
        self.version_input = QLineEdit()
        self.version_input.setPlaceholderText("例如: 1.0.2")
        self.version_input.setStyleSheet(lineedit_style)
        form.addRow("版本号:", self.version_input)

        # 作者
        self.author_input = QLineEdit()
        self.author_input.setPlaceholderText("例如: 老谈")
        self.author_input.setStyleSheet(lineedit_style)
        form.addRow("作者:", self.author_input)

        layout.addLayout(form)

        # 变更内容
        changes_label = QLabel("变更内容（每行一条）:")
        layout.addWidget(changes_label)

        self.changes_input = QTextEdit()
        self.changes_input.setPlaceholderText("例如:\n修复了XXX问题\n优化了YYY性能")
        self.changes_input.setMinimumHeight(80)
        layout.addWidget(self.changes_input)

        # Bug修复
        bug_label = QLabel("Bug修复（每行一条，可选）:")
        layout.addWidget(bug_label)

        self.bug_input = QTextEdit()
        self.bug_input.setPlaceholderText("例如:\n修复了界面挤压问题")
        self.bug_input.setMinimumHeight(60)
        layout.addWidget(self.bug_input)

        # 新功能
        feature_label = QLabel("新功能（每行一条，可选）:")
        layout.addWidget(feature_label)

        self.feature_input = QTextEdit()
        self.feature_input.setPlaceholderText("例如:\n新增版本记录功能")
        self.feature_input.setMinimumHeight(60)
        layout.addWidget(self.feature_input)

        layout.addStretch()

        # 按钮
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        # [问题3] 统一按钮样式标准：补充 pressed 和 disabled 状态
        btn_style = """
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
            QPushButton:pressed {
                background-color: #F3F4F6;
            }
            QPushButton:disabled {
                background-color: #F3F4F6;
                color: #9CA3AF;
                border-color: #E5E7EB;
            }
        """
        
        # [问题3] 统一按钮样式标准：补充 pressed 和 disabled 状态
        btn_primary_style = """
            QPushButton {
                background-color: #2563EB;
                color: #FFFFFF;
                border: none;
                border-radius: 6px;
                font-size: 13px;
                font-weight: 500;
                min-height: 36px;
                padding: 0 16px;
            }
            QPushButton:hover {
                background-color: #3B82F6;
            }
            QPushButton:pressed {
                background-color: #1D4ED8;
            }
            QPushButton:disabled {
                background-color: #9CA3AF;
                color: #FFFFFF;
            }
        """

        btn_cancel = QPushButton("取消")
        btn_cancel.setStyleSheet(btn_style)
        btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(btn_cancel)

        btn_save = QPushButton("保存")
        btn_save.setStyleSheet(btn_primary_style)
        btn_save.clicked.connect(self._on_save)
        btn_layout.addWidget(btn_save)

        layout.addLayout(btn_layout)

    def _on_save(self):
        """保存按钮点击"""
        version = self.version_input.text().strip()
        author = self.author_input.text().strip()

        if not version:
            self.version_input.setFocus()
            return
        if not author:
            self.author_input.setFocus()
            return

        changes = [line.strip() for line in self.changes_input.toPlainText().split("\n") if line.strip()]
        bugs = [line.strip() for line in self.bug_input.toPlainText().split("\n") if line.strip()]
        features = [line.strip() for line in self.feature_input.toPlainText().split("\n") if line.strip()]

        if not changes and not bugs and not features:
            self.changes_input.setFocus()
            return

        # 保存记录
        vm = VersionManager()
        vm.add_record(version, author, changes, bugs, features)

        self.accept()
