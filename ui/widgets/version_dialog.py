"""
版本记录对话框

显示版本更新历史记录。

使用方式：
    from ui.widgets.version_dialog import VersionDialog

    dialog = VersionDialog(parent_window)
    dialog.exec_()
"""

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


class VersionDialog(QDialog):
    """
    版本记录对话框。
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("版本记录")
        self.setMinimumSize(600, 500)
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
        btn_close.setMinimumWidth(100)
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
        title.setFont(QFont("PingFang SC", 16, QFont.Bold))  # macOS 优先字体
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
        """创建版本记录卡片"""
        widget = QWidget()
        widget.setObjectName("versionCard")

        layout = QVBoxLayout(widget)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(8)

        # 版本标题行
        header_layout = QHBoxLayout()
        header_layout.setSpacing(8)

        version_label = QLabel(f"v{record.version}")
        version_label.setObjectName("versionTitle")
        header_layout.addWidget(version_label)

        date_label = QLabel(record.date)
        date_label.setObjectName("versionDate")
        header_layout.addWidget(date_label)

        header_layout.addStretch()

        author_label = QLabel(f"👤 {record.author}")
        author_label.setObjectName("versionAuthor")
        header_layout.addWidget(author_label)

        layout.addLayout(header_layout)

        # 分割线
        line = QWidget()
        line.setFixedHeight(1)
        line.setObjectName("versionLine")
        layout.addWidget(line)

        # 变更内容
        if record.changes:
            section = self._create_section("【变更】", record.changes)
            layout.addWidget(section)

        # Bug修复
        if record.bug_fixes:
            section = self._create_section("【Bug修复】", record.bug_fixes, is_bug=True)
            layout.addWidget(section)

        # 新功能
        if record.features:
            section = self._create_section("【新功能】", record.features, is_feature=True)
            layout.addWidget(section)

        return widget

    def _create_section(self, title: str, items: list, is_bug: bool = False, is_feature: bool = False) -> QWidget:
        """创建内容分区"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 4, 0, 4)
        layout.setSpacing(4)

        # 分区标题
        title_label = QLabel(title)
        if is_bug:
            title_label.setObjectName("sectionBug")
        elif is_feature:
            title_label.setObjectName("sectionFeature")
        else:
            title_label.setObjectName("sectionTitle")
        layout.addWidget(title_label)

        # 内容项
        for item in items:
            item_label = QLabel(f"• {item}")
            item_label.setWordWrap(True)
            item_label.setObjectName("sectionItem")
            layout.addWidget(item_label)

        return widget


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

        # 表单布局
        form = QFormLayout()
        form.setSpacing(12)

        # 版本号
        self.version_input = QLineEdit()
        self.version_input.setPlaceholderText("例如: 1.0.2")
        form.addRow("版本号:", self.version_input)

        # 作者
        self.author_input = QLineEdit()
        self.author_input.setPlaceholderText("例如: 老谈")
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

        btn_cancel = QPushButton("取消")
        btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(btn_cancel)

        btn_save = QPushButton("保存")
        btn_save.setObjectName("btnPrimary")
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
