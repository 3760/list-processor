"""
UI层 - 主窗口（MainWindow）

基于 UI设计规范-v1.0-20260417 实现

功能流程：
1. 用户选择文件（必选：一线名单、数据字典；可选：三方名单、HW名单）
2. 配置处理选项（去重字段、输出目录、执行模块）
3. 点击"开始处理"
4. 后台线程执行数据处理
5. 实时显示处理进度
6. 处理完成后显示结果摘要
"""

from typing import Optional

import polars as pl

from PyQt5.QtCore import Qt, QPropertyAnimation, QEasingCurve, QTimer, pyqtSlot, Q_ARG
from PyQt5.QtWidgets import (
    QAction,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpacerItem,
    QStatusBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from core.context import ProcessContext
from infra.log_manager import get_logger
from ui.widgets.error_dialog import show_critical_error
from ui.widgets.history_dialog import HistoryDialog
from ui.widgets.result_viewer import ResultViewerDialog
from ui.widgets.progress_panel import ProgressPanel
from ui.widgets.sheet_select_dialog import SheetSelectDialog
from ui.widgets.spec_import_dialog import SpecImportDialog
from ui.widgets.dedup_field_dialog import DedupFieldDialog
from ui.widgets.version_dialog import VersionDialog, VersionAddDialog
from ui.widgets.version_manager import VersionManager
from ui.worker import ProcessingWorker

logger = get_logger(__name__)

# ============================================================
# 界面尺寸常量
# ============================================================
# [FIX S1] 窗口尺寸修正：1100x1500 → 1000x750
WINDOW_WIDTH = 1000
WINDOW_HEIGHT = 750
WINDOW_MIN_WIDTH = 900
WINDOW_MIN_HEIGHT = 700
WINDOW_MAX_WIDTH = 1200
WINDOW_MAX_HEIGHT = 900
WINDOW_TITLE = "客户名单数据预处理工具 v1.0.6"

# ============================================================
# 区块样式常量（统一圆角 8px）
# ============================================================
GROUPBOX_STYLE = """
    QGroupBox {
        background-color: #FFFFFF;
        border: 1px solid #E5E7EB;
        border-radius: 8px;
        margin-top: 8px;
        padding: 12px 16px 16px;
        font-size: 14px;
        font-weight: 600;
        color: #111827;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left;
        left: 12px;
        top: -4px;
        padding: 0 8px;
        color: #374151;
    }
"""

# ============================================================
# 布局间距常量
# ============================================================
# [FIX M2] 区块间距修正：spacing 16→12
MARGIN_MAIN = (20, 20, 20, 16)       # 主布局外边距 (上右下左)
MARGIN_SECTION = (16, 12, 16, 16)   # 区块内边距（标题占用30px）
MARGIN_SECTION_NARROW = (16, 12, 16, 16)  # 文件加载区块
MARGIN_ROW = (0, 0, 0, 0)           # 行Widget无边距
MARGIN_BANNER = (16, 12, 16, 12)    # 结果横幅
MARGIN_ACTION_BAR = (0, 26, 32, 32)  # 底部操作区（左侧32与区块内容对齐，右侧+滚动条宽度）

SPACING_MAIN = 12                   # [FIX M2] 区块之间间距 16→12
SPACING_ROW = 15                    # 行内间距（标签与输入框）
SPACING_MODULE_ROW = 30             # 模块区块行内间距（复选框之间）
SPACING_BANNER = 12                 # 横幅内部间距
SPACING_CHECKBOX = 48               # [FIX M4] 复选框之间间距 30→48

# ============================================================
# 高度常量
# ============================================================
# [FIX S2] 组件高度统一：36px
HEIGHT_GROUP_FILE = 200             # 文件加载区块（220→200）
HEIGHT_GROUP_CONFIG = 110           # 处理配置区块（130→110）
HEIGHT_GROUP_MODULE = 160           # 执行模块区块（180→160）
HEIGHT_PROGRESS = 200              # 处理进度区块（280→200）
HEIGHT_BANNER = 50                  # 结果横幅
HEIGHT_LOG = 80                   # 日志区域（100→80）
HEIGHT_ROW_NORMAL = 40              # 普通行高度（与input_row一致）
HEIGHT_ROW_MODULE = 36              # 模块行高度
HEIGHT_ELEMENT = 36                 # 控件元素高度
HEIGHT_CHECKBOX = 36                # 复选框高度
HEIGHT_BUTTON_BANNER = 32           # 横幅按钮高度
HEIGHT_BUTTON_LARGE = 44            # 大按钮高度（开始处理）
HEIGHT_CLOSE_BANNER = 24            # 关闭按钮尺寸
LABEL_WIDTH = 110                   # [FIX M1] 标签宽度统一为110px

# ============================================================
# 按钮样式常量
# ============================================================
BUTTON_STYLE_SECONDARY = """
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

BUTTON_STYLE_CLOSE = """
    QPushButton {
        background-color: transparent;
        color: #6B7280;
        border: none;
        border-radius: 4px;
        font-size: 16px;
        font-weight: bold;
    }
    QPushButton:hover {
        background-color: rgba(0, 0, 0, 0.1);
        color: #374151;
    }
"""

# ============================================================
# 输入框样式常量
# ============================================================
LINEEDIT_STYLE = """
    QLineEdit {
        border: 1px solid #E5E7EB;
        border-radius: 6px;
        background-color: #FFFFFF;
        font-size: 13px;
        color: #111827;
        padding: 0 12px;
    }
    QLineEdit:focus {
        border-color: #2563EB;
    }
    QLineEdit:hover:not(:focus):not(:disabled) {
        border-color: #9CA3AF;
    }
    QLineEdit:disabled {
        background-color: #F3F4F6;
        color: #9CA3AF;
    }
"""

# ============================================================
# 复选框样式常量
# ============================================================
CHECKBOX_STYLE = """
    QCheckBox {
        font-size: 13px;
        color: #374151;
        spacing: 8px;
    }
    QCheckBox::indicator {
        width: 18px;
        height: 18px;
        border-radius: 4px;
        border: 2px solid #D1D5DB;
        background-color: #FFFFFF;
    }
    QCheckBox::indicator:hover {
        border-color: #9CA3AF;
    }
    QCheckBox::indicator:checked {
        background-color: #2563EB;
        border-color: #2563EB;
    }
    QCheckBox::indicator:checked:hover {
        background-color: #3B82F6;
        border-color: #3B82F6;
    }
"""


class MainWindow(QMainWindow):
    """
    应用主窗口。

    布局结构（基于设计规范）：
    ┌─────────────────────────────────────────────────────────┐
    │ 标题栏：客户名单数据预处理工具 v1.0                        │
    ├─────────────────────────────────────────────────────────┤
    │ 📁 文件加载                                              │
    │   一线人员名单 *：[输入框] [浏览]                         │
    │   三方系统名单：[输入框] [浏览]                           │
    │   HW系统名单：[输入框] [浏览]                            │
    │   数据字典 *：[输入框] [浏览]                            │
    │   字段规范：[输入框] [导入]                             │
    ├─────────────────────────────────────────────────────────┤
    │ ⚙️ 处理配置                                              │
    │   去重字段：[下拉框]                                     │
    │   输出目录：[输入框] [浏览]                              │
    ├─────────────────────────────────────────────────────────┤
    │ ▶️ 执行模块（可勾选）                                     │
    │   ☑ 字段合规性检查    ☑ 跨名单去重标注                    │
    │   ☑ 数据字典上码      ☑ 字典值合规校验                    │
    │   ☑ 名单内部重复检查                                    │
    ├─────────────────────────────────────────────────────────┤
    │ 📊 处理进度                                              │
    │   [进度条] 45%                                          │
    │   F3: 跨名单去重标注...                                 │
    │   [日志区域]                                            │
    ├─────────────────────────────────────────────────────────┤
    │              [      开始处理      ]                       │
    └─────────────────────────────────────────────────────────┘
    │ 状态栏：就绪                                             │
    └─────────────────────────────────────────────────────────┘
    """

    def __init__(self, config: dict, parent=None):
        super().__init__(parent)
        self.config = config
        self.worker: Optional[ProcessingWorker] = None
        self.result_viewer: Optional[ResultViewerDialog] = None

        # 文件路径
        self.file_paths = {
            "frontline": None,      # 一线人员名单
            "third_party": None,    # 三方系统名单
            "hw": None,             # HW系统名单
            "dict": None,           # 数据字典文件
            "spec": None,           # 字段规范文件
        }
        self._dict_version_display = ""  # 标题栏字典版本显示

        self._init_ui()
        self._init_status_bar()
        self._apply_stylesheet()
        
        # [20260420-老谈] ISSUE-14: 初始化时按钮应置灰（等待必填文件选择）
        self._update_start_button_state()
        
        # [20260420-老谈] 优化2.2: 从数据库加载上次使用的字典配置作为默认值
        self._load_default_config()
        
        logger.info("主窗口初始化完成")

    def _update_window_title(self, version_suffix: str = ""):
        """更新窗口标题（带字典版本显示）- 原型风格"""
        self._dict_version_display = version_suffix
        title = WINDOW_TITLE
        if version_suffix:
            # 原型格式：📚 字典 v2.3 (2026-04-10)
            title = f"{WINDOW_TITLE}  |  📚 {version_suffix}"
        self.setWindowTitle(title)

    def _apply_stylesheet(self):
        """加载 QSS 样式表（支持深色模式自动检测）"""
        import os

        # 检测 macOS 深色模式
        dark_mode = self._is_dark_mode()
        qss_filename = "dark.qss" if dark_mode else "default.qss"
        qss_path = os.path.join(os.path.dirname(__file__), "styles", qss_filename)

        if os.path.exists(qss_path):
            with open(qss_path, "r", encoding="utf-8") as f:
                self.setStyleSheet(f.read())
            # [FIX] 记录当前样式文件，便于主题变化检测
            self._current_qss = qss_filename
            logger.info(f"样式已加载: {qss_filename} (深色模式: {dark_mode})")
    
    def _is_dark_mode(self) -> bool:
        """检测 macOS 深色模式"""
        try:
            import subprocess
            result = subprocess.run(
                ["defaults", "read", "-g", "AppleInterfaceStyle"],
                capture_output=True, text=True, timeout=2
            )
            return "Dark" in result.stdout
        except Exception:
            return False

    def _check_and_apply_theme(self):
        """[FIX] 检测并应用当前系统主题"""
        dark_mode = self._is_dark_mode()
        current_qss = "dark.qss" if dark_mode else "default.qss"

        # 如果样式文件路径已设置，比较是否需要更新
        if hasattr(self, '_current_qss') and self._current_qss == current_qss:
            return

        self._current_qss = current_qss
        self._apply_stylesheet()

    def changeEvent(self, event):
        """[FIX] 监听系统主题变化事件"""
        # 检测 macOS 主题变化 (QEvent::PaletteChange)
        from PyQt5.QtCore import QEvent
        if event.type() == QEvent.PaletteChange:
            logger.info("检测到系统主题变化，重新应用样式")
            self._check_and_apply_theme()
        super().changeEvent(event)

    def _init_ui(self):
        """初始化UI布局"""
        self._update_window_title()  # 带字典版本的标题
        self.setMinimumSize(WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT)
        self.setMaximumSize(WINDOW_MAX_WIDTH, WINDOW_MAX_HEIGHT)
        self.resize(WINDOW_WIDTH, WINDOW_HEIGHT)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # 创建菜单栏
        self._create_menu_bar()

        # 创建中心部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 主布局：垂直排列（滚动区域 + 固定底部）
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)  # 边缘无边距
        main_layout.setSpacing(0)

        # 创建滚动区域
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QScrollArea.NoFrame)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll_area.setStyleSheet("background-color: #FFFFFF;")

        # 滚动内容容器（内边距容器）
        scroll_content = QWidget()
        scroll_content.setStyleSheet("background-color: #FFFFFF;")
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.setContentsMargins(*MARGIN_MAIN)
        scroll_layout.setSpacing(SPACING_MAIN)
        scroll_layout.setSizeConstraint(QVBoxLayout.SetMinAndMaxSize)

        # 内容包装器（承载区块）- 白色背景无圆角
        content_wrapper = QWidget()
        content_wrapper.setStyleSheet("background-color: #FFFFFF;")
        wrapper_layout = QVBoxLayout(content_wrapper)
        wrapper_layout.setContentsMargins(0, 0, 0, 0)
        wrapper_layout.setSpacing(SPACING_MAIN)

        # 1. 文件加载区块
        wrapper_layout.addWidget(self._create_file_section())

        # 2. 处理配置区块
        wrapper_layout.addWidget(self._create_config_section())

        # 3. 执行模块区块
        wrapper_layout.addWidget(self._create_module_section())

        # 4. 处理进度区块
        self.progress_container = self._create_progress_section()
        wrapper_layout.addWidget(self.progress_container)

        # 5. 结果横幅（默认隐藏）
        self.result_banner = self._create_result_banner()
        self.result_banner.setVisible(False)
        wrapper_layout.addWidget(self.result_banner)

        # 弹性空间
        wrapper_layout.addStretch()

        # 将包装器添加到滚动内容
        scroll_layout.addWidget(content_wrapper)

        # 滚动内容添加到滚动区域
        scroll_area.setWidget(scroll_content)

        # 主布局添加滚动区域
        main_layout.addWidget(scroll_area, 1)  # stretch=1 占据剩余空间

        # 6. 底部操作区（固定在底部）
        self.action_bar = self._create_action_bar()
        main_layout.addWidget(self.action_bar)

    def _create_file_section(self) -> QGroupBox:
        """创建文件加载区块"""
        group = QGroupBox()
        group.setTitle("📁 文件加载")
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        group.setStyleSheet(GROUPBOX_STYLE)

        # [原型还原] 双行布局：第一行（标签+输入框+按钮）、第二行（文件信息）
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(*MARGIN_SECTION_NARROW)
        main_layout.setSpacing(12)

        self.file_inputs = {}
        self.file_labels = {}
        self.file_buttons = {}

        # 文件配置：key, 标签, 提示, 是否必填
        file_configs = [
            ("frontline", "一线人员名单", "请选择文件...", True),
            ("third_party", "三方系统名单", "（可选）", False),
            ("hw", "HW系统名单", "（可选）", False),
            ("dict", "数据字典", "请选择文件...", True),
            ("spec", "字段规范", "config/field_spec.yaml", False),
        ]

        for key, label_text, placeholder, required in file_configs:
            # 第一行：标签 + 输入框 + 按钮（固定高度 40px）
            input_row = QWidget()
            input_row.setFixedHeight(40)
            input_layout = QHBoxLayout(input_row)
            input_layout.setContentsMargins(0, 0, 0, 0)
            input_layout.setSpacing(8)

            # 标签
            lbl = QLabel(label_text)
            lbl.setFixedWidth(LABEL_WIDTH)
            lbl.setStyleSheet(
                "font-size: 13px; font-weight: 500; color: #111827;" if required
                else "font-size: 13px; color: #6B7280;"
            )
            input_layout.addWidget(lbl)

            # 输入框
            le = QLineEdit()
            le.setPlaceholderText(placeholder)
            le.setFixedHeight(36)  # 输入框高度保持 36px
            le.setStyleSheet(LINEEDIT_STYLE)
            if not required:
                le.setDisabled(True)
            input_layout.addWidget(le, 1)
            self.file_inputs[key] = le

            # 浏览/导入按钮（统一尺寸 80x36）
            btn = QPushButton("导入" if key == "spec" else "浏览")
            btn.setFixedSize(80, 36)  # 按钮高度保持 36px
            btn.setStyleSheet(BUTTON_STYLE_SECONDARY)
            btn.clicked.connect(lambda checked, k=key: self._select_file(k))
            input_layout.addWidget(btn)
            self.file_buttons[key] = btn

            main_layout.addWidget(input_row)

            # 第二行：文件信息（固定高度 20px）
            info_row = QWidget()
            info_row.setFixedHeight(20)
            info_row_layout = QHBoxLayout(info_row)
            info_row_layout.setContentsMargins(134, 0, 0, 0)
            info_row_layout.setSpacing(0)

            info_lbl = QLabel()
            info_lbl.setObjectName("fileInfo")
            info_lbl.setStyleSheet("font-size: 12px; color: #6B7280;")
            info_row_layout.addWidget(info_lbl)
            self.file_labels[key] = info_lbl

            main_layout.addWidget(info_row)

        group.setLayout(main_layout)
        return group

    def _create_config_section(self) -> QGroupBox:
        """创建处理配置区块"""
        group = QGroupBox()
        group.setTitle("⚙️ 处理配置")
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        group.setStyleSheet(GROUPBOX_STYLE)

        # 使用 QVBoxLayout + QHBoxLayout 嵌套，确保每行独立布局
        layout = QVBoxLayout()
        layout.setContentsMargins(*MARGIN_SECTION)
        layout.setSpacing(12)  # 区块内间距 12px

        # ========== 第一行：去重字段 ==========
        row1_widget = QWidget()
        row1_widget.setFixedHeight(HEIGHT_ROW_NORMAL)
        row1_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row1_layout = QHBoxLayout(row1_widget)
        row1_layout.setContentsMargins(*MARGIN_ROW)
        row1_layout.setSpacing(8)  # 与文件加载区块一致

        lbl_dedup = QLabel("去重字段")
        lbl_dedup.setStyleSheet("font-size: 13px; font-weight: 500; color: #111827;")
        lbl_dedup.setFixedWidth(LABEL_WIDTH)  # [FIX M1] 标签宽度 75→110px
        lbl_dedup.setFixedHeight(HEIGHT_ELEMENT)
        lbl_dedup.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        row1_layout.addWidget(lbl_dedup)

        # 显示当前选择的字段
        self.lbl_dedup_field = QLabel("（自动识别...）")
        self.lbl_dedup_field.setStyleSheet("""
            font-size: 13px;
            color: #6B7280;
            font-style: italic;
            background-color: #F9FAFB;
            border: 1px solid #E5E7EB;
            border-radius: 6px;
            padding: 0 12px;
        """)
        self.lbl_dedup_field.setFixedHeight(HEIGHT_ELEMENT)
        self.lbl_dedup_field.setMinimumWidth(200)
        self.lbl_dedup_field.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.lbl_dedup_field.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row1_layout.addWidget(self.lbl_dedup_field, 1)

        btn_select_dedup = QPushButton("选择字段")
        btn_select_dedup.setObjectName("btnSelectDedup")
        btn_select_dedup.setFixedSize(80, 36)  # 统一按钮尺寸
        btn_select_dedup.setStyleSheet(BUTTON_STYLE_SECONDARY)
        btn_select_dedup.clicked.connect(self._select_dedup_field)
        row1_layout.addWidget(btn_select_dedup)

        layout.addWidget(row1_widget)

        # ========== 第二行：输出目录 ==========
        row2_widget = QWidget()
        row2_widget.setFixedHeight(HEIGHT_ROW_NORMAL)
        row2_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row2_layout = QHBoxLayout(row2_widget)
        row2_layout.setContentsMargins(*MARGIN_ROW)
        row2_layout.setSpacing(8)  # 与文件加载区块一致

        lbl_output = QLabel("输出目录")
        lbl_output.setStyleSheet("font-size: 13px; font-weight: 500; color: #111827;")
        lbl_output.setFixedWidth(LABEL_WIDTH)  # [FIX M1] 标签宽度 75→110px
        lbl_output.setFixedHeight(HEIGHT_ELEMENT)
        lbl_output.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        row2_layout.addWidget(lbl_output)

        self.txt_output = QLineEdit()
        self.txt_output.setPlaceholderText("默认：输入文件所在目录")
        self.txt_output.setFixedHeight(HEIGHT_ELEMENT)
        self.txt_output.setMinimumWidth(200)
        self.txt_output.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.txt_output.setStyleSheet(LINEEDIT_STYLE)
        row2_layout.addWidget(self.txt_output, 1)

        btn_output = QPushButton("浏览")
        btn_output.setObjectName("btnBrowse")
        btn_output.setFixedSize(80, 36)  # 统一按钮尺寸
        btn_output.setStyleSheet(BUTTON_STYLE_SECONDARY)
        btn_output.clicked.connect(self._select_output_dir)
        row2_layout.addWidget(btn_output)

        layout.addWidget(row2_widget)

        group.setLayout(layout)
        return group

    def _select_dedup_field(self):
        """[20260420-老谈] ISSUE-12: 选择去重字段弹窗 [FIX] 智能推荐"""
        # 先获取一线名单的列名
        frontline_path = self.file_paths.get("frontline")
        if not frontline_path:
            QMessageBox.warning(self, "提示", "请先选择一线人员名单文件")
            return

        if str(frontline_path).lower().endswith('.csv'):
            # CSV 文件直接用 polars 读取
            import polars as pl
            df_sample = pl.read_csv(frontline_path, nrows=1)
            columns = df_sample.columns
        else:
            # Excel 文件：使用用户选择的 sheet 获取列名
            # [FIX v1.0.7] 从 _sheet_selections 获取用户选择的 sheet
            selected_sheet = getattr(self, '_sheet_selections', {}).get("frontline")
            logger.debug(f"[_select_dedup_field] 获取列名，sheet={selected_sheet}")
            columns = self._get_excel_columns(frontline_path, selected_sheet)

        if not columns:
            QMessageBox.warning(self, "错误", "无法读取文件列名")
            return

        # [FIX] 智能推荐去重字段
        recommended_field = self._auto_detect_dedup_field(columns)
        current_field = getattr(self, '_dedup_field', None) or recommended_field

        # 显示去重字段选择对话框
        dialog = DedupFieldDialog(columns, current_field, self)
        if dialog.exec_() == dialog.Accepted:
            selected = dialog.get_selected_field()
            if selected:
                self._dedup_field = selected
                self.lbl_dedup_field.setText(selected)
                self.lbl_dedup_field.setStyleSheet("""
                    font-size: 13px;
                    font-weight: 500;
                    color: #111827;
                    background-color: #F9FAFB;
                    border: 1px solid #E5E7EB;
                    border-radius: 6px;
                    padding: 0 12px;
                """)
                self._log_message("INFO", f"已选择去重字段: {selected}")
                logger.info(f"用户选择去重字段: {selected}")

    def _auto_detect_dedup_field(self, columns: list) -> str:
        """
        [FIX] 自动识别推荐的去重字段
        优先级：email > 手机 > phone > 身份证 > id > 第一个字段
        """
        # 转换为小写方便匹配
        col_lower = [c.lower() for c in columns]
        
        # 推荐字段模式
        priority_patterns = [
            ('email', '邮箱'),
            ('mail', '邮箱'),
            ('phone', '手机'),
            ('mobile', '手机'),
            ('tel', '电话'),
            ('idcard', '身份证'),
            ('身份证', '身份证'),
            ('id', 'ID'),
            ('uid', '用户ID'),
        ]
        
        for pattern, _ in priority_patterns:
            for i, col in enumerate(col_lower):
                if pattern in col:
                    return columns[i]
        
        # 没有匹配，返回第一个字段
        return columns[0] if columns else ""

    def _get_excel_columns(self, file_path: str, sheet_name: str = None) -> list:
        """
        [20260420-老谈] ISSUE-12: 获取Excel文件的所有列名（只读第一行）
        [优化 v1.0.7] 使用 ZIP 直接解析 worksheet XML，只读第一行（比 openpyxl 快 5-10 倍）
        
        Parameters
        ----------
        file_path : str
            Excel 文件路径
        sheet_name : str, optional
            指定要读取的 sheet 名称。如果为 None，则读取第一个 sheet。
            优先使用用户已选择的 sheet（从 self._sheet_selections 获取）
        """
        import re, zipfile
        import time as time_module

        logger.debug(f"[_get_excel_columns] 开始获取列名: file={file_path}, sheet={sheet_name}")
        t0 = time_module.time()

        try:
            with zipfile.ZipFile(file_path) as z:
                all_names = z.namelist()
                
                # 动态查找 workbook.xml
                wb_path = None
                for name in all_names:
                    if name.endswith('workbook.xml') and '_rels' not in name:
                        wb_path = name
                        break
                
                if not wb_path:
                    raise ValueError("无法在 zip 内找到 workbook.xml")
                
                wb_xml = z.read(wb_path).decode('utf-8', errors='ignore')
                
                # 解析 sheet 列表，找到目标 sheet 的 rId
                # 格式: <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
                sheet_rid_map = dict(re.findall(r'<sheet[^>]+name="([^"]+)"[^>]+r:id="([^"]+)"', wb_xml))
                
                # 读取 rels 文件获取 rId -> 文件名 的映射
                base_dir = wb_path.rsplit('/', 1)[0] if '/' in wb_path else ''
                rels_path = f"{base_dir}/_rels/workbook.xml.rels" if base_dir else "_rels/workbook.xml.rels"
                rels_xml = z.read(rels_path).decode('utf-8', errors='ignore')
                # 遍历提取 Id 和 Target（属性顺序可能不同）
                rid_file_map = {}
                for rel_match in re.finditer(r'<Relationship[^>]+>', rels_xml):
                    rel_text = rel_match.group()
                    id_val = re.search(r'\bId="([^"]+)"', rel_text)
                    target_val = re.search(r'\bTarget="([^"]+)"', rel_text)
                    if id_val and target_val:
                        rid_file_map[id_val.group(1)] = target_val.group(1)
                
                # worksheet 所在目录
                ws_dir = base_dir + '/' if base_dir else ''
                
                # 确定要读取的 sheet
                if sheet_name and sheet_name in sheet_rid_map:
                    rid = sheet_rid_map[sheet_name]
                    sheet_file = rid_file_map.get(rid, "")
                    # 去掉 Target 中的前导 /
                    if sheet_file.startswith('/'):
                        sheet_file = sheet_file[1:]
                    # 如果是相对路径（不包含 /），加上 ws_dir
                    if '/' not in sheet_file:
                        sheet_file = ws_dir + sheet_file
                else:
                    # 默认读取第一个 sheet
                    first_sheet_name = list(sheet_rid_map.keys())[0] if sheet_rid_map else None
                    logger.debug(f"[_get_excel_columns] 未指定 sheet_name 或找不到，读取第一个: {first_sheet_name}")
                    if not first_sheet_name:
                        return []
                    rid = sheet_rid_map[first_sheet_name]
                    sheet_file = rid_file_map.get(rid, "")
                    # 去掉 Target 中的前导 /
                    if sheet_file.startswith('/'):
                        sheet_file = sheet_file[1:]
                    # 如果是相对路径（不包含 /），加上 ws_dir
                    if '/' not in sheet_file:
                        sheet_file = ws_dir + sheet_file
                
                logger.debug(f"[_get_excel_columns] 读取 worksheet: {sheet_file}")
                xml_content = z.read(sheet_file).decode('utf-8', errors='ignore')

            logger.debug(f"[_get_excel_columns] 读取 XML 完成，耗时: {time_module.time()-t0:.3f}s")

            # 解析第一行（<row r="1" ...>...</row>）
            first_row_match = re.search(r'<row[^>]+r="1"[^>]*>(.*?)</row>', xml_content, re.DOTALL)
            if not first_row_match:
                logger.debug(f"[_get_excel_columns] 未找到第一行")
                return []

            row_content = first_row_match.group(1)
            
            # 检查是否使用共享字符串
            has_shared_strings = 't="s"' in row_content or 'sharedStrings' in z.namelist() if 'z' in dir() else False
            
            # 获取列引用的顺序（A, B, C... -> 0, 1, 2...）
            def col_to_num(col):
                num = 0
                for c in col:
                    num = num * 26 + (ord(c) - ord('A') + 1)
                return num
            
            # 重新打开 zip 读取共享字符串（如果需要）
            shared_strings = []
            if has_shared_strings:
                try:
                    with zipfile.ZipFile(file_path) as z:
                        all_names = z.namelist()
                        # 动态查找 sharedStrings.xml
                        ss_path = None
                        for name in all_names:
                            if name.endswith('sharedStrings.xml'):
                                ss_path = name
                                break
                        if ss_path:
                            ss_xml = z.read(ss_path).decode('utf-8', errors='ignore')
                            # 解析共享字符串: <si><t>值</t></si>
                            shared_strings = re.findall(r'<si>.*?<t[^>]*>([^<]*)</t>', ss_xml, re.DOTALL)
                except:
                    pass
            
            # 解析单元格值
            columns = []
            for cell_match in re.finditer(r'<c r="([A-Z]+1)"([^>]*)>(.*?)</c>', row_content, re.DOTALL):
                col_ref = cell_match.group(1)  # 列引用 (A, B, C...)
                attrs = cell_match.group(2)     # 属性 (t="s" 等)
                cell_content = cell_match.group(3)
                
                # 获取值
                value = None
                if 't="inlineStr"' in attrs:
                    # inline string
                    t_match = re.search(r'<t>([^<]*)</t>', cell_content)
                    value = t_match.group(1) if t_match else ""
                elif 't="s"' in attrs:
                    # 共享字符串
                    v_match = re.search(r'<v>(\d+)</v>', cell_content)
                    if v_match and shared_strings:
                        idx = int(v_match.group(1))
                        value = shared_strings[idx] if idx < len(shared_strings) else ""
                else:
                    # 普通值
                    v_match = re.search(r'<v>([^<]*)</v>', cell_content)
                    value = v_match.group(1) if v_match else ""
                
                if value:
                    columns.append((col_to_num(col_ref), str(value)))
            
            # 按列顺序排序
            columns.sort(key=lambda x: x[0])
            columns = [c[1] for c in columns if c[1]]
            
            logger.debug(f"[_get_excel_columns] 完成，获取 {len(columns)} 列，耗时: {time_module.time()-t0:.3f}s")
            return columns

        except Exception as e:
            logger.warning(f"[_get_excel_columns] 读取Excel列名失败: {e}")
            return []

    def _create_module_section(self) -> QGroupBox:
        """创建执行模块区块"""
        group = QGroupBox()
        group.setTitle("▶️ 执行模块（可勾选）")
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        group.setStyleSheet(GROUPBOX_STYLE)

        layout = QVBoxLayout()
        layout.setContentsMargins(*MARGIN_SECTION)
        layout.setSpacing(15)

        # 初始化复选框字典
        self.checkboxes = {}

        # ========== 第一行复选框 ==========
        row1_widget = QWidget()
        row1_widget.setFixedHeight(HEIGHT_ROW_MODULE)
        row1_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row1_layout = QHBoxLayout(row1_widget)
        row1_layout.setContentsMargins(*MARGIN_ROW)
        row1_layout.setSpacing(SPACING_CHECKBOX)

        cb1 = QCheckBox("字段合规性检查")
        cb1.setChecked(True)
        cb1.setFixedHeight(HEIGHT_CHECKBOX)
        cb1.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        cb1.setStyleSheet(CHECKBOX_STYLE)
        row1_layout.addWidget(cb1)
        self.checkboxes["field_validation"] = cb1

        cb2 = QCheckBox("跨名单去重标注")
        cb2.setChecked(True)
        cb2.setFixedHeight(HEIGHT_CHECKBOX)
        cb2.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        cb2.setStyleSheet(CHECKBOX_STYLE)
        row1_layout.addWidget(cb2)
        self.checkboxes["cross_dedup"] = cb2

        row1_layout.addStretch()
        layout.addWidget(row1_widget)

        # ========== 第二行复选框 ==========
        row2_widget = QWidget()
        row2_widget.setFixedHeight(HEIGHT_ROW_MODULE)
        row2_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row2_layout = QHBoxLayout(row2_widget)
        row2_layout.setContentsMargins(*MARGIN_ROW)
        row2_layout.setSpacing(SPACING_CHECKBOX)

        cb3 = QCheckBox("数据字典上码")
        cb3.setChecked(True)
        cb3.setFixedHeight(HEIGHT_CHECKBOX)
        cb3.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        cb3.setStyleSheet(CHECKBOX_STYLE)
        row2_layout.addWidget(cb3)
        self.checkboxes["dict_encode"] = cb3

        cb4 = QCheckBox("字典值合规校验")
        cb4.setChecked(True)
        cb4.setFixedHeight(HEIGHT_CHECKBOX)
        cb4.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        cb4.setStyleSheet(CHECKBOX_STYLE)
        row2_layout.addWidget(cb4)
        self.checkboxes["dict_validation"] = cb4

        row2_layout.addStretch()
        layout.addWidget(row2_widget)

        # ========== 第三行复选框 ==========
        row3_widget = QWidget()
        row3_widget.setFixedHeight(HEIGHT_ROW_MODULE)
        row3_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row3_layout = QHBoxLayout(row3_widget)
        row3_layout.setContentsMargins(*MARGIN_ROW)
        row3_layout.setSpacing(SPACING_CHECKBOX)

        cb5 = QCheckBox("名单内部重复检查")
        cb5.setChecked(True)
        cb5.setFixedHeight(HEIGHT_CHECKBOX)  # [FIX M3] 使用统一高度常量
        cb5.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        cb5.setStyleSheet(CHECKBOX_STYLE)
        row3_layout.addWidget(cb5)
        self.checkboxes["internal_dedup"] = cb5

        # 占位块确保与第二列对齐
        row3_layout.addSpacing(SPACING_CHECKBOX)

        row3_layout.addStretch()
        layout.addWidget(row3_widget)

        group.setLayout(layout)
        return group

    def _create_progress_section(self) -> QGroupBox:
        """创建处理进度区块"""
        container = QGroupBox()
        container.setTitle("📊 处理进度")
        container.setObjectName("progressContainer")
        container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        container.setStyleSheet(GROUPBOX_STYLE)

        layout = QVBoxLayout(container)
        layout.setContentsMargins(16, 20, 16, 12)  # 区块内边距
        layout.setSpacing(8)

        # 状态标签
        self.lbl_module = QLabel("等待处理...")
        self.lbl_module.setStyleSheet("color: #6B7280; font-size: 12px;")
        layout.addWidget(self.lbl_module)

        # 使用 ProgressPanel
        self.progress_panel = ProgressPanel()
        layout.addWidget(self.progress_panel)

        # 日志区域
        self.log_text = QTextEdit()
        self.log_text.setObjectName("logArea")
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(HEIGHT_LOG)
        self.log_text.setStyleSheet("""
            QTextEdit {
                background-color: #1F2937;
                color: #F9FAFB;
                font-family: 'Monaco', 'Menlo', monospace;
                font-size: 12px;
                border-radius: 8px;
                padding: 10px;
            }
        """)
        layout.addWidget(self.log_text)

        return container

    def _create_result_banner(self) -> QWidget:
        """创建结果横幅组件"""
        container = QWidget()
        container.setObjectName("resultBanner")
        container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        container.setMinimumHeight(HEIGHT_BANNER)

        layout = QHBoxLayout(container)
        layout.setContentsMargins(*MARGIN_BANNER)
        layout.setSpacing(SPACING_BANNER)

        # 图标
        self.banner_icon = QLabel()
        self.banner_icon.setFixedWidth(HEIGHT_CLOSE_BANNER)
        layout.addWidget(self.banner_icon)

        # 消息文本
        self.banner_message = QLabel()
        self.banner_message.setStyleSheet("font-size: 14px; font-weight: 500;")
        layout.addWidget(self.banner_message, 1)

        # 操作按钮区域
        banner_btn_layout = QHBoxLayout()
        banner_btn_layout.setContentsMargins(0, 0, 0, 0)
        banner_btn_layout.setSpacing(SPACING_BANNER)

        # 查看详情按钮（警告状态用）
        self.btn_view_detail = QPushButton("查看详情")
        self.btn_view_detail.setObjectName("btnViewDetail")
        self.btn_view_detail.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_view_detail.setVisible(False)
        self.btn_view_detail.clicked.connect(self._on_view_detail_clicked)
        banner_btn_layout.addWidget(self.btn_view_detail)

        # 打开输出目录按钮（成功状态用）
        self.btn_open_output = QPushButton("📂 打开输出目录")
        self.btn_open_output.setObjectName("btnOpenOutput")
        self.btn_open_output.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_open_output.setVisible(False)
        self.btn_open_output.clicked.connect(self._on_open_output_dir)
        banner_btn_layout.addWidget(self.btn_open_output)

        layout.addLayout(banner_btn_layout)

        # 关闭按钮
        btn_close = QPushButton("×")
        btn_close.setObjectName("btnBannerClose")
        btn_close.setFixedSize(HEIGHT_CLOSE_BANNER, HEIGHT_CLOSE_BANNER)
        btn_close.setStyleSheet(BUTTON_STYLE_CLOSE)
        btn_close.clicked.connect(lambda: self.result_banner.setVisible(False))
        layout.addWidget(btn_close)

        return container

    def _show_result_banner(self, status: str, message: str, detail_count: int = 0):
        """
        显示结果横幅（规范2.4：带滑入动画）
        status: 'success' | 'warning' | 'error'
        detail_count: 问题数量，用于警告状态显示
        
        动画参数：
        - 持续时间：400ms
        - 缓动曲线：OutBack（弹性效果）
        - 位移距离：20px
        - 淡入时间：300ms
        """
        icon_map = {
            "success": ("✅", "#D1FAE5", "#065F46"),
            "warning": ("⚠️", "#FEF3C7", "#92400E"),
            "error": ("❌", "#FEE2E2", "#991B1B"),
        }
        icon, bg_color, text_color = icon_map.get(status, icon_map["success"])

        self.banner_icon.setText(f'<span style="font-size: 18px;">{icon}</span>')
        self.banner_message.setText(message)
        self.banner_message.setStyleSheet(f"font-size: 14px; font-weight: 500; color: {text_color};")

        # 根据状态显示不同按钮
        if status == "warning":
            self.btn_view_detail.setVisible(True)
            self.btn_open_output.setVisible(False)
            if detail_count > 0:
                self.btn_view_detail.setText(f"查看详情 ({detail_count}条)")
        elif status == "success":
            self.btn_view_detail.setVisible(False)
            self.btn_open_output.setVisible(True)
        else:
            self.btn_view_detail.setVisible(False)
            self.btn_open_output.setVisible(False)

        self.result_banner.setStyleSheet(f"""
            QWidget#resultBanner {{
                background-color: {bg_color};
                border-radius: 8px;
                border: 1px solid {text_color}40;
            }}
            QPushButton#btnViewDetail, QPushButton#btnOpenOutput {{
                background-color: {text_color}20;
                color: {text_color};
                border: 1px solid {text_color}40;
                border-radius: 4px;
                padding: 4px 12px;
                font-size: 12px;
                transition: background-color 200ms ease;
            }}
            QPushButton#btnViewDetail:hover, QPushButton#btnOpenOutput:hover {{
                background-color: {text_color}30;
            }}
            QPushButton#btnBannerClose {{
                background-color: transparent;
                color: {text_color};
                border: none;
                font-size: 18px;
                font-weight: bold;
                transition: background-color 200ms ease;
            }}
            QPushButton#btnBannerClose:hover {{
                background-color: {text_color}20;
                border-radius: 4px;
            }}
        """)
        
        # 执行滑入动画
        self._animate_result_banner_slide_in()

    def _animate_result_banner_slide_in(self):
        """
        结果横幅滑入动画（规范2.4）
        - 持续时间：400ms
        - 缓动曲线：OutBack（弹性效果）
        - 位移距离：20px（从上方滑入）
        """
        # 确保组件可见
        self.result_banner.setVisible(True)

        # 清理旧动画实例，防止内存泄漏
        if hasattr(self, '_banner_slide_animation') and self._banner_slide_animation:
            self._banner_slide_animation.stop()
            self._banner_slide_animation.deleteLater()
        if hasattr(self, '_banner_fade_animation') and self._banner_fade_animation:
            self._banner_fade_animation.stop()
            self._banner_fade_animation.deleteLater()

        # 获取当前几何信息
        current_geometry = self.result_banner.geometry()

        # 创建位置动画：从上方20px滑入
        self._banner_slide_animation = QPropertyAnimation(self.result_banner, b"geometry")
        self._banner_slide_animation.setDuration(400)
        self._banner_slide_animation.setEasingCurve(QEasingCurve.OutBack)

        # 起始位置：向上偏移20px
        start_geometry = current_geometry
        start_geometry.moveTop(current_geometry.top() - 20)
        self._banner_slide_animation.setStartValue(start_geometry)
        self._banner_slide_animation.setEndValue(current_geometry)

        self._banner_slide_animation.start()

        # 同时执行淡入动画
        self._animate_result_banner_fade_in()

        logger.info("结果横幅滑入动画已触发")

    def _animate_result_banner_fade_in(self):
        """结果横幅淡入动画"""
        self.result_banner.setWindowOpacity(0)
        
        self._banner_fade_animation = QPropertyAnimation(self.result_banner, b"windowOpacity")
        self._banner_fade_animation.setDuration(300)
        self._banner_fade_animation.setStartValue(0)
        self._banner_fade_animation.setEndValue(1)
        self._banner_fade_animation.start()

    def _on_view_detail_clicked(self):
        """查看详情按钮点击事件"""
        if hasattr(self, '_last_context') and self._last_context is not None:
            self._show_result_viewer(self._last_context)
        else:
            QMessageBox.information(self, "提示", "无可查看的详情")

    def _on_open_output_dir(self):
        """打开输出目录按钮点击事件"""
        output_path = None
        if hasattr(self, '_last_context') and self._last_context is not None:
            output_path = getattr(self._last_context, 'output_path', None)

        if not output_path:
            # 尝试从一线名单路径推断输出目录
            frontline_path = self.file_paths.get("frontline")
            if frontline_path:
                import os
                output_path = os.path.dirname(frontline_path)

        if output_path:
            import subprocess
            try:
                subprocess.run(["open", output_path], check=True)
                self._log_message("INFO", f"已打开输出目录: {output_path}")
                logger.info(f"用户打开输出目录: {output_path}")
            except Exception as e:
                QMessageBox.warning(self, "提示", f"无法打开输出目录：{e}")
                logger.warning(f"打开输出目录失败: {e}")
        else:
            QMessageBox.warning(self, "提示", "未找到输出目录")

    def _reset_after_processing(self):
        """[FIX] 处理完成后重置界面状态"""
        self.btn_start.setEnabled(True)
        self._log_message("INFO", "处理完成，可进行下一批次处理")

    def _create_action_bar(self) -> QWidget:
        """创建底部操作区"""
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(16, 26, 32, 0)  # 左边0，右边32(16+滚动条)
        layout.setSpacing(SPACING_MAIN)

        # 左侧：历史按钮
        self.btn_history = QPushButton("📋 处理历史")
        self.btn_history.setObjectName("btnHistory")
        self.btn_history.setMinimumWidth(110)
        self.btn_history.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_history.clicked.connect(self._on_show_history)
        layout.addWidget(self.btn_history)

        # 中间：开始处理按钮居中 [FIX S4]
        layout.addStretch()

        self.btn_start = QPushButton("▶️ 开始处理")
        self.btn_start.setObjectName("btnStart")
        self.btn_start.setFixedWidth(200)
        self.btn_start.setMinimumHeight(44)
        self.btn_start.setStyleSheet("""
            QPushButton {
                background-color: #2563EB;
                color: #FFFFFF;
                border: none;
                border-radius: 8px;
                font-size: 14px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #3B82F6;
            }
            QPushButton:pressed {
                background-color: #1D4ED8;
            }
            QPushButton:disabled {
                background-color: #E5E7EB;
                color: #9CA3AF;
            }
        """)
        self.btn_start.clicked.connect(self._on_start_processing)
        layout.addWidget(self.btn_start)

        layout.addStretch()

        # 右侧：取消按钮 + 版本按钮
        self.btn_cancel = QPushButton("取消")
        self.btn_cancel.setObjectName("btnCancel")
        self.btn_cancel.setMinimumWidth(80)
        self.btn_cancel.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._on_cancel_processing)
        layout.addWidget(self.btn_cancel)

        self.btn_version = QPushButton("ℹ️ 版本")
        self.btn_version.setObjectName("btnVersion")
        self.btn_version.setMinimumWidth(80)
        self.btn_version.setStyleSheet(BUTTON_STYLE_SECONDARY)
        self.btn_version.clicked.connect(self._on_show_version)
        layout.addWidget(self.btn_version)

        return container

    def _init_status_bar(self):
        """初始化状态栏"""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("就绪")

    def _log_message(self, level: str, message: str):
        """
        向日志区域和状态栏写入消息（规范2.5：带新条目高亮闪烁）
        
        动画参数：
        - 新条目高亮：500ms后移除
        """
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] [{level}] {message}"
        
        # 追加到日志区域
        if hasattr(self, 'log_text'):
            self.log_text.append(formatted)
            # 高亮最新条目
            self._highlight_latest_log_entry()
        
        # 更新状态栏
        self.status_bar.showMessage(message, 5000)  # 5秒后恢复

    def _highlight_latest_log_entry(self):
        """
        高亮最新的日志条目（规范2.5）
        新日志短暂高亮提示，500ms后移除
        """
        if not hasattr(self, 'log_text') or self.log_text is None:
            return
        
        # 设置高亮颜色
        self.log_text.setStyleSheet("""
            QTextEdit {
                background-color: #1F2937;
                color: #F9FAFB;
                font-family: 'Monaco', 'Menlo', monospace;
                font-size: 12px;
                border-radius: 8px;
                padding: 10px;
            }
            QTextEdit QWidget {
                background-color: #1F2937;
            }
        """)
        
        # 延迟500ms后恢复原样式
        QTimer.singleShot(500, self._reset_log_highlight)

    def _reset_log_highlight(self):
        """重置日志高亮状态"""
        if hasattr(self, 'log_text') and self.log_text is not None:
            self.log_text.setStyleSheet("""
                QTextEdit {
                    background-color: #1F2937;
                    color: #F9FAFB;
                    font-family: 'Monaco', 'Menlo', monospace;
                    font-size: 12px;
                    border-radius: 8px;
                    padding: 10px;
                }
            """)

    def _create_menu_bar(self):
        """创建菜单栏"""
        menubar = self.menuBar()

        # 文件菜单
        file_menu = menubar.addMenu("文件")

        # [规范四] Ctrl+O: 打开文件
        open_action = QAction("打开一线名单", self)
        open_action.setShortcut("Ctrl+O")
        open_action.triggered.connect(lambda: self._select_file("frontline"))
        file_menu.addAction(open_action)

        # [规范四] Ctrl+S: 保存配置
        save_action = QAction("保存配置", self)
        save_action.setShortcut("Ctrl+S")
        save_action.triggered.connect(self._save_default_config)
        file_menu.addAction(save_action)

        file_menu.addSeparator()

        # [规范四] Ctrl+R: 重新开始
        restart_action = QAction("重新开始", self)
        restart_action.setShortcut("Ctrl+R")
        restart_action.triggered.connect(self._on_restart_processing)
        file_menu.addAction(restart_action)

        file_menu.addSeparator()

        exit_action = QAction("退出", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # 帮助菜单
        help_menu = menubar.addMenu("帮助")

        # [规范四] F1: 打开帮助文档
        help_action = QAction("帮助文档", self)
        help_action.setShortcut("F1")
        help_action.triggered.connect(self._show_help)
        help_menu.addAction(help_action)

        help_menu.addSeparator()

        # 版本记录
        version_action = QAction("📋 版本记录", self)
        version_action.triggered.connect(self._on_show_version)
        help_menu.addAction(version_action)

        # 处理历史
        history_action = QAction("📋 处理历史", self)
        history_action.triggered.connect(self._on_show_history)
        help_menu.addAction(history_action)

        help_menu.addSeparator()

        # 添加版本记录（开发者功能）
        add_version_action = QAction("➕ 添加版本记录", self)
        add_version_action.triggered.connect(self._add_version_record)
        help_menu.addAction(add_version_action)

        help_menu.addSeparator()

        # 关于
        about_action = QAction("ℹ️ 关于", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

    def _show_version_history(self):
        """显示版本历史记录"""
        dialog = VersionDialog(self)
        dialog.exec_()

    def _add_version_record(self):
        """添加新版本记录"""
        dialog = VersionAddDialog(self)
        if dialog.exec_():
            QMessageBox.information(self, "成功", "版本记录已保存！")

    def _open_log_dir(self):
        """[ISSUE-24] 打开日志目录"""
        import subprocess
        from infra.log_manager import LOG_DIR
        try:
            subprocess.run(["open", LOG_DIR], check=True)
            self._log_message("INFO", f"已打开日志目录: {LOG_DIR}")
            logger.info(f"用户打开日志目录: {LOG_DIR}")
        except Exception as e:
            QMessageBox.warning(self, "提示", f"无法打开日志目录：{e}")
            logger.warning(f"打开日志目录失败: {e}")

    def _update_dict_version_label(self, dict_file_path: str):
        """
        [ISSUE-13] 显示字典版本信息
        读取字典文件MD5并显示在界面
        """
        import hashlib
        import os
        try:
            md5_hash = hashlib.md5()
            with open(dict_file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    md5_hash.update(chunk)
            short_hash = md5_hash.hexdigest()[:8]
            filename = os.path.basename(dict_file_path)
            # 原型格式：字典 v{hash} (日期)
            self._update_window_title(f"字典 v{short_hash} ({datetime.now().strftime('%Y-%m-%d')})")
            logger.info(f"字典版本: {filename} (MD5: {short_hash})")
        except Exception as e:
            self._update_window_title(f"字典 版本检测失败")
            logger.warning(f"字典版本检测失败: {e}")

    def _show_about(self):
        """显示关于对话框"""
        vm = VersionManager()
        latest = vm.get_latest_version()
        QMessageBox.about(
            self,
            "关于",
            f"<h3>客户名单数据预处理工具</h3>"
            f"<p>版本: v{latest}</p>"
            f"<p>基于 PyQt5 构建</p>"
            f"<p>Copyright © 2026</p>"
        )

    def _show_help(self):
        """[规范四] 显示帮助文档"""
        import subprocess
        help_path = "/Users/mars/Desktop/00_Work-Ipsos/01_项目/00_售前项目/20260315_华为/名单处理工具-AI流程/README.md"
        try:
            subprocess.run(["open", help_path], check=True)
            self._log_message("INFO", "已打开帮助文档")
        except Exception:
            QMessageBox.information(
                self,
                "帮助",
                "帮助文档路径不存在，请联系管理员。"
            )

    def _on_restart_processing(self):
        """[规范四] Ctrl+R: 重新开始处理"""
        # 检查是否有正在进行的处理
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self, "确认重新开始", "处理正在进行中，确定要取消并重新开始吗？",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self.worker.requestInterruption()
                self.worker.wait(3000)
            else:
                return
        
        # 重置界面状态
        self._reset_all_inputs()
        self._log_message("INFO", "已重新开始")

    def _reset_all_inputs(self):
        """重置所有输入状态"""
        # 清除文件路径
        for key in self.file_paths:
            self.file_paths[key] = None
        
        # 清除输入框
        for key, le in self.file_inputs.items():
            le.clear()
            le.setStyleSheet("")
        
        # 禁用可选输入框
        self.file_inputs["third_party"].setDisabled(True)
        self.file_inputs["hw"].setDisabled(True)
        self.file_inputs["spec"].setDisabled(True)
        
        # 清除文件信息标签
        for key, lbl in self.file_labels.items():
            lbl.setText("")
            lbl.setProperty("fileInfoState", "")
            lbl.style().unpolish(lbl)
            lbl.style().polish(lbl)
        
        # 清除去重字段
        if hasattr(self, '_dedup_field'):
            self._dedup_field = None
        self.lbl_dedup_field.setText("（自动识别...）")
        
        # 隐藏结果横幅
        self.result_banner.setVisible(False)
        
        # 重置开始按钮状态
        self._update_start_button_state()

    def keyPressEvent(self, event):
        """
        键盘快捷键处理（规范四）
        
        - Enter: 确认操作（如果开始按钮可用则触发开始处理）
        - Esc: 取消/关闭对话框
        - Space: 勾选/取消复选框（当复选框聚焦时）
        """
        key = event.key()
        
        # [规范四] Enter: 确认操作
        if key == Qt.Key_Return or key == Qt.Key_Enter:
            # 如果开始按钮可用且未在处理中，触发开始处理
            if self.btn_start.isEnabled() and not (self.worker and self.worker.isRunning()):
                self._on_start_processing()
            return
        
        # [规范四] Esc: 取消/关闭
        if key == Qt.Key_Escape:
            # 如果有对话框在运行，关闭对话框
            # 否则询问是否退出
            if self.worker and self.worker.isRunning():
                reply = QMessageBox.question(
                    self, "确认取消", "确定要取消当前处理吗？",
                    QMessageBox.Yes | QMessageBox.No, QMessageBox.No
                )
                if reply == QMessageBox.Yes:
                    self.worker.requestInterruption()
            return
        
        # 传递到父类处理默认行为
        super().keyPressEvent(event)

    # ==================== 业务逻辑方法 ====================

    def _select_file(self, file_type: str):
        """选择文件"""
        import os
        import time

        logger.debug(f"[_select_file] 开始选择文件: file_type={file_type}")

        # [FIX v1.0.6] spec 类型使用特殊导入对话框
        if file_type == "spec":
            logger.debug(f"[_select_file] 使用特殊导入对话框处理 spec 类型")
            self._import_spec_file()
            return

        # 根据类型确定对话框标题和文件过滤器
        if file_type == "frontline":
            title = "选择一线人员名单"
            filters = "Excel 文件 (*.xlsx *.xls);;CSV 文件 (*.csv);;所有文件 (*)"
        elif file_type == "third_party":
            title = "选择三方系统名单"
            filters = "Excel 文件 (*.xlsx *.xls);;CSV 文件 (*.csv);;所有文件 (*)"
        elif file_type == "hw":
            title = "选择HW系统名单"
            filters = "Excel 文件 (*.xlsx *.xls);;CSV 文件 (*.csv);;所有文件 (*)"
        elif file_type == "dict":
            title = "选择数据字典"
            filters = "Excel 文件 (*.xlsx *.xls);;所有文件 (*)"
        else:
            # 未知类型直接返回，避免阻塞
            logger.warning(f"未知的 file_type: {file_type}")
            return

        logger.debug(f"[_select_file] 打开文件对话框: title={title}")
        t0 = time.time()
        file_path, _ = QFileDialog.getOpenFileName(self, title, "", filters)
        logger.debug(f"[_select_file] 文件对话框关闭，耗时: {time.time()-t0:.3f}s")

        if not file_path:
            logger.debug(f"[_select_file] 用户取消选择")
            return

        logger.info(f"[_select_file] 用户选择了文件: {file_path}")

        # 保存文件路径
        self.file_paths[file_type] = file_path

        # 更新输入框
        basename = os.path.basename(file_path)
        self.file_inputs[file_type].setText(basename)
        self.file_inputs[file_type].setStyleSheet("")  # 清除错误样式

        # [优化 v1.0.7] 将 Sheet 检测和文件信息获取都移至后台执行，避免 UI 卡顿
        if file_type in ("frontline", "third_party", "hw"):
            # 立即显示加载状态
            self._animate_file_info_updating(file_type)
            
            # [优化] 后台线程：同时获取 Sheet 列表和文件信息
            logger.debug(f"[_select_file] 启动后台线程: 检测Sheet + 获取文件信息")
            def _fetch_file_and_sheet_async():
                t0 = time.time()
                
                # 1. 先获取 Sheet 列表
                logger.info(f"[_fetch_file_and_sheet_async] [1/4] 开始获取 Sheet 列表...")
                sheet_names = self._get_excel_sheet_names_from_xml(file_path)
                logger.info(f"[_fetch_file_and_sheet_async] [1/4] Sheet 列表: {sheet_names}，耗时: {time.time()-t0:.3f}s")
                
                # 2. 确定要使用的 sheet
                selected_sheet = None
                if len(sheet_names) == 1:
                    selected_sheet = sheet_names[0]
                    logger.info(f"[_fetch_file_and_sheet_async] [2/4] 单 Sheet，直接使用: {selected_sheet}")
                elif len(sheet_names) > 1:
                    # 需要用户选择，在主线程弹出对话框
                    logger.info(f"[_fetch_file_and_sheet_async] [2/4] 多 Sheet ({len(sheet_names)}个)，弹出选择对话框...")
                    # 通过 Qt 信号在主线程执行
                    from PyQt5.QtCore import QMetaObject, Qt, Q_ARG
                    QMetaObject.invokeMethod(self, "_show_sheet_selection_dialog",
                        Qt.QueuedConnection,
                        Q_ARG(str, file_type),
                        Q_ARG(object, sheet_names))
                    # 注意：selected_sheet 将在对话框回调中设置
                    # 暂时跳过文件信息获取，等用户选择 sheet 后再获取
                    return
                else:
                    logger.warning(f"[_fetch_file_and_sheet_async] [2/4] Sheet 列表为空！")
                    # 返回空，不继续获取文件信息
                    return
                
                # 3. 获取文件信息
                logger.info(f"[_fetch_file_and_sheet_async] [3/4] 调用 _get_file_info...")
                file_info = self._get_file_info(file_path, selected_sheet)
                logger.info(f"[_fetch_file_and_sheet_async] [3/4] _get_file_info 完成，耗时: {time.time()-t0:.3f}s, 结果: {file_info}")
                
                # 4. 更新 UI
                from PyQt5.QtCore import QMetaObject, Qt, Q_ARG
                QMetaObject.invokeMethod(self, "_update_file_info_safe",
                    Qt.QueuedConnection,
                    Q_ARG(str, file_type),
                    Q_ARG(object, file_info),
                    Q_ARG(object, selected_sheet),
                    Q_ARG(str, basename))

            import threading
            thread = threading.Thread(target=_fetch_file_and_sheet_async, daemon=True)
            thread.start()

        # 字典文件处理
        elif file_type == "dict":
            self._animate_file_info_updating(file_type)
            # 检查MD5变更并显示版本信息
            self._update_dict_version_label(file_path)
            self._check_dict_file_change(file_path)
            # 显示成功状态
            self._animate_file_info_success(file_type)
            self._log_message("INFO", f"已选择 [dict]: {basename}")

        # 字段规范文件处理
        elif file_type == "spec":
            self._animate_file_info_updating(file_type)
            self.file_labels[file_type].setText("✅ 字段规范已加载")
            self._animate_file_info_success(file_type)
            self._log_message("INFO", f"已加载字段规范: {basename}")

    def _animate_file_info_updating(self, file_type: str):
        """
        文件信息标签更新中动画（规范2.7）
        [优化 v1.0.7] 添加明确文字提示，告知用户正在读取文件信息
        """
        label = self.file_labels.get(file_type)
        if label:
            label.setProperty("fileInfoState", "updating")
            label.style().unpolish(label)
            label.style().polish(label)
            # [优化] 显示明确的加载状态文字
            label.setText("⏳ 正在读取文件信息...")

    def _get_excel_sheet_names_from_xml(self, file_path: str) -> list:
        """
        [优化 v1.0.7] 从 XLSX XML 直接获取 Sheet 名称列表（比 openpyxl 快）
        """
        import re, zipfile
        logger.info(f"[_get_excel_sheet_names_from_xml] ★★★ 开始获取 Sheet 列表: {file_path}")
        try:
            with zipfile.ZipFile(file_path) as z:
                all_names = z.namelist()
                logger.info(f"[_get_excel_sheet_names_from_xml] [A] zip 内文件数: {len(all_names)}")
                logger.info(f"[_get_excel_sheet_names_from_xml] [A] 文件列表: {all_names}")
                
                # 动态查找 workbook.xml
                wb_path = None
                for name in all_names:
                    if name.endswith('workbook.xml') and '_rels' not in name:
                        wb_path = name
                        logger.info(f"[_get_excel_sheet_names_from_xml] [B] 找到 workbook.xml: {wb_path}")
                        break
                
                if not wb_path:
                    logger.error(f"[_get_excel_sheet_names_from_xml] [ERROR] 找不到 workbook.xml！")
                    raise ValueError("无法在 zip 内找到 workbook.xml")
                
                wb_xml = z.read(wb_path).decode('utf-8', errors='ignore')
                logger.info(f"[_get_excel_sheet_names_from_xml] [C] workbook.xml 读取完成，长度: {len(wb_xml)}")
            
            # 解析 <sheet name="Sheet1" sheetId="1" r:id="rId1"/> 格式
            sheet_names = re.findall(r'<sheet[^>]+name="([^"]+)"', wb_xml)
            logger.info(f"[_get_excel_sheet_names_from_xml] [D] 解析到 Sheet 列表: {sheet_names}")
            return sheet_names
        except Exception as e:
            logger.warning(f"[_get_excel_sheet_names_from_xml] [ERROR] 获取 Sheet 列表失败: {e}")
            return []

    @pyqtSlot(str, object)
    def _show_sheet_selection_dialog(self, file_type: str, sheet_names: list):
        """
        [优化 v1.0.7] 在主线程显示 Sheet 选择对话框（由后台线程调用）
        """
        logger.debug(f"[_show_sheet_selection_dialog] 显示 Sheet 选择对话框: {sheet_names}")
        dialog = SheetSelectDialog(sheet_names, parent=self)
        dialog.exec_()

        if dialog.was_auto_selected():
            selected = sheet_names[0]
            self._log_message("WARNING", f"[{file_type}] 选择超时，自动使用: {selected}")
        else:
            selected = dialog.get_selected_sheet()
            if selected:
                self._log_message("INFO", f"[{file_type}] 用户选择 Sheet: {selected}")
            else:
                # 用户取消
                logger.debug(f"[_show_sheet_selection_dialog] 用户取消选择")
                self.file_paths[file_type] = None
                self.file_inputs[file_type].clear()
                return

        # 保存 Sheet 选择结果
        if not hasattr(self, '_sheet_selections'):
            self._sheet_selections = {}
        self._sheet_selections[file_type] = selected

        # 继续获取文件信息
        file_path = self.file_paths.get(file_type)
        basename = os.path.basename(file_path) if file_path else ""
        
        logger.debug(f"[_show_sheet_selection_dialog] 获取文件信息: sheet={selected}")
        file_info = self._get_file_info(file_path, selected)
        
        # 更新 UI
        self._update_file_info_safe(file_type, file_info, selected, basename)

    def _animate_file_info_success(self, file_type: str):
        """
        文件信息标签成功动画（规范2.7）
        """
        label = self.file_labels.get(file_type)
        if label:
            label.setProperty("fileInfoState", "success")
            label.style().unpolish(label)
            label.style().polish(label)

        # 更新开始按钮状态
        self._update_start_button_state()

    def _check_dict_file_change(self, file_path: str):
        """[20260420-老谈] ISSUE-13: 检查字典文件是否变更"""
        import hashlib
        
        try:
            with open(file_path, 'rb') as f:
                md5_hash = hashlib.md5(f.read()).hexdigest()
            md5_short = md5_hash[:8]
            
            # 获取上次的MD5（兼容处理：如果是32位则取前8位）
            last_md5 = getattr(self, '_last_dict_md5', None)
            if last_md5:
                last_md5 = last_md5[:8] if len(last_md5) > 8 else last_md5
            
            if last_md5 and last_md5 != md5_short:
                # MD5变更，提示用户
                reply = QMessageBox.question(
                    self,
                    "字典文件变更",
                    f"检测到字典文件已变更！\n\n"
                    f"旧版本: {last_md5}\n"
                    f"新版本: {md5_short}\n\n"
                    f"是否继续使用新版本？",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.Yes
                )
                if reply == QMessageBox.No:
                    self.file_paths["dict"] = None
                    self.file_inputs["dict"].clear()
                    self.file_inputs["dict"].setPlaceholderText("请重新选择文件...")
                    return
            
            self._last_dict_md5 = md5_short
            self._update_window_title(f"字典 v{md5_short} ({datetime.now().strftime('%Y-%m-%d')})")
            
        except Exception as e:
            logger.warning(f"MD5计算失败: {e}")

    def _handle_multi_sheet_selection(self, file_path: str, file_type: str) -> Optional[str]:
        """
        [FIX v1.0.6] Bug 3: 多 Sheet 选择处理
        [优化 v1.0.7] 使用 ZIP 直接读取 workbook.xml，避免加载整个文件

        对于名单文件（非CSV），检测 Sheet 数量：
        - Sheet=1：直接使用
        - Sheet>1：强制弹出选择窗口，禁止静默默认选择第一个

        Parameters
        ----------
        file_path : str
            Excel 文件路径
        file_type : str
            文件类型（frontline/third_party/hw）

        Returns
        -------
        str | None
            选择的 Sheet 名称，返回 None 表示用户取消
        """
        import re, zipfile

        logger.debug(f"[_handle_multi_sheet_selection] 开始检测 Sheet: file_type={file_type}, path={file_path}")

        # 初始化 Sheet 选择存储（如果不存在）
        if not hasattr(self, '_sheet_selections'):
            self._sheet_selections = {}

        # CSV 文件不需要 Sheet 选择
        if file_path.lower().endswith('.csv'):
            logger.debug(f"[_handle_multi_sheet_selection] CSV 文件，无需 Sheet 选择")
            self._sheet_selections[file_type] = None
            return None

        try:
            # [优化] 直接用 ZIP 读取 workbook.xml 获取 Sheet 名称（比 openpyxl 快）
            logger.debug(f"[_handle_multi_sheet_selection] 读取 workbook.xml...")
            with zipfile.ZipFile(file_path) as z:
                all_names = z.namelist()
                # 动态查找 workbook.xml
                wb_path = None
                for name in all_names:
                    if name.endswith('workbook.xml') and '_rels' not in name:
                        wb_path = name
                        break
                if not wb_path:
                    raise ValueError("无法在 zip 内找到 workbook.xml")
                wb_xml = z.read(wb_path).decode('utf-8', errors='ignore')
            
            # 解析 <sheet name="Sheet1" sheetId="1" r:id="rId1"/> 格式
            sheet_names = re.findall(r'<sheet[^>]+name="([^"]+)"', wb_xml)
            logger.debug(f"[_handle_multi_sheet_selection] 检测到 {len(sheet_names)} 个 Sheet: {sheet_names}")

            if len(sheet_names) <= 1:
                # 单 Sheet 无需选择
                logger.debug(f"[_handle_multi_sheet_selection] 单 Sheet，直接使用: {sheet_names[0] if sheet_names else None}")
                self._sheet_selections[file_type] = sheet_names[0] if sheet_names else None
                return self._sheet_selections[file_type]

            # [FIX v1.0.6] Bug 3: 多 Sheet 强制弹窗，禁止静默默认
            logger.debug(f"[_handle_multi_sheet_selection] 多 Sheet，弹出选择对话框...")
            self._log_message("INFO", f"[{file_type}] 检测到 {len(sheet_names)} 个Sheet，弹出选择窗口")
            dialog = SheetSelectDialog(sheet_names, parent=self)
            dialog.exec_()

            if dialog.was_auto_selected():
                # 超时自动选择（用户未响应）
                selected = sheet_names[0]
                logger.debug(f"[_handle_multi_sheet_selection] 选择超时，自动使用: {selected}")
                self._log_message("WARNING", f"[{file_type}] 选择超时，自动使用: {selected}")
            else:
                selected = dialog.get_selected_sheet()
                if selected:
                    logger.debug(f"[_handle_multi_sheet_selection] 用户选择 Sheet: {selected}")
                    self._log_message("INFO", f"[{file_type}] 用户选择 Sheet: {selected}")
                else:
                    # 用户取消
                    logger.debug(f"[_handle_multi_sheet_selection] 用户取消选择")
                    return None

            self._sheet_selections[file_type] = selected
            return selected

        except Exception as e:
            logger.error(f"[_handle_multi_sheet_selection] 检测 Sheet 失败: {e}")
            self._sheet_selections[file_type] = None
            return None

    def _import_spec_file(self):
        """
        [FIX v1.0.6] Bug 1: 导入字段规范文件

        使用 SpecImportDialog 让用户选择属性导入模版 Excel 文件，
        解析后生成 field_spec.yaml 并保存路径到配置。
        """
        dialog = SpecImportDialog(parent=self)
        if dialog.exec_() != dialog.Accepted:
            return

        output_path = dialog.get_output_path()
        imported_count = dialog.get_imported_count()

        if output_path:
            import os
            # 保存原始 Excel 文件名用于显示
            excel_path = dialog.get_excel_path()
            self._last_spec_excel_name = os.path.basename(excel_path) if excel_path else os.path.basename(output_path)

            self.file_paths["spec"] = output_path
            self.file_inputs["spec"].setText(self._last_spec_excel_name)
            self.file_inputs["spec"].setStyleSheet("")
            self.file_labels["spec"].setText(f"已导入：{imported_count} 个字段")

            self._log_message("INFO", f"字段规范已导入：{imported_count} 个字段 ({self._last_spec_excel_name})")
            logger.info(f"字段规范已导入: {output_path}, {imported_count} 个字段")

            # 更新开始按钮状态
            self._update_start_button_state()

    def _get_excel_sheet_names(self, file_path: str) -> list:
        """获取Excel文件的所有Sheet名称"""
        import openpyxl
        try:
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            sheet_names = wb.sheetnames
            wb.close()
            return sheet_names
        except Exception as e:
            logger.warning(f"读取Excel Sheet列表失败: {e}")
            return []

    def _get_file_info(self, file_path: str, sheet_name: str = None) -> dict:
        """
        获取文件基本信息（列数、行数）
        [优化 v1.0.7] CSV 使用 line count，Excel 直接解析 XML（比 openpyxl 快 5-10 倍）
        
        Parameters
        ----------
        file_path : str
            文件路径
        sheet_name : str, optional
            指定要读取的 sheet 名称。如果为 None，则读取第一个 sheet。
        """
        import re, zipfile
        import time as time_module

        logger.info(f"[_get_file_info] ★★★ 开始获取文件信息: {file_path}, sheet={sheet_name}")
        t0 = time_module.time()

        result = {"cols": 0, "rows": 0}
        
        try:
            if str(file_path).lower().endswith('.csv'):
                # CSV 文件：逐行计数（无需加载数据到内存）
                logger.info(f"[_get_file_info] [1] CSV 文件，开始逐行计数...")
                t1 = time_module.time()
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    result["rows"] = sum(1 for _ in f) - 1  # 减去表头
                logger.info(f"[_get_file_info] [2] CSV 行数: {result['rows']}, 耗时: {time_module.time()-t1:.3f}s")

                # 列数用 Polars 只读表头
                logger.info(f"[_get_file_info] [3] CSV 文件，使用 Polars 读取表头...")
                t1 = time_module.time()
                df = pl.scan_csv(file_path).head(1).collect()
                result["cols"] = len(df.columns)
                logger.info(f"[_get_file_info] [4] CSV 列数: {result['cols']}, 耗时: {time_module.time()-t1:.3f}s")
            else:
                # Excel 文件：直接解析 XLSX XML 获取行数（快 5-10 倍）
                logger.info(f"[_get_file_info] [1] Excel 文件，直接解析 XLSX XML...")
                t1 = time_module.time()
                
                # 获取 zip 内所有文件，动态查找 workbook.xml 位置
                with zipfile.ZipFile(file_path) as z:
                    all_names = z.namelist()
                    logger.info(f"[_get_file_info] [2] zip 内文件数: {len(all_names)}, 文件列表: {all_names}")
                    
                    # 动态查找 workbook.xml（可能在 xl/ 或根目录）
                    wb_path = None
                    rels_path = None
                    for name in all_names:
                        if name.endswith('workbook.xml') and '_rels' not in name:
                            wb_path = name
                            # 同级目录下找 _rels
                            base_dir = name.rsplit('/', 1)[0] if '/' in name else ''
                            rels_path = f"{base_dir}/_rels/workbook.xml.rels" if base_dir else "_rels/workbook.xml.rels"
                            logger.info(f"[_get_file_info] [3] 找到 workbook.xml: {wb_path}, rels_path: {rels_path}")
                            break
                    
                    if not wb_path:
                        logger.error(f"[_get_file_info] [ERROR] 找不到 workbook.xml，可用文件: {all_names}")
                        raise ValueError(f"无法在 zip 内找到 workbook.xml，可用文件: {all_names[:20]}...")
                    
                    logger.info(f"[_get_file_info] [4] 读取 workbook.xml: {wb_path}")
                    wb_xml = z.read(wb_path).decode('utf-8', errors='ignore')
                    sheet_rid_map = dict(re.findall(r'<sheet[^>]+name="([^"]+)"[^>]+r:id="([^"]+)"', wb_xml))
                    logger.info(f"[_get_file_info] [5] sheet_rid_map: {sheet_rid_map}")
                    
                    # 读取 rels 获取 rId -> 文件名映射
                    logger.info(f"[_get_file_info] [6] 读取 rels: {rels_path}")
                    rels_xml = z.read(rels_path).decode('utf-8', errors='ignore')
                    # 遍历提取 Id 和 Target（属性顺序可能不同）
                    rid_file_map = {}
                    for rel_match in re.finditer(r'<Relationship[^>]+>', rels_xml):
                        rel_text = rel_match.group()
                        id_val = re.search(r'\bId="([^"]+)"', rel_text)
                        target_val = re.search(r'\bTarget="([^"]+)"', rel_text)
                        if id_val and target_val:
                            rid_file_map[id_val.group(1)] = target_val.group(1)
                    logger.info(f"[_get_file_info] [7] rid_file_map: {rid_file_map}")
                    
                    # worksheet 所在目录
                    ws_dir = wb_path.rsplit('/', 1)[0] if '/' in wb_path else ''
                    if ws_dir:
                        ws_dir += '/'
                    logger.info(f"[_get_file_info] [8] ws_dir: '{ws_dir}'")
                    
                    # 确定要读取的 worksheet
                    sheet_file = None
                    if sheet_name and sheet_name in sheet_rid_map:
                        rid = sheet_rid_map[sheet_name]
                        sheet_file = rid_file_map.get(rid, "")
                        # 去掉 Target 中的前导 /
                        if sheet_file.startswith('/'):
                            sheet_file = sheet_file[1:]
                        # 如果是相对路径（不包含 /），加上 ws_dir
                        if '/' not in sheet_file:
                            sheet_file = ws_dir + sheet_file
                        logger.info(f"[_get_file_info] [9] 使用指定 sheet: {sheet_name} -> {sheet_file}")
                    else:
                        # 默认读取第一个 sheet
                        first_sheet_name = list(sheet_rid_map.keys())[0] if sheet_rid_map else None
                        logger.info(f"[_get_file_info] [9] 未指定 sheet 或找不到，使用第一个: {first_sheet_name}")
                        if not first_sheet_name:
                            raise ValueError("未找到 sheet")
                        rid = sheet_rid_map[first_sheet_name]
                        sheet_file = rid_file_map.get(rid, "")
                        # 去掉 Target 中的前导 /
                        if sheet_file.startswith('/'):
                            sheet_file = sheet_file[1:]
                        # 如果是相对路径（不包含 /），加上 ws_dir
                        if '/' not in sheet_file:
                            sheet_file = ws_dir + sheet_file
                        logger.info(f"[_get_file_info] [10] 最终 sheet_file: {sheet_file}")
                    
                    logger.info(f"[_get_file_info] [11] 读取 worksheet: {sheet_file}")
                    xml_content = z.read(sheet_file).decode('utf-8', errors='ignore')
                    xml_size_kb = len(xml_content) / 1024
                    logger.info(f"[_get_file_info] [12] worksheet XML 完成: {sheet_file}, 大小: {xml_size_kb:.1f} KB")
                
                # 统计 <row 标签数量获取行数
                t1 = time_module.time()
                result["rows"] = len(re.findall(r'<row ', xml_content))
                logger.info(f"[_get_file_info] [13] Excel 行数: {result['rows']}, 耗时: {time_module.time()-t1:.3f}s")
                
                # 解析第一行获取列数（从 <row r="1" 开始到第一个 </row>）
                t1 = time_module.time()
                first_row_match = re.search(r'<row[^>]+r="1"[^>]*>(.*?)</row>', xml_content, re.DOTALL)
                if first_row_match:
                    # 统计 <c r="XX" 格式的单元格数量
                    result["cols"] = len(re.findall(r'<c r="[A-Z]+', first_row_match.group(1)))
                    logger.info(f"[_get_file_info] [14] Excel 列数: {result['cols']}")
            
            logger.info(f"[_get_file_info] [15] 完成: cols={result['cols']}, rows={result['rows']}, 总耗时: {time_module.time()-t0:.3f}s")
        except Exception as e:
            logger.warning(f"[_get_file_info] [ERROR] 获取文件信息失败: {e}")
        
        return result

    @pyqtSlot(str, object, object, str)
    def _update_file_info_safe(self, file_type: str, file_info: dict, selected_sheet, basename: str):
        """[优化] 在主线程安全更新文件信息UI（由后台线程调用）"""
        logger.debug(f"[_update_file_info_safe] 收到文件信息更新: file_type={file_type}, file_info={file_info}, basename={basename}")
        try:
            if file_info and (file_info.get('rows', 0) > 0 or file_info.get('cols', 0) > 0):
                sheet_info = f" | Sheet: {selected_sheet}" if selected_sheet else ""
                info_text = f"✅ 已识别：{file_info['cols']} 列，{file_info['rows']:,} 行{sheet_info}"
                logger.debug(f"[_update_file_info_safe] 更新UI标签: {info_text}")
                self.file_labels[file_type].setText(info_text)
                # 显示成功状态
                self._animate_file_info_success(file_type)
                self._log_message("INFO", f"已选择 [{file_type}]: {basename} ({file_info['rows']:,} 行){sheet_info}")
                logger.debug(f"[_update_file_info_safe] UI更新完成")
            else:
                # 数据无效（0行0列），显示警告
                logger.warning(f"[_update_file_info_safe] 文件数据无效，跳过信息展示")
                self.file_labels[file_type].setText("⚠️ 文件读取失败，请检查文件格式")
        except Exception as e:
            logger.warning(f"[_update_file_info_safe] 更新文件信息UI失败: {e}")
            self.file_labels[file_type].setText("✅ 已选择")
            self._animate_file_info_success(file_type)

    def _validate_inputs(self) -> tuple[bool, str]:
        """验证输入文件"""
        if not self.file_paths.get("frontline"):
            return False, "请选择一线人员名单文件（必选）"
        if not self.file_paths.get("dict"):
            return False, "请选择数据字典文件（必选）"
        return True, ""

    def _select_output_dir(self):
        """选择输出目录"""
        dir_path = QFileDialog.getExistingDirectory(
            self,
            "选择输出目录",
            "",
            QFileDialog.ShowDirsOnly | QFileDialog.DontResolveSymlinks
        )

        if dir_path:
            self.txt_output.setText(dir_path)
            self._log_message("INFO", f"输出目录: {dir_path}")

    def _update_start_button_state(self):
        """[ISSUE-14] 更新开始按钮状态"""
        # 检查必填文件是否已选择
        frontline = self.file_paths.get("frontline")
        dict_file = self.file_paths.get("dict")

        if frontline and dict_file:
            self.btn_start.setEnabled(True)
        else:
            self.btn_start.setEnabled(False)

    def _on_start_processing(self):
        """开始处理"""
        import os
        from datetime import datetime

        # 再次检查文件
        frontline_path = self.file_paths.get("frontline")
        dict_path = self.file_paths.get("dict")

        if not frontline_path:
            QMessageBox.warning(self, "提示", "请选择一线人员名单文件")
            return

        if not dict_path:
            QMessageBox.warning(self, "提示", "请选择数据字典文件")
            return

        # 准备输入文件字典
        input_files = {
            "yixian": frontline_path,
            "sanfang": self.file_paths.get("third_party"),
            "hw": self.file_paths.get("hw"),
        }

        # 确定去重字段
        dedup_field = getattr(self, '_dedup_field', None)
        if not dedup_field:
            # 尝试自动识别
            # [FIX v1.0.7] 从 _sheet_selections 获取用户选择的 sheet
            selected_sheet = getattr(self, '_sheet_selections', {}).get("frontline")
            columns = self._get_excel_columns(frontline_path, selected_sheet)
            if columns:
                dedup_field = self._auto_detect_dedup_field(columns)
                self._dedup_field = dedup_field
                self.lbl_dedup_field.setText(dedup_field)
                self._log_message("INFO", f"自动识别去重字段: {dedup_field}")

        # 确定输出目录
        output_dir = self.txt_output.text().strip()
        if not output_dir:
            # 默认使用一线名单所在目录
            output_dir = os.path.dirname(frontline_path)

        # 收集勾选的模块
        selected_modules = []
        if self.checkboxes["field_validation"].isChecked():
            selected_modules.append("field_validation")
        if self.checkboxes["cross_dedup"].isChecked():
            selected_modules.append("cross_dedup")
        if self.checkboxes["dict_encode"].isChecked():
            selected_modules.append("dict_encode")
        if self.checkboxes["dict_validation"].isChecked():
            selected_modules.append("dict_validation")
        if self.checkboxes["internal_dedup"].isChecked():
            selected_modules.append("internal_dedup")

        if not selected_modules:
            QMessageBox.warning(self, "提示", "请至少选择一个执行模块")
            return

        # 更新UI状态
        self.btn_start.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.progress_container.setVisible(True)
        self.result_banner.setVisible(False)
        self.log_text.clear()
        self._log_message("INFO", "=" * 50)
        self._log_message("INFO", f"开始处理批次: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._log_message("INFO", f"输出目录: {output_dir}")
        self._log_message("INFO", f"执行模块: {', '.join(selected_modules)}")

        # [20260420-老谈] 保存当前配置作为默认值
        self._save_default_config()

        # 创建处理上下文
        context = ProcessContext()
        context.input_files = input_files
        context.dict_file_path = dict_path
        context.spec_file_path = self.file_paths.get("spec")
        context.dedup_field = dedup_field
        context.output_path = output_dir

        # 构建模块列表
        modules = self._build_modules(selected_modules)

        # 创建工作线程
        self.worker = ProcessingWorker(
            modules=modules,
            context=context,
            progress_callback=self._on_progress_update
        )
        self.worker.finished.connect(self._on_processing_finished)
        self.worker.progress_updated.connect(self._on_progress_update)

        # 启动处理线程
        self.worker.start()

    def _build_modules(self, selected_modules: list):
        """
        [FIX v1.0.6] Bug 4: 重构模块执行顺序

        PRD §6.2 规定的执行顺序：
        F1（文件加载，必选）
          → F2（字段合规检查）
          → F6（名单内部重复检查）
          → F4（数据字典上码）
          → F5（字典值合规校验）
          → F3（跨名单去重标注）
          → F7（结果输出，必选）

        关键规则：F4 先行、F5 后行。F4 上码后 Code 列为"未匹配"的记录即为 F5 不合规记录。
        """
        from modules.f1_loader import FileLoaderModule
        from modules.f2_field_validator import FieldValidatorModule
        from modules.f3_priority_dedup import PriorityDedupModule
        from modules.f4_dict_encoder import DictEncoderModule
        from modules.f5_dict_validator import DictValidatorModule
        from modules.f6_internal_dedup import InternalDedupModule
        from modules.f7_output_exporter import OutputExporterModule

        modules = []

        # F1: 文件加载（始终执行）
        f1_module = FileLoaderModule()
        # [FIX v1.0.6] Bug 3: 传递预选的 Sheet 名称给 F1
        if hasattr(self, '_sheet_selections'):
            f1_module._pre_selected_sheets = self._sheet_selections.copy()
        modules.append(f1_module)

        # [FIX v1.0.6] Bug 4: 按 PRD §6.2 固定顺序构建模块列表
        # 优先级：field_validation > internal_dedup > dict_encode > dict_validation > cross_dedup
        execution_order = ["field_validation", "internal_dedup", "dict_encode", "dict_validation", "cross_dedup"]

        for module_key in execution_order:
            if module_key in selected_modules:
                module_mapping = {
                    "field_validation": FieldValidatorModule,
                    "cross_dedup": PriorityDedupModule,
                    "dict_encode": DictEncoderModule,
                    "dict_validation": DictValidatorModule,
                    "internal_dedup": InternalDedupModule,
                }
                modules.append(module_mapping[module_key]())

        # F7: 输出导出（始终执行）
        modules.append(OutputExporterModule())

        return modules

    def _on_progress_update(self, module_name: str, percent: int):
        """进度更新回调"""
        self.lbl_module.setText(f"{module_name}... {percent}%")
        self.progress_panel.on_progress(module_name, percent)

    def _on_processing_finished(self, context: ProcessContext):
        """处理完成回调"""
        self._log_message("INFO", "=" * 50)
        self._log_message("INFO", "处理完成！")

        # 保存上下文供按钮使用
        self._last_context = context

        # 更新结果横幅
        summary = context.summary
        total_input = summary.get("total_input_records", 0)
        total_output = summary.get("total_output_records", 0)
        total_errors = summary.get("total_error_records", 0)

        if total_errors == 0:
            # 成功状态
            banner_msg = f"处理完成，一线名单: {total_input} 条 | 输出: {total_output} 条"
            self._show_result_banner("success", banner_msg)
        else:
            # 有错误状态
            banner_msg = f"处理完成，一线名单: {total_input} 条 | 输出: {total_output} 条 | 错误: {total_errors} 条"
            self._show_result_banner("warning", banner_msg, total_errors)

        self.result_banner.setVisible(True)

        # [FIX] 保存输出目录到历史记录
        try:
            from db.dao.processing_history import ProcessingHistoryDAO
            if hasattr(context, 'run_id') and context.run_id:
                # 更新历史记录中的输出目录
                output_dir = summary.get("output_dir")
                if output_dir:
                    # complete_run 已经在 orchestrator 中调用
                    pass
        except Exception as e:
            logger.warning(f"更新历史输出目录失败: {e}")

        # 禁用取消按钮
        self.btn_cancel.setEnabled(False)

        # 延迟重置界面
        self._reset_after_processing()

    def _on_processing_error(self, error_msg: str):
        """处理错误回调"""
        self._log_message("ERROR", f"处理失败: {error_msg}")
        show_critical_error(self, "处理失败", error_msg)
        self.btn_start.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        self.progress_container.setVisible(False)

    def _on_cancel_processing(self):
        """取消处理"""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self, "确认取消", "确定要取消当前处理吗？",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                logger.info("用户取消处理")
                self.worker.requestInterruption()

    def _on_view_result(self):
        """查看结果"""
        # 获取最新的输出文件
        output_dir = self.txt_output.text().strip()
        if not output_dir:
            output_dir = os.path.dirname(self.file_paths.get("frontline", ""))

        # 打开输出目录
        if os.path.exists(output_dir):
            os.startfile(output_dir) if os.name == "nt" else subprocess.run(["open", output_dir])
        else:
            QMessageBox.warning(self, "提示", f"输出目录不存在: {output_dir}")

    def _on_show_history(self):
        """显示历史对话框"""
        dialog = HistoryDialog(self)
        dialog.exec_()

    def _on_show_version(self):
        """显示版本信息"""
        dialog = VersionDialog(self)
        dialog.exec_()

    def _show_result_viewer(self, context: ProcessContext):
        """显示结果查看器"""
        if self.result_viewer is None:
            self.result_viewer = ResultViewerDialog(context, self)
        self.result_viewer.show()
        self.result_viewer.raise_()
        self.result_viewer.activateWindow()

    def _set_inputs_enabled(self, enabled: bool):
        """设置输入控件启用状态"""
        for le in self.file_inputs.values():
            if le.isEnabled() or not enabled:
                le.setEnabled(enabled)
        for btn in self.file_buttons.values():
            btn.setEnabled(enabled)
        # 去重字段选择按钮
        if hasattr(self, 'lbl_dedup_field'):
            self.lbl_dedup_field.setEnabled(enabled)
        self.txt_output.setEnabled(enabled)

    def closeEvent(self, event):
        """窗口关闭事件"""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self, "确认退出", "处理正在进行中，确定要退出吗？",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.No:
                event.ignore()
                return
            self.worker.requestInterruption()
            self.worker.wait(3000)

        if self.result_viewer:
            self.result_viewer.close()

        event.accept()
        logger.info("主窗口关闭")

    # ==================== 配置保存/加载 ====================

    def _save_default_config(self):
        """[20260420-老谈] ISSUE-13: 保存当前配置作为默认值"""
        import os
        from datetime import datetime
        from db.dao.app_config import AppConfigDAO

        current_time = datetime.now().isoformat()

        # 保存输出目录
        output_dir = self.txt_output.text().strip()
        if output_dir:
            AppConfigDAO.set("last_output_dir", output_dir, "上次使用的输出目录")

        # 保存去重字段
        dedup_field = getattr(self, '_dedup_field', None)
        if dedup_field:
            AppConfigDAO.set("last_dedup_field", dedup_field, "上次使用的去重字段")

        # 保存字典配置
        dict_path = self.file_paths.get("dict")
        if dict_path:
            AppConfigDAO.set("last_dict_file_path", dict_path, "上次使用的字典文件路径")
            AppConfigDAO.set("last_dict_md5", getattr(self, '_last_dict_md5', ''), "上次字典文件的MD5")
            AppConfigDAO.set("last_dict_import_time", current_time, "上次导入字典的时间")

        # 保存字段规范配置
        spec_path = self.file_paths.get("spec")
        if spec_path:
            AppConfigDAO.set("last_spec_file_path", spec_path, "上次使用的字段规范文件路径")
            # [20260420-老谈] 保存原始Excel文件名（用于显示）
            spec_excel_name = getattr(self, '_last_spec_excel_name', None)
            if not spec_excel_name:
                # 如果没有保存过，从路径获取（兼容旧数据）
                spec_excel_name = os.path.basename(spec_path)
            AppConfigDAO.set("last_spec_excel_name", spec_excel_name, "上次导入的原始Excel文件名")
            # 保存导入时间
            AppConfigDAO.set("last_spec_import_time", current_time, "上次导入字段规范的时间")

        logger.info("配置已保存为默认值")

    def _load_default_config(self):
        """[20260420-老谈] ISSUE-13: 从数据库加载上次的配置"""
        import os
        from datetime import datetime
        from db.dao.app_config import AppConfigDAO

        # 加载输出目录
        last_output_dir = AppConfigDAO.get("last_output_dir")
        if last_output_dir and os.path.exists(last_output_dir):
            self.txt_output.setText(last_output_dir)

        # 加载去重字段
        last_dedup_field = AppConfigDAO.get("last_dedup_field")
        if last_dedup_field:
            self._dedup_field = last_dedup_field
            self.lbl_dedup_field.setText(last_dedup_field)
            self.lbl_dedup_field.setStyleSheet("""
                font-size: 13px;
                font-weight: 500;
                color: #111827;
                background-color: #F9FAFB;
                border: 1px solid #E5E7EB;
                border-radius: 6px;
                padding: 0 12px;
            """)

        # 加载字典配置
        last_dict_path = AppConfigDAO.get("last_dict_file_path")
        last_dict_time = AppConfigDAO.get("last_dict_import_time")
        last_dict_md5 = AppConfigDAO.get("last_dict_md5")
        if last_dict_path and os.path.exists(last_dict_path):
            self.file_paths["dict"] = last_dict_path
            self.file_inputs["dict"].setText(os.path.basename(last_dict_path))
            self.file_inputs["dict"].setStyleSheet("")
            
            # 兼容处理：如果是32位MD5则取前8位
            if last_dict_md5:
                last_dict_md5 = last_dict_md5[:8] if len(last_dict_md5) > 8 else last_dict_md5
            self._last_dict_md5 = last_dict_md5 or ""
            
            # 显示导入时间（与字段规范格式一致）
            time_info = ""
            if last_dict_time:
                try:
                    dt = datetime.fromisoformat(last_dict_time)
                    time_info = f" | 导入: {dt.strftime('%Y-%m-%d %H:%M')}"
                except Exception:
                    time_info = f" | 导入: {last_dict_time}"
            
            display_md5 = f"v{last_dict_md5 or '?'}" if last_dict_md5 else ""
            self._update_window_title(f"字典 {display_md5} (默认值)")
            self._log_message("INFO", f"已自动加载字典：{os.path.basename(last_dict_path)} {display_md5}{time_info}")
            logger.info(f"自动加载字典: {last_dict_path}")

        # 加载字段规范配置
        last_spec_path = AppConfigDAO.get("last_spec_file_path")
        last_spec_time = AppConfigDAO.get("last_spec_import_time")
        # [20260420-老谈] 加载原始Excel文件名（用于显示）
        last_spec_excel_name = AppConfigDAO.get("last_spec_excel_name")
        if last_spec_path and os.path.exists(last_spec_path):
            self.file_paths["spec"] = last_spec_path
            # [20260420-老谈] 显示原始Excel文件名，而非转换后的YAML文件名
            display_name = last_spec_excel_name if last_spec_excel_name else os.path.basename(last_spec_path)
            self.file_inputs["spec"].setText(display_name)
            self.file_inputs["spec"].setStyleSheet("")
            
            # 显示导入时间
            time_info = ""
            if last_spec_time:
                try:
                    dt = datetime.fromisoformat(last_spec_time)
                    time_info = f" | 导入: {dt.strftime('%Y-%m-%d %H:%M')}"
                except Exception:
                    time_info = f" | 导入: {last_spec_time}"
            
            self.file_labels["spec"].setText(f"✅ 默认值{time_info}")
            # [20260420-老谈] 保存显示用的文件名，以便后续保存配置时使用
            self._last_spec_excel_name = display_name
            self._log_message("INFO", f"已自动加载字段规范：{display_name}{time_info}")
            logger.info(f"自动加载字段规范: {last_spec_path}, 显示名: {display_name}")

        # 更新开始按钮状态
        self._update_start_button_state()

from datetime import datetime
import os
import subprocess
