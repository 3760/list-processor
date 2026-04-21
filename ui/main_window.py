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

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QAction,
    QApplication,
    QCheckBox,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMenuBar,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QSpacerItem,
    QStatusBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from core.context import ProcessContext
from core.orchestrator import ProcessOrchestrator
from infra.exceptions import CriticalError
from infra.log_manager import get_logger
from modules.f1_loader import FileLoaderModule
from modules.f2_field_validator import FieldValidatorModule
from modules.f3_priority_dedup import PriorityDedupModule
from modules.f4_dict_encoder import DictEncoderModule
from modules.f5_dict_validator import DictValidatorModule
from modules.f6_internal_dedup import InternalDedupModule
from modules.f7_output_exporter import OutputExporterModule
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
WINDOW_WIDTH = 1100
WINDOW_HEIGHT = 1500
WINDOW_MIN_WIDTH = 1050
WINDOW_MIN_HEIGHT = 980
WINDOW_MAX_WIDTH = 1600
WINDOW_MAX_HEIGHT = 1500
WINDOW_TITLE = "客户名单数据预处理工具 v1.0.6"

# ============================================================
# 布局间距常量
# ============================================================
MARGIN_MAIN = (20, 20, 20, 16)       # 主布局外边距 (上右下左)
MARGIN_SECTION = (16, 30, 16, 16)   # 区块内边距（标题占用30px）
MARGIN_SECTION_NARROW = (16, 25, 16, 16)  # 文件加载区块
MARGIN_ROW = (0, 0, 0, 0)           # 行Widget无边距
MARGIN_BANNER = (16, 12, 16, 12)    # 结果横幅
MARGIN_ACTION_BAR = (0, 10, 0, 0)   # 底部操作区

SPACING_MAIN = 16                   # 区块之间间距
SPACING_ROW = 15                    # 行内间距（标签与输入框）
SPACING_MODULE_ROW = 30             # 模块区块行内间距（复选框之间）
SPACING_BANNER = 12                 # 横幅内部间距
SPACING_CHECKBOX = 30               # 复选框之间间距

# ============================================================
# 高度常量
# ============================================================
HEIGHT_GROUP_FILE = 220             # 文件加载区块
HEIGHT_GROUP_CONFIG = 150           # 处理配置区块
HEIGHT_GROUP_MODULE = 220           # 执行模块区块
HEIGHT_PROGRESS = 300              # 处理进度区块
HEIGHT_BANNER = 50                  # 结果横幅
HEIGHT_LOG = 120                   # 日志区域
HEIGHT_ROW_NORMAL = 50              # 普通行高度
HEIGHT_ROW_MODULE = 44              # 模块行高度
HEIGHT_ELEMENT = 40                 # 控件元素高度（输入框、按钮）
HEIGHT_CHECKBOX = 36                # 复选框高度
HEIGHT_BUTTON_BANNER = 32           # 横幅按钮高度
HEIGHT_BUTTON_LARGE = 44            # 大按钮高度（开始处理）
HEIGHT_CLOSE_BANNER = 24            # 关闭按钮尺寸


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

        self._init_ui()
        self._init_status_bar()
        self._apply_stylesheet()
        
        # [20260420-老谈] ISSUE-14: 初始化时按钮应置灰（等待必填文件选择）
        self._update_start_button_state()
        
        # [20260420-老谈] 优化2.2: 从数据库加载上次使用的字典配置作为默认值
        self._load_default_config()
        
        logger.info("主窗口初始化完成")

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
            from PyQt5.QtWidgets import QApplication
            # macOS: 通过 NSApp.userInterfaceLayoutDirection 检测
            # 或者通过系统外观设置
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
        self.setWindowTitle(WINDOW_TITLE)
        self.setMinimumSize(WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT)
        self.setMaximumSize(WINDOW_MAX_WIDTH, WINDOW_MAX_HEIGHT)
        self.resize(WINDOW_WIDTH, WINDOW_HEIGHT)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # 创建菜单栏
        self._create_menu_bar()

        # 创建中心部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 主布局
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(*MARGIN_MAIN)
        main_layout.setSpacing(SPACING_MAIN)

        # 1. 文件加载区块
        main_layout.addWidget(self._create_file_section())

        # 2. 处理配置区块
        main_layout.addWidget(self._create_config_section())

        # 3. 执行模块区块
        main_layout.addWidget(self._create_module_section())

        # 4. 处理进度区块
        self.progress_container = self._create_progress_section()
        main_layout.addWidget(self.progress_container)

        # 5. 结果横幅（默认隐藏）
        self.result_banner = self._create_result_banner()
        self.result_banner.setVisible(False)
        main_layout.addWidget(self.result_banner)

        # 6. 底部操作区
        main_layout.addWidget(self._create_action_bar())

        # 6. 弹性空间
        main_layout.addSpacerItem(QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Expanding))

    def _create_section_title(self, title: str, parent: QWidget = None) -> QLabel:
        """创建区块标题（带 emoji）"""
        label = QLabel(title)
        label.setStyleSheet("""
            QLabel {
                font-size: 14px;
                font-weight: 600;
                color: #111827;
                padding-bottom: 8px;
            }
        """)
        return label

    def _create_file_section(self) -> QGroupBox:
        """创建文件加载区块"""
        group = QGroupBox()
        group.setTitle("📁 文件加载")
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        group.setMinimumHeight(HEIGHT_GROUP_FILE)

        layout = QGridLayout()
        layout.setContentsMargins(*MARGIN_SECTION_NARROW)
        layout.setSpacing(12)
        
        # [FIX] 设置列拉伸因子，防止缩小时控件重叠
        # 列: 标签(0) | 输入框(1) | 输入框(2) | 按钮(3) | 信息(4) | 信息(5)
        layout.setColumnStretch(0, 0)   # 标签列不拉伸
        layout.setColumnStretch(1, 3)   # 输入框列拉伸
        layout.setColumnStretch(2, 0)   # 空列不拉伸
        layout.setColumnStretch(3, 0)   # 按钮列不拉伸
        layout.setColumnStretch(4, 2)   # 信息列少量拉伸
        layout.setColumnStretch(5, 0)   # 额外空间

        # 文件配置：key, 标签, 提示, 是否必填
        file_configs = [
            ("frontline", "一线人员名单", "请选择文件...", True),
            ("third_party", "三方系统名单", "（可选）", False),
            ("hw", "HW系统名单", "（可选）", False),
            ("dict", "数据字典", "请选择文件...", True),
            ("spec", "字段规范", "config/field_spec.yaml", False),
        ]

        self.file_inputs = {}
        self.file_labels = {}
        self.file_buttons = {}

        for row, (key, label_text, placeholder, required) in enumerate(file_configs):
            # 标签
            lbl = QLabel(label_text)
            if required:
                lbl.setText(f"{label_text} *")
                lbl.setStyleSheet("font-size: 13px; font-weight: 500; color: #111827;")
            else:
                lbl.setStyleSheet("font-size: 13px; color: #6B7280;")
            layout.addWidget(lbl, row, 0, 1, 1)

            # 输入框 - [FIX] 设置最小宽度，防止被压缩
            le = QLineEdit()
            le.setPlaceholderText(placeholder)
            le.setMinimumWidth(200)  # 输入框最小宽度
            if not required:
                le.setDisabled(True)
            le.setObjectName(f"txt{key.capitalize()}")
            layout.addWidget(le, row, 1, 1, 2)
            self.file_inputs[key] = le

            # 浏览/导入按钮 - [FIX] 设置最小宽度
            btn_text = "导入" if key == "spec" else "浏览"
            btn = QPushButton(btn_text)
            btn.setObjectName("btnBrowse")
            btn.setMinimumWidth(70)  # 按钮最小宽度
            btn.setMaximumWidth(90)
            btn.clicked.connect(lambda checked, k=key: self._select_file(k))
            layout.addWidget(btn, row, 3)
            self.file_buttons[key] = btn

            # 文件信息标签 - [FIX] 设置最小宽度
            info_lbl = QLabel()
            info_lbl.setObjectName("fileInfo")
            info_lbl.setStyleSheet("font-size: 12px; color: #6B7280;")
            info_lbl.setMinimumWidth(100)
            layout.addWidget(info_lbl, row, 4, 1, 2)
            self.file_labels[key] = info_lbl

        # [20260420-老谈] ISSUE-14: 开始处理按钮初始应置灰（等待必填文件选择）
        # [20260420-老谈] ISSUE-13: 添加字典版本显示区域（加载字典后更新）
        row = len(file_configs)
        lbl_version = QLabel("字典版本：")
        lbl_version.setStyleSheet("font-size: 13px; font-weight: 500; color: #111827;")
        layout.addWidget(lbl_version, row, 0, 1, 1)
        
        self.lbl_dict_version = QLabel("（未加载）")
        self.lbl_dict_version.setObjectName("dictVersionLabel")
        self.lbl_dict_version.setStyleSheet("font-size: 13px; color: #6B7280;")
        layout.addWidget(self.lbl_dict_version, row, 1, 1, 3)

        group.setLayout(layout)
        return group

    def _create_config_section(self) -> QGroupBox:
        """创建处理配置区块"""
        group = QGroupBox()
        group.setTitle("⚙️ 处理配置")
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        group.setMinimumHeight(HEIGHT_GROUP_CONFIG)

        # 使用 QVBoxLayout + QHBoxLayout 嵌套，确保每行独立布局
        layout = QVBoxLayout()
        layout.setContentsMargins(*MARGIN_SECTION)
        layout.setSpacing(20)

        # ========== 第一行：去重字段 ==========
        row1_widget = QWidget()
        row1_widget.setFixedHeight(HEIGHT_ROW_NORMAL)
        row1_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row1_layout = QHBoxLayout(row1_widget)
        row1_layout.setContentsMargins(*MARGIN_ROW)
        row1_layout.setSpacing(SPACING_ROW)

        lbl_dedup = QLabel("去重字段")
        lbl_dedup.setStyleSheet("font-size: 13px; font-weight: 500; color: #111827;")
        lbl_dedup.setFixedWidth(75)
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
        btn_select_dedup.setFixedSize(80, HEIGHT_ELEMENT)
        btn_select_dedup.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        btn_select_dedup.clicked.connect(self._select_dedup_field)
        row1_layout.addWidget(btn_select_dedup)

        layout.addWidget(row1_widget)

        # ========== 第二行：输出目录 ==========
        row2_widget = QWidget()
        row2_widget.setFixedHeight(HEIGHT_ROW_NORMAL)
        row2_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row2_layout = QHBoxLayout(row2_widget)
        row2_layout.setContentsMargins(*MARGIN_ROW)
        row2_layout.setSpacing(SPACING_ROW)

        lbl_output = QLabel("输出目录")
        lbl_output.setStyleSheet("font-size: 13px; font-weight: 500; color: #111827;")
        lbl_output.setFixedWidth(75)
        lbl_output.setFixedHeight(HEIGHT_ELEMENT)
        lbl_output.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        row2_layout.addWidget(lbl_output)

        self.txt_output = QLineEdit()
        self.txt_output.setPlaceholderText("默认：输入文件所在目录")
        self.txt_output.setFixedHeight(HEIGHT_ELEMENT)
        self.txt_output.setMinimumWidth(200)
        self.txt_output.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row2_layout.addWidget(self.txt_output, 1)

        btn_output = QPushButton("浏览")
        btn_output.setObjectName("btnBrowse")
        btn_output.setFixedSize(80, HEIGHT_ELEMENT)
        btn_output.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
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
            # Excel 文件：只读第一行获取列名 [ISSUE-12 方案A]
            columns = self._get_excel_columns(frontline_path)

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
        
        for pattern, name in priority_patterns:
            for i, col in enumerate(col_lower):
                if pattern in col:
                    return columns[i]
        
        # 没有匹配，返回第一个字段
        return columns[0] if columns else ""

    def _get_excel_columns(self, file_path: str) -> list:
        """[20260420-老谈] ISSUE-12: 获取Excel文件的所有列名（只读第一行）"""
        import openpyxl
        try:
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            sheet_names = wb.sheetnames
            if not sheet_names:
                wb.close()
                return []

            # 只读取第一个 Sheet 的列名
            ws = wb[sheet_names[0]]
            columns = [str(cell.value) if cell.value is not None else "" 
                      for cell in next(ws.iter_rows(min_row=1, max_row=1))]
            columns = [c for c in columns if c]  # 过滤空值
            wb.close()
            return columns
        except Exception as e:
            logger.warning(f"读取Excel列名失败: {e}")
            return []

    def _create_module_section(self) -> QGroupBox:
        """创建执行模块区块"""
        group = QGroupBox()
        group.setTitle("▶️ 执行模块（可勾选）")
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        group.setMinimumHeight(HEIGHT_GROUP_MODULE)

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
        row1_layout.addWidget(cb1)
        self.checkboxes["field_validation"] = cb1

        cb2 = QCheckBox("跨名单去重标注")
        cb2.setChecked(True)
        cb2.setFixedHeight(HEIGHT_CHECKBOX)
        cb2.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
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
        row2_layout.addWidget(cb3)
        self.checkboxes["dict_encode"] = cb3

        cb4 = QCheckBox("字典值合规校验")
        cb4.setChecked(True)
        cb4.setFixedHeight(HEIGHT_CHECKBOX)
        cb4.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
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
        cb5.setFixedHeight(36)
        cb5.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        row3_layout.addWidget(cb5)
        self.checkboxes["internal_dedup"] = cb5

        row3_layout.addStretch()
        layout.addWidget(row3_widget)

        group.setLayout(layout)
        return group

    def _create_progress_section(self) -> QWidget:
        """创建处理进度区块"""
        container = QWidget()
        container.setObjectName("progressContainer")
        container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        container.setMinimumHeight(HEIGHT_PROGRESS)

        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(SPACING_MAIN)

        # 区块标题
        lbl_title = QLabel("📊 处理进度")
        lbl_title.setStyleSheet("font-size: 14px; font-weight: 600; color: #111827;")
        layout.addWidget(lbl_title)

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
        self.btn_view_detail.setMinimumHeight(HEIGHT_BUTTON_BANNER)
        self.btn_view_detail.setVisible(False)
        self.btn_view_detail.clicked.connect(self._on_view_detail_clicked)
        banner_btn_layout.addWidget(self.btn_view_detail)

        # 打开输出目录按钮（成功状态用）
        self.btn_open_output = QPushButton("📂 打开输出目录")
        self.btn_open_output.setObjectName("btnOpenOutput")
        self.btn_open_output.setMinimumHeight(32)
        self.btn_open_output.setVisible(False)
        self.btn_open_output.clicked.connect(self._on_open_output_dir)
        banner_btn_layout.addWidget(self.btn_open_output)

        layout.addLayout(banner_btn_layout)

        # 关闭按钮
        btn_close = QPushButton("×")
        btn_close.setObjectName("btnBannerClose")
        btn_close.setFixedSize(HEIGHT_CLOSE_BANNER, HEIGHT_CLOSE_BANNER)
        btn_close.clicked.connect(lambda: self.result_banner.setVisible(False))
        layout.addWidget(btn_close)

        return container

    def _show_result_banner(self, status: str, message: str, detail_count: int = 0):
        """
        显示结果横幅
        status: 'success' | 'warning' | 'error'
        detail_count: 问题数量，用于警告状态显示
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
            }}
            QPushButton#btnBannerClose:hover {{
                background-color: {text_color}20;
                border-radius: 4px;
            }}
        """)
        self.result_banner.setVisible(True)

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
        layout.setContentsMargins(*MARGIN_ACTION_BAR)
        layout.setSpacing(SPACING_MAIN)

        # 左侧：历史按钮
        self.btn_history = QPushButton("📋 处理历史")
        self.btn_history.setObjectName("btnHistory")
        self.btn_history.setMinimumWidth(110)
        self.btn_history.clicked.connect(self._on_show_history)
        layout.addWidget(self.btn_history)

        layout.addStretch()

        # 右侧：取消按钮 + 版本按钮 + 开始按钮
        self.btn_cancel = QPushButton("取消")
        self.btn_cancel.setObjectName("btnCancel")
        self.btn_cancel.setMinimumWidth(80)
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._on_cancel_processing)
        layout.addWidget(self.btn_cancel)

        self.btn_version = QPushButton("ℹ️ 版本")
        self.btn_version.setObjectName("btnVersion")
        self.btn_version.setMinimumWidth(80)
        self.btn_version.clicked.connect(self._on_show_version)
        layout.addWidget(self.btn_version)

        self.btn_start = QPushButton("▶️ 开始处理")
        self.btn_start.setObjectName("btnStart")
        self.btn_start.setMinimumWidth(140)
        self.btn_start.setMinimumHeight(HEIGHT_BUTTON_LARGE)
        self.btn_start.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.btn_start.clicked.connect(self._on_start_processing)
        layout.addWidget(self.btn_start)

        return container

    def _init_status_bar(self):
        """初始化状态栏"""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("就绪")

    def _log_message(self, level: str, message: str):
        """向日志区域和状态栏写入消息"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] [{level}] {message}"
        
        # 追加到日志区域
        if hasattr(self, 'log_text'):
            self.log_text.append(formatted)
        
        # 更新状态栏
        self.status_bar.showMessage(message, 5000)  # 5秒后恢复

    def _create_menu_bar(self):
        """创建菜单栏"""
        menubar = self.menuBar()

        # 文件菜单
        file_menu = menubar.addMenu("文件")

        exit_action = QAction("退出", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # 帮助菜单
        help_menu = menubar.addMenu("帮助")

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
            self.lbl_dict_version.setText(f"{filename} (v{short_hash})")
            self.lbl_dict_version.setStyleSheet("font-size: 13px; font-weight: 500; color: #059669;")
            logger.info(f"字典版本: {filename} (MD5: {short_hash})")
        except Exception as e:
            self.lbl_dict_version.setText(f"（版本检测失败: {e}）")
            self.lbl_dict_version.setStyleSheet("font-size: 13px; color: #DC2626;")
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

    # ==================== 业务逻辑方法 ====================

    def _select_file(self, file_type: str):
        """选择文件"""
        import os

        # [FIX v1.0.6] spec 类型使用特殊导入对话框
        if file_type == "spec":
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

        file_path, _ = QFileDialog.getOpenFileName(self, title, "", filters)

        if not file_path:
            return

        # [FIX v1.0.6] Bug 3: 激活多 Sheet 选择对话框
        # 对于名单文件，检测多 Sheet 并强制弹出选择窗口
        selected_sheet = None
        if file_type in ("frontline", "third_party", "hw"):
            selected_sheet = self._handle_multi_sheet_selection(file_path, file_type)
            if selected_sheet is None:
                # 用户取消选择，清除文件路径
                self.file_paths[file_type] = None
                return

        # 保存文件路径
        self.file_paths[file_type] = file_path

        # [FIX] 存储Sheet选择结果到上下文，供F1使用
        if not hasattr(self, '_selected_sheets'):
            self._selected_sheets = {}
        self._selected_sheets[file_type] = selected_sheet

        # 更新输入框
        basename = os.path.basename(file_path)
        self.file_inputs[file_type].setText(basename)
        self.file_inputs[file_type].setStyleSheet("")  # 清除错误样式

        # [FIX] 更新文件信息标签（列数/行数/Sheet名称）
        if file_type in ("frontline", "third_party", "hw"):
            file_info = self._get_file_info(file_path)
            if file_info:
                sheet_info = f" | Sheet: {selected_sheet}" if selected_sheet else ""
                info_text = f"✅ 已识别：{file_info['cols']} 列，{file_info['rows']:,} 行{sheet_info}"
                self.file_labels[file_type].setText(info_text)
                self._log_message("INFO", f"已选择 [{file_type}]: {basename} ({file_info['rows']:,} 行){sheet_info}")

        # 如果是字典文件，检查MD5变更并显示版本信息
        if file_type == "dict":
            self._update_dict_version_label(file_path)
            self._check_dict_file_change(file_path)

        self._log_message("INFO", f"已选择 {file_type}: {basename}")

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
            self.lbl_dict_version.setText(f"v{md5_short}（{datetime.now().strftime('%H:%M')}）")
            
        except Exception as e:
            logger.warning(f"MD5计算失败: {e}")

    def _handle_multi_sheet_selection(self, file_path: str, file_type: str) -> Optional[str]:
        """
        [FIX v1.0.6] Bug 3: 多 Sheet 选择处理

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
        import openpyxl

        # 初始化 Sheet 选择存储（如果不存在）
        if not hasattr(self, '_sheet_selections'):
            self._sheet_selections = {}

        # CSV 文件不需要 Sheet 选择
        if file_path.lower().endswith('.csv'):
            self._sheet_selections[file_type] = None
            return None

        try:
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            sheet_names = wb.sheetnames
            wb.close()

            if len(sheet_names) <= 1:
                # 单 Sheet 无需选择
                self._sheet_selections[file_type] = sheet_names[0] if sheet_names else None
                return self._sheet_selections[file_type]

            # [FIX v1.0.6] Bug 3: 多 Sheet 强制弹窗，禁止静默默认
            self._log_message("INFO", f"[{file_type}] 检测到 {len(sheet_names)} 个Sheet，弹出选择窗口")
            dialog = SheetSelectDialog(sheet_names, parent=self)
            dialog.exec_()

            if dialog.was_auto_selected():
                # 超时自动选择（用户未响应）
                selected = sheet_names[0]
                self._log_message("WARNING", f"[{file_type}] 选择超时，自动使用: {selected}")
            else:
                selected = dialog.get_selected_sheet()
                if selected:
                    self._log_message("INFO", f"[{file_type}] 用户选择 Sheet: {selected}")
                else:
                    # 用户取消
                    return None

            self._sheet_selections[file_type] = selected
            return selected

        except Exception as e:
            logger.error(f"检测 Sheet 失败: {e}")
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

    def _get_file_info(self, file_path: str) -> dict:
        """
        获取文件基本信息（列数、行数）
        使用 Polars 快速读取，避免加载整个文件
        """
        import os
        result = {"cols": 0, "rows": 0}
        
        try:
            if str(file_path).lower().endswith('.csv'):
                # CSV 文件使用 Polars
                df = pl.scan_csv(file_path).fetch(1000)  # 只读前1000行获取列信息
                result["cols"] = len(df.columns)
                # 统计总行数需要完整扫描，这里使用估算
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    result["rows"] = sum(1 for _ in f) - 1  # 减去表头
            else:
                # Excel 文件使用 openpyxl 快速读取
                import openpyxl
                wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
                sheet_name = wb.sheetnames[0] if wb.sheetnames else None
                if sheet_name:
                    ws = wb[sheet_name]
                    # 获取列数（第一行）
                    first_row = next(ws.iter_rows(min_row=1, max_row=1), None)
                    if first_row:
                        result["cols"] = len([c for c in first_row if c.value is not None])
                    # 获取行数
                    result["rows"] = ws.max_row - 1 if ws.max_row else 0  # 减去表头
                wb.close()
        except Exception as e:
            logger.warning(f"获取文件信息失败: {e}")
        
        return result

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
            columns = self._get_excel_columns(frontline_path)
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
        modules = self._build_modules(selected_modules, context)

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

    def _build_modules(self, selected_modules: list, context: ProcessContext):
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
        from PyQt5.QtWidgets import QFileDialog
        
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
            
            display_md5 = f"v{last_dict_md5 or '?'}" if last_dict_md5 else "（无版本）"
            self.lbl_dict_version.setText(f"{display_md5}{time_info} ✅默认值")
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
