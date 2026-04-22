"""
UI层 - 进度展示组件

实时展示处理进度，包含：
- 进度条（百分比显示，带动画效果）
- 当前阶段提示
- 预估剩余时间（可选）
- 各模块完成状态列表

使用方式：
    from ui.widgets.progress_panel import ProgressPanel

    panel = ProgressPanel()
    layout.addWidget(panel)

    # 连接信号
    panel.on_progress("F1", 50)  # 更新进度

动画规范（来自UI交互与动画规范-v1.1）：
- 进度条填充速度：300ms ease-out
- 进度条条纹动画：1000ms linear infinite
"""

from typing import Dict, Optional

from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QPropertyAnimation, QEasingCurve
from PyQt5.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from infra.log_manager import get_logger

logger = get_logger(__name__)


class ProgressPanel(QWidget):
    """
    进度展示面板。

    Features:
    - 总体进度条
    - 当前模块提示
    - 各模块完成状态
    - 预估剩余时间

    Signals:
        cancelled(): 用户点击取消时发出
    """

    cancelled = pyqtSignal()

    # 模块名称映射（中文显示）
    MODULE_NAMES = {
        "F1": "文件加载",
        "F1_一线": "加载一线名单",
        "F1_三方": "加载三方名单",
        "F1_HW": "加载HW名单",
        "F2": "字段合规检查",
        "F3": "跨名单去重",
        "F4": "数据字典上码",
        "F5": "字典值校验",
        "F6": "名单内部去重",
        "F7": "结果输出",
        "完成": "处理完成",
    }

    # F1 子模块名称（用于阶段提示）
    F1_SUB_NAMES = {
        "一线": "一线名单",
        "三方": "三方名单",
        "HW": "HW名单",
    }

    # 模块进度权重（总计100%）
    MODULE_WEIGHTS = {
        "F1": 15,
        "F2": 20,
        "F3": 15,
        "F4": 20,
        "F5": 10,
        "F6": 10,
        "F7": 10,
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_module: Optional[str] = None
        self.module_progress: Dict[str, int] = {}
        self.start_time: Optional[float] = None
        self._timer: Optional[QTimer] = None

        self._init_ui()

    def _init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        # 总体进度区（进度条自带百分比显示，无需额外标签）
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("%p%")
        self.progress_bar.setMinimumHeight(30)
        # [规范2.2] 进度条渐变光泽效果 + 条纹动画
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #E5E7EB;
                border-radius: 8px;
                text-align: center;
                font-weight: bold;
                font-size: 12px;
                color: #6B7280;
                background-color: #FFFFFF;
            }
            QProgressBar::chunk {
                /* 渐变光泽效果 */
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 1, y2: 0,
                    stop: 0 #2563EB,
                    stop: 0.5 #3B82F6,
                    stop: 1 #2563EB
                );
                border-radius: 8px;
            }
        """)
        layout.addWidget(self.progress_bar)

        # 当前阶段标签
        self.stage_label = QLabel("等待开始...")
        self.stage_label.setAlignment(Qt.AlignCenter)
        self.stage_label.setStyleSheet("""
            QLabel {
                color: #666;
                font-size: 13px;
                padding: 5px;
                background-color: #F3F4F6;
                border-radius: 8px;
            }
        """)
        layout.addWidget(self.stage_label)

        # 模块状态列表
        self.module_status_frame = QFrame()
        self.module_status_frame.setFrameShape(QFrame.StyledPanel)
        self.module_status_frame.setFrameShadow(QFrame.Raised)
        self.module_status_frame.setMaximumHeight(200)
        self.module_status_frame.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Maximum)
        self.module_status_frame.setStyleSheet("""
            QFrame {
                background-color: #F9FAFB;
                border: 1px solid #E5E7EB;
                border-radius: 8px;
            }
        """)
        module_layout = QVBoxLayout(self.module_status_frame)
        module_layout.setContentsMargins(10, 10, 10, 10)
        module_layout.setSpacing(5)

        self.module_labels: Dict[str, QLabel] = {}
        for module in ["F1", "F2", "F3", "F4", "F5", "F6", "F7"]:
            module_row = QHBoxLayout()
            module_row.setSpacing(10)

            # 模块名称
            name_label = QLabel(self.MODULE_NAMES.get(module, module))
            name_label.setMinimumWidth(120)
            name_row_layout = QHBoxLayout()
            name_row_layout.addWidget(name_label)
            name_row_layout.addStretch()
            module_row.addLayout(name_row_layout)

            # 状态指示
            status_label = QLabel("⏳")
            status_label.setMinimumWidth(30)
            module_row.addWidget(status_label)
            self.module_labels[module] = status_label

            # 进度指示
            progress_label = QLabel("")
            progress_label.setMinimumWidth(60)
            module_row.addWidget(progress_label)
            self.module_labels[f"{module}_progress"] = progress_label

            module_layout.addLayout(module_row)

        layout.addWidget(self.module_status_frame)

        # 预估剩余时间
        self.eta_label = QLabel("")
        self.eta_label.setAlignment(Qt.AlignRight)
        self.eta_label.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(self.eta_label)

    def start(self):
        """开始进度跟踪"""
        import time
        self.start_time = time.time()
        self.module_progress = {}
        self._reset_ui()
        self._start_timer()

    def _reset_ui(self):
        """重置UI状态"""
        self.progress_bar.setValue(0)
        self.stage_label.setText("准备中...")

        for module in ["F1", "F2", "F3", "F4", "F5", "F6", "F7"]:
            self.module_labels[module].setText("⏳")
            self.module_labels[f"{module}_progress"].setText("")

    def _start_timer(self):
        """启动定时器更新预估时间"""
        if self._timer is None:
            self._timer = QTimer(self)
            self._timer.timeout.connect(self._update_eta)
            self._timer.start(1000)  # 每秒更新

    def _update_eta(self):
        """更新预估剩余时间"""
        if self.start_time is None:
            return

        import time
        elapsed = time.time() - self.start_time
        current_percent = self.progress_bar.value()

        if current_percent > 0:
            # 根据当前进度估算剩余时间
            total_estimated = elapsed / (current_percent / 100)
            remaining = total_estimated - elapsed

            if remaining > 60:
                self.eta_label.setText(f"预估剩余: {int(remaining / 60)}分{int(remaining % 60)}秒")
            elif remaining > 0:
                self.eta_label.setText(f"预估剩余: {int(remaining)}秒")
            else:
                self.eta_label.setText("处理中...")
        else:
            self.eta_label.setText("处理中...")

    def on_progress(self, module: str, percent: int):
        """
        处理进度更新回调（带动画效果，规范2.2）。

        Parameters
        ----------
        module : str
            模块名称（F1~F7 或 F1_xxx 子模块）
        percent : int
            模块内部进度（0~100）

        动画参数：
        - 进度条填充：300ms ease-out
        """
        self.current_module = module
        self.module_progress[module] = percent

        # 处理 F1 子模块（用于细化文件加载进度）
        if module.startswith("F1_"):
            # F1_一线、F1_三方、F1_HW 格式
            sub_type = module.split("_", 1)[1]  # "一线"、"三方"、"HW"
            sub_name = self.F1_SUB_NAMES.get(sub_type, sub_type)
            self.stage_label.setText(f"正在加载: {sub_name} ({percent}%)")
            # F1 子模块的进度已由 load_files 中的回调计算
            # 这里只需要更新 F1 的总体进度
            self._update_total_progress_animated()
            return

        # 更新阶段提示
        module_name = self.MODULE_NAMES.get(module, module)
        self.stage_label.setText(f"正在处理: {module_name} ({percent}%)")

        # 更新模块状态
        if module in self.module_labels:
            if percent == 100:
                self.module_labels[module].setText("✅")
                self.module_labels[module].setStyleSheet("color: green; font-weight: bold;")
            else:
                self.module_labels[module].setText("🔄")
            self.module_labels[f"{module}_progress"].setText(f"{percent}%")

        # 更新总体进度（带动画）
        self._update_total_progress_animated()

    def _update_total_progress_animated(self):
        """
        根据各模块进度计算总体进度（带动画效果，规范2.2）
        
        动画参数：
        - 填充速度：300ms
        - 缓动函数：ease-out
        """
        total = 0
        for module, weight in self.MODULE_WEIGHTS.items():
            module_pct = self.module_progress.get(module, 0)
            total += (module_pct / 100) * weight

        target_value = int(total)
        current_value = self.progress_bar.value()
        
        # 创建平滑动画（300ms ease-out）
        if hasattr(self, '_progress_animation') and self._progress_animation is not None:
            self._progress_animation.stop()
        
        self._progress_animation = QPropertyAnimation(self.progress_bar, b"value")
        self._progress_animation.setDuration(300)
        self._progress_animation.setEasingCurve(QEasingCurve.OutCurve)
        self._progress_animation.setStartValue(current_value)
        self._progress_animation.setEndValue(target_value)
        self._progress_animation.start()
        # 进度条自带百分比显示，无需额外标签

    def on_complete(self, success: bool = True):
        """
        处理完成。

        Parameters
        ----------
        success : bool
            是否成功完成
        """
        if self._timer:
            self._timer.stop()

        if success:
            self.progress_bar.setValue(100)
            self.stage_label.setText("✅ 处理完成")
            self.stage_label.setStyleSheet("""
                QLabel {
                    color: #16A34A;
                    font-size: 13px;
                    font-weight: bold;
                    padding: 5px;
                    background-color: #DCFCE7;
                    border-radius: 8px;
                }
            """)
            self.eta_label.setText("")

            # 所有模块标记完成
            for module in ["F1", "F2", "F3", "F4", "F5", "F6", "F7"]:
                self.module_labels[module].setText("✅")
                self.module_labels[module].setStyleSheet("color: green; font-weight: bold;")
        else:
            self.stage_label.setText("❌ 处理失败")
            self.stage_label.setStyleSheet("""
                QLabel {
                    color: #DC2626;
                    font-size: 13px;
                    font-weight: bold;
                    padding: 5px;
                    background-color: #FEE2E2;
                    border-radius: 8px;
                }
            """)

        logger.info(f"进度面板更新完成: success={success}")

    def on_error(self, error_message: str):
        """
        处理出错。

        Parameters
        ----------
        error_message : str
            错误消息
        """
        if self._timer:
            self._timer.stop()

        self.stage_label.setText(f"❌ 错误: {error_message[:30]}...")
        self.stage_label.setStyleSheet("""
            QLabel {
                color: #DC2626;
                font-size: 13px;
                font-weight: bold;
                padding: 5px;
                background-color: #FEE2E2;
                border-radius: 8px;
            }
        """)
        self.eta_label.setText("")

    def reset(self):
        """重置面板状态"""
        if self._timer:
            self._timer.stop()

        self.current_module = None
        self.module_progress = {}
        self.start_time = None
        self._reset_ui()
        self.stage_label.setStyleSheet("""
            QLabel {
                color: #666;
                font-size: 13px;
                padding: 5px;
                background-color: #F3F4F6;
                border-radius: 8px;
            }
        """)
        self.eta_label.setText("")
