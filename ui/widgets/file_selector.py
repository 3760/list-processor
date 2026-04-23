"""
UI层 - 文件选择组件

独立的文件选择组件，支持：
- 三类名单文件选择（一线、三方、HW）
- 数据字典文件选择
- 字段规范文件选择
- 文件拖拽支持（可选）

使用方式：
    from ui.widgets.file_selector import FileSelector

    selector = FileSelector()
    layout.addWidget(selector)

    # 获取选择的文件路径
    paths = selector.get_file_paths()
"""

import os
from typing import Dict, Optional

from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from infra.log_manager import get_logger
from ui.styles.button_styles import BUTTON_STYLE_SECONDARY

logger = get_logger(__name__)


class FileSelector(QWidget):
    """
    文件选择组件。

    Signals:
        file_changed(str, str): (file_type, file_path) 当文件选择改变时发出
        validation_changed(bool): 验证状态改变时发出
    """

    file_changed = pyqtSignal(str, str)
    validation_changed = pyqtSignal(bool)

    # 文件类型配置（统一使用内部键: yixian, sanfang, hw）
    FILE_CONFIGS = {
        "yixian": {
            "label": "一线人员名单",
            "required": True,
            "filters": "Excel 文件 (*.xlsx *.xls);;CSV 文件 (*.csv);;所有文件 (*.*)",
            "hint": "必选",
        },
        "sanfang": {
            "label": "三方系统名单",
            "required": False,
            "filters": "Excel 文件 (*.xlsx *.xls);;CSV 文件 (*.csv);;所有文件 (*.*)",
            "hint": "可选",
        },
        "hw": {
            "label": "HW系统名单",
            "required": False,
            "filters": "Excel 文件 (*.xlsx *.xls);;CSV 文件 (*.csv);;所有文件 (*.*)",
            "hint": "可选",
        },
        "dict": {
            "label": "数据字典文件",
            "required": True,
            "filters": "Excel 文件 (*.xlsx *.xls)",
            "hint": "必选",
        },
        "spec": {
            "label": "字段规范文件",
            "required": False,
            "filters": "Excel 文件 (*.xlsx *.xls)",
            "hint": "可选",
        },
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.file_paths: Dict[str, Optional[str]] = {
            key: None for key in self.FILE_CONFIGS
        }
        self._init_ui()

    def _init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        for file_type, config in self.FILE_CONFIGS.items():
            row = self._create_file_row(file_type, config)
            layout.addLayout(row)

    def _create_file_row(self, file_type: str, config: dict) -> QHBoxLayout:
        """
        创建单行文件选择器。

        Parameters
        ----------
        file_type : str
            文件类型标识
        config : dict
            文件配置

        Returns
        -------
        QHBoxLayout
        """
        row = QHBoxLayout()
        row.setSpacing(10)

        # 文件类型标签
        label = QLabel(f"{config['label']}（{config['hint']}）")
        label.setMinimumWidth(150)
        label.setStyleSheet("font-weight: bold;")
        row.addWidget(label)

        # 选择文件按钮
        btn = QPushButton("选择文件...")
        btn.setMaximumWidth(100)
        btn.setStyleSheet(BUTTON_STYLE_SECONDARY)
        btn.clicked.connect(lambda: self._select_file(file_type))
        row.addWidget(btn)
        setattr(self, f"btn_{file_type}", btn)

        # 文件路径显示
        path_label = QLabel("未选择")
        path_label.setStyleSheet("color: gray;")
        path_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        path_label.setMinimumWidth(200)
        row.addWidget(path_label)
        setattr(self, f"label_{file_type}", path_label)

        # 状态指示
        status_label = QLabel("⏳")
        row.addWidget(status_label)
        setattr(self, f"status_{file_type}", status_label)

        row.addStretch()

        return row

    def _select_file(self, file_type: str):
        """选择文件"""
        config = self.FILE_CONFIGS[file_type]

        file_path, _ = QFileDialog.getOpenFileName(
            self,
            f"选择{config['label']}",
            "",
            config["filters"],
        )

        if file_path:
            self._set_file_path(file_type, file_path)

    def _set_file_path(self, file_type: str, file_path: str):
        """设置文件路径"""
        self.file_paths[file_type] = file_path

        # 更新标签显示
        label = getattr(self, f"label_{file_type}")
        filename = os.path.basename(file_path)
        label.setText(filename)
        label.setStyleSheet("color: #333;")

        # 更新状态指示
        status = getattr(self, f"status_{file_type}")
        status.setText("✅")

        # 发出信号
        self.file_changed.emit(file_type, file_path)
        logger.info(f"用户选择文件 [{file_type}]: {file_path}")

        # 检查验证状态
        self.validation_changed.emit(self.is_valid())

    def get_file_paths(self) -> Dict[str, Optional[str]]:
        """
        获取所有文件路径。

        Returns
        -------
        Dict[str, Optional[str]]
            文件路径字典
        """
        return self.file_paths.copy()

    def get_required_paths(self) -> Dict[str, str]:
        """
        获取必选文件路径。

        Returns
        -------
        Dict[str, str]
            只包含必选且已选择的文件路径

        Raises
        ------
        ValueError
            如果有必选文件未选择
        """
        missing = []
        required_paths = {}

        for file_type, config in self.FILE_CONFIGS.items():
            if config["required"] and not self.file_paths[file_type]:
                missing.append(config["label"])

        if missing:
            raise ValueError(f"请先选择以下必选文件：{', '.join(missing)}")

        for file_type, config in self.FILE_CONFIGS.items():
            if self.file_paths[file_type]:
                required_paths[file_type] = self.file_paths[file_type]

        return required_paths

    def is_valid(self) -> bool:
        """
        检查必选文件是否都已选择。

        Returns
        -------
        bool
        """
        for file_type, config in self.FILE_CONFIGS.items():
            if config["required"] and not self.file_paths[file_type]:
                return False
        return True

    def clear(self):
        """清空所有选择"""
        for file_type in self.FILE_CONFIGS:
            self.file_paths[file_type] = None

            # 重置标签
            label = getattr(self, f"label_{file_type}")
            label.setText("未选择")
            label.setStyleSheet("color: gray;")

            # 重置状态
            status = getattr(self, f"status_{file_type}")
            status.setText("⏳")

        self.validation_changed.emit(False)
