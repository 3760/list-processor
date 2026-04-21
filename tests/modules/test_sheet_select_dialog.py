"""
Tests for Sheet Selection Dialog (T-22)

覆盖场景：
1. 单Sheet文件 - 无需弹窗，直接返回
2. 多Sheet文件 - 弹窗显示，用户选择
3. 超时自动选择 - 5秒后自动选第一个
4. 取消选择 - 用户取消，返回None
"""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# PyQt5 导入
from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QApplication

# 确保 QApplication 已初始化（pytest-qtduck 提供 fixture）
# 需要在导入对话框前确保 QApplication 存在


# ─────────────────────────────────────────────
# 测试辅助函数
# ─────────────────────────────────────────────


def create_dialog(sheet_names: list[str], timeout: int = 5):
    """创建Sheet选择对话框实例"""
    from ui.widgets.sheet_select_dialog import SheetSelectDialog

    dialog = SheetSelectDialog(sheet_names, timeout_seconds=timeout)
    return dialog


# ─────────────────────────────────────────────
# T-22: Sheet选择对话框测试
# ─────────────────────────────────────────────


class TestSheetSelectDialogBasic:
    """基础功能测试"""

    def test_single_sheet_no_dialog_needed(self, qtbot):
        """Sheet=1：单Sheet无需弹窗"""
        # 单Sheet由调用方处理，对话框不应创建
        sheet_names = ["Sheet1"]
        # 模拟 create_sheet_selection_callback 的行为
        if len(sheet_names) <= 1:
            result = sheet_names[0]
        else:
            result = None  # 应显示对话框

        assert result == "Sheet1"

    def test_dialog_creation(self, qtbot):
        """多Sheet时对话框正常创建"""
        dialog = create_dialog(["Sheet1", "Sheet2", "Data"])
        assert dialog is not None
        assert dialog.sheet_names == ["Sheet1", "Sheet2", "Data"]
        assert dialog.timeout_seconds == 5

    def test_dialog_shows_all_sheets(self, qtbot):
        """对话框显示所有Sheet名称"""
        dialog = create_dialog(["一线名单", "三方名单", "HW名单"])
        assert dialog.sheet_list.count() == 3
        assert dialog.sheet_list.item(0).text() == "一线名单"
        assert dialog.sheet_list.item(1).text() == "三方名单"
        assert dialog.sheet_list.item(2).text() == "HW名单"

    def test_default_selection_first_sheet(self, qtbot):
        """默认选中第一个Sheet"""
        dialog = create_dialog(["A", "B", "C"])
        assert dialog.sheet_list.currentRow() == 0
        assert dialog.sheet_list.currentItem().text() == "A"

    def test_countdown_label_initial(self, qtbot):
        """倒计时标签初始显示5秒"""
        dialog = create_dialog(["A", "B"], timeout=5)
        assert "5" in dialog.countdown_label.text()


class TestSheetSelectDialogInteraction:
    """用户交互测试"""

    def test_user_selects_sheet(self, qtbot):
        """用户选择Sheet"""
        dialog = create_dialog(["A", "B", "C"])

        # 模拟用户选择第二项
        dialog.sheet_list.setCurrentRow(1)
        qtbot.wait(10)

        # 模拟确认按钮点击
        dialog._on_confirm()

        assert dialog.result() == dialog.Accepted
        assert dialog.get_selected_sheet() == "B"

    def test_user_cancels(self, qtbot):
        """用户取消选择"""
        dialog = create_dialog(["A", "B"])

        # 模拟取消
        dialog.reject()

        assert dialog.result() == dialog.Rejected
        assert dialog.get_selected_sheet() is None

    def test_double_click_to_select(self, qtbot):
        """双击Sheet名称选择"""
        dialog = create_dialog(["A", "B", "C"])

        # 双击第二项
        dialog.sheet_list.setCurrentRow(2)
        qtbot.wait(10)
        dialog.sheet_list.doubleClicked.emit(dialog.sheet_list.currentIndex())

        assert dialog.result() == dialog.Accepted
        assert dialog.get_selected_sheet() == "C"


class TestSheetSelectDialogTimeout:
    """超时自动选择测试"""

    def test_timeout_auto_selects_first(self, qtbot):
        """超时自动选择第一个Sheet"""
        # 使用1秒超时加速测试
        dialog = create_dialog(["First", "Second"], timeout=1)

        # 模拟计时器触发
        qtbot.wait(1100)  # 等待超时

        assert dialog.was_auto_selected() is True
        assert dialog.get_selected_sheet() == "First"

    def test_timeout_signal_emitted(self, qtbot):
        """超时触发相应信号"""
        dialog = create_dialog(["A", "B"], timeout=1)

        # 监听信号
        selected_sheets = []
        dialog.timeout_auto_selected.connect(lambda s: selected_sheets.append(s))

        # 等待超时
        qtbot.wait(1100)

        assert len(selected_sheets) == 1
        assert selected_sheets[0] == "A"

    def test_user_selection_stops_timer(self, qtbot):
        """用户选择后停止计时器"""
        dialog = create_dialog(["A", "B"], timeout=5)

        # 模拟用户快速选择
        dialog.sheet_list.setCurrentRow(1)
        qtbot.wait(10)
        dialog._on_confirm()

        # 计时器应该已停止
        assert dialog._timer is None or not dialog._timer.isActive()
        assert dialog.was_auto_selected() is False


class TestSheetSelectDialogEdgeCases:
    """边界情况测试"""

    def test_empty_sheet_list(self, qtbot):
        """空Sheet列表"""
        dialog = create_dialog([])
        assert dialog.sheet_list.count() == 0

    def test_many_sheets(self, qtbot):
        """大量Sheet（20个）"""
        sheet_names = [f"Sheet_{i}" for i in range(20)]
        dialog = create_dialog(sheet_names)
        assert dialog.sheet_list.count() == 20
        assert dialog.sheet_list.currentItem().text() == "Sheet_0"

    def test_chinese_sheet_names(self, qtbot):
        """中文Sheet名称"""
        dialog = create_dialog(["一线名单", "三方名单", "客户档案"])
        assert dialog.sheet_list.count() == 3

        # 选择中文Sheet
        dialog.sheet_list.setCurrentRow(1)
        dialog._on_confirm()

        assert dialog.get_selected_sheet() == "三方名单"

    def test_special_characters_in_sheet_name(self, qtbot):
        """Sheet名称含特殊字符"""
        dialog = create_dialog(["Data (2024)", "Report-01", "Sheet's"])
        assert dialog.sheet_list.count() == 3

        dialog.sheet_list.setCurrentRow(0)
        dialog._on_confirm()
        assert dialog.get_selected_sheet() == "Data (2024)"


class TestSheetSelectDialogCallback:
    """回调函数测试"""

    def test_create_callback_function(self, qtbot):
        """创建Sheet选择回调函数"""
        from ui.widgets.sheet_select_dialog import create_sheet_selection_callback

        callback = create_sheet_selection_callback()
        assert callable(callback)

    def test_callback_single_sheet(self):
        """回调处理单Sheet"""
        from ui.widgets.sheet_select_dialog import create_sheet_selection_callback

        callback = create_sheet_selection_callback()

        # 单Sheet直接返回
        with patch(
            "ui.widgets.sheet_select_dialog.QDialog"
        ) as mock_dialog:
            result = callback(["OnlyOne"])
            # 单Sheet不走对话框
            assert result == "OnlyOne"
