"""
UI 样式集中管理模块

提供跨平台统一的字体配置，避免在 QSS 文件和代码中硬编码字体。
"""
import platform

from PyQt5.QtGui import QFont


def get_global_font_family() -> str:
    """
    获取平台适配的全局字体回退链（用于 QSS）。

    Returns:
        QSS font-family 字符串，已包含引号。
    """
    system = platform.system()
    if system == "Darwin":
        # macOS: 优先 PingFang SC，然后系统默认西文无衬线字体
        return '"PingFang SC", "Helvetica Neue", Helvetica, Arial, sans-serif'
    elif system == "Windows":
        # Windows: 优先 Microsoft YaHei，然后通用回退
        return '"Microsoft YaHei", "PingFang SC", "Helvetica Neue", Helvetica, Arial, sans-serif'
    else:
        # Linux / 其他
        return '"PingFang SC", "Helvetica Neue", Helvetica, Arial, sans-serif'


def get_title_qfont(point_size: int = 16, bold: bool = True) -> QFont:
    """
    获取平台适配的标题 QFont。

    Args:
        point_size: 字号，默认 16。
        bold: 是否加粗，默认 True。

    Returns:
        配置好的 QFont 对象。
    """
    system = platform.system()
    if system == "Darwin":
        family = "PingFang SC"
    elif system == "Windows":
        family = "Microsoft YaHei"
    else:
        family = "PingFang SC"

    font = QFont(family, point_size)
    font.setBold(bold)
    return font
