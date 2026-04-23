"""
按钮样式集中管理

提供统一的按钮样式定义，避免在多个文件中重复定义。
所有按钮样式统一在此文件维护。

样式分类：
- BUTTON_STYLE_SECONDARY: 次要按钮（浏览、关闭、取消等）
- BUTTON_STYLE_PRIMARY: 主按钮（确定、提交等）
- BUTTON_STYLE_DANGER: 危险按钮（删除等）
- BUTTON_STYLE_PRIMARY_START: 开始处理按钮（主按钮的特例，尺寸更大）
"""

# 次要按钮样式（浏览、关闭、取消等）
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

# 主按钮样式（确定、提交等）
BUTTON_STYLE_PRIMARY = """
QPushButton {
    background-color: #2563EB;
    color: #FFFFFF;
    border: none;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    min-height: 36px;
    padding: 0 24px;
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

# 开始处理按钮样式（主按钮特例，尺寸更大）
BUTTON_STYLE_PRIMARY_START = """
QPushButton {
    background-color: #2563EB;
    color: #FFFFFF;
    border: none;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 600;
    min-width: 200px;
    min-height: 44px;
    padding: 0 24px;
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
"""

# 危险按钮样式（删除等）
BUTTON_STYLE_DANGER = """
QPushButton {
    color: #991B1B;
    border: 1px solid #FECACA;
    background-color: #FFFFFF;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    min-height: 36px;
    padding: 0 16px;
}
QPushButton:hover {
    background-color: #FEF2F2;
    border-color: #FCA5A5;
}
QPushButton:pressed {
    background-color: #FEE2E2;
}
QPushButton:disabled {
    background-color: #F3F4F6;
    color: #9CA3AF;
    border-color: #E5E7EB;
}
"""
