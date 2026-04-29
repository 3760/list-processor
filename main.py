"""
客户名单数据预处理单机工具 - 应用入口
"""

import sys
import os

# 将项目根目录加入 Python 路径，确保各模块可正常 import
# PyInstaller 打包后，资源文件位于 sys._MEIPASS 目录
if getattr(sys, 'frozen', False):
    # 打包后的可执行文件：BASE_DIR 指向 Resources 目录
    BASE_DIR = sys._MEIPASS
else:
    # 开发模式：指向源码目录
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# [20260420-老谈] macOS KVO 警告抑制 - PyQt5 在 macOS 上的已知问题
# 禁用 Qt 窗口相关的调试警告（全平台通用）
os.environ.setdefault("QT_LOGGING_RULES", "qt.qpa.window.debug=false;qt.qpa.window=false")

# macOS 专属：KVO 警告抑制
if __import__('platform').system() == "Darwin":
    os.environ.setdefault("QT_MAC_WINDOW_LAYER_IGNORE_OBSERVERS", "1")

from PyQt5.QtWidgets import QApplication, QMessageBox
from infra.log_manager import get_logger
from infra.exceptions import CriticalError, ProcessingError
from infra.app_config_loader import load_app_config
from ui.main_window import MainWindow

logger = get_logger(__name__)


def main():
    """应用主入口"""
    try:
        # 加载应用配置
        config = load_app_config()
        if not config:
            logger.critical("应用配置为空或无效")
            QMessageBox.critical(None, "配置错误", "应用配置加载失败，请检查配置文件")
            sys.exit(1)
        logger.info("应用配置加载成功")

        # macOS 高 DPI 支持
        os.environ.setdefault("QT_AUTO_SCREEN_SCALE_FACTOR", "1")
        os.environ.setdefault("QT_SCALE_FACTOR_ROUNDING_POLICY", "PassThrough")

        # 启动 GUI（防止重复创建）
        if QApplication.instance() is None:
            app = QApplication(sys.argv)
        else:
            app = QApplication.instance()

        app.setApplicationName("客户名单数据预处理工具")
        app.setApplicationVersion("v1.1.2")
        # macOS 高 DPI 属性
        app.setAttribute(0x00000020, True)  # Qt.AA_EnableHighDpiScaling
        app.setAttribute(0x00000021, True)  # Qt.AA_UseHighDpiPixmaps

        main_window = MainWindow(config)
        main_window.show()

        logger.info("主窗口已显示")
        exit_code = app.exec_()
        logger.info(f"应用正常退出，退出码: {exit_code}")
        sys.exit(exit_code)

    except (KeyboardInterrupt, SystemExit):
        # 用户强制终止或 sys.exit() 调用，不需要弹窗
        logger.info("应用被强制终止")
        sys.exit(0)

    except CriticalError as e:
        logger.critical(f"关键错误，应用终止: {e}")
        QMessageBox.critical(None, "关键错误", f"应用遇到关键错误，必须退出。\n\n{e}")
        sys.exit(1)

    except (ProcessingError, RuntimeError, AttributeError) as e:
        logger.critical(f"未预期异常，应用终止: {e}", exc_info=True)
        QMessageBox.critical(None, "未预期错误", f"发生未预期的错误，请查看日志。\n\n{e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
