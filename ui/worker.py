"""
UI层 - 工作线程（Worker）

QThread 子类，用于在后台线程执行耗时操作，
通过信号（Signal）向主线程报告进度和结果，
避免 GUI 阻塞。
"""

from typing import Any, Callable, Dict, List, Optional

from PyQt5.QtCore import QThread, pyqtSignal

from core.context import ProcessContext
from core.orchestrator import ProcessOrchestrator
from infra.exceptions import CriticalError, ProcessingError
from infra.log_manager import get_logger

logger = get_logger(__name__)


class ProcessingWorker(QThread):
    """
    数据处理工作线程。

    Signals（信号）：
    - progress_updated(module: str, percent: int)
        进度更新信号
    - status_changed(message: str)
        状态消息更新
    - finished(context: ProcessContext, success: bool, error_msg: str)
        处理完成信号（成功或失败）
    - log_message(level: str, message: str)
        日志消息（可实时展示在 UI 文本框中）
    """

    # 信号定义
    progress_updated = pyqtSignal(str, int)      # (模块名, 百分比)
    status_changed = pyqtSignal(str)            # 状态文本
    finished = pyqtSignal(object, bool, str)     # (context, success, error)
    log_message = pyqtSignal(str, str)           # (level, message)

    def __init__(
        self,
        modules: List[Any],
        context: ProcessContext,
        progress_callback: Optional[Callable] = None,
        parent=None,
    ):
        """
        初始化工作线程。

        Parameters
        ----------
        modules : List[BaseModule]
            已构建的业务模块列表
        context : ProcessContext
            处理上下文
        progress_callback : Callable, optional
            进度回调函数，签名为 (module_name: str, percent: int) -> None
        """
        super().__init__(parent)
        self.modules = modules
        self.context = context
        self.progress_callback = progress_callback
        self.orchestrator = None  # 延迟初始化

    def run(self):
        """线程主方法（由 QThread 自动调用）"""
        self.status_changed.emit("处理开始...")
        self._emit_log("INFO", f"处理流程启动 run_id={self.context.run_id}")

        try:
            def on_progress(module_name: str, percent: int):
                self.progress_updated.emit(module_name, percent)
                self.status_changed.emit(f"正在处理: {module_name} ({percent}%)")

            # 创建编排器
            self.orchestrator = ProcessOrchestrator(
                modules=self.modules,
                progress_callback=on_progress,
            )

            # 执行流程
            result_context = self.orchestrator.run(self.context)

            self.status_changed.emit("处理完成")
            self._emit_log("INFO", f"处理完成 run_id={result_context.run_id}")
            self.progress_updated.emit("完成", 100)
            self.finished.emit(result_context, True, "")

        except CriticalError as e:
            error_msg = f"关键错误: {e}"
            self.status_changed.emit("处理失败（关键错误）")
            self._emit_log("CRITICAL", error_msg)
            self.finished.emit(self.context, False, error_msg)

        except ProcessingError as e:
            error_msg = f"处理错误: {e}"
            self.status_changed.emit("处理失败")
            self._emit_log("ERROR", error_msg)
            self.finished.emit(self.context, False, error_msg)

        except Exception as e:
            error_msg = f"未预期错误: {e}"
            self.status_changed.emit("处理异常")
            self._emit_log("ERROR", error_msg)
            self.finished.emit(self.context, False, error_msg)

    def _emit_log(self, level: str, message: str):
        """发送日志消息信号"""
        self.log_message.emit(level, message)
        logger.info(f"[{level}] {message}")
