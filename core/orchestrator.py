"""
核心层 - 流程编排器

ProcessOrchestrator 负责控制整体处理流程的执行顺序、
异常捕获、进度回调、事务回滚等核心协调工作。

执行顺序（PRD §6）：
F1 → F2 → F6 → F4 → F5 → F3 → F7
"""

import time
from typing import Any, Callable, Dict, List, Optional

from core.base_module import BaseModule
from core.context import ProcessContext
from infra.exceptions import CriticalError, ProcessingError
from infra.log_manager import get_logger
from db.dao.processing_history import ProcessingHistoryDAO

logger = get_logger(__name__)


class ProcessOrchestrator:
    """
    处理流程编排器

    Attributes
    ----------
    modules : List[BaseModule]
        按执行顺序排列的业务模块列表
    progress_callback : Callable[[str, int], None], optional
        进度回调函数，签名：(module_name, percent)
    status_callback : Callable[[str, str], None], optional
        状态回调函数，签名：(module_name, status) - status: pending/running/skipped/completed/failed
    """

    def __init__(
        self,
        modules: List[BaseModule],
        progress_callback: Optional[Callable[[str, int], None]] = None,
        status_callback: Optional[Callable[[str, str], None]] = None,
    ):
        if not modules:
            raise ValueError("模块列表不能为空")

        self.modules = modules
        self.progress_callback = progress_callback
        self.status_callback = status_callback  # 状态回调
        total_weight = sum(m.get_progress_weight() for m in modules)
        self._total_weight = total_weight if total_weight > 0 else 1  # [FIX #6] 防止零除错误

    def run(self, context: ProcessContext) -> ProcessContext:
        """
        执行完整处理流程。

        Parameters
        ----------
        context : ProcessContext
            处理上下文（包含输入文件等）

        Returns
        -------
        ProcessContext
            处理完成后的上下文
        """
        run_id = ProcessingHistoryDAO.create_run(
            input_yixian=context.get_input_file("yixian"),
            input_sanfang=context.get_input_file("sanfang"),
            input_hw=context.get_input_file("hw"),
            dict_file=context.dict_file_path,
            spec_file=context.spec_file_path,
        )
        context.run_id = run_id
        logger.info(f"流程启动 run_id={run_id}")

        completed_weight = 0

        try:
            # [问题1修复] 在开始执行前，先将未包含在模块列表中的模块标记为跳过
            # 这样 progress_panel 可以正确计算 active_weight，使进度条能正确显示100%
            all_modules = {"F1", "F2", "F3", "F4", "F5", "F6", "F7"}
            executed_modules = {m.get_module_name() for m in self.modules}
            for module_name in all_modules - executed_modules:
                logger.info(f"模块 {module_name} 未勾选，标记为跳过")
                self._report_status(module_name, "skipped")

            for module in self.modules:
                module_name = module.get_module_name()
                logger.info(f"========== 开始模块 {module_name} ==========")

                # 1. 前置校验
                valid, err_msg = module.validate_input(context)
                if not valid:
                    logger.warning(f"模块 {module_name}  {err_msg}===前置校验未通过")
                    context.record_module_result(
                        module_name,
                        success_count=0,
                        fail_count=0,
                        skip_count=0,
                        message=f"{err_msg}===前置校验未通过",
                    )
                    # 通知模块跳过
                    self._report_status(module_name, "skipped")
                    continue

                # 通知模块开始执行
                self._report_status(module_name, "running")

                # 2. 计时执行
                start_time = time.time()
                try:
                    # [优化] 将进度回调传递给模块，支持模块内部细化进度
                    module._progress_callback = self.progress_callback
                    context = module.execute(context)
                except (ProcessingError, CriticalError, RuntimeError, AttributeError, TypeError) as e:
                    elapsed_ms = int((time.time() - start_time) * 1000)
                    logger.error(
                        f"模块 {module_name} 执行异常: {e}",
                        exc_info=True,
                    )
                    context.record_module_result(
                        module_name,
                        success_count=0,
                        fail_count=0,
                        skip_count=0,
                        message=f"执行异常: {e}",
                    )
                    module.on_error(context, e)
                    # 通知模块失败
                    self._report_status(module_name, "failed")

                    # 关键模块失败 → 整体失败
                    if isinstance(e, CriticalError):
                        raise

                elapsed_ms = int((time.time() - start_time) * 1000)
                logger.info(
                    f"========== 完成模块 {module_name}，耗时 {elapsed_ms}ms =========="
                )

                # 通知模块完成
                self._report_status(module_name, "completed")

                # 3. 更新进度（包含耗时）
                completed_weight += module.get_progress_weight()
                self._report_progress(module_name, completed_weight, elapsed_ms)

            # 4. 全流程完成
            context.status = "completed"
            summary = context.build_summary()
            ProcessingHistoryDAO.complete_run(
                run_id=run_id,
                status="completed",
                total_records=summary.get("total_input_records", 0),
                output_records=summary.get("total_output_records", 0),
                error_records=summary.get("total_error_records", 0),
                summary=summary,
            )
            logger.info(f"流程完成 run_id={run_id}")

        except CriticalError as e:
            context.status = "failed"
            context.build_summary()
            ProcessingHistoryDAO.complete_run(
                run_id=run_id,
                status="failed",
                summary=context.summary,
            )
            logger.critical(f"流程关键错误 run_id={run_id}: {e}")
            raise

        except CriticalError:
            context.status = "failed"
            context.build_summary()
            ProcessingHistoryDAO.complete_run(
                run_id=run_id,
                status="failed",
                summary=context.summary,
            )
            logger.critical(f"流程关键错误 run_id={run_id}")
            raise

        return context

    def _report_progress(self, module_name: str, completed_weight: int, duration_ms: int = None):
        """向回调函数报告进度"""
        if self.progress_callback:
            percent = min(int(completed_weight / self._total_weight * 100), 99)
            self.progress_callback(module_name, percent, duration_ms)

    def _report_status(self, module_name: str, status: str):
        """向回调函数报告模块状态"""
        if self.status_callback:
            self.status_callback(module_name, status)
