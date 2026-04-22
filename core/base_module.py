"""
核心层 - 业务模块基类与接口定义

所有 F1~F7 业务模块均继承 BaseModule，
统一实现 execute / validate / get_module_name 接口。

[方案C] 子任务进度机制：
- Orchestrator 负责主进度条（0%→100%）
- 模块内部通过 _report_progress() 报告子任务细节进度
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Callable

import polars as pl

from core.context import ProcessContext
from infra.log_manager import get_logger

logger = get_logger(__name__)


class BaseModule(ABC):
    """
    业务模块抽象基类。

    所有处理模块（F1~F7）均需实现：
    - get_module_name() : 返回模块标识（用于日志和进度展示）
    - validate_input()  : 校验输入数据是否符合本模块要求
    - execute()         : 执行核心处理逻辑
    """

    @abstractmethod
    def get_module_name(self) -> str:
        """
        返回模块名称。

        Returns
        -------
        str
            如 "F1", "F2", "F4" 等
        """
        pass

    def validate_input(self, context: ProcessContext) -> tuple[bool, str]:
        """
        校验输入数据是否符合本模块的前置条件。

        Parameters
        ----------
        context : ProcessContext
            处理上下文

        Returns
        -------
        tuple[bool, str]
            (是否通过, 错误信息)
        """
        return True, ""

    @abstractmethod
    def execute(self, context: ProcessContext) -> ProcessContext:
        """
        执行本模块的核心处理逻辑。

        Parameters
        ----------
        context : ProcessContext
            处理上下文（输入状态）

        Returns
        -------
        ProcessContext
            更新后的处理上下文
        """
        pass

    def get_progress_weight(self) -> int:
        """
        返回本模块在整体进度中的权重（百分比）。

        子类可覆盖，默认返回 10（所有模块等权重）。
        """
        return 10

    def _report_progress(self, percent: int) -> None:
        """
        [方案C] 报告子任务进度。

        由 Orchestrator 在 execute() 调用前注入 _progress_callback，
        模块内部在关键阶段调用此方法更新子任务进度。

        Parameters
        ----------
        percent : int
            当前进度百分比（0~100），会被追加到模块名下显示
        """
        callback: Optional[Callable] = getattr(self, '_progress_callback', None)
        if callback:
            module_name = self.get_module_name()
            callback(module_name, percent)

    def on_error(self, context: ProcessContext, error: Exception):
        """
        模块执行出错时的回调。

        子类可覆盖以实现错误恢复或清理逻辑。
        """
        logger.error(
            f"模块 {self.get_module_name()} 执行出错: {error}",
            exc_info=True,
        )
        context.status = "failed"

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.get_module_name()})>"
