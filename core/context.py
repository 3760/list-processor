"""
核心层 - 处理上下文数据结构

ProcessContext 是贯穿整个处理流程的上下文对象，
在各模块之间传递输入文件路径、DataFrame 状态、处理摘要等信息。
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

import polars as pl

from infra.log_manager import get_logger

logger = get_logger(__name__)


@dataclass
class ProcessContext:
    """
    处理上下文

    Attributes
    ----------
    run_id : str
        唯一运行标识符（UUID）
    input_files : Dict[str, str]
        输入文件路径字典
        键：一线 / 三方 / HW
        值：文件绝对路径
    start_time : datetime
        处理开始时间
    end_time : Optional[datetime]
        处理结束时间
    status : str
        running / completed / failed
    field_spec : Optional[Dict]
        字段规范（从 field_spec.yaml 加载）
    dict_loader : Optional[Any]
        字典加载器实例
    dataframes : Dict[str, Optional[pl.DataFrame]]
        各名单处理后的 DataFrame（键同上）
    error_records : Dict[str, pl.DataFrame]
        各模块的错误记录 DataFrame
    module_results : Dict[str, Dict]
        各模块的执行结果（成功数、失败数等）
    summary : Dict[str, Any]
        处理摘要（最终汇总）
    """

    input_files: Dict[str, str] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    status: str = "running"
    field_spec: Optional[Dict] = None
    dict_loader: Optional[Any] = None
    dataframes: Dict[str, Optional[pl.DataFrame]] = field(
        default_factory=lambda: {
            "yixian": None,
            "sanfang": None,
            "hw": None,
        }
    )
    error_records: Dict[str, pl.DataFrame] = field(
        default_factory=lambda: {
            "yixian": None,
            "sanfang": None,
            "hw": None,
        }
    )
    module_results: Dict[str, Dict] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    dedup_field: Optional[str] = None  # NQ-02 三级去重字段识别策略确定的字段名
    output_path: Optional[str] = None  # F7 输出文件路径
    dict_file_path: Optional[str] = None  # 数据字典文件路径（F1 加载后设置）
    spec_file_path: Optional[str] = None  # 字段规范文件路径（F1 加载后设置）

    def set_input_file(self, list_type: str, file_path: str) -> None:
        """设置指定类型名单的输入文件路径"""
        key_map = {"一线": "yixian", "三方": "sanfang", "hw": "hw", "HW": "hw"}
        key = key_map.get(list_type, list_type)
        self.input_files[key] = file_path

    def get_input_file(self, list_type: str) -> Optional[str]:
        """获取指定类型名单的输入文件路径"""
        key_map = {"一线": "yixian", "三方": "sanfang", "hw": "hw", "HW": "hw"}
        key = key_map.get(list_type, list_type)
        return self.input_files.get(key)

    def set_dataframe(self, list_type: str, df: Optional[pl.DataFrame]) -> None:
        """存储处理后的 DataFrame"""
        self.dataframes[list_type] = df

    def get_dataframe(self, list_type: str) -> Optional[pl.DataFrame]:
        """获取处理后的 DataFrame"""
        return self.dataframes.get(list_type)

    def record_module_result(
        self,
        module: str,
        success_count: int = 0,
        fail_count: int = 0,
        skip_count: int = 0,
        message: Optional[str] = None,
        rejected: Optional["pl.DataFrame"] = None,  # [20260420-老谈] ISSUE-08: 支持重复行详情
    ) -> None:
        """记录模块执行结果"""
        self.module_results[module] = {
            "success": success_count,
            "fail": fail_count,
            "skip": skip_count,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "rejected": rejected,  # ISSUE-08: 重复行详情供 F7 使用
        }
        logger.debug(
            f"模块 {module} 结果: 成功={success_count}, "
            f"失败={fail_count}, 跳过={skip_count}"
        )

    def build_summary(self) -> Dict[str, Any]:
        """构建处理摘要"""
        self.end_time = datetime.now()
        total_input = 0
        total_output = 0
        total_error = 0

        for list_type, df in self.dataframes.items():
            if df is not None:
                total_input += len(df)

        for module, result in self.module_results.items():
            total_output += result.get("success", 0)
            total_error += result.get("fail", 0)

        # [20260420] ISSUE-08: 转换 module_results 中的 DataFrame 为可序列化格式
        module_results_serializable = {}
        for module, result in self.module_results.items():
            module_results_serializable[module] = {
                "success": result.get("success", 0),
                "fail": result.get("fail", 0),
                "skip": result.get("skip", 0),
                "message": result.get("message"),
                "timestamp": result.get("timestamp"),
                "rejected_count": len(result["rejected"]) if result.get("rejected") is not None else 0,
            }

        self.summary = {
            "run_id": self.run_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_sec": (self.end_time - self.start_time).total_seconds(),
            "input_files": self.input_files,
            "total_input_records": total_input,
            "total_output_records": total_output,
            "total_error_records": total_error,
            "module_results": module_results_serializable,
            "status": self.status,
        }
        return self.summary
