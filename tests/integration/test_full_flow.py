"""
集成测试 - 全流程端到端测试

测试场景：
1. ProcessContext 创建和基本操作
2. 错误处理流程

运行方式：
    pytest tests/integration/test_full_flow.py -v
"""

import os
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

import pytest

# 确保项目根目录在 Python 路径中
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.context import ProcessContext


class TestProcessContext:
    """ProcessContext 集成测试"""

    def test_context_creation(self):
        """测试 ProcessContext 创建"""
        ctx = ProcessContext(
            run_id="test-123",
            start_time=datetime.now(),
            input_files={"yixian": "/path/to/file.xlsx"},
        )

        assert ctx.run_id == "test-123"
        assert ctx.status == "running"
        assert ctx.dataframes["yixian"] is None

    def test_context_set_get_dataframe(self):
        """测试 Context 的 DataFrame 存取"""
        import polars as pl

        ctx = ProcessContext()
        df = pl.DataFrame({"col1": [1, 2, 3]})

        ctx.set_dataframe("yixian", df)
        result = ctx.get_dataframe("yixian")

        assert result is not None
        assert len(result) == 3

    def test_context_record_module_result(self):
        """测试 Context 记录模块结果"""
        ctx = ProcessContext()

        ctx.record_module_result("F1", success_count=100, fail_count=5, message="测试完成")

        assert "F1" in ctx.module_results
        assert ctx.module_results["F1"]["success"] == 100
        assert ctx.module_results["F1"]["fail"] == 5

    def test_context_set_input_file(self):
        """测试设置输入文件"""
        ctx = ProcessContext()

        ctx.set_input_file("一线", "/path/to/yixian.xlsx")
        result = ctx.get_input_file("一线")

        assert result == "/path/to/yixian.xlsx"

    def test_context_build_summary(self):
        """测试构建处理摘要"""
        import polars as pl

        ctx = ProcessContext(
            run_id="test-summary",
            start_time=datetime.now(),
        )
        ctx.set_dataframe("yixian", pl.DataFrame({"a": [1, 2, 3]}))
        ctx.record_module_result("F1", success_count=100, fail_count=0)

        summary = ctx.build_summary()

        assert summary["run_id"] == "test-summary"
        assert summary["total_input_records"] == 3
        assert summary["module_results"]["F1"]["success"] == 100


class TestErrorHandling:
    """错误处理测试"""

    def test_context_with_missing_files(self):
        """测试缺少文件的上下文"""
        ctx = ProcessContext(
            run_id=str(uuid.uuid4()),
            start_time=datetime.now(),
            input_files={
                "yixian": None,  # 必选文件缺失
                "sanfang": None,
                "hw": None,
            },
        )

        assert ctx.input_files["yixian"] is None
        assert ctx.status == "running"

    def test_context_status_transitions(self):
        """测试上下文状态转换"""
        ctx = ProcessContext()

        assert ctx.status == "running"

        ctx.status = "completed"
        assert ctx.status == "completed"

        ctx.status = "failed"
        assert ctx.status == "failed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
