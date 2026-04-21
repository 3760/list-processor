"""
F6 名单内部重复检查模块 - 单元测试

覆盖范围：
- validate_input: 前置条件检查
- execute: 核心去重逻辑
  - F6-01: 重复标注（"原始"/"重复"）
  - F6-02: 重复组完整输出
  - F6-03: 空值不参与去重
  - F6-05/F6-06: 全部重复/全部唯一场景
"""

import pytest

import polars as pl

from core.context import ProcessContext
from modules.f6_internal_dedup import InternalDedupModule


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def dedup_module():
    """创建 F6 模块实例"""
    return InternalDedupModule()


@pytest.fixture
def base_context():
    """创建基础上下文"""
    ctx = ProcessContext()
    ctx.dedup_field = "邮箱"
    return ctx


@pytest.fixture
def duplicate_df():
    """创建含重复数据的 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "李四", "王五", "赵六", "孙七"],
        "邮箱": ["a@example.com", "b@example.com", "a@example.com", "c@example.com", "a@example.com"],
        "_来源": ["一线", "一线", "一线", "一线", "一线"],
    })


@pytest.fixture
def all_unique_df():
    """创建无重复数据的 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "李四", "王五"],
        "邮箱": ["a@example.com", "b@example.com", "c@example.com"],
        "_来源": ["一线", "一线", "一线"],
    })


@pytest.fixture
def all_duplicate_df():
    """创建全部重复数据的 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "李四", "王五"],
        "邮箱": ["same@example.com", "same@example.com", "same@example.com"],
        "_来源": ["一线", "一线", "一线"],
    })


@pytest.fixture
def null_email_df():
    """创建含空邮箱的 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "李四", "王五", "赵六"],
        "邮箱": ["a@example.com", None, "b@example.com", None],
        "_来源": ["一线", "一线", "一线", "一线"],
    })


# ============================================================
# validate_input 测试
# ============================================================

class TestValidateInput:
    """前置条件检查测试"""

    def test_validate_input_success(self, dedup_module, base_context, duplicate_df):
        """前置检查通过"""
        base_context.set_dataframe("yixian", duplicate_df)

        passed, msg = dedup_module.validate_input(base_context)

        assert passed is True
        assert msg == ""

    def test_validate_input_no_yixian(self, dedup_module, base_context):
        """前置检查失败：一线名单不存在"""
        passed, msg = dedup_module.validate_input(base_context)

        assert passed is False
        assert "F1" in msg

    def test_validate_input_no_dedup_field(self, dedup_module, duplicate_df):
        """前置检查失败：去重字段未设置"""
        ctx = ProcessContext()
        ctx.set_dataframe("yixian", duplicate_df)

        passed, msg = dedup_module.validate_input(ctx)

        assert passed is False
        assert "去重字段" in msg

    def test_validate_input_dedup_field_missing(self, dedup_module, duplicate_df):
        """前置检查失败：去重字段不存在"""
        ctx = ProcessContext()
        ctx.dedup_field = "不存在的字段"
        ctx.set_dataframe("yixian", duplicate_df)

        passed, msg = dedup_module.validate_input(ctx)

        assert passed is False
        assert "不存在的字段" in msg


# ============================================================
# execute 测试
# ============================================================

class TestExecute:
    """核心去重逻辑测试"""

    def test_execute_normal_duplicate(self, dedup_module, base_context, duplicate_df):
        """正常重复场景"""
        base_context.set_dataframe("yixian", duplicate_df)

        result_ctx = dedup_module.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        assert "内部去重结果" in result_df.columns
        results = result_df["内部去重结果"].to_list()
        assert results.count("原始") == 3
        assert results.count("重复") == 2

    def test_execute_all_unique(self, dedup_module, base_context, all_unique_df):
        """F6-06: 全部唯一场景"""
        base_context.set_dataframe("yixian", all_unique_df)

        result_ctx = dedup_module.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        results = result_df["内部去重结果"].to_list()
        assert all(r == "原始" for r in results)

    def test_execute_all_duplicate(self, dedup_module, base_context, all_duplicate_df):
        """F6-05: 全部重复场景"""
        base_context.set_dataframe("yixian", all_duplicate_df)

        result_ctx = dedup_module.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        results = result_df["内部去重结果"].to_list()
        assert results == ["原始", "重复", "重复"]

    def test_execute_empty_df(self, dedup_module, base_context):
        """边界用例：空 DataFrame"""
        base_context.set_dataframe("yixian", pl.DataFrame())

        result_ctx = dedup_module.execute(base_context)

        module_result = result_ctx.module_results.get("F6", {})
        assert module_result["success"] == 0
        assert module_result["fail"] == 0


# ============================================================
# F6-01: 重复标注测试
# ============================================================

class TestDuplicateMarking:
    """F6-01 重复标注测试"""

    def test_first_occurrence_marked_original(self, dedup_module, base_context):
        """第1次出现标注为"原始" """
        df = pl.DataFrame({
            "姓名": ["张三", "李四"],
            "邮箱": ["same@example.com", "same@example.com"],
            "_来源": ["一线", "一线"],
        })
        base_context.set_dataframe("yixian", df)

        result_ctx = dedup_module.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        assert result_df["内部去重结果"][0] == "原始"

    def test_second_occurrence_marked_duplicate(self, dedup_module, base_context):
        """第2次出现标注为"重复" """
        df = pl.DataFrame({
            "姓名": ["张三", "李四"],
            "邮箱": ["same@example.com", "same@example.com"],
            "_来源": ["一线", "一线"],
        })
        base_context.set_dataframe("yixian", df)

        result_ctx = dedup_module.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        assert result_df["内部去重结果"][1] == "重复"

    def test_multiple_duplicates_all_marked(self, dedup_module, base_context):
        """多次重复均标注"""
        df = pl.DataFrame({
            "姓名": ["张三", "李四", "王五", "赵六"],
            "邮箱": ["x@x.com", "x@x.com", "x@x.com", "x@x.com"],
            "_来源": ["一线", "一线", "一线", "一线"],
        })
        base_context.set_dataframe("yixian", df)

        result_ctx = dedup_module.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        results = result_df["内部去重结果"].to_list()
        assert results == ["原始", "重复", "重复", "重复"]


# ============================================================
# F6-03: 空值处理测试
# ============================================================

class TestNullHandling:
    """F6-03 空值处理测试"""

    def test_null_not_in_dedup(self, dedup_module, base_context, null_email_df):
        """空值不参与去重"""
        base_context.set_dataframe("yixian", null_email_df)

        result_ctx = dedup_module.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        assert len(result_df) == 4
        assert len(result_df["内部去重结果"].to_list()) == 4


# ============================================================
# F6-02: 重复组完整输出测试
# ============================================================

class TestDuplicateGroupOutput:
    """F6-02 重复组完整输出测试"""

    def test_duplicate_group_complete(self, dedup_module, base_context, duplicate_df):
        """重复组完整输出"""
        base_context.set_dataframe("yixian", duplicate_df)

        result_ctx = dedup_module.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        assert "姓名" in result_df.columns
        assert "邮箱" in result_df.columns
        assert len(result_df) == 5


# ============================================================
# 模块结果测试
# ============================================================

class TestModuleResult:
    """模块结果测试"""

    def test_module_result_format(self, dedup_module, base_context, duplicate_df):
        """模块结果格式正确"""
        base_context.set_dataframe("yixian", duplicate_df)

        result_ctx = dedup_module.execute(base_context)
        module_result = result_ctx.module_results.get("F6", {})

        assert "success" in module_result
        assert "fail" in module_result
        assert "message" in module_result

    def test_module_result_counts(self, dedup_module, base_context, duplicate_df):
        """模块结果计数正确"""
        base_context.set_dataframe("yixian", duplicate_df)

        result_ctx = dedup_module.execute(base_context)
        module_result = result_ctx.module_results.get("F6", {})

        assert module_result["success"] == 3
        assert module_result["fail"] == 2


# ============================================================
# 运行入口
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
