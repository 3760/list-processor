"""
F3 跨名单去重标注模块 - 单元测试

覆盖范围：
- validate_input: 前置条件检查
- execute: 核心去重逻辑
  - F3-01: 三方 vs 一线标注
  - F3-02: HW vs 一线标注
  - F3-03: HW vs 三方标注
  - F3-04: 标注结果保存
  - F3-05: 大小写不敏感 + 去首尾空格
"""

import pytest

import polars as pl

from core.context import ProcessContext
from modules.f3_priority_dedup import PriorityDedupModule


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def dedup_module():
    """创建 F3 模块实例"""
    return PriorityDedupModule()


@pytest.fixture
def base_context():
    """创建基础上下文"""
    ctx = ProcessContext()
    ctx.dedup_field = "邮箱"
    return ctx


@pytest.fixture
def yixian_df():
    """创建一线名单 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "李四", "王五"],
        "邮箱": ["a@example.com", "b@example.com", "c@example.com"],
        "_来源": ["一线", "一线", "一线"],
    })


@pytest.fixture
def sanfang_df():
    """创建三方名单 DataFrame"""
    return pl.DataFrame({
        "姓名": ["赵六", "孙七", "周八"],
        "邮箱": ["a@example.com", "d@example.com", "e@example.com"],
        "_来源": ["三方", "三方", "三方"],
    })


@pytest.fixture
def hw_df():
    """创建 HW 名单 DataFrame"""
    return pl.DataFrame({
        "姓名": ["吴九", "郑十", "钱十一"],
        "邮箱": ["b@example.com", "d@example.com", "f@example.com"],
        "_来源": ["HW", "HW", "HW"],
    })


# ============================================================
# validate_input 测试
# ============================================================

class TestValidateInput:
    """前置条件检查测试"""

    def test_validate_input_success(self, dedup_module, base_context, yixian_df):
        """前置检查通过：有一线名单"""
        base_context.set_dataframe("yixian", yixian_df)

        passed, msg = dedup_module.validate_input(base_context)

        assert passed is True
        assert msg == ""

    def test_validate_input_only_hw(self, dedup_module, base_context, hw_df):
        """前置检查通过：只有 HW 名单"""
        base_context.set_dataframe("hw", hw_df)

        passed, msg = dedup_module.validate_input(base_context)

        assert passed is True

    def test_validate_input_no_df(self, dedup_module, base_context):
        """前置检查失败：三份名单均不存在"""
        passed, msg = dedup_module.validate_input(base_context)

        assert passed is False
        assert "F1" in msg

    def test_validate_input_no_dedup_field(self, dedup_module, yixian_df):
        """前置检查失败：去重字段未设置"""
        ctx = ProcessContext()
        ctx.set_dataframe("yixian", yixian_df)

        passed, msg = dedup_module.validate_input(ctx)

        assert passed is False
        assert "去重字段" in msg

    def test_validate_input_dedup_field_missing(self, dedup_module, yixian_df):
        """前置检查失败：去重字段不存在"""
        ctx = ProcessContext()
        ctx.dedup_field = "不存在的字段"
        ctx.set_dataframe("yixian", yixian_df)

        passed, msg = dedup_module.validate_input(ctx)

        assert passed is False


# ============================================================
# execute 测试
# ============================================================

class TestExecute:
    """核心去重逻辑测试"""

    def test_execute_all_three_lists(self, dedup_module, base_context, yixian_df, sanfang_df, hw_df):
        """完整场景：三份名单都存在"""
        base_context.set_dataframe("yixian", yixian_df)
        base_context.set_dataframe("sanfang", sanfang_df)
        base_context.set_dataframe("hw", hw_df)

        result_ctx = dedup_module.execute(base_context)

        # 检查三方标注
        sanfang_result = result_ctx.get_dataframe("sanfang")
        assert "是否已在一线名单" in sanfang_result.columns
        assert sanfang_result["是否已在一线名单"].to_list() == ["是", "否", "否"]

        # 检查 HW 标注
        hw_result = result_ctx.get_dataframe("hw")
        assert "是否已在一线名单" in hw_result.columns
        assert "是否已在三方名单" in hw_result.columns

    def test_execute_only_sanfang(self, dedup_module, base_context, yixian_df, sanfang_df):
        """部分场景：只有一线和三方"""
        base_context.set_dataframe("yixian", yixian_df)
        base_context.set_dataframe("sanfang", sanfang_df)

        result_ctx = dedup_module.execute(base_context)

        sanfang_result = result_ctx.get_dataframe("sanfang")
        assert "是否已在一线名单" in sanfang_result.columns

    def test_execute_empty_context(self, dedup_module, base_context):
        """边界用例：无任何名单"""
        result_ctx = dedup_module.execute(base_context)

        module_result = result_ctx.module_results.get("F3", {})
        assert module_result["success"] == 0


# ============================================================
# F3-01: 三方 vs 一线标注测试
# ============================================================

class TestSanfangVsYixian:
    """F3-01 三方 vs 一线标注测试"""

    def test_sanfang_in_yixian(self, dedup_module, base_context, yixian_df, sanfang_df):
        """三方名单中与一线重复的标注"是" """
        base_context.set_dataframe("yixian", yixian_df)
        base_context.set_dataframe("sanfang", sanfang_df)

        result_ctx = dedup_module.execute(base_context)
        sanfang_result = result_ctx.get_dataframe("sanfang")

        # sanfang_df 第一行邮箱是 "a@example.com"，与一线重复
        assert sanfang_result["是否已在一线名单"][0] == "是"

    def test_sanfang_not_in_yixian(self, dedup_module, base_context, yixian_df, sanfang_df):
        """三方名单中与一线不重复的标注"否" """
        base_context.set_dataframe("yixian", yixian_df)
        base_context.set_dataframe("sanfang", sanfang_df)

        result_ctx = dedup_module.execute(base_context)
        sanfang_result = result_ctx.get_dataframe("sanfang")

        # sanfang_df 第二、三行邮箱与一线不重复
        assert sanfang_result["是否已在一线名单"][1] == "否"
        assert sanfang_result["是否已在一线名单"][2] == "否"


# ============================================================
# F3-02: HW vs 一线标注测试
# ============================================================

class TestHWVsYixian:
    """F3-02 HW vs 一线标注测试"""

    def test_hw_in_yixian(self, dedup_module, base_context, yixian_df, hw_df):
        """HW 名单中与一线重复的标注"是" """
        base_context.set_dataframe("yixian", yixian_df)
        base_context.set_dataframe("hw", hw_df)

        result_ctx = dedup_module.execute(base_context)
        hw_result = result_ctx.get_dataframe("hw")

        # hw_df 第一行邮箱是 "b@example.com"，与一线重复
        assert hw_result["是否已在一线名单"][0] == "是"


# ============================================================
# F3-03: HW vs 三方标注测试
# ============================================================

class TestHWVsSanfang:
    """F3-03 HW vs 三方标注测试"""

    def test_hw_in_sanfang(self, dedup_module, base_context, yixian_df, sanfang_df, hw_df):
        """HW 名单中与三方重复的标注"是" """
        base_context.set_dataframe("yixian", yixian_df)
        base_context.set_dataframe("sanfang", sanfang_df)
        base_context.set_dataframe("hw", hw_df)

        result_ctx = dedup_module.execute(base_context)
        hw_result = result_ctx.get_dataframe("hw")

        # hw_df 第二行邮箱是 "d@example.com"，与三方重复
        assert hw_result["是否已在三方名单"][1] == "是"


# ============================================================
# F3-05: 大小写不敏感 + 去首尾空格测试
# ============================================================

class TestCaseInsensitive:
    """F3-05 大小写不敏感 + 去首尾空格测试"""

    def test_case_insensitive(self, dedup_module, base_context):
        """大小写不敏感匹配"""
        yixian = pl.DataFrame({
            "姓名": ["张三"],
            "邮箱": ["User@Example.com"],
            "_来源": ["一线"],
        })
        sanfang = pl.DataFrame({
            "姓名": ["李四"],
            "邮箱": ["user@example.com"],  # 小写
            "_来源": ["三方"],
        })

        base_context.set_dataframe("yixian", yixian)
        base_context.set_dataframe("sanfang", sanfang)

        result_ctx = dedup_module.execute(base_context)
        sanfang_result = result_ctx.get_dataframe("sanfang")

        assert sanfang_result["是否已在一线名单"][0] == "是"

    def test_strip_whitespace(self, dedup_module, base_context):
        """去除首尾空格匹配"""
        yixian = pl.DataFrame({
            "姓名": ["张三"],
            "邮箱": ["user@example.com"],
            "_来源": ["一线"],
        })
        sanfang = pl.DataFrame({
            "姓名": ["李四"],
            "邮箱": ["  user@example.com  "],  # 首尾空格
            "_来源": ["三方"],
        })

        base_context.set_dataframe("yixian", yixian)
        base_context.set_dataframe("sanfang", sanfang)

        result_ctx = dedup_module.execute(base_context)
        sanfang_result = result_ctx.get_dataframe("sanfang")

        assert sanfang_result["是否已在一线名单"][0] == "是"


# ============================================================
# F3-04: 标注结果保存测试
# ============================================================

class TestAnnotationPreservation:
    """F3-04 标注结果保存测试"""

    def test_original_data_preserved(self, dedup_module, base_context, yixian_df, sanfang_df):
        """原始数据保留"""
        base_context.set_dataframe("yixian", yixian_df)
        base_context.set_dataframe("sanfang", sanfang_df)

        result_ctx = dedup_module.execute(base_context)
        sanfang_result = result_ctx.get_dataframe("sanfang")

        # 原始列保留
        assert "姓名" in sanfang_result.columns
        assert "邮箱" in sanfang_result.columns
        assert "邮箱" in sanfang_result.columns


# ============================================================
# 模块结果测试
# ============================================================

class TestModuleResult:
    """模块结果测试"""

    def test_module_result_format(self, dedup_module, base_context, yixian_df, sanfang_df):
        """模块结果格式正确"""
        base_context.set_dataframe("yixian", yixian_df)
        base_context.set_dataframe("sanfang", sanfang_df)

        result_ctx = dedup_module.execute(base_context)
        module_result = result_ctx.module_results.get("F3", {})

        assert "success" in module_result
        assert "fail" in module_result
        assert "message" in module_result


# ============================================================
# 运行入口
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
