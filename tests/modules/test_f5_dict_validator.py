"""
F5 模块单元测试

测试覆盖：
- 前置检查 validate_input（dict_loader + _Code 列）
- 核心校验逻辑 execute
- 边界场景

A-11 前置检查要求（测试用例评审 F-09）：
- dict_loader 未初始化 → 阻止执行
- 一线名单 _Code 列不存在 → 阻止执行
"""

import pytest
import polars as pl

from unittest.mock import MagicMock

from core.context import ProcessContext
from modules.f5_dict_validator import DictValidatorModule


class TestDictValidatorModule:
    """F5 字典值合规校验模块测试"""

    @pytest.fixture
    def module(self):
        """创建 F5 模块实例"""
        return DictValidatorModule()

    @pytest.fixture
    def base_context(self):
        """基础上下文（一线名单已加载）"""
        ctx = ProcessContext()
        df = pl.DataFrame({
            "姓名": ["张三", "李四", "王五"],
            "邮箱": ["zhang@example.com", "li@example.com", "wang@example.com"],
        })
        ctx.set_dataframe("yixian", df)
        return ctx

    # ── 前置检查测试（validate_input）─────────────────────────────

    def test_validate_input_dict_loader_not_initialized(self, module, base_context):
        """
        前置检查：dict_loader 未初始化
        预期：返回 False，提示"请先执行 F4 字典上码"
        """
        # dict_loader 未设置（默认为 None）
        assert base_context.dict_loader is None

        passed, error_msg = module.validate_input(base_context)

        assert passed is False
        assert "请先执行 F4 字典上码" in error_msg

    def test_validate_input_dict_loader_initialized_but_no_code_column(
        self, module, base_context
    ):
        """
        前置检查：dict_loader 已初始化，但一线名单无 _Code 列
        预期：返回 False，提示"请先执行 F4 字典上码"
        """
        # 模拟 dict_loader 已初始化
        base_context.dict_loader = MagicMock()

        # 一线名单无 _Code 列
        passed, error_msg = module.validate_input(base_context)

        assert passed is False
        assert "请先执行 F4 字典上码" in error_msg

    def test_validate_input_success(self, module, base_context):
        """
        前置检查：dict_loader 已初始化，一线名单有 _Code 列
        预期：返回 True
        """
        # 模拟 dict_loader 已初始化
        base_context.dict_loader = MagicMock()

        # 添加 _Code 列（模拟 F4 上码结果）
        df = base_context.get_dataframe("yixian")
        df_with_code = df.with_columns(
            pl.lit("A1").alias("邮箱_Code")
        )
        base_context.set_dataframe("yixian", df_with_code)

        passed, error_msg = module.validate_input(base_context)

        assert passed is True
        assert error_msg == ""

    def test_validate_input_no_yixian_dataframe(self, module):
        """
        前置检查：一线名单 DataFrame 不存在
        预期：返回 False，提示"请先执行 F1 文件加载"
        """
        ctx = ProcessContext()
        # dict_loader 已初始化，但无一线名单
        ctx.dict_loader = MagicMock()

        passed, error_msg = module.validate_input(ctx)

        assert passed is False
        assert "请先执行 F1 文件加载" in error_msg

    # ── 核心校验逻辑测试（execute）────────────────────────────────

    def test_execute_all_valid(self, module, base_context):
        """
        场景：所有数据均合规（_Code 列值非"未匹配"）
        预期：error_records 为空，模块结果记录成功数
        """
        # 设置 dict_loader 和 _Code 列
        base_context.dict_loader = MagicMock()
        df = base_context.get_dataframe("yixian")
        df_with_code = df.with_columns(
            pl.lit("A1").alias("邮箱_Code")
        )
        base_context.set_dataframe("yixian", df_with_code)

        # 执行
        ctx = module.execute(base_context)

        # 验证：error_records 应为空
        assert ctx.error_records.get("yixian") is None

        # 验证：模块结果
        result = ctx.module_results.get("F5")
        assert result["success"] == 3  # 3 条合规
        assert result["fail"] == 0
        assert "0 条不合规" in result["message"]

    def test_execute_some_invalid(self, module, base_context):
        """
        场景：部分数据不合规（_Code 列值包含"未匹配"）
        预期：error_records 包含不合规记录，模块结果记录失败数
        """
        # 设置 dict_loader 和 _Code 列（包含"未匹配"）
        base_context.dict_loader = MagicMock()
        df = base_context.get_dataframe("yixian")
        df_with_code = df.with_columns(
            pl.Series("邮箱_Code", ["A1", "未匹配", "A2"])
        )
        base_context.set_dataframe("yixian", df_with_code)

        # 执行
        ctx = module.execute(base_context)

        # 验证：error_records 应包含 1 条不合规记录
        error_df = ctx.error_records.get("yixian")
        assert error_df is not None
        assert len(error_df) == 1

        # 验证：模块结果
        result = ctx.module_results.get("F5")
        assert result["success"] == 2  # 2 条合规
        assert result["fail"] == 1  # 1 条不合规

    def test_execute_all_invalid(self, module, base_context):
        """
        场景：所有数据均不合规（_Code 列值全为"未匹配"）
        预期：error_records 包含所有记录，模块结果记录 0 成功
        """
        # 设置 dict_loader 和 _Code 列（全为"未匹配"）
        base_context.dict_loader = MagicMock()
        df = base_context.get_dataframe("yixian")
        df_with_code = df.with_columns(
            pl.Series("邮箱_Code", ["未匹配", "未匹配", "未匹配"])
        )
        base_context.set_dataframe("yixian", df_with_code)

        # 执行
        ctx = module.execute(base_context)

        # 验证：error_records 应包含 3 条不合规记录
        error_df = ctx.error_records.get("yixian")
        assert error_df is not None
        assert len(error_df) == 3

        # 验证：模块结果
        result = ctx.module_results.get("F5")
        assert result["success"] == 0
        assert result["fail"] == 3

    def test_execute_multiple_code_columns(self, module, base_context):
        """
        场景：一线名单有多个 _Code 列
        预期：正确统计所有列的不合规记录
        """
        # 设置 dict_loader 和多个 _Code 列
        base_context.dict_loader = MagicMock()
        df = base_context.get_dataframe("yixian")
        df_with_codes = df.with_columns(
            pl.Series("邮箱_Code", ["A1", "未匹配", "A2"]),
            pl.Series("区域_Code", ["B1", "B2", "未匹配"]),
        )
        base_context.set_dataframe("yixian", df_with_codes)

        # 执行
        ctx = module.execute(base_context)

        # 验证：error_records 应包含 2 条不合规记录（邮箱1条 + 区域1条）
        error_df = ctx.error_records.get("yixian")
        assert error_df is not None
        assert len(error_df) == 2

        # 验证：错误记录包含正确的字段信息
        fields = error_df["字段名"].to_list()
        assert "邮箱" in fields
        assert "区域" in fields

    def test_execute_empty_yixian(self, module):
        """
        场景：一线名单为空
        预期：跳过校验，记录空结果
        """
        ctx = ProcessContext()
        ctx.dict_loader = MagicMock()
        ctx.set_dataframe("yixian", pl.DataFrame())  # 空 DataFrame

        ctx = module.execute(ctx)

        # 验证：模块结果（空名单不触发跳过逻辑，进入正常校验）
        result = ctx.module_results.get("F5")
        assert result["success"] == 0
        assert result["fail"] == 0
        assert "0 条不合规" in result["message"]

    # ── 集成测试：完整前置检查流程 ─────────────────────────────────

    def test_full_validation_flow(self, module, base_context):
        """
        完整流程：前置检查 → 执行校验
        场景：正常情况下 dict_loader 已初始化且 _Code 列存在
        """
        # 1. 前置检查（应通过）
        base_context.dict_loader = MagicMock()
        df = base_context.get_dataframe("yixian")
        df_with_code = df.with_columns(
            pl.Series("邮箱_Code", ["A1", "A2", "A3"])
        )
        base_context.set_dataframe("yixian", df_with_code)

        passed, error_msg = module.validate_input(base_context)
        assert passed is True

        # 2. 执行校验
        ctx = module.execute(base_context)

        # 验证结果
        result = ctx.module_results.get("F5")
        assert result is not None
        assert result["success"] == 3
        assert result["fail"] == 0


# ── 边界场景测试 ─────────────────────────────────────────────────

class TestDictValidatorBoundaryCases:
    """边界场景测试"""

    @pytest.fixture
    def module(self):
        return DictValidatorModule()

    def test_special_characters_in_value(self, module):
        """
        场景：字段值包含特殊字符
        预期：正确处理
        """
        ctx = ProcessContext()
        ctx.dict_loader = MagicMock()

        df = pl.DataFrame({
            "姓名": ["测试'用户\"1", "测试\\转义", "正常"],
            "邮箱": ["a@example.com", "b@example.com", "c@example.com"],
        }).with_columns(
            pl.Series("邮箱_Code", ["A1", "未匹配", "A2"])
        )
        ctx.set_dataframe("yixian", df)

        ctx = module.execute(ctx)

        error_df = ctx.error_records.get("yixian")
        assert error_df is not None
        assert len(error_df) == 1

    def test_none_value_in_code_column(self, module):
        """
        场景：_Code 列包含 None 值
        预期：正确识别（非"未匹配"）
        """
        ctx = ProcessContext()
        ctx.dict_loader = MagicMock()

        df = pl.DataFrame({
            "姓名": ["张三", "李四"],
            "邮箱": ["a@example.com", "b@example.com"],
        }).with_columns(
            pl.Series("邮箱_Code", ["A1", None])
        )
        ctx.set_dataframe("yixian", df)

        ctx = module.execute(ctx)

        # None 不应被识别为"未匹配"
        error_df = ctx.error_records.get("yixian")
        # 如果 None 没有被转换为字符串"None"，则不会匹配"未匹配"
        assert error_df is None or len(error_df) == 0

    def test_case_sensitivity_of_unmatched_marker(self, module):
        """
        场景：Code 列值包含"未匹配"的大小写变体
        预期：严格匹配"未匹配"（全字匹配）
        """
        ctx = ProcessContext()
        ctx.dict_loader = MagicMock()

        df = pl.DataFrame({
            "姓名": ["张三", "李四", "王五"],
            "邮箱": ["a@example.com", "b@example.com", "c@example.com"],
        }).with_columns(
            pl.Series("邮箱_Code", ["未匹配", "未匹配1", "未匹配"])
        )
        ctx.set_dataframe("yixian", df)

        ctx = module.execute(ctx)

        # 只有严格等于"未匹配"的才会被识别
        error_df = ctx.error_records.get("yixian")
        assert error_df is not None
        assert len(error_df) == 2  # 两个"未匹配"
