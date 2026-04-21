"""
F2 字段合规检查模块 - 单元测试

覆盖范围：
- validate_input: 前置条件检查
- execute: 核心校验逻辑
  - F2-01: 必填字段检查
  - F2-02: 数据类型检查
  - F2-03: 长度上限检查
  - F2-04: 正则规则检查
"""

import pytest

import polars as pl
from polars.testing import assert_frame_equal

from core.context import ProcessContext
from infra.exceptions import ValidationError
from modules.f2_field_validator import FieldValidatorModule


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def field_validator():
    """创建 F2 模块实例"""
    return FieldValidatorModule()


@pytest.fixture
def base_context():
    """创建基础上下文（含一线名单）"""
    ctx = ProcessContext()
    ctx.field_spec = {
        "fields": {
            "姓名": {"attr_code": "name", "required": True, "type": "文本型"},
            "邮箱": {"attr_code": "email", "required": True, "type": "文本型", "regex": "email"},
            "手机号": {"attr_code": "phone", "required": False, "type": "数值型", "regex": "phone"},
            "部门": {"attr_code": "dept", "required": False, "type": "文本型", "max_length": 50},
            "身份证": {"attr_code": "id", "required": False, "type": "文本型", "regex": "id_card"},
        }
    }
    return ctx


@pytest.fixture
def valid_df():
    """创建有效数据 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "李四", "王五"],
        "邮箱": ["zhangsan@example.com", "lisi@example.com", "wangwu@example.com"],
        "手机号": ["13800138000", "13900139000", "13700137000"],
        "部门": ["研发部", "市场部", "人事部"],
        "身份证": ["110101199001011234", "110101199001011235", "110101199001011236"],
        "_来源": ["一线", "一线", "一线"],
    })


@pytest.fixture
def invalid_df():
    """创建包含各类错误的数据 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "", "王五"],  # 空值
        "邮箱": ["invalid_email", "lisi@example.com", "wangwu@example.com"],  # 无效邮箱
        "手机号": ["abc", "13900139000", "13700137000"],  # 无效手机号
        "部门": ["A" * 100, "市场部", "人事部"],  # 超长
        "身份证": ["110101199001011234", "invalid_id", "110101199001011236"],  # 无效身份证
        "_来源": ["一线", "一线", "一线"],
    })


# ============================================================
# validate_input 测试
# ============================================================

class TestValidateInput:
    """前置条件检查测试"""

    def test_validate_input_success(self, field_validator, base_context, valid_df):
        """前置检查通过：field_spec 已加载 + 一线名单存在"""
        base_context.set_dataframe("yixian", valid_df)

        passed, msg = field_validator.validate_input(base_context)

        assert passed is True
        assert msg == ""

    def test_validate_input_no_field_spec(self, field_validator, valid_df):
        """前置检查失败：field_spec 未加载"""
        ctx = ProcessContext()
        ctx.set_dataframe("yixian", valid_df)
        # ctx.field_spec = None（默认）

        passed, msg = field_validator.validate_input(ctx)

        assert passed is False
        assert "字段规范导入" in msg

    def test_validate_input_no_yixian(self, field_validator, base_context):
        """前置检查失败：一线名单不存在"""
        # ctx.set_dataframe("yixian", None)  # 默认

        passed, msg = field_validator.validate_input(base_context)

        assert passed is False
        assert "F1" in msg


# ============================================================
# execute 测试
# ============================================================

class TestExecute:
    """核心校验逻辑测试"""

    def test_execute_all_valid(self, field_validator, base_context, valid_df):
        """正向用例：所有数据合规"""
        base_context.set_dataframe("yixian", valid_df)

        result_ctx = field_validator.execute(base_context)

        # 无错误记录
        assert result_ctx.error_records["yixian"] is None
        # 模块结果：全部成功
        module_result = result_ctx.module_results.get("F2", {})
        assert module_result["success"] == 3
        assert module_result["fail"] == 0

    def test_execute_all_invalid(self, field_validator, base_context, invalid_df):
        """异常用例：所有数据不合规"""
        base_context.set_dataframe("yixian", invalid_df)

        result_ctx = field_validator.execute(base_context)

        # 有错误记录
        assert result_ctx.error_records["yixian"] is not None
        error_df = result_ctx.error_records["yixian"]
        assert len(error_df) > 0
        # 模块结果：全部失败
        module_result = result_ctx.module_results.get("F2", {})
        assert module_result["fail"] > 0

    def test_execute_empty_df(self, field_validator, base_context):
        """边界用例：空 DataFrame"""
        base_context.set_dataframe("yixian", pl.DataFrame())

        result_ctx = field_validator.execute(base_context)

        assert result_ctx.error_records["yixian"] is None
        module_result = result_ctx.module_results.get("F2", {})
        assert module_result["success"] == 0
        assert module_result["fail"] == 0


# ============================================================
# F2-01: 必填字段检查测试
# ============================================================

class TestRequiredFields:
    """F2-01 必填字段检查"""

    def test_required_field_missing(self, field_validator, base_context):
        """必填字段缺失（整列缺失）"""
        df = pl.DataFrame({
            "邮箱": ["test@example.com"],
            "_来源": ["一线"],
        })
        base_context.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(base_context)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        required_errors = error_df.filter(pl.col("问题类型") == "REQUIRED_MISSING")
        assert len(required_errors) == 1

    def test_required_field_empty(self, field_validator, base_context):
        """必填字段值为空"""
        df = pl.DataFrame({
            "姓名": ["", "张三", "李四"],  # 第1行为空
            "邮箱": ["a@a.com", "b@b.com", "c@c.com"],
            "_来源": ["一线", "一线", "一线"],
        })
        base_context.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(base_context)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        empty_errors = error_df.filter(pl.col("问题类型") == "REQUIRED_EMPTY")
        assert len(empty_errors) >= 1


# ============================================================
# F2-02: 数据类型检查测试
# ============================================================

class TestDataTypes:
    """F2-02 数据类型检查"""

    def test_type_mismatch_integer(self, field_validator):
        """数值型字段类型错误"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                # [20260420-老谈] ISSUE-02: 测试数据对齐 PRD §8.1 键名 "type"
                "手机号": {"attr_code": "phone", "required": False, "type": "integer"},
            }
        }
        df = pl.DataFrame({
            "手机号": ["abc123", "12345678901"],  # 第1行非数值
            "_来源": ["一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        type_errors = error_df.filter(pl.col("问题类型") == "TYPE_MISMATCH")
        assert len(type_errors) >= 1

    def test_type_mismatch_date(self, field_validator):
        """日期型字段类型错误"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                # [20260420-老谈] ISSUE-02: 测试数据对齐 PRD §8.1 键名 "type"
                "生日": {"attr_code": "birthday", "required": False, "type": "date"},
            }
        }
        df = pl.DataFrame({
            "生日": ["2024-01-01", "not_a_date"],
            "_来源": ["一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        type_errors = error_df.filter(pl.col("问题类型") == "TYPE_MISMATCH")
        assert len(type_errors) >= 1


# ============================================================
# F2-03: 长度上限检查测试
# ============================================================

class TestMaxLength:
    """F2-03 长度上限检查"""

    def test_length_exceed(self, field_validator):
        """字段值超出长度上限"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                "部门": {"attr_code": "dept", "required": False, "data_type": "text", "max_length": 10},
            }
        }
        df = pl.DataFrame({
            "部门": ["ABCDEFGHIJKLMNOP", "研发部"],  # 第1行超过10字符
            "_来源": ["一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        length_errors = error_df.filter(pl.col("问题类型") == "LENGTH_EXCEED")
        assert len(length_errors) == 1
        assert length_errors[0, "字段名"] == "部门"

    def test_length_no_limit(self, field_validator):
        """长度上限为0（不限制）"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                "描述": {"attr_code": "desc", "required": False, "type": "文本型", "max_length": 0},
            }
        }
        df = pl.DataFrame({
            "描述": ["A" * 10000, "B" * 20000],  # 很长但不限制
            "_来源": ["一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        # max_length=0 表示不限制，不应有长度错误
        length_errors = error_df.filter(pl.col("问题类型") == "LENGTH_EXCEED") if error_df is not None else pl.DataFrame()
        assert len(length_errors) == 0


# ============================================================
# F2-04: 正则规则检查测试
# ============================================================

class TestRegexRules:
    """F2-04 正则规则检查"""

    def test_email_regex_valid(self, field_validator):
        """邮箱格式正确"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                "邮箱": {"attr_code": "email", "required": True, "regex": "email"},
            }
        }
        df = pl.DataFrame({
            "邮箱": ["test@example.com", "user.name@domain.co.uk"],
            "_来源": ["一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        if error_df is not None:
            regex_errors = error_df.filter(pl.col("问题类型") == "REGEX_FAILED")
            assert len(regex_errors) == 0

    def test_email_regex_invalid(self, field_validator):
        """邮箱格式错误"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                "邮箱": {"attr_code": "email", "required": True, "regex": "email"},
            }
        }
        df = pl.DataFrame({
            "邮箱": ["invalid_email", "test@example.com"],
            "_来源": ["一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        regex_errors = error_df.filter(pl.col("问题类型") == "REGEX_FAILED")
        assert len(regex_errors) == 1

    def test_phone_regex_invalid(self, field_validator):
        """手机号格式错误"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                "手机号": {"attr_code": "phone", "required": False, "regex": "phone"},
            }
        }
        df = pl.DataFrame({
            "手机号": ["12345", "13800138000"],  # 第1行格式错误
            "_来源": ["一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        regex_errors = error_df.filter(pl.col("问题类型") == "REGEX_FAILED")
        assert len(regex_errors) >= 1

    def test_id_card_regex_invalid(self, field_validator):
        """身份证格式错误"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                "身份证": {"attr_code": "id", "required": False, "regex": "id_card"},
            }
        }
        df = pl.DataFrame({
            "身份证": ["123456789012345678", "110101199001011234"],  # 第1行格式错误
            "_来源": ["一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        regex_errors = error_df.filter(pl.col("问题类型") == "REGEX_FAILED")
        assert len(regex_errors) >= 1


# ============================================================
# 错误汇总测试
# ============================================================

class TestErrorSummary:
    """错误汇总测试（F2-06）"""

    def test_error_type_distribution(self, field_validator, base_context, invalid_df):
        """错误类型分布统计"""
        base_context.set_dataframe("yixian", invalid_df)

        result_ctx = field_validator.execute(base_context)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None

        # 验证包含多种错误类型
        error_types = error_df["问题类型"].unique().to_list()
        assert len(error_types) > 1  # 至少有两种错误类型

        # 验证错误记录包含必要字段
        assert "字段名" in error_df.columns
        assert "行号" in error_df.columns
        assert "原始值" in error_df.columns
        assert "问题类型" in error_df.columns
        assert "说明" in error_df.columns

    def test_module_result_summary(self, field_validator, base_context, invalid_df):
        """模块结果汇总"""
        base_context.set_dataframe("yixian", invalid_df)

        result_ctx = field_validator.execute(base_context)

        module_result = result_ctx.module_results.get("F2", {})
        assert "success" in module_result
        assert "fail" in module_result
        assert "message" in module_result

        # 失败数 > 0
        assert module_result["fail"] > 0


# ============================================================
# 边界用例测试
# ============================================================

class TestBoundaryCases:
    """边界用例测试"""

    def test_special_characters(self, field_validator):
        """特殊字符处理"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                "姓名": {"attr_code": "name", "required": True, "type": "文本型"},
            }
        }
        df = pl.DataFrame({
            "姓名": ["张三", "李四★", "王五🎉", ""],  # 特殊字符
            "_来源": ["一线", "一线", "一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        # 空字符串应触发必填字段检查
        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None

    def test_null_values_handling(self, field_validator):
        """NULL 值处理"""
        ctx = ProcessContext()
        ctx.field_spec = {
            "fields": {
                "姓名": {"attr_code": "name", "required": True, "type": "文本型"},
            }
        }
        df = pl.DataFrame({
            "姓名": [None, "张三", None],
            "_来源": ["一线", "一线", "一线"],
        })
        ctx.set_dataframe("yixian", df)

        result_ctx = field_validator.execute(ctx)

        error_df = result_ctx.error_records["yixian"]
        assert error_df is not None
        empty_errors = error_df.filter(pl.col("问题类型").is_in(["REQUIRED_EMPTY", "REQUIRED_MISSING"]))
        assert len(empty_errors) >= 2


# ============================================================
# Mock 数据测试
# ============================================================

class TestMockData:
    """使用 Mock 数据的集成测试"""

    def test_mock_spec_and_data(self, field_validator, base_context, valid_df):
        """使用完整 mock 数据测试"""
        base_context.set_dataframe("yixian", valid_df)

        result_ctx = field_validator.execute(base_context)

        # 全部合规，无错误
        if result_ctx.error_records["yixian"] is not None:
            regex_errors = result_ctx.error_records["yixian"].filter(
                pl.col("问题类型") == "REGEX_FAILED"
            )
            assert len(regex_errors) == 0


# ============================================================
# 运行入口
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
