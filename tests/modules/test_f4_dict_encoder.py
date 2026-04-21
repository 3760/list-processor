"""
F4 字典上码模块 - 单元测试

覆盖范围：
- validate_input: 前置条件检查
- execute: 核心上码逻辑
  - F4-01: 识别字典类型字段
  - F4-03: 新增 _Code 列
  - F4-04: 未匹配填写"未匹配"
  - F4-05: 不区分大小写匹配
  - F4-08: 上码统计
"""

import pytest

import polars as pl

from core.context import ProcessContext
from modules.f4_dict_encoder import DictEncoderModule


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def dict_encoder():
    """创建 F4 模块实例"""
    return DictEncoderModule()


@pytest.fixture
def mock_dict_loader():
    """创建 Mock 字典加载器（与真实 dict_loader 一致，存储 {Code: Label}）"""
    class MockDictLoader:
        def __init__(self):
            self.file_path = "mock_data_dict.xlsx"
            self.md5_hash = "mock_md5_hash"
            # 真实 DictLoader 存储 {Code: Label}（dict_loader.py:82-83）
            # F4 上码时反向查找：原始数据中的 Label → 输出对应的 Code
            self.mappings = {
                "性别": {"M": "男", "F": "女", "U": "未知"},
                "部门": {"RD": "研发部", "MKT": "市场部", "HR": "人事部"},
                "A1": {"Y": "是", "N": "否"},  # 模拟 A1/A2 字典类型
                "A2": {"H": "高", "M": "中", "L": "低"},
            }

        def get_all_dict_names(self):
            return list(self.mappings.keys())

        def lookup(self, dict_name, code_value):
            """按 Code 值查找对应的 Label（真实 DictLoader 行为）"""
            mapping = self.mappings.get(dict_name, {})
            return mapping.get(code_value)

        def get_all_codes(self, dict_name):
            """返回所有 Code 值（即 mapping 的 keys）"""
            mapping = self.mappings.get(dict_name, {})
            return list(mapping.keys())

    return MockDictLoader()


@pytest.fixture
def base_context(mock_dict_loader):
    """创建基础上下文"""
    ctx = ProcessContext()
    ctx.field_spec = {
        "fields": {
            "姓名": {"attr_code": "name", "required": True},
            "性别": {"attr_code": "gender", "required": True, "dict_id": "性别"},  # 字典类型
            "部门": {"attr_code": "dept", "required": False, "dict_id": "部门"},  # 字典类型
            "等级": {"attr_code": "level", "required": False, "dict_id": "A2"},  # 字典类型
            "备注": {"attr_code": "remark", "required": False},  # 非字典类型
        }
    }
    ctx.dict_loader = mock_dict_loader
    return ctx


@pytest.fixture
def valid_df():
    """创建有效数据 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "李四", "王五"],
        "性别": ["男", "女", "男"],
        "部门": ["研发部", "市场部", "人事部"],
        "等级": ["高", "中", "低"],
        "备注": ["正式员工", "外包", ""],
        "_来源": ["一线", "一线", "一线"],
    })


@pytest.fixture
def partial_invalid_df():
    """创建部分无效数据 DataFrame"""
    return pl.DataFrame({
        "姓名": ["张三", "李四", "王五", "赵六"],
        "性别": ["男", "女", "保密", "其他"],  # "男"/"女"匹配，"保密"/"其他"不匹配
        "部门": ["研发部", "市场部", "未知部门", ""],  # "未知部门"不匹配，空值不匹配
        "等级": ["高", "中", "低", "超高"],  # "超高"不匹配
        "备注": ["a", "b", "c", "d"],
        "_来源": ["一线", "一线", "一线", "一线"],
    })


# ============================================================
# validate_input 测试
# ============================================================

class TestValidateInput:
    """前置条件检查测试"""

    def test_validate_input_success(self, dict_encoder, base_context, valid_df):
        """前置检查通过：field_spec + dict_loader + 一线名单均存在"""
        base_context.set_dataframe("yixian", valid_df)

        passed, msg = dict_encoder.validate_input(base_context)

        assert passed is True
        assert msg == ""

    def test_validate_input_no_field_spec(self, dict_encoder, mock_dict_loader, valid_df):
        """前置检查失败：field_spec 未加载"""
        ctx = ProcessContext()
        ctx.dict_loader = mock_dict_loader
        ctx.set_dataframe("yixian", valid_df)

        passed, msg = dict_encoder.validate_input(ctx)

        assert passed is False
        assert "字段规范" in msg

    def test_validate_input_no_dict_loader(self, dict_encoder, base_context, valid_df):
        """前置检查失败：dict_loader 未加载"""
        base_context.dict_loader = None
        base_context.set_dataframe("yixian", valid_df)

        passed, msg = dict_encoder.validate_input(base_context)

        assert passed is False
        assert "数据字典" in msg

    def test_validate_input_no_yixian(self, dict_encoder, base_context):
        """前置检查失败：一线名单不存在"""
        # base_context.set_dataframe("yixian", None)

        passed, msg = dict_encoder.validate_input(base_context)

        assert passed is False
        assert "F1" in msg


# ============================================================
# execute 测试
# ============================================================

class TestExecute:
    """核心上码逻辑测试"""

    def test_execute_all_valid(self, dict_encoder, base_context, valid_df):
        """正向用例：所有数据都能匹配"""
        base_context.set_dataframe("yixian", valid_df)

        result_ctx = dict_encoder.execute(base_context)

        # 检查 _Code 列已生成
        result_df = result_ctx.get_dataframe("yixian")
        assert "性别_Code" in result_df.columns
        assert "部门_Code" in result_df.columns
        assert "等级_Code" in result_df.columns

        # 检查匹配结果
        assert result_df["性别_Code"].to_list() == ["M", "F", "M"]
        assert result_df["部门_Code"].to_list() == ["RD", "MKT", "HR"]
        assert result_df["等级_Code"].to_list() == ["H", "M", "L"]

        # 检查模块结果
        module_result = result_ctx.module_results.get("F4", {})
        assert module_result["success"] > 0
        assert module_result["fail"] == 0

    def test_execute_partial_invalid(self, dict_encoder, base_context, partial_invalid_df):
        """异常用例：部分数据不匹配"""
        base_context.set_dataframe("yixian", partial_invalid_df)

        result_ctx = dict_encoder.execute(base_context)

        # 检查 _Code 列已生成
        result_df = result_ctx.get_dataframe("yixian")
        assert "性别_Code" in result_df.columns
        assert "部门_Code" in result_df.columns

        # 检查"未匹配"标记
        assert "未匹配" in result_df["性别_Code"].to_list()
        assert "未匹配" in result_df["部门_Code"].to_list()

        # 检查模块结果有失败
        module_result = result_ctx.module_results.get("F4", {})
        assert module_result["fail"] > 0

    def test_execute_empty_df(self, dict_encoder, base_context):
        """边界用例：空 DataFrame"""
        base_context.set_dataframe("yixian", pl.DataFrame())

        result_ctx = dict_encoder.execute(base_context)

        module_result = result_ctx.module_results.get("F4", {})
        assert module_result["success"] == 0
        assert module_result["fail"] == 0


# ============================================================
# F4-05: 不区分大小写匹配测试
# ============================================================

class TestCaseInsensitive:
    """F4-05 不区分大小写匹配测试"""

    def test_case_insensitive_match(self, dict_encoder, base_context):
        """不区分大小写匹配"""
        df = pl.DataFrame({
            "姓名": ["张三", "李四", "王五"],
            "性别": ["男", "MALE", "FEMALE"],  # 大小写混合
            "部门": ["研发部", "研发部", "研发部"],
            "_来源": ["一线", "一线", "一线"],
        })

        # 添加性别字段到 field_spec
        base_context.field_spec["fields"]["性别"] = {
            "attr_code": "gender",
            "dict_id": "性别"
        }

        base_context.set_dataframe("yixian", df)

        result_ctx = dict_encoder.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        # "MALE"和"FEMALE"应匹配失败（字典中是"男"/"女"）
        assert "未匹配" in result_df["性别_Code"].to_list()


# ============================================================
# F4-03: 新增 _Code 列测试
# ============================================================

class TestCodeColumn:
    """F4-03 新增 _Code 列测试"""

    def test_code_column_position(self, dict_encoder, base_context, valid_df):
        """_Code 列应在原始列右侧"""
        base_context.set_dataframe("yixian", valid_df)

        result_ctx = dict_encoder.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        # 检查原始列保留
        assert "姓名" in result_df.columns
        assert "性别" in result_df.columns
        assert "部门" in result_df.columns

        # 检查 _Code 列新增
        assert "性别_Code" in result_df.columns
        assert "部门_Code" in result_df.columns

        # 检查非字典类型字段无 _Code 列
        assert "姓名_Code" not in result_df.columns
        assert "备注_Code" not in result_df.columns

    def test_non_dict_fields_unchanged(self, dict_encoder, base_context, valid_df):
        """非字典类型字段不受影响"""
        base_context.set_dataframe("yixian", valid_df)

        result_ctx = dict_encoder.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        # 原始数据保留
        assert result_df["姓名"].to_list() == ["张三", "李四", "王五"]


# ============================================================
# F4-04: 未匹配测试
# ============================================================

class TestUnmatchedValues:
    """F4-04 未匹配值测试"""

    def test_unmatched_marked(self, dict_encoder, base_context):
        """未匹配的值标记为"未匹配" """
        df = pl.DataFrame({
            "姓名": ["张三", "李四", "王五"],
            "性别": ["未知性别", "男", "无效值"],
            "_来源": ["一线", "一线", "一线"],
        })

        base_context.field_spec["fields"]["性别"] = {
            "attr_code": "gender",
            "dict_id": "性别"
        }

        base_context.set_dataframe("yixian", df)

        result_ctx = dict_encoder.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        # 未匹配的标记为"未匹配"
        assert result_df["性别_Code"].to_list()[0] == "未匹配"
        assert result_df["性别_Code"].to_list()[1] == "M"
        assert result_df["性别_Code"].to_list()[2] == "未匹配"

    def test_null_value_unmatched(self, dict_encoder, base_context):
        """空值标记为"未匹配" """
        df = pl.DataFrame({
            "姓名": ["张三", "李四"],
            "性别": ["男", None],
            "_来源": ["一线", "一线"],
        })

        base_context.field_spec["fields"]["性别"] = {
            "attr_code": "gender",
            "dict_id": "性别"
        }

        base_context.set_dataframe("yixian", df)

        result_ctx = dict_encoder.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        # 空值标记为"未匹配"
        assert result_df["性别_Code"].to_list()[0] == "M"
        assert result_df["性别_Code"].to_list()[1] == "未匹配"


# ============================================================
# F4-08: 上码统计测试
# ============================================================

class TestEncodingStats:
    """F4-08 上码统计测试"""

    def test_stats_recorded(self, dict_encoder, base_context, partial_invalid_df):
        """上码统计已记录"""
        base_context.set_dataframe("yixian", partial_invalid_df)

        result_ctx = dict_encoder.execute(base_context)

        # 检查模块结果包含统计信息
        assert "F4" in result_ctx.module_results
        module_result = result_ctx.module_results["F4"]
        assert "success" in module_result
        assert "fail" in module_result
        assert "message" in module_result

        # 检查 F4_stats 额外统计
        assert "F4_stats" in result_ctx.module_results
        stats = result_ctx.module_results["F4_stats"]
        assert "性别" in stats
        assert "部门" in stats

    def test_match_rate_calculation(self, dict_encoder, base_context, partial_invalid_df):
        """匹配率计算正确"""
        base_context.set_dataframe("yixian", partial_invalid_df)

        result_ctx = dict_encoder.execute(base_context)
        stats = result_ctx.module_results["F4_stats"]

        # 性别：4行，2匹配，2未匹配
        gender_stats = stats["性别"]
        assert gender_stats["matched"] == 2
        assert gender_stats["unmatched"] == 2


# ============================================================
# 字典不存在测试
# ============================================================

class TestDictNotFound:
    """字典不存在的场景测试"""

    def test_dict_not_found_handling(self, dict_encoder, base_context, valid_df):
        """引用不存在的字典"""
        # 修改 field_spec 引用不存在的字典
        base_context.field_spec["fields"]["性别"] = {
            "attr_code": "gender",
            "dict_id": "不存在的字典"
        }

        base_context.set_dataframe("yixian", valid_df)

        result_ctx = dict_encoder.execute(base_context)
        result_df = result_ctx.get_dataframe("yixian")

        # 全部标记为"未匹配"
        assert result_df["性别_Code"].to_list() == ["未匹配", "未匹配", "未匹配"]

        # 检查状态为 dict_not_found
        stats = result_ctx.module_results.get("F4_stats", {})
        assert stats["性别"]["status"] == "dict_not_found"


# ============================================================
# 运行入口
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
