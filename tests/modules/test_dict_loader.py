"""
Tests for DictLoader: Dictionary Loader Module

DictLoader 功能覆盖（F4-02 / F5 核心基础设施）：
- 字典文件加载（单Sheet横排结构）
- MD5 版本感知
- Code 列 + Details 列配对解析
- 字典名称推导
- 代码值验证 (is_code_valid)
- 字典查询 (lookup)
- filter_valid_rows 合法行过滤
"""

import hashlib
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from infra.dict_loader import DictLoader
from infra.exceptions import DataQualityError, ValidationError


# ─────────────────────────────────────────────
# 辅助函数：生成测试字典 Excel 文件
# ─────────────────────────────────────────────

def _make_dict_excel(path: Path, headers: list[str], data: list[list]):
    """
    生成字典测试文件。

    Args:
        path: 文件路径
        headers: 表头列表（如 ["客户Code", "客户Details", "区域Code", "区域Details"]）
        data: 数据行列表
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for row in data:
        ws.append(row)
    wb.save(path)
    wb.close()


def _compute_md5(path: Path) -> str:
    """计算文件 MD5"""
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


# ─────────────────────────────────────────────
# 字典文件加载
# ─────────────────────────────────────────────

class TestDictLoaderInit:
    """DictLoader 初始化与文件加载"""

    def test_load_valid_dict_file(self, tmp_path):
        """正常加载：单Sheet横排结构字典文件"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [
                ["C001", "客户A"],
                ["C002", "客户B"],
                ["C003", "客户C"],
            ]
        )
        loader = DictLoader(str(path))

        assert loader.file_path == str(path)
        assert loader.md5_hash is not None
        assert len(loader.md5_hash) == 32  # MD5 hex length
        assert "客户" in loader.get_all_dict_names()

    def test_file_not_found_raises_error(self, tmp_path):
        """文件不存在：抛出 DataQualityError"""
        fake_path = str(tmp_path / "nonexistent.xlsx")
        with pytest.raises(DataQualityError, match="不存在"):
            DictLoader(fake_path)

    def test_md5_hash_changes_on_content_change(self, tmp_path):
        """MD5 版本感知：文件内容修改后 hash 变化"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader1 = DictLoader(str(path))
        original_hash = loader1.md5_hash

        # 修改文件内容
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"], ["C002", "客户B"]]
        )
        loader2 = DictLoader(str(path))

        assert loader2.md5_hash != original_hash

    def test_md5_hash_stable_for_same_content(self, tmp_path):
        """MD5 版本感知：相同内容 hash 不变"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        hash1 = DictLoader(str(path)).md5_hash
        hash2 = DictLoader(str(path)).md5_hash
        assert hash1 == hash2


# ─────────────────────────────────────────────
# 表头解析
# ─────────────────────────────────────────────

class TestDictHeaderParsing:
    """表头解析：Code 列 + Details 列配对识别"""

    def test_parse_single_code_details_pair(self, tmp_path):
        """单组字典：Code + Details 配对"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader = DictLoader(str(path))
        assert "客户" in loader.get_all_dict_names()

    def test_parse_multiple_code_details_pairs(self, tmp_path):
        """多组字典：多组 Code + Details 并列"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details", "区域Code", "区域Details"],
            [
                ["C001", "客户A", "R01", "北京"],
                ["C002", "客户B", "R02", "上海"],
            ]
        )
        loader = DictLoader(str(path))
        dict_names = loader.get_all_dict_names()
        assert "客户" in dict_names
        assert "区域" in dict_names

    def test_parse_code_column_variations(self, tmp_path):
        """Code 列名变体：Code/code/编码"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["Code", "Details", "编码", "说明"],
            [["A", "选项A", "X", "类型X"]]
        )
        loader = DictLoader(str(path))
        dict_names = loader.get_all_dict_names()
        # 解析结果：Code组推导为"Details列前缀"，编码组推导为"说明列前缀"
        # 或其他有效字典名
        assert len(dict_names) == 2

    def test_empty_header_row(self, tmp_path):
        """空表头行：跳过空单元格"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            [None, None, "客户Code", "客户Details"],
            [["", "", "C001", "客户A"]]
        )
        loader = DictLoader(str(path))
        assert "客户" in loader.get_all_dict_names()

    def test_no_valid_code_column_raises_error(self, tmp_path):
        """无效表头：无 Code 列时抛出 ValidationError"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["姓名", "年龄", "地址"],
            [["张三", 25, "北京"]]
        )
        with pytest.raises(ValidationError, match="Code"):
            DictLoader(str(path))


# ─────────────────────────────────────────────
# 字典名称推导
# ─────────────────────────────────────────────

class TestDictNameDerivation:
    """字典名称推导：从表头文本推断字典名"""

    def test_derive_name_from_code_suffix(self, tmp_path):
        """前缀提取：移除 Code/代码 后缀"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader = DictLoader(str(path))
        assert "客户" in loader.get_all_dict_names()

    def test_derive_name_from_details_suffix(self, tmp_path):
        """备选推导：使用 Details 列名"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader = DictLoader(str(path))
        assert "客户" in loader.get_all_dict_names()

    def test_derive_name_fallback_to_column_letter(self, tmp_path):
        """回退策略：使用列索引"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["Code", "Details"],
            [["C001", "选项A"]]
        )
        loader = DictLoader(str(path))
        # 应回退到 Dict_Col0（当Code列无法推导有效名称时）
        dict_names = loader.get_all_dict_names()
        assert len(dict_names) >= 1


# ─────────────────────────────────────────────
# 数据行解析
# ─────────────────────────────────────────────

class TestDictDataParsing:
    """数据行解析：构建内存映射"""

    def test_parse_normal_rows(self, tmp_path):
        """正常数据行解析"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [
                ["C001", "客户A"],
                ["C002", "客户B"],
            ]
        )
        loader = DictLoader(str(path))
        codes = loader.get_all_codes("客户")
        assert "C001" in codes
        assert "C002" in codes

    def test_skip_empty_rows(self, tmp_path):
        """跳过空行"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [
                ["C001", "客户A"],
                [None, None],  # 空行
                ["C002", "客户B"],
            ]
        )
        loader = DictLoader(str(path))
        codes = loader.get_all_codes("客户")
        assert len(codes) == 2

    def test_strip_whitespace(self, tmp_path):
        """去除首尾空格"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [
                ["  C001  ", "  客户A  "],
            ]
        )
        loader = DictLoader(str(path))
        codes = loader.get_all_codes("客户")
        assert "C001" in codes
        assert "C001" not in [c for c in codes if c != "C001"]


# ─────────────────────────────────────────────
# 查询接口
# ─────────────────────────────────────────────

class TestDictLookup:
    """字典查询接口"""

    def test_is_code_valid_existing(self, tmp_path):
        """is_code_valid: 存在的代码返回 True"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"], ["C002", "客户B"]]
        )
        loader = DictLoader(str(path))
        assert loader.is_code_valid("客户", "C001") is True
        assert loader.is_code_valid("客户", "C002") is True

    def test_is_code_valid_nonexisting(self, tmp_path):
        """is_code_valid: 不存在的代码返回 False"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader = DictLoader(str(path))
        assert loader.is_code_valid("客户", "C999") is False

    def test_is_code_valid_nonexistent_dict(self, tmp_path):
        """is_code_valid: 不存在的字典返回 False"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader = DictLoader(str(path))
        assert loader.is_code_valid("不存在的字典", "C001") is False

    def test_lookup_returns_label(self, tmp_path):
        """lookup: 返回显示值"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"], ["C002", "客户B"]]
        )
        loader = DictLoader(str(path))
        assert loader.lookup("客户", "C001") == "客户A"
        assert loader.lookup("客户", "C002") == "客户B"

    def test_lookup_returns_none_for_invalid(self, tmp_path):
        """lookup: 无效代码返回 None"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader = DictLoader(str(path))
        assert loader.lookup("客户", "C999") is None

    def test_get_all_codes(self, tmp_path):
        """get_all_codes: 返回所有代码值列表"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "A"], ["C002", "B"], ["C003", "C"]]
        )
        loader = DictLoader(str(path))
        codes = loader.get_all_codes("客户")
        assert set(codes) == {"C001", "C002", "C003"}

    def test_get_all_dict_names(self, tmp_path):
        """get_all_dict_names: 返回所有字典名称"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details", "区域Code", "区域Details"],
            [["C001", "A", "R01", "北京"]]
        )
        loader = DictLoader(str(path))
        names = loader.get_all_dict_names()
        assert "客户" in names
        assert "区域" in names


# ─────────────────────────────────────────────
# filter_valid_rows
# ─────────────────────────────────────────────

class TestFilterValidRows:
    """filter_valid_rows: DataFrame 合法行过滤"""

    def test_filter_adds_error_column_returns_all_rows(self, tmp_path):
        """指定 error_column 时：返回所有行并添加错误列"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"], ["C002", "客户B"]]
        )
        loader = DictLoader(str(path))

        df = pl.DataFrame({
            "姓名": ["张三", "李四", "王五"],
            "客户": ["C001", "C999", "C002"],
        })

        result = loader.filter_valid_rows(df, "客户", "客户", "错误")
        # 返回所有3行，添加错误列
        assert len(result) == 3
        assert result["姓名"].to_list() == ["张三", "李四", "王五"]
        assert "错误" in result.columns
        # 合法行错误信息为 None
        assert result["错误"][0] is None
        # 非法行有错误信息
        assert "C999" in result["错误"][1]

    def test_filter_without_error_column(self, tmp_path):
        """不指定错误列时，直接过滤非法行"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"], ["C002", "客户B"]]
        )
        loader = DictLoader(str(path))

        df = pl.DataFrame({
            "姓名": ["张三", "李四"],
            "客户": ["C001", "C999"],
        })

        result = loader.filter_valid_rows(df, "客户", "客户")
        assert len(result) == 1
        assert result["姓名"][0] == "张三"

    def test_filter_adds_error_column(self, tmp_path):
        """指定错误列时，添加错误信息列"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader = DictLoader(str(path))

        df = pl.DataFrame({
            "姓名": ["张三", "李四"],
            "客户": ["C001", "C999"],
        })

        result = loader.filter_valid_rows(df, "客户", "客户", "错误信息")
        assert "错误信息" in result.columns
        # 第一行合法，错误信息应为 None
        assert result["错误信息"][0] is None
        # 第二行非法，应有错误信息
        assert "C999" in result["错误信息"][1]

    def test_filter_handles_null_values(self, tmp_path):
        """Polars map_elements 特性：None 值会被过滤"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["C001", "客户A"]]
        )
        loader = DictLoader(str(path))

        df = pl.DataFrame({
            "姓名": ["张三", "李四"],
            "客户": ["C001", None],
        })

        result = loader.filter_valid_rows(df, "客户", "客户")
        # None 值被过滤（Polars map_elements 行为）
        assert len(result) == 1
        assert result["姓名"][0] == "张三"


# ─────────────────────────────────────────────
# 边界情况
# ─────────────────────────────────────────────

class TestDictLoaderEdgeCases:
    """边界情况处理"""

    def test_empty_dict_file(self, tmp_path):
        """空字典文件：只有表头无数据"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            []  # 无数据行
        )
        loader = DictLoader(str(path))
        codes = loader.get_all_codes("客户")
        assert codes == []

    def test_code_with_only_whitespace(self, tmp_path):
        """代码值为纯空格时跳过"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details"],
            [["   ", "无效"], ["C001", "客户A"]]
        )
        loader = DictLoader(str(path))
        codes = loader.get_all_codes("客户")
        assert "C001" in codes
        assert "   " not in codes

    def test_multiple_empty_columns_between_groups(self, tmp_path):
        """多组字典之间有空列"""
        path = tmp_path / "data_dict.xlsx"
        _make_dict_excel(path,
            ["客户Code", "客户Details", None, "区域Code", "区域Details"],
            [["C001", "客户A", "", "R01", "北京"]]
        )
        loader = DictLoader(str(path))
        assert "客户" in loader.get_all_dict_names()
        assert "区域" in loader.get_all_dict_names()
