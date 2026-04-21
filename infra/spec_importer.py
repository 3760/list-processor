"""
基础设施层 - 字段规范导入器（F1-07）

负责将 CEM 系统的「属性导入模版」Excel 文件解析为
结构化的 field_spec.yaml 格式，供系统内部使用。

输入：CEM 属性导入模版（Excel）
输出：field_spec.yaml（YAML）
"""

import os
import yaml
from typing import Any, Dict, List

import openpyxl

from infra.exceptions import ValidationError, DataQualityError
from infra.log_manager import get_logger

logger = get_logger(__name__)


def import_field_spec(excel_path: str, output_path: str = None) -> Dict[str, Any]:
    """
    将属性导入模版 Excel 转换为 field_spec.yaml。

    Parameters
    ----------
    excel_path : str
        CEM 属性导入模版 Excel 文件路径
    output_path : str, optional
        输出的 YAML 文件路径，默认为 config/field_spec.yaml

    Returns
    -------
    Dict
        解析后的字段规范字典
    """
    if not os.path.exists(excel_path):
        raise DataQualityError(
            f"属性导入模版文件不存在: {excel_path}",
            field="excel_path",
        )

    try:
        workbook = openpyxl.load_workbook(excel_path, data_only=True)
    except Exception as e:
        raise DataQualityError(
            f"无法打开 Excel 文件: {e}",
            field="excel_path",
        )

    sheet = workbook.active
    headers = [cell.value for cell in sheet[1]]

    # 查找关键列索引（根据 CEM 模版格式）
    col_map = _parse_column_map(headers)
    fields: Dict[str, Any] = {}

    for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
        if all(v is None for v in row):
            continue  # 跳过空行

        try:
            field_def = _parse_field_row(row, col_map)
            if field_def:
                attr_name = field_def.pop("attr_name")
                fields[attr_name] = field_def
        except (ValueError, KeyError) as e:
            logger.warning(f"第 {row_idx} 行解析失败，跳过: {e}")
            continue

    workbook.close()

    if not fields:
        raise ValidationError(
            "属性导入模版中未找到有效字段定义",
            code="SPEC_IMPORT_EMPTY",
        )

    # 构建输出规范
    spec = {
        "version": "1.0",
        "source": excel_path,
        "fields": fields,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(spec, f, allow_unicode=True, sort_keys=False)
        logger.info(f"字段规范已导出至: {output_path}")

    return spec


def _parse_column_map(headers: List) -> Dict[str, int]:
    """
    从表头行解析列名到索引的映射。

    根据 CEM 属性导入模版，表头包含以下关键列：
    - 属性编码（对应 attr_code）
    - 属性名称（对应 field_name）
    - 是否必填 / 允许空值
    - 数据类型
    - 值域（允许值列表）
    """
    col_map = {}
    for idx, header in enumerate(headers):
        if header is None:
            continue
        header_str = str(header).strip()
        if "属性编码" in header_str or "attr_code" in header_str.lower():
            col_map["attr_code"] = idx
        elif "属性名称" in header_str or "字段名" in header_str:
            col_map["field_name"] = idx
        elif "必填" in header_str or "允许空" in header_str:
            col_map["required"] = idx
        elif "数据类型" in header_str:
            col_map["data_type"] = idx
        elif "值域" in header_str or "允许值" in header_str:
            col_map["enum_values"] = idx

    # 校验必需列
    required_cols = ["attr_code", "field_name"]
    missing = [c for c in required_cols if c not in col_map]
    if missing:
        raise ValidationError(
            f"属性导入模版缺少必需列: {', '.join(missing)}",
            code="COL_MISSING",
        )

    return col_map


def _parse_field_row(row: tuple, col_map: Dict[str, int]) -> Dict[str, Any]:
    """解析单行字段定义"""
    field_name = str(row[col_map["field_name"]]).strip()
    attr_code = str(row[col_map["attr_code"]]).strip()

    if not field_name or not attr_code:
        return None

    field_def: Dict[str, Any] = {
        "attr_name": field_name,
        "attr_code": attr_code,
        "required": True,  # 默认必填，后续覆盖
        "data_type": "string",
    }

    # 是否必填
    if "required" in col_map:
        val = row[col_map["required"]]
        if val is not None:
            val_str = str(val).strip().lower()
            field_def["required"] = val_str not in ("否", "允许空", "n", "no", "0")

    # 数据类型
    if "data_type" in col_map:
        dtype = row[col_map["data_type"]]
        if dtype:
            field_def["data_type"] = str(dtype).strip().lower()

    # 枚举值
    if "enum_values" in col_map:
        enum_val = row[col_map["enum_values"]]
        if enum_val:
            # 枚举值以分号分隔
            values = [v.strip() for v in str(enum_val).split(";") if v.strip()]
            if values:
                field_def["enum_values"] = values

    return field_def
