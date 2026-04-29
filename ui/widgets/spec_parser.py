"""
基础设施层 - 字段规范导入对话框解析逻辑

独立于 PyQt5 UI 的解析逻辑，供 SpecImportDialog 和单测共同使用。
"""

import os
from typing import Optional

import openpyxl
import yaml

from infra.exceptions import ValidationError, DataQualityError
from infra.log_manager import get_logger

logger = get_logger(__name__)


# ============================================================
# 解析入口函数（供单测和UI层调用）
# ============================================================

def parse_spec_excel(excel_path: str) -> list[dict]:
    """
    解析属性导入模版 Excel，返回字段规范列表。

    Parameters
    ----------
    excel_path : str
        属性导入模版 Excel 文件路径

    Returns
    -------
    list[dict]
        字段规范列表，每项包含:
        - attr_code: 属性编码
        - attr_name: 属性名称
        - required: 是否必填
        - data_type: 数据类型
        - data_subtype: 数据子类型（可选）
        - dict_id: 数据字典ID（可选）

    Raises
    ------
    DataQualityError
        文件不存在或无法打开
    ValidationError
        缺少必需列或无有效数据
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

    # 解析列映射
    col_map = _parse_column_map(headers)

    specs = []
    for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
        if all(v is None for v in row):
            continue

        try:
            spec = _parse_row(row, col_map)
            if spec:
                specs.append(spec)
        except Exception as e:
            logger.warning(f"第 {row_idx} 行解析失败: {e}")
            continue

    workbook.close()

    if not specs:
        raise ValidationError(
            "属性导入模版中未找到有效字段定义",
            code="SPEC_IMPORT_EMPTY",
        )

    return specs


def write_spec_yaml(specs: list[dict], excel_path: str, output_path: str):
    """
    将解析结果写入 YAML 文件。

    Parameters
    ----------
    specs : list[dict]
        parse_spec_excel 返回的字段规范列表
    excel_path : str
        原始 Excel 文件路径（记录到 YAML 的 source 字段）
    output_path : str
        输出 YAML 文件路径
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    spec = {
        "version": "1.0",
        "source": excel_path,
        "fields": {},
    }

    for s in specs:
        field_name = s["attr_name"]
        spec["fields"][field_name] = {
            "attr_code": s["attr_code"],
            "required": s["required"],
            # [20260420-老谈] ISSUE-02+21: 对齐 PRD §8.1 键名规范 "type"（非 "data_type"）
            # [20260420] ISSUE-02+21: 对齐 field_spec.yaml 键名规范 "type"（非 "data_type"）
            "type": s["data_type"],
        }
        if s.get("data_subtype"):
            # [20260420] ISSUE-02+21: 对齐 field_spec.yaml 键名规范 "sub_type"（非 "data_subtype"）
            spec["fields"][field_name]["sub_type"] = s["data_subtype"]
        if s.get("dict_id"):
            spec["fields"][field_name]["dict_id"] = s["dict_id"]

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(spec, f, allow_unicode=True, sort_keys=False)


# ============================================================
# 内部函数
# ============================================================

def _parse_column_map(headers: list) -> dict:
    """从表头解析列名到索引的映射"""
    col_map = {}
    for idx, header in enumerate(headers):
        if header is None:
            continue
        header_str = str(header).strip()
        # 属性编码 / attr_code
        if "属性编码" in header_str or "属性code" in header_str:
            col_map["attr_code"] = idx
        # 属性名称 / 字段名
        elif "属性名称" in header_str or "字段名" in header_str:
            col_map["attr_name"] = idx
        # 必填
        elif "属性值必填" in header_str or "必填" in header_str:
            col_map["required"] = idx
        # 数据类型
        elif "数据类型" in header_str:
            col_map["data_type"] = idx
        # 数据子类型
        elif "数据子类型" in header_str:
            col_map["data_subtype"] = idx
        # 数据字典ID
        elif "数据字典" in header_str:
            col_map["dict_id"] = idx

    # 校验必需列
    required_cols = ["attr_code", "attr_name"]
    missing = [c for c in required_cols if c not in col_map]
    if missing:
        raise ValidationError(
            f"属性导入模版缺少必需列: {', '.join(missing)}",
            code="COL_MISSING",
        )

    return col_map


def _parse_row(row: tuple, col_map: dict) -> Optional[dict]:
    """解析单行字段定义"""
    # 安全获取列值
    def get_val(key: str) -> any:
        idx = col_map.get(key)
        if idx is None or idx >= len(row):
            return None
        return row[idx]

    attr_code = str(get_val("attr_code") or "").strip()
    attr_name = str(get_val("attr_name") or "").strip()

    if not attr_code and not attr_name:
        return None

    # 必填判断：默认 False（可选）
    required = False
    val = get_val("required")
    if val is not None:
        val_str = str(val).strip().lower()
        required = val_str in ("是", "√", "1", "true")

    # 数据类型
    data_type = "string"
    val = get_val("data_type")
    if val:
        data_type = str(val).strip().lower()

    # 数据子类型
    data_subtype = None
    val = get_val("data_subtype")
    if val and str(val).strip():
        data_subtype = str(val).strip().lower()

    # 数据字典ID
    dict_id = None
    val = get_val("dict_id")
    if val and str(val).strip():
        dict_id = str(val).strip()

    return {
        "attr_code": attr_code,
        "attr_name": attr_name,
        "required": required,
        "data_type": data_type,
        "data_subtype": data_subtype,
        "dict_id": dict_id,
    }
