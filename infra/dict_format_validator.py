"""
基础设施层 - 字典格式校验器

提供独立的字典文件格式校验逻辑，供 DictLoader 和 UI 层共用。
避免代码重复，确保校验规则一致性。

Author: 20260422
"""

from typing import List, Optional, Tuple

import openpyxl

from infra.exceptions import ValidationError
from infra.log_manager import get_logger

logger = get_logger(__name__)


def validate_dict_format(file_path: str) -> Tuple[bool, str, Optional[str]]:
    """
    静态校验字典文件格式（只读取表头，不加载完整数据）。

    支持两种字典文件格式：
    - 模版格式（第1行有"字典类型标识"标签）
    - 业务格式（第1行直接是dict_id）

    Parameters
    ----------
    file_path : str
        字典文件路径

    Returns
    -------
    Tuple[bool, str, Optional[str]]
        (is_valid, message, md5_hash)
        - is_valid: 格式是否有效
        - message: 校验结果描述
        - md5_hash: 文件MD5值（格式无效时为None）
    """
    import hashlib

    try:
        # 计算MD5
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        md5_hash = md5.hexdigest()

        # 读取表头行（3行）
        workbook = openpyxl.load_workbook(file_path, data_only=True)
        sheet = workbook.active

        header_rows = []
        for row_idx in range(1, min(4, sheet.max_row + 1)):
            header_rows.append([cell.value for cell in sheet[row_idx]])

        workbook.close()

        # 解析表头
        groups = _parse_header(header_rows)

        if not groups:
            return (False, "[DICT_HEADER] 字典文件格式无法解析。请确认文件格式符合规范。", None)

        dict_count = len(groups)
        dict_names = [g[0] for g in groups]
        return (True, f"格式校验通过，共 {dict_count} 个字典组", md5_hash)

    except ValidationError as e:
        logger.warning(f"[validate_dict_format] 校验失败：{e}")
        return (False, f"[DICT_HEADER] {e.message}", None)
    except (OSError, IOError) as e:
        logger.error(f"[validate_dict_format] 校验异常：{e}")
        return (False, f"[DICT_HEADER] 文件读取失败: {str(e)}", None)


def _parse_header(header_rows: List[List]) -> List[Tuple[str, int, int]]:
    """
    解析表头行，识别字典组。（与 DictLoader._parse_header 逻辑完全一致）

    Parameters
    ----------
    header_rows : List[List]
        前3行表头数据

    Returns
    -------
    List[(dict_name, code_col_idx, details_col_idx)]
        字典组列表
    """
    groups = []
    col_count = len(header_rows[0])

    row3_col_header = header_rows[2]  # 第3行：Code/Details列头

    # 判断格式：第1行是否包含"字典类型标识"
    row1_has_label = any("字典类型标识" in str(v) for v in header_rows[0] if v)

    if row1_has_label:
        # 模版格式：从第2行获取dict_id
        dict_id_row = header_rows[1]
    else:
        # 业务格式：从第1行获取dict_id
        dict_id_row = header_rows[0]

    # 扫描第3行的Code列（Code列在左，Details列在右）
    for col in range(col_count):
        code_header = row3_col_header[col] if col < len(row3_col_header) else None
        if code_header is None:
            continue

        # 检查是否为Code列
        if not ("code" in str(code_header).lower() or "编码" in str(code_header)):
            continue

        details_col = col + 1
        if details_col >= col_count:
            continue

        details_header = row3_col_header[details_col] if details_col < len(row3_col_header) else None
        if details_header is None:
            continue

        # 验证Details列
        if not (
            "details" in str(details_header).lower()
            or "说明" in str(details_header)
            or "标签" in str(details_header)
        ):
            continue

        # 从dict_id_row获取dict_id
        code_cell_val = dict_id_row[col] if col < len(dict_id_row) else None
        dict_id = None
        if code_cell_val:
            dict_id = _extract_dict_id(str(code_cell_val).strip())

        # 如果提取失败，使用列索引作为fallback
        if not dict_id:
            dict_id = f"Dict_{col}"

        groups.append((dict_id, col, details_col))
        logger.info(f"[dict_format_validator] 解析到字典：{dict_id}，Code列={col}，Details列={details_col}")

    return groups


def _extract_dict_id(cell_str: str) -> str:
    """
    从单元格文本中提取纯净的dict_id。

    支持格式：
    - "示例：A1" -> "A1"
    - "A1" -> "A1"
    """
    # 移除常见前缀
    prefixes_to_remove = ["示例：", "示例:", "Dict:", "Dict："]
    result = cell_str
    for prefix in prefixes_to_remove:
        if result.startswith(prefix):
            result = result[len(prefix):]
            break
    return result.strip()
