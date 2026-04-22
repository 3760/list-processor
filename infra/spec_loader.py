"""
基础设施层 - 字段规范加载器

负责加载并校验 config/field_spec.yaml（或用户导入的字段规范文件），
检查 attr_code 重复、必填字段完整性，并将规范缓存在内存中供各模块使用。
"""

import os
import yaml
from typing import Any, Dict, List, Optional

from infra.exceptions import ConfigError, ValidationError
from infra.log_manager import get_logger

logger = get_logger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SPEC_PATH = os.path.join(BASE_DIR, "config", "field_spec.yaml")

_spec_cache: Optional[Dict[str, Any]] = None


def load_field_spec(spec_path: str = None) -> Dict[str, Any]:
    """
    加载字段规范文件并校验。

    Parameters
    ----------
    spec_path : str, optional
        字段规范文件路径，默认为 config/field_spec.yaml

    Returns
    -------
    Dict
        结构化字段规范
    """
    global _spec_cache

    if _spec_cache is not None:
        return _spec_cache

    path = spec_path or DEFAULT_SPEC_PATH

    if not os.path.exists(path):
        raise ConfigError(
            f"字段规范文件不存在: {path}",
            config_key="field_spec_path",
        )

    with open(path, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)

    _validate_spec(spec, path)
    _spec_cache = spec
    logger.info(f"字段规范加载成功: {path}, 共 {len(spec.get('fields', {}))} 个字段")
    return spec


def clear_spec_cache() -> None:
    """
    [FIX #11] 清除字段规范缓存。
    
    在测试场景或配置重载时调用此函数可确保重新加载规范文件。
    
    Usage:
        from infra.spec_loader import clear_spec_cache
        clear_spec_cache()
    """
    global _spec_cache
    _spec_cache = None
    logger.debug("[spec_loader] 字段规范缓存已清除")


def _validate_spec(spec: Dict, source_path: str):
    """校验字段规范的完整性和一致性"""
    if not isinstance(spec, dict):
        raise ValidationError(
            "字段规范格式错误：根对象应为字典",
            code="SPEC_FMT",
        )

    fields = spec.get("fields", {})
    if not fields:
        raise ValidationError(
            "字段规范为空：未找到 fields 定义",
            code="SPEC_EMPTY",
        )

    # 检查 attr_code 重复
    attr_codes = []
    required_fields = []
    for field_name, field_def in fields.items():
        if not isinstance(field_def, dict):
            continue
        attr_code = field_def.get("attr_code")
        if attr_code:
            attr_codes.append((attr_code, field_name))
        if field_def.get("required", False):
            required_fields.append(field_name)

    # 检测 attr_code 重复
    seen = {}
    for attr_code, field_name in attr_codes:
        if attr_code in seen:
            raise ValidationError(
                f"attr_code 重复: '{attr_code}' 同时出现在字段 "
                f"'{seen[attr_code]}' 和 '{field_name}' 中",
                code="ATTR_DUP",
            )
        seen[attr_code] = field_name

    # 检查必填字段不为空
    missing_required = [f for f in required_fields if not fields.get(f)]
    if missing_required:
        raise ValidationError(
            f"以下必填字段定义缺失: {', '.join(missing_required)}",
            code="FIELD_MISSING",
        )

    logger.debug(
        f"字段规范校验通过: {len(attr_codes)} 个 attr_code, "
        f"{len(required_fields)} 个必填字段"
    )


def get_attr_code_mapping(spec: Dict = None) -> Dict[str, str]:
    """
    获取 字段名 → attr_code 的映射字典。

    Returns
    -------
    Dict[字段名, attr_code]
    """
    field_spec = spec or load_field_spec()
    mapping = {}
    for field_name, field_def in field_spec.get("fields", {}).items():
        if isinstance(field_def, dict) and "attr_code" in field_def:
            mapping[field_name] = field_def["attr_code"]
    return mapping


def get_required_fields(spec: Dict = None) -> List[str]:
    """获取所有必填字段名列表"""
    field_spec = spec or load_field_spec()
    return [
        name
        for name, defn in field_spec.get("fields", {}).items()
        if isinstance(defn, dict) and defn.get("required", False)
    ]


def clear_cache() -> None:
    """清除规范缓存（测试用）"""
    global _spec_cache
    _spec_cache = None
