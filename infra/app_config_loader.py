"""
基础设施层 - 应用配置加载器

负责加载并解析 config/app_config.yaml，
返回结构化的配置对象供各模块使用。
"""

import os
import sys
import yaml
from typing import Any, Dict, Optional

from infra.exceptions import ConfigError
from infra.log_manager import get_logger

logger = get_logger(__name__)

# 支持 PyInstaller 打包后的路径解析
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULT_CONFIG_PATH = os.path.join(BASE_DIR, "config", "app_config.yaml")

_config_cache: Optional[Dict[str, Any]] = None


def load_app_config(config_path: str = None) -> Dict[str, Any]:
    """
    加载应用配置。

    Parameters
    ----------
    config_path : str, optional
        配置文件路径，默认为 config/app_config.yaml

    Returns
    -------
    Dict
        解析后的配置字典
    """
    global _config_cache

    if _config_cache is not None:
        return _config_cache

    path = config_path or DEFAULT_CONFIG_PATH

    if not os.path.exists(path):
        raise ConfigError(
            f"配置文件不存在: {path}",
            config_key="app_config_path",
        )

    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(
            f"YAML 解析失败: {e}",
            config_key=path,
        )

    _validate_required_keys(config)
    _config_cache = config
    logger.info(f"应用配置加载成功: {path}")
    return config


def _validate_required_keys(config: Dict):
    """校验必要的配置项是否存在"""
    required_sections = ["deduplication", "timeout", "logging"]
    for section in required_sections:
        if section not in config:
            raise ConfigError(
                f"缺少必要配置项: {section}",
                config_key=section,
            )


def get_dedup_fields(list_type: str = "yixian") -> list:
    """获取指定名单类型的去重字段列表"""
    config = load_app_config()
    dedup = config.get("deduplication", {})
    fields = dedup.get(list_type, [])
    if not fields:
        logger.warning(f"名单类型 '{list_type}' 未配置去重字段，使用默认值 [手机号]")
        return ["手机号"]
    return fields


def get_cross_list_dedup_fields() -> list:
    """获取跨名单去重字段"""
    return get_dedup_fields("cross_list")


def clear_cache():
    """清除配置缓存（测试用）"""
    global _config_cache
    _config_cache = None
