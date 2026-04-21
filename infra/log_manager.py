"""
基础设施层 - 日志管理器

提供统一的日志接口，支持控制台输出和文件记录，
并可按模块配置不同的日志级别。
"""

import os
import sys
import logging
import logging.handlers
from pathlib import Path

# 日志根目录：开发模式=代码根目录/logs/；打包后=用户主目录/.ipsos-customer-list-tool/logs/
if getattr(sys, 'frozen', False):
    _DATA_BASE = os.path.join(os.path.expanduser("~"), ".ipsos-customer-list-tool")
else:
    _DATA_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOG_DIR = os.path.join(_DATA_BASE, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# 全局日志格式
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 全局日志级别（默认 INFO）
_global_level = logging.INFO

# 已注册的 logger 缓存，避免重复创建
_registered_loggers = {}


def get_logger(
    name: str,
    level: int = None,
    to_file: bool = True,
    to_console: bool = True,
) -> logging.Logger:
    """
    获取指定名称的 logger。

    Parameters
    ----------
    name : str
        logger 名称，建议传入 __name__（模块级别）
    level : int, optional
        日志级别，默认为全局设置（INFO）
    to_file : bool
        是否输出到文件，默认为 True
    to_console : bool
        是否输出到控制台，默认为 True

    Returns
    -------
    logging.Logger
    """
    if name in _registered_loggers:
        return _registered_loggers[name]

    logger = logging.getLogger(name)
    effective_level = level if level is not None else _global_level
    logger.setLevel(effective_level)

    # 避免重复添加 handler
    if logger.handlers:
        _registered_loggers[name] = logger
        return logger

    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(effective_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if to_file:
        log_file = os.path.join(LOG_DIR, f"{name.split('.')[0] or 'app'}.log")
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB per file
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(effective_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    _registered_loggers[name] = logger
    return logger


def set_global_level(level: int):
    """
    设置全局日志级别。

    Parameters
    ----------
    level : int
        日志级别，如 logging.DEBUG / logging.INFO / logging.WARNING
    """
    global _global_level
    _global_level = level
    for logger in _registered_loggers.values():
        logger.setLevel(level)
        for handler in logger.handlers:
            handler.setLevel(level)
