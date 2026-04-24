"""
基础设施层 - 日志管理器

提供统一的日志接口，所有模块统一写入：
- app.log：所有日志（DEBUG 及以上）
- warn.log：INFO + WARNING
- error.log：仅 ERROR
- 控制台：INFO 及以上
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

# 日志文件路径
APP_LOG_FILE = os.path.join(LOG_DIR, "app.log")
WARN_LOG_FILE = os.path.join(LOG_DIR, "warn.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "error.log")

# 全局日志格式（丰富格式）
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 全局日志级别（默认 INFO，可通过 set_global_level() 修改）
GLOBAL_LEVEL = logging.DEBUG

# 已注册的 logger 缓存
_registered_loggers = {}

# 全局 Handlers（所有 logger 共享）
_app_handler = None
_warn_handler = None
_error_handler = None
_console_handler = None


class InfoWarningFilter(logging.Filter):
    """过滤器：只允许 INFO 和 WARNING 级别"""
    def filter(self, record):
        return record.levelno in (logging.INFO, logging.WARNING)


def _setup_global_handlers():
    """初始化全局 Handlers"""
    global _app_handler, _warn_handler, _error_handler, _console_handler
    
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    
    # app.log - 所有日志（级别跟随 GLOBAL_LEVEL）
    if _app_handler is None:
        _app_handler = logging.handlers.RotatingFileHandler(
            APP_LOG_FILE,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        _app_handler.setLevel(GLOBAL_LEVEL)
        _app_handler.setFormatter(formatter)
    
    # warn.log - INFO + WARNING
    if _warn_handler is None:
        _warn_handler = logging.handlers.RotatingFileHandler(
            WARN_LOG_FILE,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        _warn_handler.setLevel(logging.DEBUG)  # 先接收所有，再过滤
        _warn_handler.setFormatter(formatter)
        _warn_handler.addFilter(InfoWarningFilter())
    
    # error.log - 仅 ERROR
    if _error_handler is None:
        _error_handler = logging.handlers.RotatingFileHandler(
            ERROR_LOG_FILE,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        _error_handler.setLevel(logging.ERROR)
        _error_handler.setFormatter(formatter)
    
    # 控制台输出（INFO 及以上）
    if _console_handler is None:
        _console_handler = logging.StreamHandler()
        _console_handler.setLevel(logging.INFO)
        _console_handler.setFormatter(formatter)


def get_logger(
    name: str,
    level: int = None,
) -> logging.Logger:
    """
    获取指定名称的 logger。
    
    所有 logger 统一写入 app.log、warn.log、error.log 和控制台。

    Parameters
    ----------
    name : str
        logger 名称，建议传入 __name__
    level : int, optional
        日志级别，默认为 GLOBAL_LEVEL（INFO）

    Returns
    -------
    logging.Logger
    """
    if name in _registered_loggers:
        return _registered_loggers[name]

    # 初始化全局 handlers
    _setup_global_handlers()
    
    logger = logging.getLogger(name)
    effective_level = level if level is not None else GLOBAL_LEVEL
    logger.setLevel(effective_level)

    # 避免重复添加 handler
    if not logger.handlers:
        # 添加 app.log handler
        logger.addHandler(_app_handler)
        # 添加 warn.log handler
        logger.addHandler(_warn_handler)
        # 添加 error.log handler
        logger.addHandler(_error_handler)
        # 添加控制台 handler
        logger.addHandler(_console_handler)

    _registered_loggers[name] = logger
    return logger


def set_global_level(level: int) -> None:
    """
    设置全局日志级别（影响 app.log 和控制台输出）
    
    Parameters
    ----------
    level : int
        日志级别，如 logging.DEBUG / logging.INFO / logging.WARNING / logging.ERROR
    """
    global GLOBAL_LEVEL
    GLOBAL_LEVEL = level
    # 更新 app.log handler 级别
    if _app_handler:
        _app_handler.setLevel(level)
    # 更新已注册的 logger
    for logger in _registered_loggers.values():
        logger.setLevel(level)


# ============================================================
# 日志文件监控器（用于 UI 实时显示日志）
# ============================================================

import threading
from typing import Callable, Optional, List


class LogWatcher:
    """
    日志文件监控器 - 增量读取日志文件新内容
    
    使用方式：
        watcher = LogWatcher(APP_LOG_FILE)
        watcher.start(callback=lambda lines: print(lines))
    """
    
    def __init__(self, filepath: str, max_lines: int = 500):
        """
        Parameters
        ----------
        filepath : str
            要监控的日志文件路径
        max_lines : int
            保留的最大行数（超出后截断旧内容）
        """
        self._filepath = filepath
        self._max_lines = max_lines
        self._position = 0  # 上次读取的文件位置
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callback: Optional[Callable[[List[str]], None]] = None
        self._file = None
        
        # 确保文件存在
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        if not os.path.exists(filepath):
            open(filepath, 'w', encoding='utf-8').close()
    
    def start(self, callback: Callable[[List[str]], None]) -> None:
        """
        启动文件监控
        
        Parameters
        ----------
        callback : callable
            读取到新行时的回调函数，签名为 callback(new_lines: List[str])
        """
        if self._running:
            return
        
        self._callback = callback
        self._running = True
        self._position = self._get_file_size()
        
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """停止文件监控"""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        if self._file:
            self._file.close()
            self._file = None
    
    def _get_file_size(self) -> int:
        """获取文件大小"""
        try:
            return os.path.getsize(self._filepath)
        except OSError:
            return 0
    
    def _watch_loop(self) -> None:
        """监控循环（在子线程中运行）"""
        import time
        
        while self._running:
            try:
                current_size = self._get_file_size()
                
                # 文件被截断（轮转），从头开始读
                if current_size < self._position:
                    self._position = 0
                
                # 有新内容
                if current_size > self._position:
                    if self._file is None:
                        self._file = open(self._filepath, 'r', encoding='utf-8')
                    
                    self._file.seek(self._position)
                    new_lines = self._file.readlines()
                    self._position = self._file.tell()
                    
                    if new_lines and self._callback:
                        self._callback(new_lines)
                
                time.sleep(0.1)  # 100ms 检查一次
                
            except Exception:
                # 忽略读取错误，继续监控
                time.sleep(0.5)
    
    def __del__(self):
        """析构时确保停止"""
        self.stop()
