"""
数据库层 - SQLite 连接管理

使用 Python 内置 sqlite3 模块管理本地 SQLite 数据库连接。
所有数据库操作均通过本模块提供的连接对象进行。
"""

import sqlite3
import os
import sys
from contextlib import contextmanager
from typing import Optional


def _get_base_dir() -> str:
    """获取基础目录，兼容 PyInstaller 打包后的路径（sys._MEIPASS）"""
    # PyInstaller 打包后 __file__ 指向可执行文件内部
    if getattr(sys, 'frozen', False):
        base = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    else:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return base


# 基础目录（源码根目录或 PyInstaller 解压目录）
BASE_DIR = _get_base_dir()
# 迁移脚本目录（只读资源，跟随 BASE_DIR）
MIGRATION_DIR = os.path.join(BASE_DIR, "db", "migrations")
# DB 目录和文件（可写数据，放在用户数据区）
if getattr(sys, 'frozen', False):
    # 打包后：DB 放在用户主目录下的 .data 子目录（与 App 同级或独立位置）
    DATA_DIR = os.path.join(
        os.path.expanduser("~"),
        ".ipsos-customer-list-tool",
    )
else:
    # 开发模式：放在代码 db/ 目录下
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "db")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "processing.db")

# 连接对象（延迟初始化）
_connection: Optional[sqlite3.Connection] = None

# 迁移脚本是否已执行的标记
_migrations_applied: bool = False


def _apply_migrations():
    """
    执行数据库迁移脚本，确保表结构存在。

    在首次获取连接时自动执行，保证 DAO 层操作前表已创建。
    """
    global _migrations_applied
    if _migrations_applied:
        return

    migration_dir = MIGRATION_DIR
    if not os.path.isdir(migration_dir):
        print(f"[DB] ⚠️ 迁移脚本目录不存在: {migration_dir}，跳过表初始化")
        return

    # 按文件名排序执行迁移脚本
    migration_files = sorted(
        f for f in os.listdir(migration_dir) if f.endswith(".sql")
    )

    conn = get_connection()
    for filename in migration_files:
        filepath = os.path.join(migration_dir, filename)
        with open(filepath, "r", encoding="utf-8") as fh:
            sql = fh.read()
        
        # 执行迁移脚本，忽略 "duplicate column name" 错误
        # 这是因为 ALTER TABLE ADD COLUMN 在列已存在时会报错
        try:
            conn.executescript(sql)
            print(f"[DB] 执行迁移脚本: {filename}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                print(f"[DB] 迁移脚本 {filename} 中的列已存在，跳过: {e}")
            else:
                raise  # 其他错误仍需抛出

    conn.commit()
    _migrations_applied = True


def get_connection() -> sqlite3.Connection:
    """
    获取数据库连接（单例模式）。
    首次调用时自动执行迁移脚本。
    若连接已关闭则自动重建。

    Returns
    -------
    sqlite3.Connection
    """
    global _connection, _migrations_applied
    if _connection is None:
        _connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        _connection.row_factory = sqlite3.Row  # 支持列名访问
        # 首次连接时自动执行迁移
        _apply_migrations()
    else:
        # 检测连接是否已关闭（如被外部 conn.close() 调用）
        try:
            _connection.execute("SELECT 1")
        except sqlite3.ProgrammingError:
            # 连接已关闭，重建
            _migrations_applied = False  # 重置迁移标记，确保表重建
            _connection = sqlite3.connect(DB_PATH, check_same_thread=False)
            _connection.row_factory = sqlite3.Row
            _apply_migrations()
    return _connection


@contextmanager
def get_cursor():
    """
    获取数据库游标的上下文管理器，自动提交或回滚。

    Usage:
        with get_cursor() as cur:
            cur.execute("SELECT * FROM table")
            rows = cur.fetchall()
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def close_connection():
    """关闭数据库连接（通常在应用退出时调用）"""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None
