"""
数据访问层 - 应用配置 DAO

提供 app_config 表的读写操作，
用于持久化用户配置和运行参数。
"""

from datetime import datetime
from typing import Optional, Dict, Any

from db.connection import get_cursor
from infra.log_manager import get_logger

logger = get_logger(__name__)


class AppConfigDAO:
    """应用配置数据访问对象"""

    @staticmethod
    def get(key: str) -> Optional[str]:
        """
        根据 key 获取配置值。

        Returns
        -------
        str or None
        """
        with get_cursor() as cur:
            cur.execute("SELECT value FROM app_config WHERE key = ?", (key,))
            row = cur.fetchone()
        return row["value"] if row else None

    @staticmethod
    def set(key: str, value: str, description: str = None):
        """设置或更新配置项（Upsert）"""
        updated_at = datetime.now().isoformat()
        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_config (key, value, description, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value      = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, description, updated_at),
            )
        logger.debug(f"保存配置 {key} = {value}")

    @staticmethod
    def get_all() -> Dict[str, str]:
        """
        获取所有配置项。

        Returns
        -------
        Dict[key, value]
        """
        with get_cursor() as cur:
            cur.execute("SELECT key, value FROM app_config ORDER BY key")
            rows = cur.fetchall()
        return {row["key"]: row["value"] for row in rows}

    @staticmethod
    def delete(key: str):
        """删除指定配置项"""
        with get_cursor() as cur:
            cur.execute("DELETE FROM app_config WHERE key = ?", (key,))
        logger.debug(f"删除配置项 {key}")
