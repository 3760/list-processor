"""
数据访问层 - 字典版本 DAO

提供 dict_version 表的增、查操作，
用于记录每次处理使用的字典文件版本（MD5 hash），
支持字典版本感知和变更检测。
"""

import hashlib
import os
from datetime import datetime
from typing import Optional, Dict, Any

from db.connection import get_cursor
from infra.log_manager import get_logger

logger = get_logger(__name__)


class DictVersionDAO:
    """字典版本数据访问对象"""

    @staticmethod
    def _compute_md5(file_path: str) -> str:
        """计算文件的 MD5 哈希值"""
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    @staticmethod
    def record_version(
        run_id: str,
        dict_file_path: str,
        file_name: str,
    ) -> str:
        """
        记录本次处理使用的字典文件版本。

        Returns
        -------
        str
            该字典文件的 MD5 哈希值
        """
        md5_hash = DictVersionDAO._compute_md5(dict_file_path)
        recorded_at = datetime.now().isoformat()

        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO dict_version
                (run_id, dict_file, file_name, md5_hash, recorded_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, dict_file_path, file_name, md5_hash, recorded_at),
            )
        logger.info(
            f"记录字典版本 run_id={run_id}, file={file_name}, md5={md5_hash}"
        )
        return md5_hash

    @staticmethod
    def get_version_history(
        dict_file_path: str,
        limit: int = 10,
    ) -> list[Dict[str, Any]]:
        """
        查询指定字典文件的历史使用版本。

        Returns
        -------
        List[Dict]
        """
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT run_id, md5_hash, recorded_at
                FROM dict_version
                WHERE dict_file = ?
                ORDER BY recorded_at DESC
                LIMIT ?
                """,
                (dict_file_path, limit),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def check_file_changed(
        dict_file_path: str,
        expected_md5: str,
    ) -> bool:
        """
        检查字典文件是否已变更（MD5 对比）。

        Parameters
        ----------
        dict_file_path : str
            字典文件路径
        expected_md5 : str
            期望的 MD5 值

        Returns
        -------
        bool
            True 表示文件已变更，False 表示未变更
        """
        if not os.path.exists(dict_file_path):
            return True  # 文件不存在视为变更
        current_md5 = DictVersionDAO._compute_md5(dict_file_path)
        changed = current_md5 != expected_md5
        if changed:
            logger.warning(
                f"字典文件变更检测: {dict_file_path} "
                f"expected={expected_md5} current={current_md5}"
            )
        return changed
