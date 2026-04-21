"""
数据访问层 - 处理历史记录 DAO

提供 processing_history 表的增、查操作。
"""

import json
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any

from db.connection import get_cursor
from infra.log_manager import get_logger

logger = get_logger(__name__)


class ProcessingHistoryDAO:
    """处理历史记录数据访问对象"""

    @staticmethod
    def create_run(
        input_yixian: str = None,
        input_sanfang: str = None,
        input_hw: str = None,
        dict_file: str = None,
        spec_file: str = None,
    ) -> str:
        """
        创建一条新的处理记录（状态 = running）。

        [20260420-老谈] 新增 dict_file 和 spec_file 参数，记录字典和字段规范文件路径。

        Returns
        -------
        str
            生成的 run_id（UUID）
        """
        run_id = str(uuid.uuid4())
        start_time = datetime.now().isoformat()

        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO processing_history
                (run_id, start_time, status, input_yixian, input_sanfang, input_hw, dict_file, spec_file)
                VALUES (?, ?, 'running', ?, ?, ?, ?, ?)
                """,
                (run_id, start_time, input_yixian, input_sanfang, input_hw, dict_file, spec_file),
            )
        logger.info(f"创建处理记录 run_id={run_id}")
        return run_id

    @staticmethod
    def complete_run(
        run_id: str,
        status: str,
        total_records: int = 0,
        output_records: int = 0,
        error_records: int = 0,
        duplicate_count: int = 0,
        summary: Dict[str, Any] = None,
        output_dir: str = None,
    ):
        """更新处理记录为完成状态，支持 output_dir 等扩展字段"""
        end_time = datetime.now().isoformat()
        summary_json = json.dumps(summary, ensure_ascii=False) if summary else None

        with get_cursor() as cur:
            cur.execute(
                """
                UPDATE processing_history
                SET end_time       = ?,
                    status         = ?,
                    total_records  = ?,
                    output_records = ?,
                    error_records  = ?,
                    duplicate_count = ?,
                    summary        = ?,
                    output_dir     = ?
                WHERE run_id = ?
                """,
                (
                    end_time,
                    status,
                    total_records,
                    output_records,
                    error_records,
                    duplicate_count,
                    summary_json,
                    output_dir,
                    run_id,
                ),
            )
        logger.info(f"更新处理记录 run_id={run_id}, status={status}")

    @staticmethod
    def get_history(limit: int = 50) -> List[Dict[str, Any]]:
        """
        获取最近的处理历史记录。

        Parameters
        ----------
        limit : int
            返回记录数量上限

        Returns
        -------
        List[Dict]
        """
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT * FROM processing_history
                ORDER BY start_time DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()

        records = []
        for row in rows:
            record = dict(row)
            # 解析 summary JSON 字符串为字典
            if record.get("summary") and isinstance(record["summary"], str):
                try:
                    record["summary"] = json.loads(record["summary"])
                except json.JSONDecodeError:
                    record["summary"] = {}
            else:
                record["summary"] = record.get("summary") or {}
            # [20260420-老谈] 构建 input_files 字典，包含字典和字段规范
            record["input_files"] = {
                "yixian": record.pop("input_yixian", None),
                "sanfang": record.pop("input_sanfang", None),
                "hw": record.pop("input_hw", None),
                "dict": record.pop("dict_file", None),
                "spec": record.pop("spec_file", None),
            }
            records.append(record)
        return records

    @staticmethod
    def get_by_run_id(run_id: str) -> Optional[Dict[str, Any]]:
        """根据 run_id 查询单条记录"""
        with get_cursor() as cur:
            cur.execute(
                "SELECT * FROM processing_history WHERE run_id = ?",
                (run_id,),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    @staticmethod
    def delete(run_id: str):
        """[20260420-老谈] 删除指定 run_id 的历史记录"""
        with get_cursor() as cur:
            cur.execute(
                "DELETE FROM processing_history WHERE run_id = ?",
                (run_id,),
            )
        logger.info(f"删除历史记录 run_id={run_id}")
