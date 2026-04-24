"""
F5: 字典值合规校验模块

功能覆盖（PRD F5）：
- F5-01: 遍历所有字典类型字段，检查每行值是否在对应字典中存在
- F5-02: 基于已上码结果判断：`_Code == "未匹配"` 的记录即为不合规记录
- F5-03: 将不合规值输出到独立 Sheet "字典校验结果"
- F5-04: 校验完成后输出汇总

A-11 前置检查要求（测试用例评审 F-09）：
- 检查 dict_loader 是否已初始化（由 F4 加载）
- 检查一线名单 DataFrame 中是否存在 _Code 列（由 F4 生成）
- 若前置条件不满足，阻止执行并提示"请先执行 F4 字典上码"
"""

import polars as pl

from core.base_module import BaseModule
from core.context import ProcessContext
from infra.log_manager import get_logger

logger = get_logger(__name__)

# 字典校验常量
UNMATCHED_PLACEHOLDER = "未匹配"  # 与 F4 保持一致


class DictValidatorModule(BaseModule):
    """
    F5 字典值合规校验模块

    继承 BaseModule，统一实现：
    - get_module_name()  : 返回 "F5"
    - validate_input()   : 前置条件检查（dict_loader 初始化 + _Code 列存在）
    - execute()         : 核心校验逻辑
    """

    def get_module_name(self) -> str:
        """返回模块名称"""
        return "F5"

    def validate_input(self, context: ProcessContext) -> tuple[bool, str]:
        """
        F5 前置条件检查（A-11）：

        检查项：
        1. dict_loader 是否已初始化（F4 已执行）
        2. 一线名单 DataFrame 是否存在
        3. 一线名单是否为非空
        4. 一线名单中是否存在 _Code 列（F4 上码结果）

        Returns
        -------
        tuple[bool, str]
            (是否通过, 错误信息)
        """
        # ── 检查1：dict_loader 是否已初始化 ──────────────────────
        if context.dict_loader is None:
            logger.warning("[F5] 前置检查未通过：dict_loader 未初始化")
            return False, "[F5] 跳过: 数据字典加载失败"

        # ── 检查2：一线名单是否存在 ────────────────────────────────
        df_yixian = context.get_dataframe("yixian")
        if df_yixian is None:
            logger.warning("[F5] 前置检查未通过：一线名单 DataFrame 不存在")
            return False, "[F5] 跳过: 请先执行 F1 文件加载"

        # ── [20260424-老谈] 修改：检查是否有合规数据 ─────────────────
        # 不再检查 is_empty()，改为检查"合规检查_状态=通过"的行数
        valid_count = 0
        if "合规检查_状态" in df_yixian.columns:
            valid_count = df_yixian.filter(pl.col("合规检查_状态") == "通过").height
        else:
            valid_count = df_yixian.height

        if valid_count == 0:
            logger.warning("[F5] 前置检查未通过：一线名单无合规数据")
            return False, "[F5] 跳过: 一线名单无合规数据"

        # ── 检查3：是否存在 _Code 列（F4 上码结果）─────────────
        code_columns = [col for col in df_yixian.columns if col.endswith("_Code")]
        if not code_columns:
            logger.warning(f"[F5] 前置检查未通过：一线名单中未找到 _Code 列")
            return False, "[F5] 跳过: 请先执行 F4 字典上码"

        logger.info(f"[F5] 前置检查通过：发现 {len(code_columns)} 个 _Code 列，合规数据 {valid_count} 行")
        return True, ""

    def execute(self, context: ProcessContext) -> ProcessContext:
        """
        执行字典值合规校验（PRD F5-01 ~ F5-04）：

        逻辑：
        1. 只对"合规检查_状态=通过"的数据进行字典校验
        2. 遍历所有 _Code 列（由 F4 生成）
        3. 识别 Code="未匹配" 的行
        4. 收集不合规记录到 error_records
        5. 添加标记列：字典校验_状态、字典校验_错误数
        6. 记录模块处理结果

        Parameters
        ----------
        context : ProcessContext
            包含 dict_loader 和 dataframes["yixian"]

        Returns
        -------
        ProcessContext
        """
        logger.info("[F5] 开始字典值校验")

        df_yixian = context.get_dataframe("yixian")

        # ── [20260424-老谈] 过滤出合规数据，只进行字典校验 ─────────────────
        if "合规检查_状态" in df_yixian.columns:
            df_valid = df_yixian.filter(pl.col("合规检查_状态") == "通过")
        else:
            df_valid = df_yixian  # 兼容旧数据
        logger.info(f"[F5] 合规数据 {len(df_valid)} 行，开始字典校验")

        # ── 查找所有 _Code 列 ────────────────────────────────────
        code_columns = [col for col in df_valid.columns if col.endswith("_Code")]

        total_invalid = 0
        all_invalid_records = []
        total_columns = len(code_columns)  # [方案C] 动态总列数

        for idx, code_col in enumerate(code_columns):
            # 识别未匹配的字段（_Code 列值 = "未匹配"）
            invalid_df = df_valid.filter(pl.col(code_col) == UNMATCHED_PLACEHOLDER)

            if len(invalid_df) > 0:
                # 提取原始字段名（去掉 "_Code" 后缀）
                original_field = code_col[:-5] if code_col.endswith("_Code") else code_col

                # 获取原始值列
                original_value_col = original_field if original_field in df_valid.columns else None

                # [FIX #5] 安全检查：确保 original_value_col 不为 None
                value_key = original_value_col if original_value_col else ""

                # [FIX] 使用正确的行号列（优先使用_行号，其次_row_num）
                for row in invalid_df.iter_rows(named=True):
                    original_value = row.get(value_key, "") if value_key else ""
                    record = {
                        "字段名": original_field,
                        "行号": row.get("_行号", row.get("_row_num", 0)),
                        "原始值": original_value,
                        "问题类型": "DICT_NOT_FOUND",
                        "说明": f"值 '{original_value}' 在数据字典中不存在",
                    }
                    all_invalid_records.append(record)

                logger.info(
                    f"  [F5] 字段 {original_field}: {len(invalid_df)} 条不合规"
                )

            total_invalid += len(invalid_df)

            # [方案C] 动态分片进度：每处理完一列报告一次进度
            col_progress = int((idx + 1) / total_columns * 100)
            self._report_progress(col_progress)

        # ── [20260424-老谈] 汇总处理结果并添加标记列 ─────────────────────
        if all_invalid_records:
            # 构建错误记录 DataFrame
            error_df = pl.DataFrame(all_invalid_records)
            # [20260420-老谈] ISSUE-06: 使用独立 key "dict_validation"，避免覆盖 F2 的 error_records["yixian"]
            context.error_records["dict_validation"] = error_df
            logger.info(
                f"[F5] 校验完成：{total_invalid} 条不合规记录，"
                f"已输出至「字典校验结果」Sheet"
            )
        else:
            logger.info("[F5] 校验完成：所有数据均合规")

        # ── [20260424-老谈] 添加字典校验标记列 ────────────────────────────
        # 统计每行的字典校验错误数量
        row_invalid_count: Dict[int, int] = {}
        for record in all_invalid_records:
            row_num = record.get("行号", 0)
            if row_num > 0:
                row_invalid_count[row_num] = row_invalid_count.get(row_num, 0) + 1

        # 添加标记列到原始 DataFrame
        if "字典校验_状态" not in df_yixian.columns:
            df_yixian = df_yixian.with_columns([
                pl.col("_row_num").map_elements(
                    lambda x: "通过" if row_invalid_count.get(x, 0) == 0 else "不通过",
                    return_dtype=pl.Utf8
                ).alias("字典校验_状态"),
                pl.col("_row_num").map_elements(
                    lambda x: row_invalid_count.get(x, 0),
                    return_dtype=pl.Int32
                ).alias("字典校验_错误数"),
            ])
            context.set_dataframe("yixian", df_yixian)
            logger.info(f"[F5] 添加字典校验标记列完成")

        # ── 记录模块结果 ──────────────────────────────────────────
        dict_pass_count = len(df_valid) - total_invalid
        if total_invalid == 0:
            msg = f"[F5] 成功: {len(df_valid)} 行数据，全部合规"
        else:
            msg = f"[F5] 失败: {len(df_valid)} 行数据，{total_invalid} 项不合规"
        context.record_module_result(
            module="F5",
            success_count=dict_pass_count,
            fail_count=total_invalid,
            message=msg,
        )

        return context
