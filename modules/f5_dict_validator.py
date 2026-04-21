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
        2. 一线名单 DataFrame 中是否存在 _Code 列（F4 上码结果）

        Returns
        -------
        tuple[bool, str]
            (是否通过, 错误信息)
        """
        # ── 检查1：dict_loader 是否已初始化 ──────────────────────
        if context.dict_loader is None:
            logger.warning("[F5] 前置检查未通过：dict_loader 未初始化")
            return False, "请先执行 F4 字典上码（数据字典加载失败）"

        # ── 检查2：一线名单是否存在 _Code 列（F4 上码结果）──────
        df_yixian = context.get_dataframe("yixian")
        if df_yixian is None:
            logger.warning("[F5] 前置检查未通过：一线名单 DataFrame 不存在")
            return False, "请先执行 F1 文件加载"

        # 查找是否存在 _Code 列（F4 上码后会产生此列）
        code_columns = [col for col in df_yixian.columns if col.endswith("_Code")]
        if not code_columns:
            logger.warning(
                f"[F5] 前置检查未通过：一线名单中未找到 _Code 列，"
                f"当前列={df_yixian.columns}"
            )
            return False, "请先执行 F4 字典上码"

        logger.info(f"[F5] 前置检查通过：发现 {len(code_columns)} 个 _Code 列")
        return True, ""

    def execute(self, context: ProcessContext) -> ProcessContext:
        """
        执行字典值合规校验（PRD F5-01 ~ F5-04）：

        逻辑：
        1. 遍历所有 _Code 列（由 F4 生成）
        2. 识别 Code="未匹配" 的行
        3. 收集不合规记录到 error_records
        4. 记录模块处理结果

        Parameters
        ----------
        context : ProcessContext
            包含 dict_loader 和 dataframes["yixian"]

        Returns
        -------
        ProcessContext
        """
        logger.info("[F5] 开始字典值合规校验")

        df_yixian = context.get_dataframe("yixian")
        if df_yixian is None:
            logger.warning("[F5] 一线名单为空，跳过校验")
            context.record_module_result(
                module="F5",
                success_count=0,
                fail_count=0,
                message="一线名单为空，跳过校验",
            )
            return context

        # ── 查找所有 _Code 列 ────────────────────────────────────
        code_columns = [col for col in df_yixian.columns if col.endswith("_Code")]

        total_invalid = 0
        all_invalid_records = []

        for code_col in code_columns:
            # 识别未匹配的字段（_Code 列值 = "未匹配"）
            invalid_df = df_yixian.filter(pl.col(code_col) == "未匹配")

            if len(invalid_df) > 0:
                # 提取原始字段名（去掉 "_Code" 后缀）
                original_field = code_col[:-5] if code_col.endswith("_Code") else code_col

                # 获取原始值列
                original_value_col = original_field if original_field in df_yixian.columns else None

                # [FIX] 使用正确的行号列（优先使用_行号，其次_row_num）
                for row in invalid_df.iter_rows(named=True):
                    record = {
                        "字段名": original_field,
                        "行号": row.get("_行号", row.get("_row_num", 0)),
                        "原始值": row.get(original_value_col, ""),
                        "问题类型": "DICT_NOT_FOUND",
                        "说明": f"值 '{row.get(original_value_col, '')}' 在数据字典中不存在",
                    }
                    all_invalid_records.append(record)

                logger.info(
                    f"  [F5] 字段 {original_field}: {len(invalid_df)} 条不合规"
                )

            total_invalid += len(invalid_df)

        # ── 汇总处理结果 ──────────────────────────────────────────
        if all_invalid_records:
            # 构建错误记录 DataFrame
            error_df = pl.DataFrame(all_invalid_records)
            # [20260420-老谈] ISSUE-06: 使用独立 key "dict_validation"，避免覆盖 F2 的 error_records["yixian"]
            context.error_records["dict_validation"] = error_df
            logger.info(
                f"[F5] 合规校验完成：{total_invalid} 条不合规记录，"
                f"已输出至「字典校验结果」Sheet"
            )
        else:
            logger.info("[F5] 合规校验完成：所有数据均合规")

        # ── 记录模块结果 ──────────────────────────────────────────
        context.record_module_result(
            module="F5",
            success_count=len(df_yixian) - total_invalid,
            fail_count=total_invalid,
            message=(
                f"校验完成：{len(df_yixian)} 行数据，"
                f"{total_invalid} 条不合规"
            ),
        )

        return context
