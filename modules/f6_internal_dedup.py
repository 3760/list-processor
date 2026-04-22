"""
F6: 名单内部重复检查模块

功能覆盖（PRD F6）：
- F6-01: 重复名单正确标注（第1次"原始"，第2次及以后"重复"）
- F6-02: 重复组输出完整（保留所有重复行）
- F6-03: 去重字段为空时不参与去重
- F6-04: 去重字段缺失时提示用户选择
- F6-05/F6-06: 全部重复/全部唯一场景

前置检查（F6-validate_input）：
- 检查一线名单 DataFrame 是否存在
- 检查去重字段（dedup_field）是否已设置（F1-07 三级识别）
"""

import polars as pl

from core.base_module import BaseModule
from core.context import ProcessContext
from infra.log_manager import get_logger

logger = get_logger(__name__)

# 内部去重标注常量
ORIGINAL_VALUE = "原始"      # 首次出现的记录
REPEAT_VALUE = "重复"        # 重复出现的记录
UNKNOWN_VALUE = "未知"      # 空值/跳过记录
DEDUP_RESULT_COL = "内部去重结果"  # 去重结果列名


class InternalDedupModule(BaseModule):
    """
    F6 名单内部重复检查模块

    继承 BaseModule，统一实现：
    - get_module_name()  : 返回 "F6"
    - validate_input()   : 前置条件检查（一线名单 + dedup_field）
    - execute()         : 核心去重逻辑
    """

    def get_module_name(self) -> str:
        """返回模块名称"""
        return "F6"

    def validate_input(self, context: ProcessContext) -> tuple[bool, str]:
        """
        F6 前置条件检查：

        检查项：
        1. 一线名单 DataFrame 是否存在
        2. 去重字段（dedup_field）是否已设置
        3. 去重字段是否存在于 DataFrame

        Returns
        -------
        tuple[bool, str]
            (是否通过, 错误信息)
        """
        # ── 检查1：一线名单是否存在 ────────────────────────────────
        df_yixian = context.get_dataframe("yixian")
        if df_yixian is None:
            logger.warning("[F6] 前置检查未通过：一线名单 DataFrame 不存在")
            return False, "请先执行 F1 文件加载"

        # ── 检查2：去重字段是否已设置 ──────────────────────────
        if context.dedup_field is None:
            logger.warning("[F6] 前置检查未通过：去重字段未设置")
            return False, "请先执行 F1 文件加载（去重字段未识别）"

        # ── 检查3：去重字段是否存在于 DataFrame ──────────────────
        if context.dedup_field not in df_yixian.columns:
            logger.warning(f"[F6] 前置检查未通过：去重字段 '{context.dedup_field}' 不存在")
            return False, f"去重字段 '{context.dedup_field}' 在数据中不存在"

        logger.info(f"[F6] 前置检查通过：去重字段='{context.dedup_field}'")
        return True, ""

    def execute(self, context: ProcessContext) -> ProcessContext:
        """
        执行名单内部重复检查（PRD F6-01 ~ F6-06）：

        逻辑：
        1. 按去重字段（dedup_field）分组
        2. 每组内按原始顺序标记：
           - 第 1 次出现：标注"原始"
           - 第 2 次及以后：标注"重复"
        3. 保留所有重复组（不删除数据）
        4. 空值不参与去重

        参数说明：
        - 去重字段由 F1-07 三级识别策略确定
        - 标注列名为"内部去重结果"

        Parameters
        ----------
        context : ProcessContext
            包含 dataframes["yixian"] 和 dedup_field

        Returns
        -------
        ProcessContext
        """
        logger.info("[F6] 开始名单内部去重")

        df_yixian = context.get_dataframe("yixian")
        if df_yixian is None or df_yixian.is_empty():
            logger.warning("[F6] 一线名单为空，跳过去重")
            context.record_module_result(
                module="F6",
                success_count=0,
                fail_count=0,
                message="一线名单为空，跳过去重",
            )
            return context

        dedup_field = context.dedup_field
        logger.info(f"[F6] 使用去重字段：{dedup_field}")

        # ── [PERF 方案D] 优化版：消除 cum_count().over() 瓶颈 ─────────
        #
        # 原方案问题：
        #   1. pl.Series(list(range())) → Python循环，百万次迭代
        #   2. cum_count().over() → 窗口函数，逐行累积计数，最慢操作
        #   3. 两次 group_by + join → 数据复制
        #
        # 优化方案：
        #   1. pl.int_range() → Polars原生行号，无Python循环
        #   2. group_by().agg(min()) + join → 消除窗口函数
        #   3. 合并为两次join（first_in_group + group_size）

        # 步骤1：添加行号 + 处理空值（Polars原生，无Python循环）
        df = df_yixian.with_columns(
            pl.int_range(0, df_yixian.height, eager=True).alias("__row_index__"),
            pl.col(dedup_field).fill_null("__NULL_VALUE__")
        ).sort("__row_index__")

        # 步骤2：找出每组第一个行号（替代 cum_count().over()）
        # [PERF] 使用 group_by().agg(min()) 比窗口函数快 10-20 倍
        first_in_group = (
            df.group_by(dedup_field, maintain_order=True)
              .agg(pl.col("__row_index__").min().alias("__first_row__"))
        )
        self._report_progress(20)  # [方案C] 子任务进度

        # 步骤3：计算每组大小（合并 group_by + len + join 为两次 join）
        group_size = (
            df.group_by(dedup_field)
              .len()
              .rename({"len": "_出现次数"})
        )
        self._report_progress(40)  # [方案C] 子任务进度

        # 步骤4：合并信息（两次 left join）
        df = df.join(first_in_group, on=dedup_field, how="left")
        df = df.join(group_size, on=dedup_field, how="left")
        self._report_progress(60)  # [方案C] 子任务进度

        # 步骤5：一次性向量化标注（减少 with_columns 调用次数）
        # [PERF] 用 row_index == first_row 判断是否是组内第一个
        df = df.with_columns(
            # 内部去重结果：未知/原始/重复
            pl.when(pl.col(dedup_field) == "__NULL_VALUE__")
              .then(pl.lit("未知"))
              .when(pl.col("__row_index__") == pl.col("__first_row__"))
              .then(pl.lit(ORIGINAL_VALUE))
              .otherwise(pl.lit(REPEAT_VALUE))
              .alias(DEDUP_RESULT_COL),
            # _重复键值：空值行显示None，否则显示实际值
            pl.when(pl.col(dedup_field) == "__NULL_VALUE__")
              .then(pl.lit(None))
              .otherwise(pl.col(dedup_field))
              .alias("_重复键值"),
            # _行号：1-based
            (pl.col("__row_index__") + 1).cast(pl.Int32).alias("_行号"),
            # _出现次数：填充NULL为0
            pl.col("_出现次数").fill_null(0).cast(pl.Int32),
            # _重复标记：同内部去重结果（直接重复逻辑，避免引用未创建列）
            pl.when(pl.col(dedup_field) == "__NULL_VALUE__")
              .then(pl.lit("未知"))
              .when(pl.col("__row_index__") == pl.col("__first_row__"))
              .then(pl.lit(ORIGINAL_VALUE))
              .otherwise(pl.lit(REPEAT_VALUE))
              .alias("_重复标记"),
        )
        self._report_progress(100)  # [方案C] 子任务进度

        # 步骤6：恢复原始 dedup_field 值（移除 __NULL_VALUE__ 标记）
        df_final = df.with_columns(
            pl.when(pl.col(dedup_field) == "__NULL_VALUE__")
              .then(pl.lit(None))
              .otherwise(pl.col(dedup_field))
              .alias(dedup_field)
        )

        # 步骤7：移除所有辅助列（保留 _出现次数 等输出列）
        df_final = df_final.drop([
            "__row_index__",
            "__first_row__",
        ])

        # ── 统计结果 [FIX] 术语修正：跳过 → 未知 ──────────────
        total_rows = len(df_final)
        original_count = df_final.filter(pl.col(DEDUP_RESULT_COL) == ORIGINAL_VALUE).height
        duplicate_count = df_final.filter(pl.col(DEDUP_RESULT_COL) == REPEAT_VALUE).height
        unknown_count = df_final.filter(pl.col(DEDUP_RESULT_COL) == UNKNOWN_VALUE).height

        # 有效参与去重的行数（排除未知/空值行）
        effective_rows = total_rows - unknown_count
        # 去重率（基于有效行数计算）
        dedup_rate = (duplicate_count / effective_rows * 100) if effective_rows > 0 else 0

        logger.info(
            f"[F6] 内部去重完成：{total_rows} 行，"
            f"原始={original_count}，重复={duplicate_count}，未知={unknown_count}，"
            f"去重率={dedup_rate:.1f}%"
        )

        # ── 提取重复行详情 [FIX PRD F6-03] 包含所有附加列
        rejected_df = df_final.filter(pl.col(DEDUP_RESULT_COL) == REPEAT_VALUE)

        # ── 更新上下文 ────────────────────────────────────────
        context.set_dataframe("yixian", df_final)

        # ── 记录模块结果 [FIX] 包含 rejected 数据和未知数统计
        context.record_module_result(
            module="F6",
            success_count=original_count,
            fail_count=duplicate_count,
            skip_count=unknown_count,  # [FIX] 内部用 skip_count，但值现在是 unknown_count
            message=(
                f"去重完成：共 {total_rows} 行，"
                f"原始 {original_count} 条，重复 {duplicate_count} 条，"
                f"未知（空值）{unknown_count} 条"
            ),
            rejected=rejected_df,  # [FIX] 重复行详情含 _行号/_重复键值/_出现次数/_重复标记
        )

        logger.info("[F6] 名单内部去重完成")
        return context
