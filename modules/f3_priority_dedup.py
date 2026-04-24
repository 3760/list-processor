"""
F3: 跨名单去重标注模块

功能覆盖（PRD F3）：
- F3-01: 三方 vs 一线标注（"是否已在一线名单"）
- F3-02: HW vs 一线标注（"是否已在一线名单"）
- F3-03: HW vs 三方标注（"是否已在三方名单"）- PRD v1.1明确
- F3-04: 标注结果保存（追加列，不修改原始数据）
- F3-05: 去重字段匹配不区分大小写，自动去除首尾空格

前置检查（F3-validate_input）：
- 检查一线、三方、HW 名单 DataFrame 是否存在
- 检查去重字段（dedup_field）是否已设置
"""

import polars as pl

from core.base_module import BaseModule
from core.context import ProcessContext
from infra.log_manager import get_logger

logger = get_logger(__name__)

# 标注常量
YES_VALUE = "是"
NO_VALUE = "否"
COL_YIXIAN_FLAG = "是否已在一线名单"
COL_SANFANG_FLAG = "是否已在三方名单"


class PriorityDedupModule(BaseModule):
    """
    F3 跨名单去重标注模块

    继承 BaseModule，统一实现：
    - get_module_name()  : 返回 "F3"
    - validate_input()   : 前置条件检查
    - execute()         : 核心去重逻辑
    """

    def get_module_name(self) -> str:
        """返回模块名称"""
        return "F3"

    def validate_input(self, context: ProcessContext) -> tuple[bool, str]:
        """
        F3 前置条件检查：

        检查项：
        1. 一线、三方、HW 名单 DataFrame 是否存在（至少有一方存在）
        2. 去重字段（dedup_field）是否已设置
        3. 去重字段是否存在于存在的名单中

        Args:
            context (ProcessContext): 处理上下文对象

        Returns
        -------
        tuple[bool, str]
            (是否通过, 错误信息)
        """
        # ── 检查1：至少有一方名单存在 ─────────────────────────
        df_yixian = context.get_dataframe("yixian")
        df_sanfang = context.get_dataframe("sanfang")
        df_hw = context.get_dataframe("hw")

        if df_yixian is None and df_sanfang is None and df_hw is None:
            logger.warning("[F3] 前置检查未通过：三份名单均不存在")
            return False, "[F3] 跳过: 请先执行 F1 文件加载"

        # ── [20260424-老谈] 修改：检查是否有合规数据 ─────────────────
        # 不再检查 is_empty()，改为检查"合规检查_状态=通过"的行数
        valid_count = 0
        if df_yixian is not None:
            if "合规检查_状态" in df_yixian.columns:
                valid_count = df_yixian.filter(pl.col("合规检查_状态") == "通过").height
            else:
                valid_count = df_yixian.height

        if valid_count == 0:
            logger.warning("[F3] 前置检查未通过：一线名单无合规数据")
            return False, "[F3] 跳过: 一线名单无合规数据"

        # ── 检查2：去重字段是否已设置 ──────────────────────────
        if context.dedup_field is None:
            logger.warning("[F3] 前置检查未通过：去重字段未设置")
            return False, "[F3] 跳过: 去重字段未识别"

        # ── 检查3：去重字段是否存在于存在的名单中 ─────────────
        has_valid_df = False
        if df_yixian is not None and context.dedup_field in df_yixian.columns:
            has_valid_df = True
        if df_sanfang is not None and context.dedup_field in df_sanfang.columns:
            has_valid_df = True
        if df_hw is not None and context.dedup_field in df_hw.columns:
            has_valid_df = True

        if not has_valid_df:
            logger.warning(f"[F3] 前置检查未通过：去重字段 '{context.dedup_field}' 在所有名单中均不存在")
            return False, f"[F3] 跳过: 去重字段 '{context.dedup_field}' 在数据中不存在"

        logger.info(f"[F3] 前置检查通过：去重字段='{context.dedup_field}'，合规数据 {valid_count} 行")
        return True, ""

    def execute(self, context: ProcessContext) -> ProcessContext:
        """
        执行跨名单去重标注（PRD F3-01 ~ F3-05）：

        标注规则：
        1. F3-01: 三方 vs 一线 → 在三方名单新增"是否已在一线名单"列
        2. F3-02: HW vs 一线 → 在 HW 名单新增"是否已在一线名单"列
        3. F3-03: HW vs 三方 → 在 HW 名单新增"是否已在三方名单"列

        匹配规则（F3-05）：
        - 不区分大小写
        - 去除首尾空格

        Parameters
        ----------
        context : ProcessContext
            包含 dataframes["yixian", "sanfang", "hw"] 和 dedup_field

        Returns
        -------
        ProcessContext
        """
        logger.info("[F3] 开始跨名单去重标注")

        dedup_field = context.dedup_field
        logger.info(f"[F3] 使用去重字段：{dedup_field}")

        total_yixian = 0
        total_sanfang = 0
        total_hw = 0
        sanfang_in_yixian = 0
        hw_in_yixian = 0
        hw_in_sanfang = 0

        # ── [20260424-老谈] 获取一线名单的去重字段集合 ─────────────────
        # 只使用"合规检查_状态=通过"的数据进行去重匹配
        df_yixian = context.get_dataframe("yixian")
        yixian_keys = set()
        
        if df_yixian is not None and dedup_field in df_yixian.columns:
            # 过滤出合规数据
            if "合规检查_状态" in df_yixian.columns:
                df_yixian_valid = df_yixian.filter(pl.col("合规检查_状态") == "通过")
            else:
                df_yixian_valid = df_yixian  # 兼容旧数据
            
            yixian_keys = self._extract_keys(df_yixian_valid, dedup_field)
            total_yixian = len(df_yixian)
            valid_count = len(df_yixian_valid)
            
            # [20260424-老谈] 当一线无合规数据时，yixian_keys为空
            logger.info(f"[F3] 一线名单：{total_yixian} 行（合规 {valid_count} 行），{len(yixian_keys)} 个唯一{dedup_field}")

        # ── F3-01: 三方 vs 一线标注 ───────────────────────────
        df_sanfang = context.get_dataframe("sanfang")
        if df_sanfang is not None and dedup_field in df_sanfang.columns:
            df_sanfang = self._annotate_in_set(df_sanfang, dedup_field, yixian_keys, COL_YIXIAN_FLAG)
            context.set_dataframe("sanfang", df_sanfang)
            sanfang_in_yixian = df_sanfang.filter(pl.col(COL_YIXIAN_FLAG) == YES_VALUE).height
            total_sanfang = len(df_sanfang)
            logger.info(f"[F3] 三方名单：{total_sanfang} 行，其中 {sanfang_in_yixian} 条在一线名单中")
        self._report_progress(33)  # [方案C] 子任务进度

        # ── F3-02/F3-03: HW vs 一线/HW vs 三方标注 ───────────
        df_hw = context.get_dataframe("hw")
        if df_hw is not None and dedup_field in df_hw.columns:
            # F3-02: HW vs 一线
            df_hw = self._annotate_in_set(df_hw, dedup_field, yixian_keys, COL_YIXIAN_FLAG)
            self._report_progress(66)  # [方案C] 子任务进度

            # F3-03: HW vs 三方
            if df_sanfang is not None:
                sanfang_keys = self._extract_keys(df_sanfang, dedup_field)
                df_hw = self._annotate_in_set(df_hw, dedup_field, sanfang_keys, COL_SANFANG_FLAG)
                hw_in_sanfang = df_hw.filter(pl.col(COL_SANFANG_FLAG) == YES_VALUE).height

            context.set_dataframe("hw", df_hw)
            hw_in_yixian = df_hw.filter(pl.col(COL_YIXIAN_FLAG) == YES_VALUE).height
            total_hw = len(df_hw)
            logger.info(f"[F3] HW名单：{total_hw} 行，其中 {hw_in_yixian} 条在一线名单，{hw_in_sanfang} 条在三方名单")
        self._report_progress(100)  # [方案C] 子任务进度

        # ── 记录模块结果 ──────────────────────────────────────────
        # 三方新增 = 三方总数 - 在一线中的重复
        sanfang_new = total_sanfang - sanfang_in_yixian if total_sanfang > 0 else 0
        context.record_module_result(
            module="F3",
            success_count=sanfang_new,
            fail_count=sanfang_in_yixian + hw_in_yixian,
            message=(
                f"[F3] 成功: 三方新增 {sanfang_new} 条（重复 {sanfang_in_yixian} 条）"
            ),
        )

        logger.info("[F3] 跨名单去重标注完成")
        return context

    def _extract_keys(self, df: pl.DataFrame, dedup_field: str) -> set:  # noqa: E731
        """
        从 DataFrame 提取去重字段值集合（标准化处理）

        F3-05: 匹配不区分大小写，自动去除首尾空格
        [PERF] 使用 Polars 向量化操作替代 iter_rows
        """
        # [PERF] 使用 Polars 向量化操作：去除空格 + 转小写 + 过滤空值
        col = df[dedup_field].cast(pl.Utf8).str.strip_chars()
        keys = col.str.to_lowercase().filter(col != "").unique().to_list()
        return set(k for k in keys if k)

    
    def _annotate_in_set(self, df: pl.DataFrame, dedup_field: str, key_set: set, col_name: str) -> pl.DataFrame:
        """
        判断值是否在集合中（F3-05: 标准化处理）

        [PERF] 使用 Polars 原生向量化操作替代 map_elements

        [20260424-老谈] 修改：当 key_set 为空时（被比对名单无合规数据），标记为"跳过"

        Returns
        -------
        pl.DataFrame
            带标注列的新 DataFrame
        """
        if not key_set:
            # [20260424-老谈] key_set为空（比对名单无合规数据），标记为"跳过"
            return df.with_columns(pl.lit(NO_VALUE).alias(col_name))

        # [PERF] 向量化操作：去除空格 + 转小写
        normalized = df[dedup_field].cast(pl.Utf8).str.strip_chars().str.to_lowercase()

        # [PERF] 使用 replace 使用集合进行映射
        # 构建映射字典：null -> "否", 空字符串 -> "否", 其他 -> ("是" 或 "否")
        result = normalized.map_elements(
            lambda x: YES_VALUE if x and x in key_set else NO_VALUE,
            return_dtype=pl.Utf8
        ).alias(col_name)

        return df.with_columns(result)
