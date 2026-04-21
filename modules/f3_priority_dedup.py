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
            return False, "请先执行 F1 文件加载"

        # ── 检查2：去重字段是否已设置 ──────────────────────────
        if context.dedup_field is None:
            logger.warning("[F3] 前置检查未通过：去重字段未设置")
            return False, "请先执行 F1 文件加载（去重字段未识别）"

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
            return False, f"去重字段 '{context.dedup_field}' 在数据中不存在"

        logger.info(f"[F3] 前置检查通过：去重字段='{context.dedup_field}'")
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

        # ── 获取一线名单的去重字段集合 ──────────────────────────
        df_yixian = context.get_dataframe("yixian")
        yixian_keys = set()
        if df_yixian is not None and dedup_field in df_yixian.columns:
            yixian_keys = self._extract_keys(df_yixian, dedup_field)
            total_yixian = len(df_yixian)
            logger.info(f"[F3] 一线名单：{total_yixian} 行，{len(yixian_keys)} 个唯一邮箱")

        # ── F3-01: 三方 vs 一线标注 ───────────────────────────
        df_sanfang = context.get_dataframe("sanfang")
        if df_sanfang is not None and dedup_field in df_sanfang.columns:
            df_sanfang = df_sanfang.with_columns(
                pl.col(dedup_field).map_elements(
                    lambda x: self._match_key(x, yixian_keys),
                    return_dtype=pl.String
                ).alias("是否已在一线名单")
            )
            context.set_dataframe("sanfang", df_sanfang)
            sanfang_in_yixian = df_sanfang.filter(pl.col("是否已在一线名单") == "是").height
            total_sanfang = len(df_sanfang)
            logger.info(f"[F3] 三方名单：{total_sanfang} 行，其中 {sanfang_in_yixian} 条在一线名单中")

        # ── F3-02/F3-03: HW vs 一线/HW vs 三方标注 ───────────
        df_hw = context.get_dataframe("hw")
        if df_hw is not None and dedup_field in df_hw.columns:
            # F3-02: HW vs 一线
            df_hw = df_hw.with_columns(
                pl.col(dedup_field).map_elements(
                    lambda x: self._match_key(x, yixian_keys),
                    return_dtype=pl.String
                ).alias("是否已在一线名单")
            )

            # F3-03: HW vs 三方
            if df_sanfang is not None:
                sanfang_keys = self._extract_keys(df_sanfang, dedup_field)
                df_hw = df_hw.with_columns(
                    pl.col(dedup_field).map_elements(
                        lambda x: self._match_key(x, sanfang_keys),
                        return_dtype=pl.String
                    ).alias("是否已在三方名单")
                )
                hw_in_sanfang = df_hw.filter(pl.col("是否已在三方名单") == "是").height

            context.set_dataframe("hw", df_hw)
            hw_in_yixian = df_hw.filter(pl.col("是否已在一线名单") == "是").height
            total_hw = len(df_hw)
            logger.info(f"[F3] HW名单：{total_hw} 行，其中 {hw_in_yixian} 条在一线名单，{hw_in_sanfang} 条在三方名单")

        # ── 记录模块结果 ──────────────────────────────────────────
        context.record_module_result(
            module="F3",
            success_count=total_sanfang + total_hw - sanfang_in_yixian - hw_in_yixian,
            fail_count=sanfang_in_yixian + hw_in_yixian,
            message=(
                f"去重完成：一线 {total_yixian} 行，三方 {total_sanfang} 行（含 {sanfang_in_yixian} 重复），"
                f"HW {total_hw} 行（含 {hw_in_yixian} 在一线、{hw_in_sanfang} 在三方）"
            ),
        )

        logger.info("[F3] 跨名单去重标注完成")
        return context

    def _extract_keys(self, df: pl.DataFrame, dedup_field: str) -> set:
        """
        从 DataFrame 提取去重字段值集合（标准化处理）

        F3-05: 匹配不区分大小写，自动去除首尾空格
        """
        keys = set()
        for val in df[dedup_field]:
            if val is not None:
                key = str(val).strip().lower()
                if key:
                    keys.add(key)
        return keys

    def _match_key(self, val, key_set: set) -> str:
        """
        判断值是否在集合中（F3-05: 标准化处理）

        Returns
        -------
        str
            "是" 或 "否"
        """
        if val is None:
            return "否"

        key = str(val).strip().lower()
        if not key:
            return "否"

        return "是" if key in key_set else "否"
