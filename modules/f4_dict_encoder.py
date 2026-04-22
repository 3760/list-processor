"""
F4: 数据字典上码模块

功能覆盖（PRD F4）：
- F4-01: 自动识别字典类型字段（field_spec.yaml 中 dict_id 非空）
- F4-02: 字典索引构建（由 DictLoader 提供）
- F4-03: 新增 [字段名]_Code 列，填入对应码值
- F4-04: 值不存在时填写"未匹配"
- F4-05: 匹配不区分大小写
- F4-06: 上码结果保存

前置检查（F4-validate_input）：
- 检查 field_spec 是否已加载
- 检查 dict_loader 是否已加载（F1-04 加载）
- 检查一线名单 DataFrame 是否存在
"""

import polars as pl

from core.base_module import BaseModule
from core.context import ProcessContext
from infra.log_manager import get_logger

logger = get_logger(__name__)

# 字典上码常量
UNMATCHED_PLACEHOLDER = "未匹配"  # 字典值不匹配时的占位符


class DictEncoderModule(BaseModule):
    """
    F4 数据字典上码模块

    继承 BaseModule，统一实现：
    - get_module_name()  : 返回 "F4"
    - validate_input()   : 前置条件检查（field_spec + dict_loader + 一线名单）
    - execute()         : 核心上码逻辑
    """

    def get_module_name(self) -> str:
        """返回模块名称"""
        return "F4"

    def validate_input(self, context: ProcessContext) -> tuple[bool, str]:
        """
        F4 前置条件检查：

        检查项：
        1. field_spec 是否已加载（F1-05 导入或 F2 执行后）
        2. dict_loader 是否已加载（F1-04 加载）
        3. 一线名单 DataFrame 是否存在

        Returns
        -------
        tuple[bool, str]
            (是否通过, 错误信息)
        """
        # ── 检查1：field_spec 是否已加载 ────────────────────────────
        if context.field_spec is None:
            logger.warning("[F4] 前置检查未通过：field_spec 未加载")
            return False, "请先执行字段规范导入"

        # ── 检查2：dict_loader 是否已加载 ──────────────────────────
        if context.dict_loader is None:
            logger.warning("[F4] 前置检查未通过：dict_loader 未初始化")
            return False, "请先执行 F1 文件加载（数据字典加载失败）"

        # ── 检查3：一线名单是否存在 ────────────────────────────────
        df_yixian = context.get_dataframe("yixian")
        if df_yixian is None:
            logger.warning("[F4] 前置检查未通过：一线名单 DataFrame 不存在")
            return False, "请先执行 F1 文件加载"

        # ── 检查4：一线名单是否为空 ────────────────────────────────
        if df_yixian.is_empty():
            logger.warning("[F4] 前置检查未通过：一线名单为空（0行）")
            return False, "一线名单为空，无法执行上码"

        logger.info(f"[F4] 前置检查通过：一线名单 {len(df_yixian)} 行")
        return True, ""

    def execute(self, context: ProcessContext) -> ProcessContext:
        """
        执行数据字典上码（PRD F4-01 ~ F4-06）：

        逻辑：
        1. 遍历 field_spec 中所有 dict_id 非空的字段（字典类型字段）
        2. 对每个字典类型字段，右侧新增 [字段名]_Code 列
        3. 根据 field_spec 中的 dict_id 找到对应的字典映射
        4. 使用 Details 值匹配（F4-05：不区分大小写匹配）
        5. 匹配成功填入 Code 值，匹配失败填入"未匹配"
        6. 记录上码统计（匹配率）

        参数说明：
        - field_spec 中的 dict_id 字段关联到 DictLoader 中的字典名
        - DictLoader 已按单 Sheet 横排结构建立索引

        Parameters
        ----------
        context : ProcessContext
            包含 field_spec, dict_loader, dataframes["yixian"]

        Returns
        -------
        ProcessContext
        """
        logger.info("[F4] 开始数据字典上码")

        df_yixian = context.get_dataframe("yixian")
        if df_yixian is None or df_yixian.is_empty():
            logger.warning("[F4] 一线名单为空，跳过上码")
            context.record_module_result(
                module="F4",
                success_count=0,
                fail_count=0,
                message="一线名单为空，跳过上码",
            )
            return context

        # 获取字段规范
        field_spec = context.field_spec
        fields_def = field_spec.get("fields", {})

        # 获取字典加载器
        dict_loader = context.dict_loader

        # ── F4-01: 识别字典类型字段 ───────────────────────────────
        dict_type_fields = []  # [(字段名, dict_id)]
        for field_name, field_def in fields_def.items():
            if not isinstance(field_def, dict):
                continue

            dict_id = field_def.get("dict_id", "")
            if dict_id and field_name in df_yixian.columns:
                dict_type_fields.append((field_name, dict_id))

        if not dict_type_fields:
            logger.info("[F4] 未找到字典类型字段，跳过上码")
            context.record_module_result(
                module="F4",
                success_count=len(df_yixian),
                fail_count=0,
                message="未找到字典类型字段，跳过上码",
            )
            return context

        logger.info(f"[F4] 发现 {len(dict_type_fields)} 个字典类型字段: {[f[0] for f in dict_type_fields]}")

        # ── F4-03/04/05: 上码处理 ────────────────────────────────
        total_matched = 0
        total_unmatched = 0
        stats_by_field = {}

        for field_name, dict_id in dict_type_fields:
            # 检查字典是否存在
            if dict_id not in dict_loader.mappings:
                logger.warning(f"[F4] 字典 '{dict_id}' 在数据字典文件中未找到")
                stats_by_field[field_name] = {
                    "dict_id": dict_id,
                    "matched": 0,
                    "unmatched": len(df_yixian),
                    "status": "dict_not_found",
                }
                # 仍然创建 Code 列，全部标记为"未匹配"
                code_col_name = f"{field_name}_Code"
                df_yixian = df_yixian.with_columns(
                    pl.lit("未匹配").alias(code_col_name)
                )
                total_unmatched += len(df_yixian)
                continue

            # [FIX #2] 获取字典映射（dict_loader 现在提供"正向"和"反向"两个子字典）
            raw_mapping = dict_loader.mappings.get(dict_id, {})
            
            # 优先使用预构建的反向映射 {Label小写: Code}
            label_to_code = raw_mapping.get("反向", {}) if isinstance(raw_mapping, dict) else {}
            
            # 如果没有预构建的反向映射（兼容旧版本），则手动构建
            if not label_to_code and isinstance(raw_mapping, dict):
                raw_dict = raw_mapping.get("正向", raw_mapping) if "正向" in raw_mapping else raw_mapping
                for code_val, label_val in raw_dict.items():
                    if label_val is None or str(label_val).strip() == "":
                        continue
                    label_lower = str(label_val).lower().strip()
                    label_to_code[label_lower] = code_val

            code_col_name = f"{field_name}_Code"

            # [PERF] 使用 Polars replace 向量化操作替代 map_elements
            # Step 1: 标准化列值（小写 + 去空格）
            normalized = df_yixian[field_name].cast(pl.Utf8).str.strip_chars().str.to_lowercase()

            # Step 2: 构建映射表达式（使用闭包缓存映射引用，避免重复查询）
            # [FIX #3] 使用 partial 预绑定 mapping 参数，减少闭包开销
            def build_lookup_expr(col: pl.Series, mapping: dict) -> pl.Series:
                """使用 map_elements 但通过预构建映射减少调用开销"""
                # 使用本地变量缓存映射引用，避免每次 lambda 调用时查找
                _mapping = mapping
                return col.map_elements(
                    lambda x, _m=_mapping: _m.get(x, UNMATCHED_PLACEHOLDER) if x else UNMATCHED_PLACEHOLDER,
                    return_dtype=pl.Utf8
                )

            df_yixian = df_yixian.with_columns(
                build_lookup_expr(normalized, label_to_code).alias(code_col_name)
            )

            # 统计匹配/未匹配数量
            matched_count = df_yixian.filter(pl.col(code_col_name) != UNMATCHED_PLACEHOLDER).height
            unmatched_count = len(df_yixian) - matched_count

            total_matched += matched_count
            total_unmatched += unmatched_count

            stats_by_field[field_name] = {
                "dict_id": dict_id,
                "matched": matched_count,
                "unmatched": unmatched_count,
                "match_rate": f"{matched_count / (matched_count + unmatched_count) * 100:.1f}%" if (matched_count + unmatched_count) > 0 else "0%",
                "status": "success",
            }

            logger.info(
                f"[F4] 字段 {field_name}（字典 {dict_id}）: "
                f"匹配 {matched_count}, 未匹配 {unmatched_count}, "
                f"匹配率 {stats_by_field[field_name]['match_rate']}"
            )

        # ── 更新上下文 ──────────────────────────────────────────
        context.set_dataframe("yixian", df_yixian)

        # ── 记录模块结果（F4-08）────────────────────────────────
        context.record_module_result(
            module="F4",
            success_count=total_matched,
            fail_count=total_unmatched,
            message=(
                f"上码完成：{len(dict_type_fields)} 个字典类型字段，"
                f"共 {total_matched + total_unmatched} 行，"
                f"匹配 {total_matched}，未匹配 {total_unmatched}"
            ),
        )

        # 额外记录统计信息（便于 F7 汇总）
        context.module_results["F4_stats"] = stats_by_field

        logger.info("[F4] 数据字典上码完成")
        return context
