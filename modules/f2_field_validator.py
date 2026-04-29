"""
F2: 字段合规性检查模块

功能覆盖（PRD F2）：
- F2-01: 检查必填字段是否存在且不为空
- F2-02: 检查字段数据类型是否符合规范（数值型/文本型/日期型）
- F2-03: 检查字段值长度是否超出上限
- F2-04: 检查字段值是否符合验证规则（如邮箱格式正则）
- F2-05: 问题数据隔离，不合规行不参与后续处理
- F2-06: 输出合规性检查汇总

前置检查（F2-validate_input）：
- 检查 field_spec 是否已加载（F1-05 导入）
- 检查一线名单 DataFrame 是否存在
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from core.base_module import BaseModule
from core.context import ProcessContext
from infra.log_manager import get_logger

logger = get_logger(__name__)


class FieldValidatorModule(BaseModule):
    """
    F2 字段合规性检查模块

    继承 BaseModule，统一实现：
    - get_module_name()  : 返回 "F2"
    - validate_input()   : 前置条件检查（field_spec 加载 + 一线名单存在）
    - execute()         : 核心校验逻辑
    """

    def get_module_name(self) -> str:
        """返回模块名称"""
        return "F2"

    def validate_input(self, context: ProcessContext) -> tuple[bool, str]:
        """
        F2 前置条件检查：

        检查项：
        1. field_spec 是否已加载（F1-05 导入）
        2. 一线名单 DataFrame 是否存在

        Returns
        -------
        tuple[bool, str]
            (是否通过, 错误信息)
        """
        # ── 检查1：field_spec 是否已加载 ────────────────────────────
        if context.field_spec is None:
            logger.warning("[F2] 前置检查未通过：field_spec 未加载")
            return False, "[F2] 跳过: 请先执行字段规范导入"

        # ── 检查2：一线名单是否存在 ────────────────────────────────
        df_yixian = context.get_dataframe("yixian")
        if df_yixian is None:
            logger.warning("[F2] 前置检查未通过：一线名单 DataFrame 不存在")
            return False, "[F2] 跳过: 请先执行 F1 文件加载"

        # ── 检查3：一线名单是否为空 ────────────────────────────────
        if df_yixian.is_empty():
            logger.warning("[F2] 前置检查未通过：一线名单为空")
            return False, "[F2] 跳过: 一线名单为空"

        logger.info(f"[F2] 前置检查通过：一线名单 {len(df_yixian)} 行")
        return True, ""

    def execute(self, context: ProcessContext) -> ProcessContext:
        """
        执行字段合规性检查（PRD F2-01 ~ F2-06）：

        校验规则（按优先级）：
        1. F2-01: 必填字段检查（空值）
        2. F2-02: 数据类型检查（数值型/文本型/日期型）
        3. F2-03: 长度上限检查
        4. F2-04: 正则规则检查（邮箱、身份证等）

        参数说明：
        - field_spec 中的字段定义作为校验依据
        - 错误数据隔离输出到 error_records["yixian"]
        - 正确数据继续保留在 dataframes["yixian"] 中

        Parameters
        ----------
        context : ProcessContext
            包含 field_spec 和 dataframes["yixian"]

        Returns
        -------
        ProcessContext
        """
        logger.info("[F2] 开始字段合规性校验")

        df_yixian = context.get_dataframe("yixian")

        # [DEBUG] 输出数据概览
        total_rows = len(df_yixian)
        df_columns = df_yixian.columns
        logger.info(f"[F2] 数据概览：一线名单 {total_rows} 行，{len(df_columns)} 列")
        logger.info(f"[F2] 数据列名：{df_columns}")

        # 获取字段规范
        field_spec = context.field_spec
        fields_def = field_spec.get("fields", {})

        # [DEBUG] 输出字段规范概览
        required_fields = [
            fn for fn, fv in fields_def.items()
            if isinstance(fv, dict) and (
                fv.get("required") is True or
                (isinstance(fv.get("required"), str) and fv.get("required", "").strip() in ("是", "√"))
            )
        ]
        logger.info(f"[F2] 字段规范：共 {len(fields_def)} 个字段定义，{len(required_fields)} 个必填字段")
        logger.info(f"[F2] 必填字段列表：{required_fields}")

        # 收集所有错误记录
        all_errors: List[Dict[str, Any]] = []
        error_row_ids = set()  # 记录有错误的行号

        # ── F2-01: 必填字段检查 ──────────────────────────────────
        required_errors = self._check_required_fields(df_yixian, fields_def)
        all_errors.extend(required_errors)
        error_row_ids.update(e["行号"] for e in required_errors)
        logger.info(f"[F2] 必填字段校验：{len(required_errors)} 条错误")
        if required_errors:
            for err in required_errors[:10]:
                logger.info(f"      └─ [{err['字段名']}] 行{err['行号']}: {err['说明']}")
            if len(required_errors) > 10:
                logger.info(f"      └─ ... 还有 {len(required_errors) - 10} 条错误")
        self._report_progress(25)  # [方案C] 子任务进度

        # ── F2-02: 数据类型检查 ─────────────────────────────────
        type_errors = self._check_data_types(df_yixian, fields_def)
        all_errors.extend(type_errors)
        error_row_ids.update(e["行号"] for e in type_errors)
        logger.info(f"[F2] 数据类型校验：{len(type_errors)} 条错误")
        if type_errors:
            for err in type_errors[:10]:
                logger.info(f"      └─ [{err['字段名']}] 行{err['行号']}: {err['说明']}，实际值={err.get('原始值', '')}")
            if len(type_errors) > 10:
                logger.info(f"      └─ ... 还有 {len(type_errors) - 10} 条错误")
        self._report_progress(50)  # [方案C] 子任务进度

        # ── F2-03: 长度上限检查 ─────────────────────────────────
        length_errors = self._check_max_length(df_yixian, fields_def)
        all_errors.extend(length_errors)
        error_row_ids.update(e["行号"] for e in length_errors)
        logger.info(f"[F2] 长度上限校验：{len(length_errors)} 条错误")
        if length_errors:
            for err in length_errors[:10]:
                logger.info(f"      └─ [{err['字段名']}] 行{err['行号']}: {err['说明']}，实际值={err.get('原始值', '')}")
            if len(length_errors) > 10:
                logger.info(f"      └─ ... 还有 {len(length_errors) - 10} 条错误")
        self._report_progress(75)  # [方案C] 子任务进度

        # ── F2-04: 正则规则检查 ─────────────────────────────────
        regex_errors = self._check_regex_rules(df_yixian, fields_def)
        all_errors.extend(regex_errors)
        error_row_ids.update(e["行号"] for e in regex_errors)
        logger.info(f"[F2] 正则规则校验：{len(regex_errors)} 条错误")
        if regex_errors:
            for err in regex_errors[:10]:
                logger.info(f"      └─ [{err['字段名']}] 行{err['行号']}: {err['说明']}，实际值={err.get('原始值', '')}")
            if len(regex_errors) > 10:
                logger.info(f"      └─ ... 还有 {len(regex_errors) - 10} 条错误")
        self._report_progress(100)  # [方案C] 子任务进度

        # ── 汇总处理结果 ──────────────────────────────────────────
        total_errors = len(all_errors)
        error_row_count = len(error_row_ids)

        if all_errors:
            # 构建错误记录 DataFrame（F2-06 输出汇总）
            error_df = pl.DataFrame(all_errors)
            context.error_records["yixian"] = error_df

            # [DEBUG] 输出前20条错误详情（INFO级别，便于排查）
            logger.info(f"[F2] === 错误详情（前20条）===")
            for i, err in enumerate(all_errors[:20], 1):
                logger.info(f"     [{i:02d}] 字段={err['字段名']}, 行号={err['行号']}, 问题={err['问题类型']}, 说明={err['说明']}")
                if '原始值' in err and err['原始值'] not in ("(空值)", "(字段缺失)"):
                    logger.info(f"          原始值={err['原始值']}")
            if len(all_errors) > 20:
                logger.info(f"     ... 还有 {len(all_errors) - 20} 条错误")

            # 构建错误类型分布统计
            error_type_counts: Dict[str, int] = {}
            for e in all_errors:
                error_type = e.get("问题类型", "UNKNOWN")
                error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1

            logger.info(f"[F2] 校验完成：{total_errors} 条错误")
            logger.info(f"     涉及 {error_row_count} 行，已输出至「合规性检查结果」Sheet")
            logger.info(f"     错误类型分布：{error_type_counts}")
        else:
            logger.info("[F2] 校验完成：所有数据均合规")

        # ── 记录模块结果（F2-06）─────────────────────────────────
        if error_row_count == 0:
            msg = f"[F2] 成功: {total_rows} 行数据，全部合规"
        else:
            msg = f"[F2] 失败: {total_rows} 行数据，{error_row_count} 项不合规"
        context.record_module_result(
            module="F2",
            success_count=total_rows - error_row_count,
            fail_count=error_row_count,
            message=msg,
        )

        # ── [20260424-老谈] 重构：错误数据逻辑标记（替代物理删除）────────
        # 核心策略：不物理删除错误行，改为添加标记列
        # - 合规检查_状态：通过 / 不通过
        # - 合规检查_错误数：该行触发的错误数量
        # - 合规检查_错误类型：逗号分隔的错误类型代码
        #
        # 下游模块（F3/F5/F6）根据标记列过滤处理

        # [FIX PRD F7-07] 保存原始数据量供处理摘要显示
        if not hasattr(context, 'original_row_counts'):
            context.original_row_counts = {}
        context.original_row_counts["yixian"] = total_rows

        # 统计每行的错误数量和错误类型
        row_error_count: Dict[int, int] = {}      # {行号: 错误数}
        row_error_types: Dict[int, List[str]] = {}  # {行号: [错误类型列表]}

        for err in all_errors:
            row_num = err.get("行号", 0)
            if row_num > 0:
                row_error_count[row_num] = row_error_count.get(row_num, 0) + 1
                if row_num not in row_error_types:
                    row_error_types[row_num] = []
                if err.get("问题类型"):
                    row_error_types[row_num].append(err["问题类型"])

        # 添加标记列到 DataFrame（使用 _row_num 列）
        df_yixian = df_yixian.with_columns([
            pl.col("_row_num").map_elements(
                lambda x: "通过" if row_error_count.get(x, 0) == 0 else "不通过",
                return_dtype=pl.Utf8
            ).alias("合规检查_状态"),
            pl.col("_row_num").map_elements(
                lambda x: row_error_count.get(x, 0),
                return_dtype=pl.Int32
            ).alias("合规检查_错误数"),
            pl.col("_row_num").map_elements(
                lambda x: ",".join(row_error_types.get(x, [])) if x in row_error_types else "",
                return_dtype=pl.Utf8
            ).alias("合规检查_错误类型"),
        ])
        context.set_dataframe("yixian", df_yixian)

        # 记录处理统计
        valid_count = total_rows - error_row_count
        logger.info(
            f"[F2] 逻辑标记完成：{total_rows} 行数据，"
            f"{valid_count} 行通过，{error_row_count} 行不通过"
        )
        # ── 逻辑标记结束 ───────────────────────────────────────────

        return context

    def _check_required_fields(
        self, df: pl.DataFrame, fields_def: Dict
    ) -> List[Dict[str, Any]]:
        """
        F2-01: 检查必填字段是否为空

        Parameters
        ----------
        df : pl.DataFrame
        fields_def : Dict
            field_spec.yaml 中的 fields 定义

        Returns
        -------
        List[Dict]
            错误记录列表
        """
        errors = []
        total_rows = len(df)

        # 统计必填字段数量
        required_fields = [
            fn for fn, fv in fields_def.items()
            if isinstance(fv, dict) and (
                fv.get("required") is True or
                (isinstance(fv.get("required"), str) and fv.get("required", "").strip() in ("是", "√"))
            )
        ]
        logger.debug(f"[_check_required_fields] 必填字段共 {len(required_fields)} 个: {required_fields}")

        for field_name, field_def in fields_def.items():
            if not isinstance(field_def, dict):
                continue

            # 只检查必填字段
            # [FIX] 处理字符串类型的"是"/"否"值
            required_val = field_def.get("required", False)
            if isinstance(required_val, str):
                # 字符串"是"或"√"表示必填，其他值表示非必填
                is_required = required_val.strip() in ("是", "√")
            else:
                is_required = bool(required_val)

            if not is_required:
                continue

            # 检查字段是否存在于 DataFrame 中
            if field_name not in df.columns:
                logger.debug(f"[_check_required_fields] [{field_name}] 字段不存在，整列 {total_rows} 行均为错误")
                # 必填字段缺失（整列缺失）
                for i in range(len(df)):
                    errors.append({
                        "字段名": field_name,
                        "行号": i + 1,
                        "原始值": "(字段缺失)",
                        "问题类型": "REQUIRED_MISSING",
                        "说明": f"必填字段 '{field_name}' 在数据中不存在",
                    })
                continue

            # [PERF] 使用 Polars 向量化操作检查空值
            null_mask = df[field_name].is_null()
            empty_mask = df[field_name].cast(pl.Utf8).str.strip_chars() == ""
            error_mask = null_mask | empty_mask
            error_count = error_mask.sum()

            logger.debug(f"[_check_required_fields] [{field_name}] 检查完成: 总行数={total_rows}, 空值数={error_count}")

            if error_count > 0:
                # 获取错误行的行号和原始值
                error_df = df.filter(error_mask).select(["_row_num", field_name])
                for row in error_df.iter_rows(named=True):
                    errors.append({
                        "字段名": field_name,
                        "行号": row.get("_row_num", 0),
                        "原始值": "(空值)",
                        "问题类型": "REQUIRED_EMPTY",
                        "说明": f"必填字段 '{field_name}' 值为空",
                    })

        return errors

    def _check_data_types(
        self, df: pl.DataFrame, fields_def: Dict
    ) -> List[Dict[str, Any]]:
        """
        F2-02: 检查字段数据类型是否符合规范

        支持的数据类型：
        - 数值型：整数、小数
        - 文本型：字符串
        - 日期型：日期格式

        Parameters
        ----------
        df : pl.DataFrame
        fields_def : Dict

        Returns
        -------
        List[Dict]
        """
        errors = []

        # [PERF] 使用 Polars 向量化操作检查数据类型
        for field_name, field_def in fields_def.items():
            if not isinstance(field_def, dict):
                continue

            # [20260420-老谈] ISSUE-02+21: PRD §8.1 映射规范中键名为 "type"（非 "data_type"）
            data_type = field_def.get("type", "")
            if not data_type:
                continue

            if field_name not in df.columns:
                continue

            # [PERF] 向量化类型检查
            val_series = df[field_name].cast(pl.Utf8).str.strip_chars()
            non_null_mask = val_series != ""

            # 根据类型选择验证
            if data_type in ["integer", "int", "数值型"]:
                error_mask = non_null_mask & ~val_series.str.contains(r"^-?\d+$", literal=False).fill_null(False)
            elif data_type in ["decimal", "float", "小数"]:
                error_mask = non_null_mask & ~val_series.map_elements(self._is_decimal, return_dtype=pl.Boolean)
            elif data_type in ["date", "日期型"]:
                error_mask = non_null_mask & ~val_series.map_elements(self._is_valid_date, return_dtype=pl.Boolean)
            elif data_type in ["text", "string", "文本型"]:
                continue  # 文本型不校验
            else:
                continue

            error_count = error_mask.sum()
            if error_count > 0:
                error_df = df.filter(error_mask).select(["_row_num", field_name])
                for row in error_df.iter_rows(named=True):
                    val_str = str(row.get(field_name, ""))[:50]
                    errors.append({
                        "字段名": field_name,
                        "行号": row.get("_row_num", 0),
                        "原始值": val_str,
                        "问题类型": "TYPE_MISMATCH",
                        "说明": f"期望类型 '{data_type}'，实际 '{val_str}'",
                    })

        return errors

    def _check_max_length(
        self, df: pl.DataFrame, fields_def: Dict
    ) -> List[Dict[str, Any]]:
        """
        F2-03: 检查字段值长度是否超出上限

        Parameters
        ----------
        df : pl.DataFrame
        fields_def : Dict

        Returns
        -------
        List[Dict]
        """
        errors = []

        for field_name, field_def in fields_def.items():
            if not isinstance(field_def, dict):
                continue

            max_length = field_def.get("max_length", 0)
            if max_length <= 0:
                continue  # 0 = 不限制

            if field_name not in df.columns:
                continue

            # [PERF] 使用 Polars 向量化操作检查长度超限
            val_series = df[field_name].cast(pl.Utf8)
            error_mask = val_series.str.len_chars() > max_length
            error_count = error_mask.sum()

            if error_count > 0:
                error_df = df.filter(error_mask).select(["_row_num", field_name])
                for row in error_df.iter_rows(named=True):
                    val_str = str(row.get(field_name, ""))
                    val_display = val_str[:50] + "..." if len(val_str) > 50 else val_str
                    errors.append({
                        "字段名": field_name,
                        "行号": row.get("_row_num", 0),
                        "原始值": val_display,
                        "问题类型": "LENGTH_EXCEED",
                        "说明": f"长度超过上限 {max_length}",
                    })

        return errors

    def _check_regex_rules(
        self, df: pl.DataFrame, fields_def: Dict
    ) -> List[Dict[str, Any]]:
        """
        F2-04: 检查字段值是否符合正则验证规则

        支持的规则（field_spec.yaml 中的 regex 字段）：
        - 邮箱格式
        - 身份证格式
        - 自定义正则

        Parameters
        ----------
        df : pl.DataFrame
        fields_def : Dict

        Returns
        -------
        List[Dict]
        """
        errors = []

        # 常用正则模式（支持中英文键名）
        REGEX_PATTERNS = {
            # 英文键名
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "id_card": r"^[1-9]\d{5}(18|19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}[\dXx]$",
            "phone": r"^1[3-9]\d{9}$",
            "mobile": r"^1[3-9]\d{9}$",
            # 中文键名（与属性导入模版"验证规则"列保持一致）
            "邮箱": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "手机号": r"^1[3-9]\d{9}$",
            "手机": r"^1[3-9]\d{9}$",
        }

        for field_name, field_def in fields_def.items():
            if not isinstance(field_def, dict):
                continue

            # 先判断：数据里是否存在该字段
            if field_name not in df.columns:
                continue

            regex_pattern = field_def.get("regex", "")
            # [新增] regex 为空时，按字段名匹配默认正则
            if not regex_pattern:
                if field_name.lower() in REGEX_PATTERNS:
                    regex_pattern = field_name.lower()
                else:
                    continue

            # 获取实际使用的正则表达式
            if regex_pattern.lower() in REGEX_PATTERNS:
                pattern = REGEX_PATTERNS[regex_pattern.lower()]
            else:
                pattern = regex_pattern

            # [PERF] 使用 Polars 向量化正则匹配
            try:
                # 使用 Polars str.contains 向量化匹配
                val_series = df[field_name].cast(pl.Utf8).str.strip_chars()
                non_null_mask = val_series != ""
                # str.contains 支持完整正则表达式
                error_mask = non_null_mask & ~val_series.str.contains(pattern, literal=False).fill_null(False)
                error_count = error_mask.sum()

                if error_count > 0:
                    error_df = df.filter(error_mask).select(["_row_num", field_name])
                    for row in error_df.iter_rows(named=True):
                        val_str = str(row.get(field_name, ""))[:50]
                        errors.append({
                            "字段名": field_name,
                            "行号": row.get("_row_num", 0),
                            "原始值": val_str,
                            "问题类型": "REGEX_FAILED",
                            "说明": f"不符合正则规则: {regex_pattern}",
                        })
            except re.error as e:
                logger.warning(f"[F2] 正则表达式匹配失败: {pattern}, {e}")
                continue

        return errors

    @staticmethod
    def _is_decimal(s: str) -> bool:
        """检查字符串是否为有效小数"""
        try:
            float(s)
            return "." in s
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _is_valid_date(s: str) -> bool:
        """检查字符串是否为有效日期"""
        if not s or not isinstance(s, str):
            return False

        date_formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y.%m.%d",
            "%Y%m%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
        ]

        for fmt in date_formats:
            try:
                datetime.strptime(s.strip(), fmt)
                return True
            except ValueError:
                continue

        return False
