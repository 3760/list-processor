"""
F1 - 文件加载模块

功能：加载三类名单文件（一线/三方/HW），处理多Sheet选择，
添加`_来源`列，统一列名后写入 ProcessContext。

处理顺序：一线 → 三方 → HW（DEV-05 多Sheet策略）
"""
from pathlib import Path
from typing import Optional

import polars as pl

from core.context import ProcessContext
from infra.exceptions import DataQualityError, ValidationError
from infra.log_manager import get_logger
from core.base_module import BaseModule

logger = get_logger(__name__)


def _detect_csv_encoding(file_path: str) -> str:
    """
    [20260420-老谈] ISSUE-23: CSV 编码自动识别
    尝试多种常见编码（UTF-8 -> GBK -> GB2312），返回第一个成功的编码。
    """
    encodings = ["utf-8", "gbk", "gb2312", "cp1252", "latin-1"]
    
    for encoding in encodings:
        try:
            with open(file_path, "rb") as f:
                raw_bytes = f.read(4096)
            # 检测 UTF-8 BOM 并移除
            if raw_bytes.startswith(b'\xef\xbb\xbf'):
                logger.debug(f"检测到 UTF-8 BOM，使用 utf-8-sig 编码")
                return "utf-8-sig"
            # 尝试解码验证
            raw_bytes.decode(encoding)
            logger.debug(f"CSV编码检测成功：{encoding}")
            return encoding
        except (UnicodeDecodeError, LookupError):
            continue
    
    # 默认返回 UTF-8（兜底）
    logger.warning(f"CSV编码检测失败，使用默认编码：utf-8")
    return "utf-8"


def load_files(
    ctx: ProcessContext,
    file_paths: dict[str, str],
    sheet_selections: Optional[dict[str, str]] = None,
    dedup_field: Optional[str] = None,
    progress_callback: Optional[callable] = None,
) -> ProcessContext:
    """
    加载三类名单文件，添加 `_来源` 列。

    DEV-05 多Sheet策略：
      - Sheet=1：直接读取，无提示
      - Sheet>1：使用预选的 Sheet 名称（如有）

    [20260420-老谈] ISSUE-09: 重构为预选模式
      原：传递 callback 函数由后台线程调用（线程安全问题）
      新：传递预选 Sheet 名称字典（UI 主线程已选择完毕）

    NQ-02 三级去重字段识别策略（由调用方传入 dedup_field，本函数记录到上下文）：
      1. 读取 app_config.yaml 中 dedup.default_field（调用方保证）
      2. 列名关键字模糊匹配（email/邮箱/mail/e-mail）
      3. 弹窗让用户手动指定（本模块不处理，由调用方在 UI 层处理）

    Parameters
    ----------
    ctx : ProcessContext
    file_paths : dict
        {"一线": path, "三方": path, "HW": path}
    sheet_selections : dict, optional
        {"一线": "Sheet1", "三方": "Data", "HW": "Sheet1"}
        UI层预选的Sheet名称，直接使用，无需弹窗回调
    dedup_field : str, optional
        去重字段名（由调用方根据三级策略确定后传入）
    progress_callback : callable, optional
        进度回调，签名为 (list_type: str, percent: int) -> None
        用于实时报告每个文件的加载进度

    Returns
    -------
    ProcessContext
    """
    logger.info(f"开始加载文件，输入: {file_paths}")

    # 初始化 sheet_selections
    if sheet_selections is None:
        sheet_selections = {}

    # 构建文件列表并计算进度
    file_list = [(lt, fp) for lt, fp in file_paths.items() if fp]
    total_files = len(file_list)
    F1_WEIGHT = 15  # F1 总权重

    # 收集各类型加载结果
    load_results = []

    for i, (list_type, file_path) in enumerate(file_list):
        # 计算基于F1总权重的进度
        base_percent = int(i / total_files * F1_WEIGHT)
        if progress_callback:
            progress_callback(f"F1_{list_type}", base_percent)

        try:
            pre_selected_sheet = sheet_selections.get(list_type)
            df = _load_single_file(file_path, list_type, pre_selected_sheet)
            # [20260424-老谈] 添加行号列（1-indexed），供下游模块使用
            df = df.with_columns(pl.arange(1, len(df) + 1).alias("_row_num"))
            ctx.set_dataframe(list_type, df)
            load_results.append((list_type, len(df), None))
            logger.info(f"  [{list_type}] 加载成功，{len(df)} 行")

            # 文件加载完成，更新进度
            if progress_callback:
                progress_callback(f"F1_{list_type}", int((i + 1) / total_files * F1_WEIGHT))

        except (DataQualityError, OSError, PermissionError) as e:
            logger.error(f"  [{list_type}] 加载失败: {e}")
            load_results.append((list_type, 0, str(e)))
            raise DataQualityError(f"[{list_type}] 文件加载失败: {e}") from e

    # 生成合并的加载结果消息
    total_rows = sum(r[1] for r in load_results if r[2] is None)
    detail_parts = [f"{r[0]} {r[1]} 行" for r in load_results if r[2] is None]
    detail_msg = "，".join(detail_parts)
    ctx.record_module_result(
        module="F1",
        success_count=total_rows,
        fail_count=sum(1 for r in load_results if r[2] is not None),
        message=f"[F1] 成功: {detail_msg}" if detail_parts else f"[F1] 失败: {load_results[0][2]}",
    )

    # 记录去重字段（供 F3/F6 使用）
    if dedup_field:
        ctx.dedup_field = dedup_field
        logger.info(f"去重字段: {dedup_field}")

    return ctx


def _load_single_file(
    file_path: str,
    list_type: str,
    pre_selected_sheet: Optional[str] = None,
) -> pl.DataFrame:
    """
    加载单个 Excel/CSV 文件，处理多Sheet逻辑（DEV-05）。

    [20260420-老谈] ISSUE-09: 重构为预选模式
      原：传递 callback 函数由后台线程调用
      新：传递预选 Sheet 名称（UI 主线程已选择完毕）

    Parameters
    ----------
    file_path : str
    list_type : str
    pre_selected_sheet : str, optional
        UI层预选的Sheet名称

    Returns
    -------
    pl.DataFrame
        含 `_来源` 列
    """
    path = Path(file_path)
    if not path.exists():
        raise ValidationError(f"文件不存在: {file_path}")

    # [20260420-老谈] ISSUE-10: 支持 CSV 文件加载
    # [20260420-老谈] ISSUE-23: 添加编码自动识别（UTF-8/GBK/GB2312）
    if str(file_path).lower().endswith('.csv'):
        # 编码探测
        encoding = _detect_csv_encoding(file_path)
        df = pl.read_csv(file_path, encoding=encoding)
        logger.info(f"  [{list_type}] CSV文件已读取（encoding: {encoding}）")
        if df.is_empty():
            raise DataQualityError(f"[{list_type}] 文件为空: {file_path}")
        # 添加来源列
        df = df.with_columns(pl.lit(list_type).alias("_来源"))
        return df

    # Excel 文件处理
    # [优化 F1] 简化逻辑：UI层已限制不会出现多Sheet，直接用 polars 读取
    # polars 默认读取第一个 sheet，无需 openpyxl
    if pre_selected_sheet:
        logger.info(f"  [{list_type}] 使用预选Sheet: {pre_selected_sheet}")
        df = pl.read_excel(file_path, sheet_name=pre_selected_sheet)
    else:
        # UI层已限制不会出现多Sheet，这里直接读取第一个sheet
        logger.info(f"  [{list_type}] 读取Excel文件（默认第一个Sheet）")
        df = pl.read_excel(file_path)

    if df.is_empty():
        raise DataQualityError(f"[{list_type}] 文件为空: {file_path}")

    # 添加来源列
    df = df.with_columns(pl.lit(list_type).alias("_来源"))

    logger.debug(f"  [{list_type}] 读取完成，Shape: {df.shape}, 列: {df.columns}")
    return df


class FileLoaderModule(BaseModule):
    """
    F1 文件加载模块（BaseModule 封装）。

    负责加载三类名单文件（一线/三方/HW），添加 _来源 列，
    将结果写入 ProcessContext.dataframes。
    """

    def get_module_name(self) -> str:
        return "F1"

    def validate_input(self, context: ProcessContext) -> tuple[bool, str]:
        """校验：至少有一份名单文件路径"""
        yixian = context.get_input_file("yixian")
        if not yixian:
            return False, "[F1] 跳过: 请选择一线人员名单文件"
        if not Path(yixian).exists():
            return False, f"[F1] 跳过: 一线人员名单文件不存在: {yixian}"
        return True, ""

    def execute(self, context: ProcessContext) -> ProcessContext:
        """执行文件加载 + 字段规范 + 数据字典 + 去重字段识别"""
        # ── Step 1：加载三类名单 Excel → DataFrame ──
        # 直接使用内部键：yixian, sanfang, hw
        file_paths = {}
        for internal_key in ["yixian", "sanfang", "hw"]:
            path = context.get_input_file(internal_key)
            if path:
                file_paths[internal_key] = path

        # [20260420-老谈] ISSUE-09: 获取预选的 Sheet 名称（UI已统一使用内部键）
        pre_selected_sheets = getattr(self, '_pre_selected_sheets', {})
        # 直接使用内部键，无需映射转换
        sheet_selections = {k: v for k, v in pre_selected_sheets.items() if v}

        dedup_field = getattr(context, 'dedup_field', None)
        # 获取 orchestrator 传递的进度回调
        progress_callback = getattr(self, '_progress_callback', None)
        context = load_files(   # ← BUG-18 修复：返回值必须回写 context
            ctx=context,
            file_paths=file_paths,
            sheet_selections=sheet_selections,  # [20260420-老谈] ISSUE-09: 传递预选Sheet
            dedup_field=dedup_field,
            progress_callback=progress_callback,
        )
        logger.info(f"[F1] 文件加载完成，dataframes keys: {list(context.dataframes.keys())}")

        # ── Step 2：加载字段规范（field_spec）──
        # PRD F1-05：支持上传 xlsx「属性导入模版」，工具自动解析覆盖 config/field_spec.yaml
        spec_path = getattr(context, 'spec_file_path', None) or self._default_spec_path()
        if spec_path:
            try:
                from infra.spec_loader import load_field_spec
                # 判断文件格式：xlsx 需要先转换，yaml 直接加载
                if spec_path.lower().endswith(('.xlsx', '.xls')):
                    converted_path = self._convert_xlsx_to_yaml(spec_path)
                    if converted_path:
                        spec_path = converted_path
                    else:
                        logger.warning(f"[F1] xlsx 字段规范转换失败（F2/F4 将跳过）")
                        return context
                context.field_spec = load_field_spec(spec_path)
                logger.info(f"[F1] 字段规范已加载: {spec_path}")
            except (yaml.YAMLError, OSError, PermissionError) as e:
                import traceback
                logger.warning(f"[F1] 字段规范加载失败（F2/F4 将跳过）: {e}")
                logger.debug(f"[F1] 详细错误: {traceback.format_exc()}")

        # ── Step 3：加载数据字典（DictLoader）──
        dict_path = getattr(context, 'dict_file_path', None)
        if dict_path:
            try:
                from infra.dict_loader import DictLoader
                context.dict_loader = DictLoader(dict_path)
                logger.info(f"[F1] 数据字典已加载：{dict_path}，MD5={context.dict_loader.md5_hash}")
            except (DataQualityError, OSError, PermissionError) as e:
                import traceback
                logger.warning(f"[F1] 数据字典加载失败（F4/F5 将跳过）: {e}")
                logger.warning(f"[F1] 请检查字典文件格式是否为「租户字典导入模版」格式")
                logger.debug(f"[F1] 详细错误: {traceback.format_exc()}")

        # ── Step 4：自动识别去重字段（NQ-02 三级策略）──
        if not context.dedup_field:
            yixian_df = context.get_dataframe("yixian")
            if yixian_df is not None and len(yixian_df) > 0:
                detected = self._resolve_dedup_field(yixian_df)
                if detected:
                    context.dedup_field = detected
                    logger.info(f"[F1] 自动识别去重字段: {detected}")
                else:
                    logger.warning("[F1] 未检测到去重字段，F3/F6 内部去重将跳过")

        return context

    @staticmethod
    def _convert_xlsx_to_yaml(xlsx_path: str) -> Optional[str]:
        """
        PRD F1-05：将 CEM「属性导入模版」xlsx 转换为 field_spec.yaml 格式。

        属性导入模版列 → field_spec.yaml 字段映射（PRD §8.1）：
          - 属性名称 → name
          - 属性code → attr_code
          - 属性类型 → category
          - 数据类型 → type
          - 数据子类型 → sub_type
          - 长度上限 → max_length
          - 数据字典 → dict_id
          - 属性值必填 → required
          - 验证规则 → regex

        Returns
        -------
        str | None
            转换后的 YAML 路径，失败返回 None
        """
        import os
        import yaml
        try:
            df = pl.read_excel(xlsx_path)
            if df.is_empty():
                logger.warning(f"[F1] 属性导入模版为空: {xlsx_path}")
                return None

            # 列名映射：CEM 模版列名 → field_spec.yaml 键名
            col_map = {
                "属性名称": "name", "属性code": "attr_code",
                "属性类型": "category", "数据类型": "type",
                "数据子类型": "sub_type", "长度上限": "max_length",
                "数据字典": "dict_id", "属性值必填": "required",
                "验证规则": "regex",
            }
            # 兼容可能的列名变体
            col_map_variants = {
                **col_map,
                "名称": "name", "code": "attr_code",
                "类型": "category", "子类型": "sub_type",
                "长度": "max_length", "字典": "dict_id",
                "必填": "required", "规则": "regex",
            }

            fields = []
            for row_idx in range(len(df)):
                row_dict = dict(df.row(row_idx, named=True))
                field = {}
                has_data = False
                for src_col, target_key in col_map_variants.items():
                    if src_col in row_dict and row_dict[src_col] is not None:
                        val = row_dict[src_col]
                        # 处理布尔/数值转换
                        if target_key in ("required",) and isinstance(val, (int, bool)):
                            val = bool(val)
                        elif target_key == "max_length" and val is not None:
                            try:
                                val = int(float(val))
                            except (ValueError, TypeError):
                                pass
                        field[target_key] = val
                        has_data = True
                    elif target_key in col_map.values():
                        # 原始映射中的字段未找到时设默认值
                        if target_key not in field:
                            if target_key == "max_length":
                                field[target_key] = 0
                            elif target_key == "required":
                                field[target_key] = False

                if has_data and field.get("name"):
                    fields.append(field)

            if not fields:
                logger.warning(f"[F1] 属性导入模版未解析到任何有效字段")
                return None

            # F1-08 预检验：attr_code 重复检测 + 必须有 attr_code
            # [FIX v1.0.6] 重复检测依据从 field_name 改为 field_code（attr_code）
            # 同时将列表转为 spec_loader.py 期望的 dict 格式：
            #   {attr_code: {name, attr_code, type, ...}}
            fields_dict = {}
            fields_by_code = {}  # 用于检测 attr_code 重复
            for f in fields:
                fname = f.get("name", "")
                fcode = f.get("attr_code", "")
                if not fname:
                    continue
                # [FIX v1.0.6] 使用 attr_code 作为唯一标识进行重复检测
                if fcode and fcode in fields_by_code:
                    raise ValidationError(
                        f"F1-08 字段编码重复: '{fcode}'（对应字段名：{fname}、{fields_by_code[fcode]}）。请检查属性导入模版后重新导入。"
                    )
                if fcode:
                    fields_by_code[fcode] = fname
                fields_dict[fname] = f

            # 检查必填字段有 attr_code
            missing_required = [
                fname for fname, fdef in fields_dict.items()
                if fdef.get("required") and not fdef.get("attr_code")
            ]
            if missing_required:
                raise ValidationError(
                    f"F1-08 必填字段缺少 attr_code: {missing_required}。请检查属性导入模版后重新导入。"
                )

            # 写入 config/field_spec.yaml —— 字典格式（兼容 spec_loader.py）
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            yaml_path = os.path.join(base_dir, "config", "field_spec.yaml")
            
            # [FIX #8] 添加备份机制：写入前先备份现有文件
            if os.path.exists(yaml_path):
                backup_path = yaml_path + ".bak"
                with open(yaml_path, "r", encoding="utf-8") as src:
                    with open(backup_path, "w", encoding="utf-8") as dst:
                        dst.write(src.read())
                logger.info(f"[F1] 已备份原有配置: {backup_path}")
            
            with open(yaml_path, "w", encoding="utf-8") as f:
                yaml.dump({"fields": fields_dict}, f, allow_unicode=True, default_flow_style=False)

            logger.info(
                f"[F1] 属性导入模版已转换为 field_spec.yaml，"
                f"共 {len(fields_dict)} 个字段，来源: {xlsx_path}"
            )
            return yaml_path

        except (OSError, yaml.YAMLError, ValueError) as e:
            logger.error(f"[F1] xlsx→yaml 转换失败: {e}")
            return None

    @staticmethod
    def _default_spec_path() -> Optional[str]:
        """获取默认字段规范路径"""
        import os
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        default = os.path.join(base_dir, "config", "field_spec.yaml")
        return default if os.path.exists(default) else None

    @staticmethod
    def _resolve_dedup_field(yixian_df: pl.DataFrame) -> Optional[str]:
        """
        NQ-02 三级去重字段识别策略：
          ① 读取 app_config.yaml 中 deduplication.yixian 默认值
          ② 列名关键字模糊匹配（email/邮箱/mail）
        """
        # 第①级：配置文件默认值
        try:
            from infra.app_config_loader import load_app_config
            cfg = load_app_config()
            if cfg:
                defaults = cfg.get("deduplication", {}).get("yixian", [])
                for candidate in defaults:
                    if candidate in yixian_df.columns:
                        logger.info(f"[F1] 去重字段来自配置（第①级）：{candidate}")
                        return candidate
        except Exception as e:
            logger.debug(f"[F1] 配置文件去重字段读取失败，降级到关键字匹配：{e}")

        # 第②级：关键字模糊匹配
        keywords = ["email", "邮箱", "mail", "e-mail", "e_mail"]
        cols_lower = [c.lower() for c in yixian_df.columns]
        for kw in keywords:
            for col, col_lc in zip(yixian_df.columns, cols_lower):
                if kw in col_lc:
                    logger.info(f"[F1] 去重字段关键字匹配（第②级）：{col}")
                    return col

        return None

    def get_progress_weight(self) -> int:
        return 15  # F1 是基础模块，权重稍高
