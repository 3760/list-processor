"""
F7: 结果输出模块

功能覆盖：
- F7-01: 输出文件生成（多Sheet Excel：处理摘要 + 各名单数据）
- F7-02: 合规检查结果 Sheet（问题数据不参与后续处理，单独输出）
- F7-03: 重复名单结果 Sheet
- F7-04: 结果提示（三种场景，PRD NQ-09）
- F7-05 ~ F7-07: 字典版本记录、输出路径、事务回滚

[PERF] 20260422: 使用 xlsxwriter 替代 openpyxl 提升写入性能
- xlsxwriter 采用批量缓冲机制，非真正的逐 cell 写入
- 内部内存缓冲，最后一次性 flush 到磁盘
- 写入速度比 openpyxl 快 2-10 倍

PRD NQ-09 结果提示规则：
  ✅ 无问题 → "处理完成，结果可直接编辑后，上传 CEM 系统"（绿色）
  ⚠️ 有警告 → "处理完成，但发现 X 条问题，建议修复后再上传 CEM 系统"（黄色）
  ❌ 失败   → "处理失败：{错误信息}"（红色）
"""

import polars as pl
from pathlib import Path
from typing import Dict
from datetime import datetime

# [PERF] 使用 xlsxwriter 替代 openpyxl，提升写入性能
import xlsxwriter

from core.context import ProcessContext
from infra.log_manager import get_logger
from core.base_module import BaseModule

logger = get_logger(__name__)


# ── [HELPER] Format 工厂函数，避免重复创建 ────────────────────────────


def _create_header_format(wb, bg_color: str = '#D9D9D9') -> "xlsxwriter.Format":
    """[PERF] 统一创建表头格式对象"""
    return wb.add_format({
        'bold': True,
        'bg_color': bg_color,
        'align': 'left',
    })


def _create_cell_format(wb, bg_color: str = None) -> "xlsxwriter.Format":
    """[PERF] 统一创建单元格格式对象"""
    fmt_dict = {'align': 'left'}
    if bg_color:
        fmt_dict['bg_color'] = bg_color
    return wb.add_format(fmt_dict)


def export_results(ctx: ProcessContext, output_path: str, progress_callback=None) -> Dict[str, str]:
    """
    将处理结果输出为 3 个独立 Excel 文件（PRD 附录 B 对齐）。

    [20260420-老谈] ISSUE-03+04 重构：
      原：单个多 Sheet xlsx（一线/三方/HW 各一个 Sheet）
      新：3 个独立 xlsx 文件，每个含该名单的处理摘要 + 数据 + 错误 + 去重

    输出文件命名（PRD 附录 B + ISSUE-04 原文件名前缀）：
      - 一线：{原文件名}_一线名单_处理结果_{时间戳}.xlsx  （5 个 Sheet）
      - 三方：{原文件名}_三方系统名单_处理结果_{时间戳}.xlsx （1 个 Sheet）
      - HW：  {原文件名}_HW系统名单_处理结果_{时间戳}.xlsx   （2 个 Sheet）

    [20260420-老谈] 优化：输出到日期时间子文件夹，便于批次管理
    [方案C] 支持 progress_callback 用于子任务进度报告

    Parameters
    ----------
    ctx : ProcessContext
        包含 dataframes / error_records / module_results / summary
    output_path : str
        输出目录路径
    progress_callback : callable, optional
        进度回调函数，签名为 (percent: int) -> None

    Returns
    -------
    Dict[str, str]
        各名单的输出文件路径映射 {"yixian": "...", "sanfang": "...", "hw": "..."}
    """
    logger.info(f"[F7] ▶ 开始导出结果，输出目录：{output_path}")
    
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # [FIX] 创建日期时间子文件夹，便于批次管理
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_dir = output_dir / f"批次_{timestamp}"
    batch_dir.mkdir(parents=True, exist_ok=True)
    
    # 更新上下文中的输出路径为批次文件夹
    ctx.output_path = str(batch_dir)
    
    # [LOG] 记录批次目录创建信息
    logger.debug(f"[F7]   → 批次目录：{batch_dir}")
    
    # 提取原始文件名（不含扩展名）作为前缀 [ISSUE-04]
    yixian_input = ctx.get_input_file("yixian") or ""
    original_filename = Path(yixian_input).stem if yixian_input else "客户名单"
    
    # ── 各名单的输出配置 ──────────────────────────────────────
    # [FIX PRD 附录B] Sheet名称对齐PRD：
    # 一线：处理摘要、原始数据（PRD要求）、合规性检查结果、字典校验结果、重复名单结果
    source_config = {
        "yixian": {
            "label": "一线",
            "file_suffix": "一线名单_处理结果",
            "sheets": ["处理摘要", "原始数据", "合规性检查结果", "字典校验结果"],
        },
        "sanfang": {
            "label": "三方",
            "file_suffix": "三方系统名单_处理结果",
            "sheets": ["处理摘要", "原始数据"],
        },
        "hw": {
            "label": "HW",
            "file_suffix": "HW系统名单_处理结果",
            "sheets": ["处理摘要", "原始数据"],
        },
    }
    
    output_paths: Dict[str, str] = {}

    # [新 20260424] 预先统计所有来源数据总行数，用于进度计算
    total_data_rows = 0
    for source_key in ["yixian", "sanfang", "hw"]:
        df = ctx.get_dataframe(source_key)
        if df is not None:
            total_data_rows += len(df)

    # [新 20260424-修正] 额外统计错误和字典总行数，用于各自的进度计算
    total_error_rows = 0
    for source_key in ["yixian", "sanfang", "hw"]:
        err = ctx.error_records.get(source_key)
        if err is not None:
            total_error_rows += len(err)

    total_dict_rows = 0
    dict_err = ctx.error_records.get("dict_validation")
    if dict_err is not None:
        total_dict_rows = len(dict_err)

    # [新 20260424-修正] 计算所有写入的总行数，用于全局进度计算
    total_all_rows = total_data_rows + total_error_rows + total_dict_rows

    # [新] 计算更新间隔（总行数的1%，最小100行）
    update_interval = max(100, total_all_rows // 100) if total_all_rows > 0 else 100

    # [新] 全局写入计数器（字典用于引用传递）
    written_rows = {"data": 0, "error": 0, "dict": 0}

    for source_key, config in source_config.items():
        df = ctx.get_dataframe(source_key)
        error_df = ctx.error_records.get(source_key)

        # [20260424-老谈] 修复：有错误数据时，即使 df 为空也生成文件
        has_main_data = df is not None and len(df) > 0
        has_error_data = error_df is not None and len(error_df) > 0

        if not has_main_data and not has_error_data:
            logger.info(f"[F7] {config['label']} 无数据，跳过生成")
            continue
        
        # 构建文件名：{原文件名}_{名单类型}_处理结果_{时间戳}.xlsx
        filename = f"{original_filename}_{config['file_suffix']}_{timestamp}.xlsx"
        file_path = str(batch_dir / filename)
        
        # [PERF] 使用 xlsxwriter 创建工作簿（高效批量缓冲写入）
        # [PERF] 20260423: constant_memory=True 逐行写入，减少内存占用
        # [FIX] 异常安全：确保 Workbook 正确关闭
        logger.info(f"[F7] 开始创建 {config['label']} 结果文件：{filename}")
        wb = None
        try:
            wb = xlsxwriter.Workbook(file_path, options={'constant_memory': True})
            # 写入各 Sheet
            # [新 20260424] 传递总行数、写入计数器、更新间隔用于进度计算
            _write_single_source_sheets(
                wb, ctx, source_key, config["label"], 
                progress_callback, total_all_rows, written_rows, update_interval
            )
            # [PERF] close() 会将缓冲数据一次性写入磁盘
            wb.close()
            output_paths[source_key] = file_path
            logger.info(f"[F7] ✅ {config['label']} 结果已输出至：{file_path}")
        except (OSError, IOError) as e:
            if wb is not None:
                wb.close()  # 确保文件被关闭
            logger.error(f"[F7] ❌ {config['label']} 写入失败：{e}")
            raise  # 重新抛出异常，让调用者知道发生了错误
    
    # [LOG] 记录导出完成统计
    logger.info(f"[F7] ▶ 导出完成，共生成 {len(output_paths)} 个文件")
    for source_key, file_path in output_paths.items():
        logger.debug(f"[F7]   → {source_key}: {file_path}")
    
    # [新 20260424-修正] 所有文件写完后，通知进度到100%
    if progress_callback:
        progress_callback("F7", 100)
    
    return output_paths


def _write_single_source_sheets(
    wb, ctx: ProcessContext, source_key: str, label: str, 
    progress_callback=None, total_all_rows=0, written_rows=None, update_interval=1000
) -> None:
    """
    为单一名单写入所有 Sheet（使用 xlsxwriter 高效写入）。

    [20260420-老谈] ISSUE-03: 从原来的统一多 Sheet 改为按名单拆分
    [PERF] 20260422: 使用 xlsxwriter 替代 openpyxl
    [方案C] 支持 progress_callback 用于子任务进度报告
    [20260424-老谈] 修复：当 df 为空但有错误数据时，使用错误记录生成"原始数据"Sheet
    [20260424-老谈] 新增：按全局累计行数动态更新进度（20%~100%），避免跳变
    """
    # [新] 初始化写入计数器引用
    if written_rows is None:
        written_rows = {"data": 0, "error": 0, "dict": 0}
    df = ctx.get_dataframe(source_key)
    error_df = ctx.error_records.get(source_key)
    row_count = len(df) if df is not None else 0
    error_count = len(error_df) if error_df is not None else 0

    # [LOG] 记录开始写入该名单的所有 Sheet
    logger.info(f"[F7] ▶ 开始写入 {label} 的各 Sheet，数据行数：{row_count}，错误行数：{error_count}")

    # 1. 处理摘要 Sheet（摘要不计入进度，只记录日志）
    _write_summary_sheet_for_source(wb, ctx, source_key, label, df)
    logger.debug(f"[F7]   ✓ 处理摘要 Sheet 完成")

    # 2. 原始数据 Sheet（全局进度：20% + 累计行数占比）
    # [20260424-老谈] 修复：df 为空但有错误时，不生成"原始数据"Sheet，只保留"合规性检查结果"
    if df is not None and len(df) > 0:
        # [新] 传递 total_all_rows 用于全局进度计算
        _write_data_sheet(wb, "原始数据", df, progress_callback, total_all_rows, written_rows, update_interval)
        logger.debug(f"[F7]   ✓ 原始数据 Sheet 完成，{row_count} 行")
    else:
        logger.debug(f"[F7]   - 原始数据 Sheet 无数据（主数据为空），跳过")

    # 3. 合规性检查结果 Sheet（继续累计进度）
    _write_error_records_sheet_for_source(wb, ctx, source_key, progress_callback, total_all_rows, written_rows, update_interval)
    logger.debug(f"[F7]   ✓ 合规性检查 Sheet 完成")

    # 4. 字典校验结果 Sheet（如有）—— 仅一线有此 Sheet
    dict_err = ctx.error_records.get("dict_validation")
    if source_key == "yixian" and dict_err is not None and len(dict_err) > 0:
        _write_dict_validation_sheet(wb, dict_err, progress_callback, total_all_rows, written_rows, update_interval)
        logger.debug(f"[F7]   ✓ 字典校验结果 Sheet 完成，{len(dict_err)} 行")

    # [问题2-Sheet合并] 移除单独的重复名单结果 Sheet
    # 重复数据已合并到"原始数据" Sheet中，包含以下新增列：
    # - _重复键值、_出现次数、_内部去重结果
    # 原代码已注释保留，便于后续需要单独Sheet时恢复
    # if source_key == "yixian":
    #     _write_repeat_records_sheet_for_source(wb, ctx)
    # [删除 20260424-修正] 不再在此回调100%，由 export_results 统一管理

    logger.info(f"[F7] ▶ {label} 的所有 Sheet 写入完成")


# ── 以下为各 Sheet 写入函数（保持原有逻辑，适配单源模式）──────


def _get_display_status(ctx: ProcessContext) -> str:
    """
    根据处理上下文获取显示用的处理状态。
    
    [20260420-老谈] 优化2.1：修复输出文件中处理状态始终为 running 的问题。
    状态判断逻辑：
    - 如果 ctx.status 已经是 completed/failed，直接使用
    - 否则根据错误记录判断
    
    Returns
    -------
    str
        处理状态：completed / failed / running
    """
    # 如果状态已明确设置，直接返回
    if ctx.status in ("completed", "failed"):
        return ctx.status
    
    # 否则根据错误记录判断
    total_failed = 0
    for key in ["yixian", "sanfang", "hw"]:
        err = ctx.error_records.get(key)
        if err is not None and len(err) > 0:
            total_failed += len(err)
    
    # 字典校验错误
    dict_err = ctx.error_records.get("dict_validation")
    if dict_err is not None and len(dict_err) > 0:
        total_failed += len(dict_err)
    
    return "completed" if total_failed == 0 else "failed"


def _write_summary_sheet_for_source(
    wb, ctx: ProcessContext, source_key: str, label: str, df: pl.DataFrame
) -> None:
    """
    [20260420-老谈] ISSUE-16: 增强处理摘要 Sheet - 补充更多统计项
    [PERF] 20260422: 使用 xlsxwriter 写入
    
    Parameters
    ----------
    wb : xlsxwriter.Workbook
        Excel 工作簿对象
    ctx : ProcessContext
        处理上下文
    source_key : str
        数据源标识
    label : str
        显示用标签
    df : pl.DataFrame
        数据 DataFrame
    """
    ws = wb.add_worksheet("处理摘要")
    
    # [FIX] None 检查（Polars DataFrame 不支持 or 运算符）
    if df is None:
        df = pl.DataFrame()
    
    # [20260420-老谈] 优化2.1：使用修复后的状态获取函数
    display_status = _get_display_status(ctx)
    status_display_map = {
        "completed": "已完成",
        "failed": "失败",
        "running": "处理中",
    }
    
    rows = [
        ["统计项", "值"],
        ["名单类型", label],
        ["处理状态", status_display_map.get(display_status, display_status)],
    ]
    
    # [ISSUE-16] 处理耗时
    if ctx.start_time and ctx.end_time:
        try:
            start = ctx.start_time
            end = ctx.end_time
            if isinstance(start, str):
                start = datetime.fromisoformat(start)
            if isinstance(end, str):
                end = datetime.fromisoformat(end)
            elapsed = (end - start).total_seconds()
            rows.append(["处理耗时(秒)", f"{elapsed:.2f}"])
        except (ValueError, TypeError, AttributeError):  # [FIX] 限定具体异常类型
            pass
    
    # [FIX PRD F7-07] 处理摘要需包含原始数据量
    original_count = len(df)
    if hasattr(ctx, 'original_row_counts') and source_key in ctx.original_row_counts:
        original_count = ctx.original_row_counts.get(source_key, len(df))

    rows.extend([
        ["开始时间", str(ctx.start_time)],
        ["结束时间", str(ctx.end_time or "")],
        [f"{label} 原始记录数", original_count],
        [f"{label} 处理后记录数", len(df)],
    ])
    
    # 模块中文名称映射
    MODULE_NAMES_CN = {
        "F1": "文件加载",
        "F2": "字段合规检查",
        "F3": "跨名单去重",
        "F4": "数据字典上码",
        "F5": "字典值校验",
        "F6": "名单内部去重",
        "F7": "结果输出",
    }
    
    # [ISSUE-16] 各模块执行结果汇总
    for module in ["F1", "F2", "F3", "F4", "F5", "F6"]:
        mod_result = ctx.module_results.get(module, {})
        success = mod_result.get("success", 0)
        fail = mod_result.get("fail", 0)
        skip = mod_result.get("skip", 0)
        if success or fail or skip:
            module_name = MODULE_NAMES_CN.get(module, module)
            rows.append([f"{module_name} 成功数", success])
            rows.append([f"{module_name} 失败数", fail])
            if skip:
                rows.append([f"{module_name} 跳过数", skip])
    
    # [ISSUE-16] 错误类型分布
    err = ctx.error_records.get(source_key)
    if err is not None and len(err) > 0:
        from collections import Counter
        error_types = [row.get("问题类型", "未知") for row in err.iter_rows(named=True)]
        type_counts = Counter(error_types)
        rows.append(["错误类型分布", ""])
        for err_type, count in sorted(type_counts.items()):
            rows.append(["  " + err_type, count])
        logger.debug(f"[F7]   → {label} 错误类型分布：{dict(type_counts)}")
    
    # [ISSUE-16] 字典信息
    if hasattr(ctx, 'dict_loader') and ctx.dict_loader is not None:
        rows.append(["字典版本", getattr(ctx.dict_loader, 'md5_hash', '未知') or '未知'])
        rows.append(["字典文件", getattr(ctx.dict_loader, 'file_path', '未知') or '未知'])
    
    # 去重信息
    if ctx.dedup_field:
        rows.append(["去重字段", ctx.dedup_field])
        f6_result = ctx.module_results.get("F6", {})
        if f6_result:
            rows.append(["去重原始数", f6_result.get("success", 0)])
            rows.append(["去重重复数", f6_result.get("fail", 0)])
            rows.append(["去重未知数（空值）", f6_result.get("skip", 0)])
    
    # [FIX Bug 1] 表头单独写入，数据行从索引1开始
    for col_idx, col_name in enumerate(rows[0]):
        ws.write(0, col_idx, col_name)

    # 数据行
    for row_idx, row_data in enumerate(rows[1:], start=1):
        ws.write_row(row_idx, 0, row_data)
    
    # [LOG] 记录处理摘要写入完成
    logger.debug(f"[F7] 处理摘要 Sheet 写入完成，共 {len(rows)} 行")


def _write_error_records_sheet_for_source(
    wb, ctx: ProcessContext, source_key: str,
    progress_callback=None, total_all_rows=0, written_rows=None, update_interval=1000
) -> None:
    """
    写入合规性检查结果 Sheet（F2 错误数据）—— 仅本来源的。

    [20260420-老谈] ISSUE-06: 从原来的合并所有来源改为按来源独立输出
    [PERF] 20260422: 使用 xlsxwriter 写入
    [新 20260424-修正]: 使用全局累计进度（20%~100%），避免文件间跳变

    Parameters
    ----------
    wb : xlsxwriter.Workbook
        Excel 工作簿对象
    ctx : ProcessContext
        处理上下文
    source_key : str
        数据源标识
    progress_callback : callable, optional
        进度回调函数
    total_all_rows : int
        所有来源（数据+错误+字典）的总行数（用于全局进度计算）
    written_rows : dict, optional
        全局写入计数器（字典引用）
    update_interval : int
        进度更新间隔
    """
    err = ctx.error_records.get(source_key)
    if err is None or len(err) == 0:
        logger.debug(f"[F7]   ✓ 合规性检查结果 Sheet：无错误数据，跳过")
        return

    ws = wb.add_worksheet("合规性检查结果")

    # [PERF] xlsxwriter 高效写入：先写表头，再批量写数据
    columns = list(err.columns)
    row_count = len(err)
    total_cols = len(columns)
    logger.info(f"[F7]   → 合规性检查结果 Sheet：{row_count} 行 × {total_cols} 列")

    # 写入表头（第0行）
    for col_idx, col_name in enumerate(columns):
        ws.write(0, col_idx, col_name)

    # [新 20260424-修正] 批量写入数据行 + 全局动态进度更新
    # 进度公式：20 + (已写总行数/全局总行数) * 80
    for row_num, row_data in enumerate(err.iter_rows(), start=1):
        ws.write_row(row_num, 0, row_data)
        written_rows["error"] += 1

        # 按间隔更新全局进度
        total_written = written_rows["data"] + written_rows["error"] + written_rows["dict"]
        if total_written % update_interval == 0 and progress_callback:
            progress = 20 + (total_written / total_all_rows) * 80 if total_all_rows > 0 else 20
            progress_callback("F7", int(min(progress, 99)))

        if row_num % 10000 == 0:
            logger.info(f"[F7]     已写入 {row_num}/{row_count} 行")

    # 完成后记录日志（进度由全局累计驱动，不单独回调）
    logger.debug(f"[F7]   ✓ 合规性检查结果 Sheet 完成，{len(err)} 行错误数据")


def _apply_generated_column_style(ws, columns: list) -> None:
    """
    为工作表中的新增列添加浅蓝色底色。
    
    [优化] 便于用户快速识别哪些是原始数据列，哪些是系统生成的列。
    [PERF] 20260422: xlsxwriter 版本 - 由于 xlsxwriter 需预先定义格式，
    此函数现在仅用于记录日志，实际格式在写入时已指定。
    """
    # 识别新增列
    generated_cols = [c for c in columns if _is_generated_column(c)]
    
    if generated_cols:
        logger.debug(f"[F7]   → 新增列 {len(generated_cols)} 个，使用浅蓝色标注：{generated_cols}")


def _write_dict_validation_sheet(
    wb, dict_err_df: pl.DataFrame,
    progress_callback=None, total_all_rows=0, written_rows=None, update_interval=1000
) -> None:
    """
    写入字典校验结果 Sheet（F5 错误数据）。

    [20260420-老谈] ISSUE-06+17: 新增独立 Sheet（原与 F2 合并导致覆盖）
    [PERF] 20260422: 使用 xlsxwriter 写入
    [新 20260424-修正]: 使用全局累计进度（20%~100%），避免文件间跳变

    Parameters
    ----------
    wb : xlsxwriter.Workbook
        Excel 工作簿对象
    dict_err_df : pl.DataFrame
        字典校验错误数据
    progress_callback : callable, optional
        进度回调函数
    total_all_rows : int
        所有来源（数据+错误+字典）的总行数（用于全局进度计算）
    written_rows : dict, optional
        全局写入计数器（字典引用）
    update_interval : int
        进度更新间隔
    """
    ws = wb.add_worksheet("字典校验结果")

    columns = list(dict_err_df.columns)
    row_count = len(dict_err_df)
    logger.info(f"[F7]   → 字典校验结果 Sheet：{row_count} 行 × {len(columns)} 列")

    # 写入表头
    for col_idx, col_name in enumerate(columns):
        ws.write(0, col_idx, col_name)

    # [新 20260424-修正] 批量写入数据行 + 全局动态进度更新
    # 进度公式：20 + (已写总行数/全局总行数) * 80
    for row_num, row_data in enumerate(dict_err_df.iter_rows(), start=1):
        ws.write_row(row_num, 0, row_data)
        written_rows["dict"] += 1

        # 按间隔更新全局进度
        total_written = written_rows["data"] + written_rows["error"] + written_rows["dict"]
        if total_written % update_interval == 0 and progress_callback:
            progress = 20 + (total_written / total_all_rows) * 80 if total_all_rows > 0 else 20
            progress_callback("F7", int(min(progress, 99)))

        if row_num % 10000 == 0:
            logger.info(f"[F7]     已写入 {row_num}/{row_count} 行")

    # 完成后记录日志（进度由全局累计驱动，不单独回调）
    logger.debug(f"[F7]   ✓ 字典校验结果 Sheet 完成，{row_count} 行")


def _write_repeat_records_sheet_for_source(wb, ctx: ProcessContext) -> None:
    """
    写入重复名单结果 Sheet（F6 去重标注）。

    [FIX PRD F6-03] 输出格式要求：
      - _重复键值：去重字段的实际值
      - _出现次数：该键值在数据中出现的总次数
      - _内部去重结果："原始"/"重复"/"未知"（空值行）
    
    [优化] 新增列（_开头列、Code列等）使用浅蓝色底色标注。
    [PERF] 20260422: 使用 xlsxwriter 写入
    """
    # 尝试从 module_results.F6.rejected 获取
    repeat_data = None
    f6_result = ctx.module_results.get("F6", {})
    if isinstance(f6_result, dict):
        repeat_data = f6_result.get("rejected")

    # 兜底：从 DataFrame 的去重列获取统计信息
    yixian_df = ctx.get_dataframe("yixian")
    has_dedup = (
        yixian_df is not None
        and "_内部去重结果" in yixian_df.columns
    )

    # [FIX PRD F6-03] 优先输出完整的重复行详情（含新增列）
    if repeat_data is not None and len(repeat_data) > 0:
        ws = wb.add_worksheet("重复名单结果")
        columns = None
        row_idx = 0
        
        logger.debug(f"[F7]   → 重复名单结果 Sheet：{len(repeat_data)} 行（完整详情模式）")

        if isinstance(repeat_data, pl.DataFrame):
            columns = list(repeat_data.columns)
            # 写入表头
            for col_idx, col_name in enumerate(columns):
                ws.write(row_idx, col_idx, col_name)
            row_idx += 1
            # [PERF] 20260424: 逐行写入，兼容 constant_memory 模式
            data_row_count = len(repeat_data)
            logger.info(f"[F7]   → 重复名单结果 Sheet：{data_row_count} 行 × {len(columns)} 列")
            LOG_INTERVAL = 10000  # 每 10000 行打印一次日志
            for row_num, row_data in enumerate(repeat_data.iter_rows(), start=1):
                ws.write_row(row_idx, 0, row_data)
                row_idx += 1
                if row_num % LOG_INTERVAL == 0:
                    logger.info(f"[F7]     已写入 {row_num}/{data_row_count} 行")
        else:
            # 如果是其他格式（如 list of dicts）
            for item in repeat_data:
                if isinstance(item, dict):
                    if columns is None:
                        columns = list(item.keys())
                        for col_idx, col_name in enumerate(columns):
                            ws.write(row_idx, col_idx, col_name)
                        row_idx += 1
                    for col_idx, value in enumerate(item.values()):
                        ws.write_row(row_idx, 0, list(item.values()))
                        row_idx += 1
                elif hasattr(item, '__iter__'):
                    row_list = list(item)
                    if columns is None:
                        columns = [f"列{i+1}" for i in range(len(row_list))]
                        for col_idx, col_name in enumerate(columns):
                            ws.write(row_idx, col_idx, col_name)
                        row_idx += 1
                    ws.write_row(row_idx, 0, row_list)
                    row_idx += 1

        logger.debug(f"[F7]   ✓ 重复名单结果 Sheet 完成，{row_idx - 1} 行数据")
    elif has_dedup:
        # 无 rejected 数据但有去重标记时，写统计摘要
        ws = wb.add_worksheet("重复名单结果")
        dedup_col = yixian_df["_内部去重结果"]
        orig_count = dedup_col.to_list().count("原始") if dedup_col is not None else 0
        unknown_count = dedup_col.to_list().count("未知") if dedup_col is not None else 0
        dup_count = dedup_col.to_list().count("重复") if dedup_col is not None else 0
        
        logger.info(f"[F7]   → 重复名单结果 Sheet：（统计摘要模式）原始={orig_count}, 重复={dup_count}, 未知={unknown_count}")
        
        rows = [
            ["统计项", "值"],
            ["原始数", orig_count],
            ["未知数（空值行）", unknown_count],
            ["重复数", dup_count],
        ]
        
        # xlsxwriter 批量写入
        for row_idx, row_data in enumerate(rows):
            ws.write_row(row_idx, 0, row_data)
        logger.debug(f"[F7]   ✓ 重复名单结果 Sheet 完成（统计摘要）")


# ── [保留] 旧版通用 Sheet 函数（向后兼容）──────────────────────


def _write_summary_sheet(wb, ctx: ProcessContext) -> None:
    """写入处理摘要 Sheet（旧版兼容接口，内部调用新实现）"""
    # 保留此函数以避免外部引用断裂
    df = ctx.get_dataframe("yixian") or pl.DataFrame()
    _write_summary_sheet_for_source(wb, ctx, "yixian", "一线", df)


def _is_generated_column(col_name: str) -> bool:
    """
    判断是否为系统自动生成的列（而非原始数据列）。
    这些列需要用浅蓝色底色标注。
    
    新增列的识别规则：
    - 下划线开头的列：_重复键值, _出现次数, _内部去重结果, _来源, _row_num等
    - 特定后缀：_Code（字典上码列：客户来源_Code, 客户等级_Code）
    - 特定前缀：是否已在一线名单, 是否已在三方名单（跨名单去重标注）
    
    Returns
    -------
    bool
        True 表示是系统生成列，需要特殊样式
    """
    if col_name.startswith("_"):
        return True
    if col_name.endswith("_Code"):
        return True
    if col_name in ["是否已在一线名单", "是否已在三方名单"]:
        return True
    return False


def _write_data_sheet(
    wb, sheet_name: str, df: pl.DataFrame,
    progress_callback=None, total_all_rows=0, written_rows=None, update_interval=1000
) -> None:
    """
    写入名单数据 Sheet。

    [问题2-Sheet合并] 修改：保留所有新增列（_开头列、Code列等），
    合并到"原始数据" Sheet中输出。

    新增列说明：
    - _重复键值：去重字段实际值
    - _出现次数：该值出现总次数
    - _内部去重结果：原始/重复/未知/跳过
    - 是否已在一线名单：跨名单标注
    - 是否已在三方名单：跨名单标注
    - {field}_Code：字典上码结果列

    [PERF] 20260422: 使用 xlsxwriter 写入，批量缓冲高效写入
    [PERF] 20260424: 还原为逐行写入，兼容 constant_memory 模式
    [新 20260424-修正]: 使用全局累计进度（20%~100%），避免文件间跳变

    Parameters
    ----------
    wb : xlsxwriter.Workbook
        Excel 工作簿对象
    sheet_name : str
        Sheet 名称
    df : pl.DataFrame
        数据 DataFrame
    progress_callback : callable, optional
        进度回调函数
    total_all_rows : int
        所有来源（数据+错误+字典）的总行数（用于全局进度计算）
    written_rows : dict, optional
        全局写入计数器（字典引用）
    update_interval : int
        进度更新间隔
    """
    ws = wb.add_worksheet(sheet_name)

    # [问题2-Sheet合并] 保留所有列，包括新增列（_开头、Code列等）
    output_columns = list(df.columns)
    output_df = df

    row_count = len(output_df)
    total_cols = len(output_columns)

    logger.info(f"[F7]   → 开始写入 {sheet_name}，{row_count} 行 × {total_cols} 列")

    # 写入表头（第0行）
    for col_idx, col_name in enumerate(output_columns):
        ws.write(0, col_idx, col_name)

    # [新 20260424-修正] 逐行写入数据 + 全局动态进度更新
    # 进度公式：20 + (已写总行数/全局总行数) * 80
    if row_count > 0:
        for row_idx, row_data in enumerate(output_df.iter_rows(), start=1):
            ws.write_row(row_idx, 0, row_data)
            written_rows["data"] += 1

            # 按间隔更新全局进度
            total_written = written_rows["data"] + written_rows["error"] + written_rows["dict"]
            if total_written % update_interval == 0 and progress_callback:
                progress = 20 + (total_written / total_all_rows) * 80 if total_all_rows > 0 else 20
                progress_callback("F7", int(min(progress, 99)))

            # 每 10000 行打印一次日志
            if row_idx % 10000 == 0:
                logger.info(f"[F7]     已写入 {row_idx}/{row_count} 行")

    logger.info(f"[F7]   ✓ {sheet_name} 写入完成，{row_count} 行 × {total_cols} 列")


def _write_error_as_main_sheet(
    wb, error_df: pl.DataFrame
) -> None:
    """
    [20260424-老谈] 新增：当主数据为空但有错误记录时，使用错误记录生成"原始数据"Sheet。

    将 error_records 转换为可读的表格格式：
    - 行号、字段名、原始值、问题类型、说明

    Parameters
    ----------
    wb : xlsxwriter.Workbook
        Excel 工作簿对象
    error_df : pl.DataFrame
        错误记录 DataFrame（来自 ctx.error_records）
    """
    ws = wb.add_worksheet("原始数据")

    # 定义输出列
    output_columns = ["行号", "字段名", "原始值", "问题类型", "说明"]
    error_count = len(error_df)

    logger.info(f"[F7]   → 开始写入「原始数据」（基于错误记录），{error_count} 行")

    # 写入表头（第0行）
    for col_idx, col_name in enumerate(output_columns):
        ws.write(0, col_idx, col_name)

    # 逐行写入错误记录
    for row_idx, row_data in enumerate(error_df.iter_rows(named=True), start=1):
        ws.write(row_idx, 0, row_data.get("行号", ""))
        ws.write(row_idx, 1, row_data.get("字段名", ""))
        ws.write(row_idx, 2, row_data.get("原始值", ""))
        ws.write(row_idx, 3, row_data.get("问题类型", ""))
        ws.write(row_idx, 4, row_data.get("说明", ""))

    logger.info(f"[F7]   ✓ 「原始数据」（基于错误记录）写入完成，{error_count} 行")


def _write_error_records_sheet(wb, ctx: ProcessContext) -> None:
    """
    旧版兼容接口：写入合并的合规性检查结果。
    
    [20260420-老谈] ISSUE-03: 保留此函数避免外部引用断裂，
    内部委托给新的单源版本
    
    Parameters
    ----------
    wb : xlsxwriter.Workbook
        Excel 工作簿对象
    ctx : ProcessContext
        处理上下文
    """
    # 兼容旧调用方式，写入一线的错误记录
    _write_error_records_sheet_for_source(wb, ctx, "yixian")
    # 同时写入三方和 HW 的
    for key in ["sanfang", "hw"]:
        _write_error_records_sheet_for_source(wb, ctx, key)


def _write_repeat_records_sheet(wb, ctx: ProcessContext) -> None:
    """旧版兼容接口，内部委托给新实现"""
    _write_repeat_records_sheet_for_source(wb, ctx)


def build_result_message(ctx: ProcessContext) -> tuple[str, str]:
    """
    根据处理上下文生成结果提示信息（PRD NQ-09）。
    
    [20260420-老谈] ISSUE-06 修正：
      原逻辑只遍历 error_records["yixian/sanfang/hw"]，
      现需额外统计 dict_validation（F5 独立 key）的错误数

    Returns
    -------
    tuple[str, str]
        (message, level)
        - level: "success" | "warning" | "error"
    """
    # 统计失败总数（error_records + dict_validation）
    total_failed = 0
    
    # 各来源的错误记录
    for key in ["yixian", "sanfang", "hw"]:
        err = ctx.error_records.get(key)
        if err is not None and len(err) > 0:
            total_failed += len(err)
    
    # [20260420-老谈] ISSUE-06: 补充字典校验错误统计（F5 独立 key）
    dict_err = ctx.error_records.get("dict_validation")
    if dict_err is not None and len(dict_err) > 0:
        total_failed += len(dict_err)

    if ctx.status == "failed":
        # ❌ 失败
        last_error = ""
        for mod_result in ctx.module_results.values():
            if isinstance(mod_result, dict) and "message" in mod_result:
                last_error = mod_result["message"]
                break
        message = f"[F7] 失败: {last_error}"
        level = "error"

    elif total_failed > 0:
        # ⚠️ 有警告
        message = (
            f"[F7] 成功: 发现 {total_failed} 条问题，建议修复后再上传 CEM 系统"
        )
        level = "warning"

    else:
        # ✅ 无问题
        message = "[F7] 成功: 处理完成，结果可直接编辑后，上传 CEM 系统"
        level = "success"

    logger.info(f"[F7] 结果提示（{level}）：{message}")
    return message, level


class OutputExporterModule(BaseModule):
    """
    F7 结果输出模块（BaseModule 封装）。

    负责将处理结果输出为多 Sheet Excel 文件，
    包含处理摘要、各名单数据、合规检查结果、重复名单结果。
    """

    def get_module_name(self) -> str:
        return "F7"

    def validate_input(self, context: ProcessContext) -> tuple[bool, str]:
        """校验：ProcessContext 中应有至少一份 DataFrame"""
        has_data = any(
            context.get_dataframe(key) is not None
            for key in ["yixian", "sanfang", "hw"]
        )
        if not has_data:
            return False, "[F7] 跳过: 没有可输出的数据"
        return True, ""

    def execute(self, context: ProcessContext) -> ProcessContext:
        """执行结果输出（[20260420-老谈] ISSUE-03+04: 拆为3个独立文件）"""
        from modules.f7_output_exporter import export_results
        
        logger.info(f"[F7] ▶ 开始执行结果输出模块")

        # 确定输出目录 [ISSUE-03: output_path 改为目录语义]
        output_dir = getattr(context, 'output_path', None)
        if not output_dir:
            # 默认输出到输入文件所在目录
            yixian_path = context.get_input_file("yixian")
            if yixian_path:
                import os
                output_dir = os.path.dirname(yixian_path)
            else:
                output_dir = os.getcwd()
        
        logger.debug(f"[F7]   → 输出目录：{output_dir}")

        # [方案C] 获取进度回调
        progress_callback = getattr(self, '_progress_callback', None)

        # export_results 现在返回 Dict[source_key -> file_path]
        result_paths = export_results(ctx=context, output_path=output_dir, progress_callback=progress_callback)
        
        # 将结果存入 context：主路径指向一线文件（兼容旧逻辑）
        if result_paths:
            context.output_path = result_paths.get("yixian", list(result_paths.values())[0])
            context._all_output_paths = result_paths  # 新增：所有输出文件映射

        # 记录结果消息
        message, level = build_result_message(context)
        context.record_module_result(
            module="F7",
            success_count=len(result_paths),
            fail_count=0,
            message=f" {message}",
        )
        logger.info(f"[F7] 结果已输出至 {len(result_paths)} 个文件")
        return context

    def get_progress_weight(self) -> int:
        return 10
