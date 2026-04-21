"""
业务验收用例真实结果执行脚本

根据业务验收用例-v2.0-20260420.md 中的测试用例，
使用实际工具代码处理测试数据，生成真实结果文件。

执行流程：F1 → F2 → F6 → F4 → F5 → F3 → F7
"""

import os
import sys
import shutil
from datetime import datetime
from pathlib import Path

# 添加代码路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CODE_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, CODE_DIR)

import polars as pl
from core.context import ProcessContext
from modules.f1_loader import FileLoaderModule
from modules.f2_field_validator import FieldValidatorModule
from modules.f3_priority_dedup import PriorityDedupModule
from modules.f4_dict_encoder import DictEncoderModule
from modules.f5_dict_validator import DictValidatorModule
from modules.f6_internal_dedup import InternalDedupModule
from modules.f7_output_exporter import export_results


# 测试数据路径
TEST_DATA_DIR = os.path.join(CODE_DIR, "业务验收", "测试数据")
OUTPUT_DIR = os.path.join(CODE_DIR, "业务验收", "测试结果20260420")


def setup_output_dir():
    """创建输出目录"""
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"输出目录: {OUTPUT_DIR}")


def load_field_spec():
    """加载字段规范"""
    spec_path = os.path.join(TEST_DATA_DIR, "field_spec_验收.xlsx")
    from modules.f1_loader import FileLoaderModule
    yaml_path = FileLoaderModule._convert_xlsx_to_yaml(spec_path)
    if yaml_path:
        from infra.spec_loader import load_field_spec
        return load_field_spec(yaml_path)
    return None


def load_dict():
    """加载数据字典"""
    dict_path = os.path.join(TEST_DATA_DIR, "data_dict_验收.xlsx")
    from infra.dict_loader import DictLoader
    return DictLoader(dict_path)


def create_context(yixian_path=None, sanfang_path=None, hw_path=None, 
                  spec_path=None, dict_path=None, dedup_field="邮箱"):
    """创建处理上下文"""
    ctx = ProcessContext()
    
    if yixian_path:
        ctx.set_input_file("一线", yixian_path)
    if sanfang_path:
        ctx.set_input_file("三方", sanfang_path)
    if hw_path:
        ctx.set_input_file("HW", hw_path)
    
    if spec_path:
        ctx.spec_file_path = spec_path
    if dict_path:
        ctx.dict_file_path = dict_path
    if dedup_field:
        ctx.dedup_field = dedup_field
    
    return ctx


def _get_df_len(df):
    """安全获取DataFrame行数"""
    if df is None:
        return 0
    return len(df)

def run_f1(ctx):
    """执行F1文件加载模块"""
    print("  [F1] 文件加载...")
    module = FileLoaderModule()
    
    # 加载字段规范
    spec_path = os.path.join(TEST_DATA_DIR, "field_spec_验收.xlsx")
    yaml_path = module._convert_xlsx_to_yaml(spec_path)
    if yaml_path:
        from infra.spec_loader import load_field_spec
        ctx.field_spec = load_field_spec(yaml_path)
    
    # 加载数据字典
    dict_path = os.path.join(TEST_DATA_DIR, "data_dict_验收.xlsx")
    if os.path.exists(dict_path):
        from infra.dict_loader import DictLoader
        ctx.dict_loader = DictLoader(dict_path)
    
    # 加载文件 - 使用小写key匹配LIST_TYPE_MAP
    file_paths = {}
    
    for list_type in ["一线", "三方", "HW"]:
        path = ctx.get_input_file(list_type.lower())
        if not path:
            path = ctx.get_input_file(list_type)
        if path:
            # LIST_TYPE_MAP使用小写key
            if list_type.lower() in ["一线", "三方"]:
                key = list_type  # "一线" -> "一线"
            else:
                key = "hw"  # "HW" -> "hw"
            file_paths[key] = path
    
    from modules.f1_loader import load_files
    ctx = load_files(ctx, file_paths, dedup_field=ctx.dedup_field)
    
    # 添加行号列
    for key in ['yixian', 'sanfang', 'hw']:
        df = ctx.get_dataframe(key)
        if df is not None and len(df) > 0:
            df = df.with_columns(pl.arange(1, len(df) + 1).alias("_row_num"))
            ctx.set_dataframe(key, df)
    
    yx_df = ctx.get_dataframe('yixian')
    sf_df = ctx.get_dataframe('sanfang')
    hw_df = ctx.get_dataframe('hw')
    print(f"    一线: {_get_df_len(yx_df)} 行")
    print(f"    三方: {_get_df_len(sf_df)} 行")
    print(f"    HW: {_get_df_len(hw_df)} 行")
    return ctx


def run_f2(ctx):
    """执行F2字段合规性检查"""
    print("  [F2] 字段合规性检查...")
    module = FieldValidatorModule()
    ctx = module.execute(ctx)
    return ctx


def run_f4(ctx):
    """执行F4数据字典上码"""
    print("  [F4] 数据字典上码...")
    module = DictEncoderModule()
    ctx = module.execute(ctx)
    return ctx


def run_f5(ctx):
    """执行F5字典值校验"""
    print("  [F5] 字典值校验...")
    module = DictValidatorModule()
    ctx = module.execute(ctx)
    return ctx


def run_f6(ctx):
    """执行F6名单内部去重"""
    print("  [F6] 名单内部去重...")
    module = InternalDedupModule()
    ctx = module.execute(ctx)
    return ctx


def run_f3(ctx):
    """执行F3跨名单去重"""
    print("  [F3] 跨名单去重...")
    module = PriorityDedupModule()
    ctx = module.execute(ctx)
    return ctx


def export_output(ctx, output_name):
    """导出结果"""
    print(f"  [F7] 导出结果...")
    ctx.start_time = datetime.now()
    ctx.end_time = datetime.now()
    ctx.status = "completed"
    
    # 创建用例输出子目录
    case_output_dir = os.path.join(OUTPUT_DIR, output_name)
    os.makedirs(case_output_dir, exist_ok=True)
    
    # 临时设置output_path
    original_output_path = ctx.output_path
    ctx.output_path = case_output_dir
    
    # 导出结果
    result_paths = export_results(ctx, case_output_dir)
    
    # 恢复output_path
    ctx.output_path = original_output_path
    
    print(f"    导出完成: {output_name}")
    for key, path in result_paths.items():
        print(f"      - {os.path.basename(path)}")
    
    return result_paths


# ============================================================
# BAT-01: 正常全流程处理
# ============================================================
def run_bat01():
    """BAT-01: 正常全流程处理"""
    print("\n" + "=" * 60)
    print("BAT-01: 正常全流程处理")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_正常.xlsx")
    sanfang_path = os.path.join(TEST_DATA_DIR, "三方名单_正常.xlsx")
    hw_path = os.path.join(TEST_DATA_DIR, "HW名单_正常.xlsx")
    
    ctx = create_context(yixian_path, sanfang_path, hw_path)
    ctx = run_f1(ctx)
    ctx = run_f2(ctx)
    ctx = run_f6(ctx)
    ctx = run_f4(ctx)
    ctx = run_f5(ctx)
    ctx = run_f3(ctx)
    
    return export_output(ctx, "BAT-01_正常全流程")


# ============================================================
# BAT-02: 必填字段为空
# ============================================================
def run_bat02():
    """BAT-02: 必填字段为空"""
    print("\n" + "=" * 60)
    print("BAT-02: 必填字段为空")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_必填空.xlsx")
    
    ctx = create_context(yixian_path)
    ctx = run_f1(ctx)
    ctx = run_f2(ctx)
    
    return export_output(ctx, "BAT-02_必填空")


# ============================================================
# BAT-03: 邮箱格式非法
# ============================================================
def run_bat03():
    """BAT-03: 邮箱格式非法"""
    print("\n" + "=" * 60)
    print("BAT-03: 邮箱格式非法")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_邮箱非法.xlsx")
    
    ctx = create_context(yixian_path)
    ctx = run_f1(ctx)
    ctx = run_f2(ctx)
    
    return export_output(ctx, "BAT-03_邮箱非法")


# ============================================================
# BAT-04: 字典上码正常匹配
# ============================================================
def run_bat04():
    """BAT-04: 字典上码正常匹配"""
    print("\n" + "=" * 60)
    print("BAT-04: 字典上码正常匹配")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_字典正常.xlsx")
    
    ctx = create_context(yixian_path)
    ctx = run_f1(ctx)
    ctx = run_f4(ctx)
    
    return export_output(ctx, "BAT-04_字典正常")


# ============================================================
# BAT-05: 字典上码存在未匹配值
# ============================================================
def run_bat05():
    """BAT-05: 字典上码存在未匹配值"""
    print("\n" + "=" * 60)
    print("BAT-05: 字典上码存在未匹配值")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_字典非法值.xlsx")
    
    ctx = create_context(yixian_path)
    ctx = run_f1(ctx)
    ctx = run_f4(ctx)
    ctx = run_f5(ctx)
    
    return export_output(ctx, "BAT-05_字典非法值")


# ============================================================
# BAT-06: 名单内部重复检查
# ============================================================
def run_bat06():
    """BAT-06: 名单内部重复检查"""
    print("\n" + "=" * 60)
    print("BAT-06: 名单内部重复检查")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_内部重复.xlsx")
    
    ctx = create_context(yixian_path)
    ctx = run_f1(ctx)
    ctx = run_f6(ctx)
    
    return export_output(ctx, "BAT-06_内部重复")


# ============================================================
# BAT-07: 空值不参与重复检查
# ============================================================
def run_bat07():
    """BAT-07: 空值不参与重复检查"""
    print("\n" + "=" * 60)
    print("BAT-07: 空值不参与重复检查")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_邮箱空值.xlsx")
    
    ctx = create_context(yixian_path)
    ctx = run_f1(ctx)
    ctx = run_f6(ctx)  # 只执行F6，不执行F2
    
    return export_output(ctx, "BAT-07_空值不参与重复")


# ============================================================
# BAT-08: 跨名单去重-三方标注
# ============================================================
def run_bat08():
    """BAT-08: 跨名单去重-三方标注"""
    print("\n" + "=" * 60)
    print("BAT-08: 跨名单去重-三方标注")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_正常.xlsx")
    sanfang_path = os.path.join(TEST_DATA_DIR, "三方名单_重复.xlsx")
    
    ctx = create_context(yixian_path, sanfang_path)
    ctx = run_f1(ctx)
    ctx = run_f3(ctx)
    
    return export_output(ctx, "BAT-08_三方标注")


# ============================================================
# BAT-09: 跨名单去重-HW双标注
# ============================================================
def run_bat09():
    """BAT-09: 跨名单去重-HW双标注"""
    print("\n" + "=" * 60)
    print("BAT-09: 跨名单去重-HW双标注")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_正常.xlsx")
    sanfang_path = os.path.join(TEST_DATA_DIR, "三方名单_正常.xlsx")
    hw_path = os.path.join(TEST_DATA_DIR, "HW名单_重复.xlsx")
    
    ctx = create_context(yixian_path, sanfang_path, hw_path)
    ctx = run_f1(ctx)
    ctx = run_f3(ctx)
    
    return export_output(ctx, "BAT-09_HW双标注")


# ============================================================
# BAT-10: 去重大小写不敏感
# ============================================================
def run_bat10():
    """BAT-10: 去重大小写不敏感"""
    print("\n" + "=" * 60)
    print("BAT-10: 去重大小写不敏感")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_正常.xlsx")
    sanfang_path = os.path.join(TEST_DATA_DIR, "三方名单_大小写.xlsx")
    
    ctx = create_context(yixian_path, sanfang_path)
    ctx = run_f1(ctx)
    ctx = run_f3(ctx)
    
    return export_output(ctx, "BAT-10_大小写不敏感")


# ============================================================
# BAT-11: 混合场景
# ============================================================
def run_bat11():
    """BAT-11: 混合场景"""
    print("\n" + "=" * 60)
    print("BAT-11: 混合场景")
    print("=" * 60)
    
    yixian_path = os.path.join(TEST_DATA_DIR, "一线名单_混合.xlsx")
    
    ctx = create_context(yixian_path)
    ctx = run_f1(ctx)
    ctx = run_f2(ctx)
    ctx = run_f6(ctx)
    ctx = run_f4(ctx)
    ctx = run_f5(ctx)
    
    return export_output(ctx, "BAT-11_混合场景")


# ============================================================
# BAT-12: 一线名单输出结构完整性
# ============================================================
def run_bat12():
    """BAT-12: 使用BAT-11结果验证结构完整性"""
    print("\n" + "=" * 60)
    print("BAT-12: 一线名单输出结构完整性")
    print("=" * 60)
    
    # BAT-12使用BAT-11的结果，只需验证结构
    bat11_dir = os.path.join(OUTPUT_DIR, "BAT-11_混合场景")
    if os.path.exists(bat11_dir):
        print(f"  BAT-12验证: 使用BAT-11结果目录 {bat11_dir}")
        # 列出BAT-11生成的文件供验证
        for f in os.listdir(bat11_dir):
            print(f"    - {f}")
        return bat11_dir
    else:
        print("  BAT-11结果不存在，请先运行BAT-11")
        return None


# ============================================================
# BAT-13: 三方/HW输出结构完整性
# ============================================================
def run_bat13():
    """BAT-13: 使用BAT-01结果验证结构完整性"""
    print("\n" + "=" * 60)
    print("BAT-13: 三方/HW输出结构完整性")
    print("=" * 60)
    
    # BAT-13使用BAT-01的结果，只需验证结构
    bat01_dir = os.path.join(OUTPUT_DIR, "BAT-01_正常全流程")
    if os.path.exists(bat01_dir):
        print(f"  BAT-13验证: 使用BAT-01结果目录 {bat01_dir}")
        # 列出BAT-01生成的文件供验证
        for f in os.listdir(bat01_dir):
            print(f"    - {f}")
        return bat01_dir
    else:
        print("  BAT-01结果不存在，请先运行BAT-01")
        return None


# ============================================================
# 主函数
# ============================================================
def main():
    print("=" * 60)
    print("业务验收用例执行 - 真实结果生成")
    print("=" * 60)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    setup_output_dir()
    
    try:
        # 按顺序执行所有用例
        run_bat01()  # 正常全流程
        run_bat02()  # 必填空
        run_bat03()  # 邮箱非法
        run_bat04()  # 字典正常
        run_bat05()  # 字典非法值
        run_bat06()  # 内部重复
        run_bat07()  # 空值不参与重复
        run_bat08()  # 三方标注
        run_bat09()  # HW双标注
        run_bat10()  # 大小写不敏感
        run_bat11()  # 混合场景
        
        # 结构验证用例（使用之前的结果）
        run_bat12()  # BAT-12使用BAT-11结果
        run_bat13()  # BAT-13使用BAT-01结果
        
        print("\n" + "=" * 60)
        print("全部用例执行完成!")
        print(f"输出目录: {OUTPUT_DIR}")
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n执行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
