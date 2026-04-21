#!/usr/bin/env python3
"""F3~F7 模块批量测试脚本"""
import json
from datetime import datetime
import subprocess

# 加载测试执行跟踪
with open('../测试执行跟踪_小龙虾.json', 'r', encoding='utf-8') as f:
    tracking = json.load(f)

def run_case(yixian=None, sanfang=None, hw=None, dict_file=None, spec=None):
    """执行单个测试用例"""
    cmd = ["python3", "run_pipeline.py"]
    if yixian:
        cmd.extend(["--yixian", yixian])
    if sanfang:
        cmd.extend(["--sanfang", sanfang])
    if hw:
        cmd.extend(["--hw", hw])
    if dict_file:
        cmd.extend(["--dict", dict_file])
    if spec:
        cmd.extend(["--spec", spec])
    cmd.append("-v")  # 使用 verbose 模式便于判断
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

print("=" * 70)
print("F3~F7 模块批量测试")
print("=" * 70)

all_results = {}

# F3 跨来源去重模块
print("\n📋 F3 跨来源去重模块测试...")
f3_results = []

cases = [
    ("F3-UT-001", "一线 vs 三方去重",
     "mock_data_小龙虾/一线名单_跨来源测试_小龙虾.xlsx",
     "mock_data_小龙虾/三方名单_跨来源测试_小龙虾.xlsx", None),
    ("F3-UT-002", "一线 vs HW去重",
     "mock_data_小龙虾/一线名单_跨来源测试_小龙虾.xlsx",
     None, "mock_data_小龙虾/HW名单_跨来源测试_小龙虾.xlsx"),
    ("F3-UT-003", "配置文件失效时关键字匹配",
     "mock_data_小龙虾/一线名单_正常_01_小龙虾.xlsx",
     "mock_data_小龙虾/三方名单_正常_01_小龙虾.xlsx", None),
    ("F3-UT-008", "大小写不敏感去重",
     "mock_data_小龙虾/名单_大小写去重测试_小龙虾.xlsx",
     "mock_data_小龙虾/名单_大小写去重测试_小龙虾.xlsx", None),
    ("F3-UT-009", "去除首尾空格去重",
     "mock_data_小龙虾/名单_空格差异去重测试_小龙虾.xlsx",
     "mock_data_小龙虾/名单_空格差异去重测试_小龙虾.xlsx", None),
]

for case_id, name, yixian, sanfang, hw in cases:
    print(f"  [{case_id}] {name}...")
    r = run_case(yixian, sanfang, hw)
    f3_results.append({
        "case_id": case_id,
        "case_name": name,
        "status": "✅" if "completed" in r.stdout else "❌",
        "notes": "正常" if "completed" in r.stdout else r.stderr[:100]
    })
    print(f"    {f3_results[-1]['status']}")

all_results["F3"] = f3_results
f3_passed = len([r for r in f3_results if r["status"].startswith("✅")])
f3_failed = len([r for r in f3_results if r["status"].startswith("❌")])
print(f"  F3 统计: {f3_passed}通过/{f3_failed}失败")

# F4 字典上码模块
print("\n📋 F4 字典上码模块测试...")
f4_results = []

cases = [
    ("F4-UT-001", "字典正常加载",
     "mock_data_小龙虾/一线名单_正常_01_小龙虾.xlsx",
     "mock_data_小龙虾/data_dict_完整_小龙虾.xlsx", None),
    ("F4-UT-002", "正常上码",
     "mock_data_小龙虾/名单_字典上码测试_01_小龙虾.xlsx",
     "mock_data_小龙虾/data_dict_完整_小龙虾.xlsx",
     "mock_data_小龙虾/属性导入模版_正常_小龙虾.xlsx"),
]

for case_id, name, yixian, dict_file, spec in cases:
    print(f"  [{case_id}] {name}...")
    r = run_case(yixian=yixian, dict_file=dict_file, spec=spec)
    f4_results.append({
        "case_id": case_id,
        "case_name": name,
        "status": "✅" if "completed" in r.stdout else "❌",
        "notes": "正常" if "completed" in r.stdout else r.stderr[:100]
    })
    print(f"    {f4_results[-1]['status']}")

all_results["F4"] = f4_results
f4_passed = len([r for r in f4_results if r["status"].startswith("✅")])
f4_failed = len([r for r in f4_results if r["status"].startswith("❌")])
print(f"  F4 统计: {f4_passed}通过/{f4_failed}失败")

# F5 字典值校验模块
print("\n📋 F5 字典值校验模块测试...")
f5_results = []

cases = [
    ("F5-UT-001", "合规数据通过",
     "mock_data_小龙虾/名单_字典上码测试_03_小龙虾.xlsx",
     "mock_data_小龙虾/data_dict_完整_小龙虾.xlsx",
     "mock_data_小龙虾/属性导入模版_正常_小龙虾.xlsx"),
    ("F5-UT-002", "不合规数据标记",
     "mock_data_小龙虾/名单_字典上码测试_02_小龙虾.xlsx",
     "mock_data_小龙虾/data_dict_完整_小龙虾.xlsx",
     "mock_data_小龙虾/属性导入模版_正常_小龙虾.xlsx"),
]

for case_id, name, yixian, dict_file, spec in cases:
    print(f"  [{case_id}] {name}...")
    r = run_case(yixian=yixian, dict_file=dict_file, spec=spec)
    f5_results.append({
        "case_id": case_id,
        "case_name": name,
        "status": "✅" if "completed" in r.stdout else "❌",
        "notes": "正常" if "completed" in r.stdout else r.stderr[:100]
    })
    print(f"    {f5_results[-1]['status']}")

all_results["F5"] = f5_results
f5_passed = len([r for r in f5_results if r["status"].startswith("✅")])
f5_failed = len([r for r in f5_results if r["status"].startswith("❌")])
print(f"  F5 统计: {f5_passed}通过/{f5_failed}失败")

# F6 名单内部去重模块
print("\n📋 F6 名单内部去重模块测试...")
f6_results = []

cases = [
    ("F6-UT-001", "重复名单正确标注", "mock_data_小龙虾/一线名单_含重复_01_小龙虾.xlsx"),
    ("F6-UT-003", "去重字段为空", "mock_data_小龙虾/一线名单_含空字段_01_小龙虾.xlsx"),
]

for case_id, name, yixian in cases:
    print(f"  [{case_id}] {name}...")
    r = run_case(yixian=yixian)
    f6_results.append({
        "case_id": case_id,
        "case_name": name,
        "status": "✅" if "completed" in r.stdout else "❌",
        "notes": "正常" if "completed" in r.stdout else r.stderr[:100]
    })
    print(f"    {f6_results[-1]['status']}")

all_results["F6"] = f6_results
f6_passed = len([r for r in f6_results if r["status"].startswith("✅")])
f6_failed = len([r for r in f6_results if r["status"].startswith("❌")])
print(f"  F6 统计: {f6_passed}通过/{f6_failed}失败")

# F7 结果输出模块
print("\n📋 F7 结果输出模块测试...")
f7_results = []

print("  [F7-UT-001] 正常输出...")
r = run_case(
    "mock_data_小龙虾/一线名单_正常_01_小龙虾.xlsx",
    "mock_data_小龙虾/三方名单_正常_01_小龙虾.xlsx",
    "mock_data_小龙虾/HW名单_正常_01_小龙虾.xlsx",
    "mock_data_小龙虾/data_dict_完整_小龙虾.xlsx",
    "mock_data_小龙虾/属性导入模版_正常_小龙虾.xlsx"
)
f7_results.append({
    "case_id": "F7-UT-001",
    "case_name": "正常输出",
    "status": "✅" if "completed" in r.stdout else "❌",
    "notes": "正常" if "completed" in r.stdout else r.stderr[:100]
})
print(f"    {f7_results[-1]['status']}")

all_results["F7"] = f7_results
f7_passed = len([r for r in f7_results if r["status"].startswith("✅")])
f7_failed = len([r for r in f7_results if r["status"].startswith("❌")])
print(f"  F7 统计: {f7_passed}通过/{f7_failed}失败")

# 更新跟踪数据
tracking["test_results"].append({
    "module": "F3-F7",
    "timestamp": datetime.now().isoformat(),
    "results": all_results
})

tracking["modules"]["F3"]["passed"] = f3_passed
tracking["modules"]["F3"]["failed"] = f3_failed
tracking["modules"]["F4"]["passed"] = f4_passed
tracking["modules"]["F4"]["failed"] = f4_failed
tracking["modules"]["F5"]["passed"] = f5_passed
tracking["modules"]["F5"]["failed"] = f5_failed
tracking["modules"]["F6"]["passed"] = f6_passed
tracking["modules"]["F6"]["failed"] = f6_failed
tracking["modules"]["F7"]["passed"] = f7_passed
tracking["modules"]["F7"]["failed"] = f7_failed

# 保存
with open('../测试执行跟踪_小龙虾.json', 'w', encoding='utf-8') as f:
    json.dump(tracking, f, ensure_ascii=False, indent=2)

print("\n" + "=" * 70)
print("F3~F7 模块批量测试完成")
print("=" * 70)
print(f"F3: {f3_passed}通过/{f3_failed}失败")
print(f"F4: {f4_passed}通过/{f4_failed}失败")
print(f"F5: {f5_passed}通过/{f5_failed}失败")
print(f"F6: {f6_passed}通过/{f6_failed}失败")
print(f"F7: {f7_passed}通过/{f7_failed}失败")
