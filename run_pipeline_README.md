# 后台直测工具使用说明

## 概述

`run_pipeline.py` 是一个**跳过 GUI、直接调用 F1~F7 全流程**的后台测试工具。

**核心思路：逻辑验证与 UI 交互解耦。**

| 方式 | 适用场景 | 特点 |
|------|---------|------|
| **后台直测（本工具）** | 验证业务逻辑正确性、覆盖测试用例 | 无需 GUI、可脚本化、CI/CD 友好 |
| **GUI 操作** | UI 交互验证、端到端验收 | 需要人工操作、依赖显示环境 |

---

## 快速开始

```bash
cd /Users/mars/.../名单处理工具-AI流程/代码

# ① 用默认 mock 数据跑全流程
python run_pipeline.py

# ② 列出所有预置场景
python run_pipeline.py --list-scenarios

# ③ 运行预置场景「正常全流程」
python run_pipeline.py --scenario 正常全流程 --verbose

# ④ 自定义文件路径
python run_pipeline.py \
    --yixian /path/to/一线.xlsx \
    --sanfang /path/to/三方.xlsx \
    --hw /path/to/HW.xlsx \
    --dict /path/to/dict.xlsx \
    --spec /path/to/spec.yaml \
    --output /tmp/output
```

---

## 命令参数

### 文件参数

| 参数 | 必填 | 说明 | 示例 |
|------|------|------|------|
| `--yixian` | **是** | 一线人员名单 (.xlsx) | `--yixian data/一线.xlsx` |
| `--sanfang` | 否 | 三方人员名单 (.xlsx) | `--sanfang data/三方.xlsx` |
| `--hw` | 否 | HW 名单 (.xlsx) | `--hw data/HW.xlsx` |
| `--dict` | 否 | 数据字典文件 (.xlsx) | `--dict data/dict.xlsx` |
| `--spec` | 否 | 字段规范 (.xlsx 或 .yaml) | `--spec config/field_spec.yaml` |
| `--output` | 否 | 输出目录 | `--output /tmp/result` |

### 场景模式

| 参数 | 说明 |
|------|------|
| `--scenario <名称>` | 运行预置场景（见下方场景列表） |
| `--list-scenarios` | 列出所有可用场景 |

### 输出控制

| 参数 | 说明 |
|------|------|
| `-v / --verbose` | 详细输出（显示进度、模块详情） |
| `-q / --quiet` | 静默模式（仅返回退出码，用于脚本） |
| `--report-json <路径>` | 导出结果为 JSON 文件 |

---

## 预置场景列表

运行 `python run_pipeline.py --list-scenarios` 查看。

| 场景名 | 描述 | 覆盖的测试点 |
|--------|------|-------------|
| **正常全流程** | 三类名单 + 字典 + 规范齐全 | F1→F7 全部执行、数据正确性 |
| **仅一线名单** | 无三方/HW/字典 | F3/F4/F5 跳过逻辑 |
| **仅一线+三方** | 有两份名单，无 HW | F3 跨名单去重触发 |
| **空字段规范** | 不提供 spec 文件 | F2/F4/F5 前置校验跳过 |
| **空数据字典** | 无字典文件 | F5 跳过、F3/F7 正常执行 |
| **一线不存在文件** | 传入不存在的路径 | F1 前置校验拦截 |

---

## 输出示例

### 正常输出

```
▶ 一线:   mock_data/mock_名单_一线.xlsx
▶ 三方:   mock_data/mock_名单_三方.xlsx
▶ HW:     mock_data/mock_名单_HW.xlsx
▶ 字典:   mock_data/mock_data_dict.xlsx
▶ 规范:   mock_data/mock_field_spec.yaml

======================================================================
  处理流水线报告
  Run ID : a1b2c3d4-e5f6-7890-abcd-ef1234567890
  状态   : ✅ completed
  耗时   : 0.342s
======================================================================

模块       状态      说明
----------------------------------------------------------------------
  F1        ✅         [一线] 加载成功，5 行
  F2        ✅         字段检查通过，0 错误
  F6        ✅         内部去重完成，原始 4 条 → 去重后 3 条（移除 1）
  F4        ✅         字典上码完成（0 个字段匹配字典）
  F5        ⏭️         前置校验未通过: _Code 列未找到（mock 数据无字典字段）
  F3        ✅         跨名单去重完成
  F7        ✅         结果已输出至 .../处理结果_20260417_xxxxxx.xlsx

  📊 数据量:
    一线输入: 5 条
    三方输入: 3 条
    HW输入: 3 条

  📁 输出: .../处理结果_20260417_xxxxxx.xlsx
```

### JSON 报告（`--report-json`）

```json
{
  "run_id": "a1b2c3d4-...",
  "status": "completed",
  "duration_sec": 0.342,
  "modules": [
    {"name": "F1", "status": "success", "message": "[一线] 加载成功，5 行"},
    {"name": "F2", "status": "success", "message": "字段检查通过，0 错误"},
    ...
  ],
  "data_summary": {"yixian_input": 5, "sanfang_input": 3, "hw_input": 3},
  "output_path": ".../处理结果_xxx.xlsx"
}
```

---

## 测试人员工作流

### 第一步：用后台工具跑通全部场景

```bash
# 跑完所有预置场景
for scenario in 正常全流程 仅一线名单 仅一线+三方 空字段规范; do
    echo "=== $scenario ==="
    python run_pipeline.py --scenario "$scenario" -q || echo "  ❌ 失败"
done
```

### 第二步：对比预期结果

每个场景在 `test_scenarios.json` 中定义了 `expected` 字段：

```json
{
  "expected": {
    "status": "completed",
    "skipped_modules": ["F4", "F5"]
  }
}
```

### 第三步：新增自定义场景

编辑 `test_scenarios.json`，添加新条目：

```json
{
  "我的自定义场景": {
    "description": "描述这个场景的目的",
    "tags": ["P2", "边界条件"],
    "params": {
      "yixian": "/path/to/custom_data.xlsx",
      "spec": null,
      "dict": null
    },
    "expected": {
      "status": "completed"
    }
  }
}
```

然后执行：
```bash
python run_pipeline.py --scenario 我的自定义场景 -v
```

### 第四步：UI 验证

后台逻辑验证通过后，再启动 GUI 做前端展示和交互验证：
```bash
python main.py
```

---

## 与 pytest 单测的关系

| 层次 | 工具 | 用途 |
|------|------|------|
| **单元测试** | `pytest tests/modules/` | 验证单个函数/方法的正确性 |
| **集成测试** | `pytest tests/integration/` | 验证模块间协作（已存在 test_full_flow.py） |
| **本工具** | `run_pipeline.py` | **端到端流程验证**，用真实 Excel 文件驱动 |

三者互补：单元测细节，集成测模块交互，本工具测完整业务流程。

---

## 退出码

| 码 | 含义 |
|----|------|
| 0 | 成功（completed） |
| 1 | 失败（参数错误/异常） |
| 2 | 流程完成但有部分失败（partial）

可用于 CI/CD pipeline：
```bash
python run_pipeline.py --scenario 正常全流程 -q && echo "✅ PASS" || echo "❌ FAIL"
```
