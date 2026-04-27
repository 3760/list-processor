# 客户名单数据预处理单机工具 v1.1.0

基于 Python + PyQt5 + Polars 的桌面端数据预处理工具，用于对客户名单数据进行合规检查、去重上码、结果输出。

## 功能流程

```
F1 文件加载 → F2 字段合规检查 → F6 内部去重 → F4 字典上码 → F5 字典校验 → F3 跨名单去重 → F7 结果输出
```

## 环境要求

- Python ≥ 3.10
- 依赖库见 `requirements.txt`

## 安装

```bash
# 1. 克隆或下载代码
cd 代码/

# 2. 创建虚拟环境（推荐）
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate    # Windows

# 3. 安装依赖
pip install -r requirements.txt
```

## 运行

```bash
python main.py
```

## 目录结构

```
代码/
├── main.py              # 应用入口
├── main.spec            # PyInstaller 打包配置
├── requirements.txt     # 依赖清单
├── config/              # 应用配置（YAML）
│   ├── app_config.yaml  # 主配置（去重字段、超时、日志级别）
│   └── field_spec.yaml  # 字段规范
├── dict/                # 运行时字典缓存
├── db/                  # SQLite 数据库
│   ├── connection.py     # 数据库连接管理
│   ├── migrations/       # 数据库迁移脚本
│   └── dao/              # 数据访问对象
├── core/                # 核心编排层
│   ├── context.py        # 处理上下文
│   ├── base_module.py    # 业务模块基类
│   └── orchestrator.py   # 流程编排器
├── modules/              # 业务模块（F1~F7）
├── infra/                # 基础设施层
│   ├── exceptions.py     # 异常类定义
│   ├── log_manager.py    # 日志管理
│   ├── app_config_loader.py  # 配置加载
│   ├── spec_loader.py    # 字段规范加载
│   ├── spec_importer.py  # 字段规范导入器
│   ├── excel_writer.py   # Excel 写入工具
│   ├── dict_loader.py    # 字典加载器
│   └── dict_format_validator.py  # 字典格式预校验
├── ui/                  # PyQt5 UI 层
│   ├── main_window.py    # 主窗口
│   ├── worker.py         # 工作线程
│   ├── widgets/          # UI 组件
│   ├── styles/           # 样式文件（QSS）
│   └── data/             # UI 数据（版本历史等）
├── mock_data/            # 开发测试用 Mock 数据
└── tests/                # 测试用例
    ├── unit/             # 单元测试
    ├── modules/          # 模块测试
    └── functional/       # 功能/集成测试
```

## 配置说明

应用默认读取 `config/app_config.yaml`，包括：
- 去重字段默认值（一线 / 三方 / HW / 跨名单）
- 超时配置（文件加载、单模块执行、全流程）
- 日志级别与轮转策略
- 字典缓存目录与字段规范默认路径

## 日志说明

日志文件默认输出到 `logs/` 目录，包含控制台日志和文件日志。支持实时日志监控、日志导出与自动截断（保留最近 500 行）。

## 打包

```bash
# macOS / Windows / Linux 通用
pyinstaller main.spec --clean --noconfirm
```

- macOS 生成 `dist/客户名单数据预处理工具.app`
- Windows/Linux 生成 `dist/客户名单数据预处理工具/` 文件夹（需整体分发）

## 版本历史

版本记录详见 `ui/data/version_history.json`，或在应用内通过【关于】→【版本记录】查看。

## 注意事项

- 使用正式数据前，请确保已在配置中指定正确的字典文件和字段规范文件路径。
- 处理大批量数据（百万行级）时，请预留足够内存。
- 首次执行后会自动记录字典/字段规范/输出目录配置，下次启动自动填充。
