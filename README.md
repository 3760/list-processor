# 客户名单数据预处理单机工具

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
├── requirements.txt     # 依赖清单
├── config/              # 应用配置
├── dict/                # 运行时字典缓存
├── db/
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
│   └── dict_loader.py    # 字典加载器
├── ui/                  # PyQt5 UI 层
│   ├── main_window.py    # 主窗口
│   ├── worker.py         # 工作线程
│   ├── widgets/          # UI 组件
│   └── styles/           # 样式文件
├── mock_data/            # 开发测试用 Mock 数据
└── tests/                # 测试用例
    ├── unit/             # 单元测试
    └── integration/       # 集成测试
```

## 配置说明

应用默认读取 `config/app_config.yaml`，包括：
- 去重字段默认值
- 超时配置
- 日志级别

## 日志说明

日志文件默认输出到 `logs/` 目录，包含控制台日志和文件日志。

## 注意事项

- 使用正式数据前，请确保已在配置中指定正确的字典文件和字段规范文件路径。
- 处理大批量数据（百万行级）时，请预留足够内存。
