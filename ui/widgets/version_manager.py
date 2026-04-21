"""
版本记录管理器

负责版本记录的存储、读取和管理。
版本记录保存在应用配置目录下。

使用方式：
    from ui.widgets.version_manager import VersionManager, VersionRecord

    # 添加记录
    vm = VersionManager()
    vm.add_record("修复了Polars filter问题")

    # 获取所有记录
    records = vm.get_records()
"""

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Optional

from infra.log_manager import get_logger

logger = get_logger(__name__)


@dataclass
class VersionRecord:
    """版本记录数据结构"""
    version: str           # 版本号，如 "1.0.1"
    date: str              # 日期时间，格式: "2026-04-20"
    author: str            # 作者
    changes: List[str]     # 变更内容列表
    bug_fixes: List[str]   # Bug修复列表
    features: List[str]    # 新功能列表
    todo: List[str] = None  # 待办事项列表 [20260420-老谈]

    def to_dict(self) -> dict:
        data = asdict(self)
        if data.get('todo') is None:
            data['todo'] = []
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "VersionRecord":
        return cls(**data)


class VersionManager:
    """版本记录管理器"""

    # 默认版本记录文件
    DEFAULT_VERSION_FILE = "version_history.json"

    # 初始版本记录
    INITIAL_RECORDS = [
        VersionRecord(
            version="1.0.0",
            date="2026-04-20",
            author="老谈",
            changes=[
                "初始版本发布",
                "F1~F7 模块框架搭建完成",
                "支持数据字典上码",
                "支持跨名单去重",
                "支持名单内部去重",
                "支持结果导出"
            ],
            bug_fixes=[],
            features=[
                "一线名单、三方名单、HW名单处理",
                "字段规范配置",
                "历史记录查看",
                "处理进度实时显示"
            ]
        ),
        VersionRecord(
            version="1.0.1",
            date="2026-04-20",
            author="AI助手",
            changes=[
                "修复 Polars filter() 不接受 lambda 函数问题",
                "修复 UI 布局挤压问题",
                "修复历史记录未保存问题",
                "修复输出路径未生效问题",
                "修复处理状态不一致问题",
                "增强 macOS 深色模式支持",
                "F1~F7 模块中文名称显示",
                "增强数据字典加载错误处理",
                "添加字典版本号显示"
            ],
            bug_fixes=[
                "Polars filter() lambda 报错: TypeError: invalid predicate",
                "处理进度区域挤压其他组件",
                "窗口缩放后组件不自适应",
                "深色模式下界面显示异常"
            ],
            features=[
                "增加版本记录入口",
                "字典版本号检测与显示",
                "深色模式自动检测"
            ]
        ),
        VersionRecord(
            version="1.0.2",
            date="2026-04-20",
            author="AI助手",
            changes=[
                "修复处理完成后进度条立即消失问题（增加3秒延迟隐藏）",
                "修复处理完成后界面选择被清空问题（保留用户上次的文件选择）",
                "修复历史按钮为空问题（修复 complete_run 参数调用）",
                "修复输出文件无批次管理问题（输出到日期时间子文件夹）",
                "删除 _on_processing_finished 中重复的 show_critical_error 调用",
                "清理未使用的导入（QTimer, QComboBox, QFrame）",
                "修复 file_labels 未随 file_inputs 同步清空的问题"
            ],
            bug_fixes=[
                "历史记录保存失败（complete_run 参数不匹配）",
                "历史记录无法查看输出目录（缺少 output_dir 列）",
                "处理完成后无法快速重新处理（界面被置空）",
                "无法区分不同批次的输出文件（都在同一目录）"
            ],
            features=[
                "输出文件自动创建日期时间子文件夹（批次_YYYYMMDD_HHMMSS）",
                "历史记录对话框新增【输出目录】列",
                "处理完成后保留用户选择的文件路径",
                "进度条完成3秒后才隐藏，用户可看清结果"
            ]
        ),
        VersionRecord(
            version="1.0.3",
            date="2026-04-20",
            author="AI助手",
            changes=[
                "修复历史按钮为空问题（get_recent -> get_history 方法名修正）",
                "修复处理完成后界面被压缩问题（result_banner 未隐藏）",
                "优化窗口最小尺寸时控件重叠问题（设置列拉伸因子和最小宽度）",
                "result_banner 添加 sizePolicy 防止挤压其他区域",
                "result_banner 在 _reset_after_processing 中延迟隐藏",
                "处理配置区块改用 QHBoxLayout 避免 GridLayout 列压缩问题",
                "执行模块区块改用 QVBoxLayout + QHBoxLayout 避免复选框重叠",
                "增加窗口最小宽度至 1050px"
            ],
            bug_fixes=[
                "历史记录加载失败（调用了不存在的 get_recent 方法）",
                "处理完成后 result_banner 一直显示导致界面被挤压",
                "窗口缩小时处理配置区块控件重叠",
                "窗口缩小时执行模块复选框重叠"
            ],
            features=[
                "处理配置区块使用嵌套水平布局，输入框自动拉伸",
                "执行模块区块使用嵌套水平布局，复选框均匀分布",
                "窗口最小宽度增加到 1050px",
                "GroupBox 设置固定最小高度，防止被压缩"
            ]
        ),
        VersionRecord(
            version="1.0.4",
            date="2026-04-20",
            author="AI助手",
            changes=[
                "修复输出Excel中'处理状态'始终显示为 running 的问题",
                "新增启动时自动加载上次使用的字典/字段规范/输出目录",
                "新增处理完成后自动保存当前配置到数据库",
                "新增字典文件MD5变更检测（文件变更时提示重新选择）",
                "修复处理完成后字典版本显示被重置为'未重新加载'的问题",
                "新增字典和字段规范的导入时间记录和显示",
                "代码备份到 backups/backup_20260420/"
            ],
            bug_fixes=[
                "输出Excel的'处理摘要'Sheet中'处理状态'始终显示为 running",
                "每次打开软件都需要重新导入字典和字段规范文件",
                "处理完成后字典版本显示被错误重置"
            ],
            features=[
                "首次执行后自动记录字典/字段规范/输出目录配置",
                "下次启动时自动填充默认值（蓝色字体显示[默认值]标识）",
                "字典文件MD5变更检测，文件变更时显示警告提示重新选择",
                "显示字典版本号(vMD5前8位)和导入时间",
                "显示字段规范导入时间",
                "新增 _load_default_config() 和 _save_default_config() 方法"
            ]
        ),
        VersionRecord(
            version="1.0.5",
            date="2026-04-20",
            author="AI助手",
            changes=[
                "修复历史记录删除功能（添加缺失的 delete 方法）",
                "历史查看详情中新增字典和字段规范文件信息显示",
                "优化处理历史列表的列宽显示",
                "处理状态在历史列表中使用图标+中文形式展示",
                "字段规范显示原始Excel文件名而非转换后的YAML文件名",
                "按钮文字从'打开输出文件夹'改为'打开输出目录'"
            ],
            bug_fixes=[
                "删除历史记录报错：'ProcessingHistoryDAO' object has no attribute 'delete'"
            ],
            features=[
                "历史记录详情中显示数据字典和字段规范文件名",
                "处理历史表格列宽优化调整",
                "处理状态图标展示：✅ 完成 / ❌ 失败 / 🔄 进行中 / ⚠️ 警告"
            ],
            todo=[
                "UI优化：考虑使用更现代的UI框架或自定义样式提升视觉效果"
            ]
        ),
        VersionRecord(
            version="1.0.6",
            date="2026-04-21",
            author="AI助手",
            changes=[
                "[Bug 1] 修复 _select_file 方法中 spec 类型无响应的断点问题",
                "[Bug 2] 修正 f1_loader.py 字段规范重复检测依据（field_name → attr_code）",
                "[Bug 3] 激活 SheetSelectDialog，多 Sheet Excel 强制弹出选择窗口",
                "[Bug 4] 重构模块执行顺序：F6→F4→F5→F3（PRD §6.2 规定）",
                "优化 version_manager.py 读取逻辑，自动同步 INITIAL_RECORDS 缺失版本",
                "main.py 版本号同步更新为 v1.0.6"
            ],
            bug_fixes=[
                "[Bug 1] _select_file('spec') 因 else:return 导致导入按钮无响应",
                "[Bug 2] 属性导入模版 attr_code 重复检测误用 field_name 作为依据",
                "[Bug 3] 多 Sheet Excel 文件静默默认选择第一个，未弹出选择窗口"
            ],
            features=[
                "[Bug 1] spec 类型在 _select_file 中调用 _import_spec_file() 处理",
                "[Bug 3] 新增 _handle_multi_sheet_selection 方法处理多 Sheet 选择",
                "[Bug 3] F1 模块接收预选 Sheet 列表，禁止静默默认",
                "[Bug 4] _build_modules 按 PRD §6.2 固定顺序构建执行链"
            ]
        )
    ]

    def __init__(self, version_file: Optional[str] = None):
        """
        初始化版本管理器

        Args:
            version_file: 版本记录文件路径，默认为应用配置目录下的 version_history.json
        """
        self.version_file = version_file or self._get_default_version_file()

        # 如果文件不存在，初始化默认记录
        if not os.path.exists(self.version_file):
            self._init_default_records()

    def _get_default_version_file(self) -> str:
        """获取默认版本记录文件路径"""
        # 优先使用应用目录
        app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(app_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        return os.path.join(data_dir, self.DEFAULT_VERSION_FILE)

    def _init_default_records(self):
        """初始化默认版本记录"""
        try:
            self._save_records(self.INITIAL_RECORDS)
            logger.info(f"初始化版本记录文件: {self.version_file}")
        except Exception as e:
            logger.error(f"初始化版本记录失败: {e}")

    def _save_records(self, records: List[VersionRecord]):
        """保存记录到文件"""
        with open(self.version_file, "w", encoding="utf-8") as f:
            data = [r.to_dict() for r in records]
            json.dump(data, f, ensure_ascii=False, indent=2)

    def get_records(self) -> List[VersionRecord]:
        """获取所有版本记录（按日期倒序）

        读取时自动检查并补充缺失的版本，确保与 INITIAL_RECORDS 同步
        """
        try:
            if not os.path.exists(self.version_file):
                # 文件不存在，直接使用默认记录并保存
                self._save_records(self.INITIAL_RECORDS)
                return self.INITIAL_RECORDS.copy()

            with open(self.version_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            records = [VersionRecord.from_dict(item) for item in data]

            # 同步检查：补充 INITIAL_RECORDS 中存在但文件中缺失的版本
            existing_versions = {r.version for r in records}
            missing_versions = [r for r in self.INITIAL_RECORDS
                              if r.version not in existing_versions]

            if missing_versions:
                logger.info(f"发现 {len(missing_versions)} 个缺失版本，自动补充: "
                           f"{[r.version for r in missing_versions]}")
                records.extend(missing_versions)
                # 排序后保存，确保一致性
                records.sort(key=lambda r: r.date, reverse=True)
                self._save_records(records)

            return records
        except Exception as e:
            logger.error(f"读取版本记录失败: {e}")
            return self.INITIAL_RECORDS.copy()

    def add_record(self, version: str, author: str, changes: List[str],
                   bug_fixes: List[str] = None, features: List[str] = None):
        """
        添加新版本记录

        Args:
            version: 版本号
            author: 作者
            changes: 变更内容
            bug_fixes: Bug修复列表
            features: 新功能列表
        """
        record = VersionRecord(
            version=version,
            date=datetime.now().strftime("%Y-%m-%d"),
            author=author,
            changes=changes,
            bug_fixes=bug_fixes or [],
            features=features or []
        )

        records = self.get_records()

        # 检查是否已存在该版本
        existing = [i for i, r in enumerate(records) if r.version == version]
        if existing:
            # 更新现有版本
            records[existing[0]] = record
        else:
            # 添加新版本到开头
            records.insert(0, record)

        self._save_records(records)
        logger.info(f"添加版本记录: v{version}")

    def get_latest_version(self) -> str:
        """获取最新版本号"""
        records = self.get_records()
        if records:
            return records[0].version
        return "1.0.0"

    def format_record_text(self, record: VersionRecord) -> str:
        """格式化单条版本记录为可读文本"""
        lines = []
        lines.append(f"v{record.version} ({record.date}) - {record.author}")
        lines.append("")

        if record.changes:
            lines.append("【变更】")
            for change in record.changes:
                lines.append(f"  • {change}")
            lines.append("")

        if record.bug_fixes:
            lines.append("【Bug修复】")
            for fix in record.bug_fixes:
                lines.append(f"  • {fix}")
            lines.append("")

        if record.features:
            lines.append("【新功能】")
            for feature in record.features:
                lines.append(f"  • {feature}")
            lines.append("")

        if record.todo:
            lines.append("【待办】")
            for item in record.todo:
                lines.append(f"  ○ {item}")
            lines.append("")

        return "\n".join(lines)
