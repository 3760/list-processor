# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller 打包配置 — 客户名单数据预处理工具

打包命令：
  macOS:  pyinstaller main.spec --clean --noconfirm
  Windows: pyinstaller main.spec --clean --noconfirm
"""

import os
import sys
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

# ============================================================
# 路径配置（兼容源码运行和 PyInstaller 执行）
# ============================================================
_SPEC_DIR = os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, 'argv') and len(sys.argv) > 0 else os.getcwd()
_PROJECT_ROOT = _SPEC_DIR

block_cipher = None

# ============================================================
# 图标文件（可选，缺失时自动跳过）
# ============================================================
_ICON = None
_icon_candidates = ['app_icon.icns', 'app_icon.ico']
for _icn in _icon_candidates:
    _p = os.path.join(_PROJECT_ROOT, _icn)
    if os.path.isfile(_p):
        _ICON = _p
        break

# ============================================================
# 数据文件（自动检测存在性）
# ============================================================
_datas = []
_data_entries = [
    ('config/app_config.yaml', 'config'),
    ('config/field_spec.yaml', 'config'),
    ('dict/data_dict.xlsx', 'dict'),
    ('ui/styles/default.qss', 'ui/styles'),
    ('ui/styles/dark.qss', 'ui/styles'),
    # 迁移脚本：打包后用于初始化 SQLite 表结构
    ('db/migrations/001_initial.sql', 'db/migrations'),
]
for _src, _dst in _data_entries:
    _full = os.path.join(_PROJECT_ROOT, _src)
    if os.path.isfile(_full):
        _datas.append((_full, _dst))

# ============================================================
# 收集动态链接库和数据文件（修复 PyInstaller 打包问题）
# ============================================================
# 收集 PyQt5 数据文件（Qt 平台插件等）
try:
    _datas += collect_data_files('PyQt5')
except Exception:
    pass

# 收集 fastexcel 数据文件
try:
    _datas += collect_data_files('fastexcel')
except Exception:
    pass

# ============================================================
# Analysis — 依赖分析
# ============================================================
a = Analysis(
    [os.path.join(_PROJECT_ROOT, 'main.py')],
    pathex=[_PROJECT_ROOT],
    binaries=[],
    datas=_datas,
    hiddenimports=[
        'PyQt5', 'PyQt5.QtCore', 'PyQt5.QtGui', 'PyQt5.QtWidgets', 'PyQt5.sip',
        'PyQt5.QtPrintSupport',
        'polars', 'openpyxl', 'xlsxwriter', 'fastexcel',
        'PyYAML',
        'logging', 'logging.handlers', 'sqlite3',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter', 'matplotlib', 'numpy', 'scipy', 'pandas',
        'pytest', 'pytest_qt', 'notebook',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# ============================================================
# PYZ + EXE + COLLECT + BUNDLE（标准四步流程，不使用 **kwargs）
# ============================================================
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='客户名单数据预处理工具',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # TODO: 调试时可改为 True，完成后改回 False
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=_ICON,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='客户名单数据预处理工具',
)

# ============================================================
# 平台打包说明
# ============================================================
# macOS: 执行 pyinstaller main.spec --clean --noconfirm
#        生成 dist/客户名单数据预处理工具.app
# Windows: 执行 pyinstaller main.spec --clean --noconfirm
#          生成 dist/客户名单数据预处理工具/ 文件夹
#          分发时需复制整个文件夹，不能只复制 exe
# Linux: 执行 pyinstaller main.spec --clean --noconfirm
#        生成 dist/客户名单数据预处理工具/ 文件夹
#
# 分发说明：
#   - Windows/Linux: 需复制 dist/客户名单数据预处理工具/ 整个文件夹
#   - 如需单文件模式：将 EXE 中 exclude_binaries=True 改为 False，并移除 COLLECT
# ============================================================

# 仅 macOS 生成 .app
if sys.platform == 'darwin':
    app = BUNDLE(
        coll,
        name='客户名单数据预处理工具.app',
        bundle_identifier='com.ipsos.customer-list-processor',
        icon=_ICON,
        info_plist={
            'CFBundleName': '客户名单数据预处理工具',
            'CFBundleDisplayName': '客户名单数据预处理工具',
            'CFBundleIdentifier': 'com.ipsos.customer-list-processor',
            'CFBundleVersion': '1.1.0',
            'CFBundleShortVersionString': '1.1.0',
            'CFBundlePackageType': 'APPL',
            'CFBundleExecutable': '客户名单数据预处理工具',
            'LSMinimumSystemVersion': '10.15',
            'NSHighResolutionCapable': True,
            'LSApplicationCategoryType': 'public.app-category.business',
        },
    )
