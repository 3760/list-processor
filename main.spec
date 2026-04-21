# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller 打包配置 — 客户名单数据预处理工具

打包命令：
  macOS:  pyinstaller main.spec --clean --noconfirm
  Windows: pyinstaller main.spec --clean --noconfirm
"""

import os
import sys

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
    # 迁移脚本：打包后用于初始化 SQLite 表结构
    ('db/migrations/001_initial.sql', 'db/migrations'),
]
for _src, _dst in _data_entries:
    _full = os.path.join(_PROJECT_ROOT, _src)
    if os.path.isfile(_full):
        _datas.append((_full, _dst))

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
        'polars', 'openpyxl',
        'yaml',
        'logging', 'logging.handlers',
        'sqlite3',
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
    console=False,
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
            'CFBundleVersion': '1.0.0',
            'CFBundleShortVersionString': '1.0.0',
            'CFBundlePackageType': 'APPL',
            'CFBundleExecutable': '客户名单数据预处理工具',
            'LSMinimumSystemVersion': '10.15',
            'NSHighResolutionCapable': True,
            'LSApplicationCategoryType': 'public.app-category.business',
        },
    )
