"""
跨平台工具函数

提供跨平台的文件/目录打开、路径处理等通用功能。
"""

import os
import platform
import subprocess


def open_file_or_dir(path: str) -> None:
    """
    跨平台打开文件或目录。

    - Windows: os.startfile()
    - macOS: open 命令
    - Linux: xdg-open 命令

    Parameters
    ----------
    path : str
        要打开的文件或目录路径

    Raises
    ------
    FileNotFoundError
        路径不存在时抛出
    """
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"路径不存在: {path}")

    system = platform.system()

    if system == "Windows":
        os.startfile(os.path.normpath(path))
    elif system == "Darwin":  # macOS
        subprocess.run(["open", path], check=False)
    else:  # Linux
        subprocess.run(["xdg-open", path], check=False)
