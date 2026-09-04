# -*- coding: utf-8 -*-
"""
运行时目录管理模块
"""
import datetime
from pathlib import Path
from typing import Dict


def create_runtime_dir(base_dir: Path) -> Path:
    """创建运行时目录 (不依赖 logger，防止提前触发 basicConfig)"""
    base_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.datetime.now()
    base_name = today.strftime('%Y_%m_%d')
    runtime_dir = base_dir / base_name

    if not runtime_dir.exists():
        runtime_dir.mkdir(parents=True, exist_ok=True)
        return runtime_dir

    seq = 1
    while True:
        new_name = f"{base_name}({seq})"
        runtime_dir = base_dir / new_name
        if not runtime_dir.exists():
            runtime_dir.mkdir(parents=True, exist_ok=True)
            return runtime_dir
        seq += 1
        if seq > 100:
            raise RuntimeError(f"无法创建运行时目录，序号超过100: {base_name}")


def create_subdirs(runtime_dir: Path, subdirs: list) -> Dict[str, Path]:
    """在运行时目录下创建子目录"""
    result = {}
    for subdir in subdirs:
        subdir_path = runtime_dir / subdir
        subdir_path.mkdir(parents=True, exist_ok=True)
        result[subdir] = subdir_path
    return result