#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化【数据底表】所需的题材/玩法/画风三张标签表 Excel。
若 labels/ 下对应文件不存在则创建带表头的空表；已存在则跳过。
需安装：pip install openpyxl

运行（项目根目录）：
  python scripts/init_label_tables.py
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LABELS_DIR = BASE_DIR / "labels"

# 表名 -> 列名列表（与数据底表展示一致）
LABEL_TABLES = {
    "题材标签表.xlsx": ["序号", "标签名", "备注"],
    "玩法标签表.xlsx": ["序号", "标签名", "备注"],
    "画风标签表.xlsx": ["序号", "标签名", "备注"],
}


def main():
    try:
        from openpyxl import Workbook
    except ImportError:
        print("  请先安装 openpyxl: pip install openpyxl")
        return 1
    LABELS_DIR.mkdir(parents=True, exist_ok=True)
    for filename, headers in LABEL_TABLES.items():
        path = LABELS_DIR / filename
        if path.exists():
            print(f"  已存在，跳过: {path.relative_to(BASE_DIR)}")
            continue
        wb = Workbook()
        ws = wb.active
        ws.append(headers)
        wb.save(path)
        print(f"  已创建: {path.relative_to(BASE_DIR)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
