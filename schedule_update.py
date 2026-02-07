#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定期更新数据脚本

可以用于cron或任务调度器定期运行，更新数据监测表
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).parent


def get_current_week_tag():
    """获取当前周的标签（格式：MMDD-MMDD）"""
    today = datetime.now()
    
    # 计算本周一
    days_since_monday = today.weekday()
    monday = today - timedelta(days=days_since_monday)
    
    # 计算本周日
    sunday = monday + timedelta(days=6)
    
    # 格式化为 MMDD-MMDD
    week_start = monday.strftime("%m%d")
    week_end = sunday.strftime("%m%d")
    week_tag = f"{week_start}-{week_end}"
    
    return week_tag, monday.year


def run_update(week_tag=None, year=None, target_type="old", limit=10):
    """运行完整的数据更新流程"""
    if week_tag is None or year is None:
        week_tag, year = get_current_week_tag()
    
    print("="*60)
    print(f"🔄 开始更新数据")
    print(f"周标签: {week_tag}")
    print(f"年份: {year}")
    print(f"目标类型: {target_type}")
    print(f"处理数量: {limit}")
    print("="*60)
    print()
    
    # 运行完整流程
    cmd = [
        sys.executable,
        str(BASE_DIR / "run_full_pipeline.py"),
        "--week", week_tag,
        "--year", str(year),
        "--target_type", target_type,
        "--limit", str(limit)
    ]
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        print("✅ 数据更新完成")
        return True
    except subprocess.CalledProcessError as e:
        print("❌ 数据更新失败:")
        print(e.stderr)
        return False


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="定期更新数据监测表")
    parser.add_argument("--week", help="周标签，例如 1201-1207（不指定则使用当前周）")
    parser.add_argument("--year", type=int, help="年份（不指定则使用当前年）")
    parser.add_argument("--target_type", choices=["old", "new"], default="old", 
                       help="目标产品类型，默认 old")
    parser.add_argument("--limit", type=int, default=10, 
                       help="处理的目标产品数量，默认 10")
    
    args = parser.parse_args()
    
    success = run_update(
        week_tag=args.week,
        year=args.year,
        target_type=args.target_type,
        limit=args.limit
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
