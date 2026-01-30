#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整数据流程：用户可自由选择要执行的阶段，不强制顺序，不强制从第一步开始。

  阶段 1：制作数据监测表 + 获得目标产品表（执行后自动更新前端）
  阶段 2：根据目标产品表调 API（地区数据/创意数据，可选数量）（执行后自动更新前端）

  前端更新不消耗 API，只要执行了阶段 1 或 2 即会自动执行，无需单独选择。

用法:
  # 交互式：按提示选择要执行的阶段（1 和/或 2）
  python run_full_pipeline.py --interactive

  # 只执行阶段 2（API），完成后自动更新前端
  python run_full_pipeline.py --year 2026 --week 0119-0125 --steps 2 --api creatives --limit top5

  # 执行阶段 1 + 2
  python run_full_pipeline.py --year 2026 --week 0119-0125 --steps 1,2 --api country,creatives
"""

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

BASE_DIR = Path(__file__).parent

# 处理数量选项（API 请求阶段）
LIMIT_CHOICES = ("top1", "top5", "top10", "all")
LIMIT_MAP = {"top1": 1, "top5": 5, "top10": 10, "all": None}


def parse_date(date_str: str):
    """
    解析日期/周段字符串，如 2026-0119-0125 → (year=2026, week_tag=0119-0125)。
    也支持 2025-1201-1207。
    """
    if not date_str or not date_str.strip():
        return None, None
    s = date_str.strip()
    # 2026-0119-0125 或 2025-1201-1207
    m = re.match(r"^(\d{4})-(\d{4}-\d{4})$", s)
    if m:
        return int(m.group(1)), m.group(2)
    # 仅周 0119-0125
    if re.match(r"^\d{4}-\d{4}$", s):
        return None, s
    return None, None


def ensure_raw_csv_for_step1(year: int, week_tag: str) -> None:
    if not year or not week_tag:
        return
    legacy_dir = BASE_DIR / f"{year}_raw_csv"
    modern_dir = BASE_DIR / "raw_csv" / str(year)
    if legacy_dir.exists():
        return
    if not modern_dir.exists() or not (modern_dir / week_tag).exists():
        return
    try:
        legacy_dir.symlink_to(modern_dir)
        print(f"  📎 已创建链接: {year}_raw_csv -> raw_csv/{year}")
    except OSError as e:
        print(f"  ⚠️ 无法创建 {year}_raw_csv 链接（{e}）")


def run_script(script_name: str, week_tag: str, year: int, extra_args=None) -> bool:
    """执行 scripts 下某脚本，传入 --week 与 --year。"""
    script = BASE_DIR / "scripts" / script_name
    if not script.exists():
        print(f"  ❌ 未找到: {script}")
        return False
    cmd = [sys.executable, str(script), "--week", week_tag, "--year", str(year)]
    if extra_args:
        cmd.extend(extra_args)
    try:
        subprocess.run(cmd, check=True, cwd=str(BASE_DIR))
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ❌ {script_name} 执行失败，退出码: {e.returncode}")
        return False


def run_frontend_script(script_name: str, year: int = None, week_tag: str = None, extra_args=None) -> bool:
    """执行 frontend 下某脚本，可选传入 --year / --week。"""
    script = BASE_DIR / "frontend" / script_name
    if not script.exists():
        print(f"  ❌ 未找到: {script}")
        return False
    cmd = [sys.executable, str(script)]
    if year is not None:
        cmd.extend(["--year", str(year)])
    if week_tag is not None:
        cmd.extend(["--week", week_tag])
    if extra_args:
        cmd.extend(extra_args)
    try:
        subprocess.run(cmd, check=True, cwd=str(BASE_DIR))
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ❌ frontend/{script_name} 执行失败，退出码: {e.returncode}")
        return False


def run_request_script(script_name: str, extra_args=None) -> bool:
    """执行 request 下某脚本。"""
    script = BASE_DIR / "request" / script_name
    if not script.exists():
        print(f"  ❌ 未找到: {script}")
        return False
    cmd = [sys.executable, str(script)]
    if extra_args:
        cmd.extend(extra_args)
    try:
        subprocess.run(cmd, check=True, cwd=str(BASE_DIR))
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ❌ request/{script_name} 执行失败，退出码: {e.returncode}")
        return False


def week_tag_to_dates(year: int, week_tag: str):
    """将 week_tag（如 0119-0125）和 year 转为 start_date、end_date（YYYY-MM-DD）。跨年周（如 1229-0104）时 end_date 用 year+1。"""
    s = (week_tag or "").strip()
    m = re.match(r"^(\d{2})(\d{2})-(\d{2})(\d{2})$", s)
    if not m:
        return None, None
    m1, d1, m2, d2 = m.group(1), m.group(2), m.group(3), m.group(4)
    try:
        start_date = f"{year}-{m1}-{d1}"
        # 跨年周：1229-0104 结束月在 01，结束日在下一年
        end_year = year if int(m2) >= int(m1) else year + 1
        end_date = f"{end_year}-{m2}-{d2}"
        return start_date, end_date
    except Exception:
        return None, None


TARGET_SOURCE_CHOICES = ("strategy", "non_strategy", "both")


def get_target_products_with_limit(
    year: int, week_tag: str, limit: str, target_source: str = "both"
):
    """
    从 target/{年}/{周}/ 下按 target_source 读取 strategy_target 和/或 non_strategy_target 的 xlsx。
    target_source: strategy=仅策略目标, non_strategy=仅非策略目标, both=两者。
    优先用「Unified ID」作为 app_id（ST API 所需），产品名为「产品归属」；无 Unified ID 时用产品归属作为 app_id。
    去重按 (app_id, 产品归属) 出现顺序，取前 limit 条。limit 为 top1/top5/top10/all。
    返回 (app_ids: list[str], app_list: list[tuple[str,str]])，app_list 为 (app_id, 产品归属)。
    """
    try:
        import pandas as pd
    except ImportError:
        print("  ❌ 需要 pandas，请安装: pip install pandas openpyxl")
        return [], []
    target_base = BASE_DIR / "target" / str(year) / week_tag
    if not target_base.exists():
        print(f"  ❌ 未找到 target 目录: {target_base}，请先执行步骤 2")
        return [], []
    if target_source == "strategy":
        subs = ("strategy_target",)
    elif target_source == "non_strategy":
        subs = ("non_strategy_target",)
    else:
        subs = ("strategy_target", "non_strategy_target")
    col_product = "产品归属"
    col_uid = "Unified ID"
    seen_order = []  # (app_id, product_name)
    seen = set()
    for sub in subs:
        sub_dir = target_base / sub
        if not sub_dir.exists():
            continue
        for f in sub_dir.glob("*.xlsx"):
            try:
                df = pd.read_excel(f)
                if col_product not in df.columns:
                    continue
                has_uid = col_uid in df.columns
                for _, row in df.iterrows():
                    product = (row.get(col_product) or "")
                    if pd.isna(product) or not str(product).strip():
                        continue
                    product = str(product).strip()
                    app_id = str(row.get(col_uid, product) or product).strip() if has_uid else product
                    if not app_id:
                        app_id = product
                    key = (app_id, product)
                    if key not in seen:
                        seen.add(key)
                        seen_order.append((app_id, product))
            except Exception as e:
                print(f"  ⚠️ 读取 {f} 失败: {e}")
                continue
    n = LIMIT_MAP.get(limit) if limit in LIMIT_MAP else None
    if n is not None:
        seen_order = seen_order[:n]
    app_ids = [x[0] for x in seen_order]
    app_list = seen_order
    return app_ids, app_list


def get_app_ids_from_strategy_file(year: int, week_tag: str, filename: str, limit: str = "all"):
    """
    从 target/{年}/{周}/strategy_target/{filename} 读取 Unified ID 列，返回 app_ids 列表。
    filename 如 target_strategy_old.xlsx、target_strategy_new.xlsx。
    limit 同 get_target_products_with_limit：top1/top5/top10/all。
    """
    try:
        import pandas as pd
    except ImportError:
        return []
    target_path = BASE_DIR / "target" / str(year) / week_tag / "strategy_target" / filename
    if not target_path.exists():
        return []
    head_df = pd.read_excel(target_path, nrows=0)
    col_uid = "Unified ID" if "Unified ID" in head_df.columns else "产品归属"
    converters = {col_uid: str} if col_uid in head_df.columns else None
    df = pd.read_excel(target_path, converters=converters)
    if col_uid not in df.columns:
        return []
    ids = df[col_uid].dropna().astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist()
    ids = [x for x in ids if (x or "").strip()]
    n = LIMIT_MAP.get(limit) if limit in LIMIT_MAP else None
    if n is not None:
        ids = ids[:n]
    return ids


# 步骤定义：数字 -> (脚本名, 描述) 或 特殊步骤 3/4/5
STEP_DEFS = {
    1: (
        [
            "step1_merge_clean.py",
            "step2_mapping.py",
            "step3_metrics.py",
            "step4_pivot.py",
            "step5_final_report.py",
            "step5_5_fix_arrow_color.py",
        ],
        "制作数据监测表（step1→step5_5）",
    ),
    2: (["generate_target.py"], "获得目标产品（策略/非策略，old/new）"),
    3: None,  # 拉取地区数据，在 run_step 中调用 request/fetch_country_data
    4: None,  # 拉取创意数据，在 run_step 中调用 request/fetch_ad_creatives
    5: None,  # 前端更新在 run_step 中单独处理
}


def run_step(num: int, week_tag: str, year: int, limit: str = "all", **kwargs) -> bool:
    """执行指定步骤。limit 仅对步骤 3、4 生效；kwargs 可传 target_source（strategy/non_strategy/both）。"""
    if num not in STEP_DEFS:
        print(f"  ❌ 未知步骤: {num}，可选: 1,2,3,4,5")
        return False
    step_def = STEP_DEFS[num]
    # 步骤 3：拉取地区数据（仅支持策略目标；非策略时跳过）
    if num == 3:
        target_src = (kwargs.get("target_source") or "both").lower()
        if target_src == "non_strategy":
            print(f"\n🔹 步骤 3: 拉取地区数据 — 已选「仅非策略目标」，地区数据仅支持策略目标，跳过")
            return True
        print(f"\n🔹 步骤 3: 拉取地区数据（处理数量: {limit}，目标: 策略）")
        start_date, end_date = week_tag_to_dates(year, week_tag)
        base_extra = ["--year", str(year), "--week", week_tag]
        if start_date:
            base_extra.extend(["--start_date", start_date])
        if end_date:
            base_extra.extend(["--end_date", end_date])
        any_ok = False
        for product_type, filename in [
            ("strategy_old", "target_strategy_old.xlsx"),
            ("strategy_new", "target_strategy_new.xlsx"),
        ]:
            app_ids = get_app_ids_from_strategy_file(year, week_tag, filename, limit=limit)
            if not app_ids:
                print(f"  ⏭ 跳过 {product_type}（无目标或文件不存在: {filename}）")
                continue
            print(f"  {product_type}: 目标产品数 {len(app_ids)}")
            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
                f.write("\n".join(app_ids))
                tmp_path = f.name
            try:
                ok = run_request_script(
                    "fetch_country_data.py",
                    ["--app_ids_file", tmp_path] + base_extra + ["--product_type", product_type],
                )
                if ok:
                    any_ok = True
            finally:
                Path(tmp_path).unlink(missing_ok=True)
        if any_ok:
            run_script("convert_country_json_to_xlsx.py", week_tag, year)
        # 若本周无任一 strategy 目标文件，仍返回 True 避免整条流水线报错
        return True
    # 步骤 4：拉取创意数据（从 target 取产品列表，按 limit 与 target_source 截断后调用 fetch_ad_creatives）
    if num == 4:
        target_src = (kwargs.get("target_source") or "both").lower()
        print(f"\n🔹 步骤 4: 拉取创意数据（处理数量: {limit}，目标: {'仅策略' if target_src == 'strategy' else '仅非策略' if target_src == 'non_strategy' else '策略+非策略'}）")
        _, app_list = get_target_products_with_limit(year, week_tag, limit, target_source=target_src)
        if not app_list:
            return False
        print(f"  目标产品数: {len(app_list)}")
        start_date, end_date = week_tag_to_dates(year, week_tag)
        extra = ["--year", str(year), "--week", week_tag]
        if target_src == "non_strategy":
            extra.extend(["--product_type", "non_strategy"])
        elif target_src == "strategy":
            extra.extend(["--product_type", "strategy_old"])
        if start_date:
            extra.extend(["--start_date", start_date])
        if end_date:
            extra.extend(["--end_date", end_date])
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
            for app_id, pname in app_list:
                f.write(f"{app_id}\t{pname}\n")
            tmp_path = f.name
        try:
            ok = run_request_script(
                "fetch_ad_creatives.py",
                ["--app_list_file", tmp_path] + extra,
            )
            return ok
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    # 步骤 5：前端数据更新（公司维度 JSON、产品维度 JSON、素材索引、weeks_index、题材/画风映射）
    if num == 5:
        print("\n🔹 步骤 5: 前端数据更新（公司/产品/素材 JSON + 周索引 + 题材/画风映射）")
        # 题材/画风：从 mapping/产品归属.xlsx 转 JSON，供产品详情页按 Unified ID 取题材、画风
        run_frontend_script("convert_product_mapping_to_json.py")
        out_excel = BASE_DIR / "output" / str(year) / f"{week_tag}_SLG数据监测表.xlsx"
        if out_excel.exists():
            if not run_frontend_script("convert_excel_with_format.py", year=year, week_tag=week_tag):
                return False
        else:
            print(f"  ⏭ 跳过 convert_excel_with_format（未找到 {out_excel}）")
        metrics_xlsx = BASE_DIR / "intermediate" / str(year) / week_tag / "metrics_total.xlsx"
        if metrics_xlsx.exists():
            run_frontend_script("convert_metrics_to_json.py", year=year, week_tag=week_tag)
            # 产品赛道排名：根据 metrics_total.json 计算排名，生成 metrics_rank.json 供产品详情页使用
            run_frontend_script("build_metrics_rank.py", year=year, week_tag=week_tag)
        else:
            print(f"  ⏭ 跳过 convert_metrics_to_json（未找到 {metrics_xlsx.relative_to(BASE_DIR)}）")
        # 用 target + country_data 生成/更新 final_join，保证新数据跑完后 final_join 会更新
        target_strategy_dir = BASE_DIR / "target" / str(year) / week_tag / "strategy_target"
        if target_strategy_dir.exists():
            if not run_script("build_final_join.py", week_tag, year):
                return False
        else:
            print(f"  ⏭ 跳过 build_final_join（未找到 target/{year}/{week_tag}/strategy_target）")
        final_dir = BASE_DIR / "final_join" / str(year) / week_tag
        if final_dir.exists():
            if not run_frontend_script("convert_final_join_to_json.py", year=year, week_tag=week_tag):
                return False
        else:
            print(f"  ⏭ 跳过 convert_final_join_to_json（未找到 final_join/{year}/{week_tag}）")
        ads_dir = BASE_DIR / "advertisements" / str(year) / week_tag
        if ads_dir.exists():
            if not run_frontend_script("build_creative_products_index.py", year=year, week_tag=week_tag):
                return False
        else:
            print(f"  ⏭ 跳过 build_creative_products_index（未找到 advertisements/{year}/{week_tag}）")
        if not run_frontend_script("build_weeks_index.py"):
            return False
        return True
    scripts, label = step_def
    if not scripts:
        print(f"  ⏭ 步骤 {num}（{label}）暂未实现，跳过")
        return True
    print(f"\n🔹 步骤 {num}: {label}")
    for script_name in scripts:
        if not run_script(script_name, week_tag, year):
            return False
    return True


def run_phase1(week_tag: str, year: int) -> bool:
    """第一步：制作数据监测表 + 获得目标产品表。"""
    ensure_raw_csv_for_step1(year, week_tag)
    return run_step(1, week_tag, year) and run_step(2, week_tag, year)


def run_phase2(
    week_tag: str,
    year: int,
    fetch_country: bool,
    fetch_creatives: bool,
    limit: str,
    target_source: str = "both",
) -> bool:
    """第二步：根据目标产品表调 API。用户已选是否请求地区数据、创意数据、处理数量及策略/非策略目标。"""
    if not fetch_country and not fetch_creatives:
        print("  第二步未选择任何 API 请求，跳过")
        return True
    kw = {"target_source": target_source}
    if fetch_country and not run_step(3, week_tag, year, limit=limit, **kw):
        return False
    if fetch_creatives and not run_step(4, week_tag, year, limit=limit, **kw):
        return False
    return True


def run_phase3(week_tag: str, year: int) -> bool:
    """第三步：前端数据更新。"""
    return run_step(5, week_tag, year)


def run_pipeline(
    week_tag: str,
    year: int,
    run_phase1_flag: bool,
    run_phase2_flag: bool,
    api_fetch_country: bool = True,
    api_fetch_creatives: bool = True,
    limit: str = "all",
    target_source: str = "both",
    yes_over_100: bool = False,
    interactive_confirm: bool = True,
) -> bool:
    """按用户选择的阶段执行。第一步=表+目标产品；第二步=调API。第一步或第二步任一步运行结束后都会自动更新前端。"""
    print("=" * 60)
    print("🚀 SLG Monitor 完整数据流程")
    print("=" * 60)
    print(f"  时间段: {year} 年 / 周 {week_tag}")
    phases = []
    if run_phase1_flag:
        phases.append("第一步(数据监测表+目标产品)")
    if run_phase2_flag:
        parts = []
        if api_fetch_country:
            parts.append("地区数据")
        if api_fetch_creatives:
            parts.append("创意数据")
        target_label = {"strategy": "策略", "non_strategy": "非策略", "both": "策略+非策略"}.get(target_source, target_source)
        phases.append(f"第二步(API: {', '.join(parts)}, 数量={limit}, 目标={target_label})")
    print(f"  本次将执行: {'、'.join(phases)}（执行后自动更新前端）")
    print("=" * 60)

    if run_phase1_flag:
        print("\n🔹 第一步: 制作数据监测表 + 获得目标产品表")
        if not run_phase1(week_tag, year):
            print("\n❌ 第一步终止")
            return False
        # 第一步结束后自动更新网页
        print("\n🔹 前端数据更新（第一步完成后自动）")
        if not run_phase3(week_tag, year):
            print("\n❌ 前端更新终止")
            return False
    if run_phase2_flag:
        # 第二步前：目标产品超过 100 个时二次确认
        _, app_list = get_target_products_with_limit(year, week_tag, limit, target_source=target_source)
        n = len(app_list)
        if n > 100 and not yes_over_100:
            print(f"\n⚠️ 目标产品共 {n} 个，超过 100 个。")
            if interactive_confirm:
                try:
                    s = input("  是否继续请求数据？[y/N]: ").strip().upper() or "N"
                    if s not in ("Y", "YES"):
                        print("  已跳过第二步。")
                        run_phase2_flag = False
                except EOFError:
                    print("  输入已结束，已跳过第二步。")
                    run_phase2_flag = False
            else:
                print("  非交互模式下请使用 --yes 以继续执行第二步，否则跳过。")
                run_phase2_flag = False
        if run_phase2_flag:
            print("\n🔹 第二步: 根据目标产品表调 API")
            if not run_phase2(week_tag, year, api_fetch_country, api_fetch_creatives, limit, target_source=target_source):
                print("\n❌ 第二步终止")
                return False
            # 第二步结束后自动更新网页
            print("\n🔹 前端数据更新（第二步完成后自动）")
            if not run_phase3(week_tag, year):
                print("\n❌ 前端更新终止")
                return False

    print("\n" + "=" * 60)
    print("✅ 所选阶段执行完毕")
    print("=" * 60)
    return True


def prompt_for_timeframe():
    """未指定时间段时，由键盘输入获取年与周。返回 (year: int, week_tag: str)。"""
    print("未指定时间段，请键盘输入：")
    print("  方式一：输入「年-周」如 2026-0119-0125")
    print("  方式二：先输入年份，再输入周标签（如 0119-0125）")
    while True:
        try:
            raw = input("  时间段或年份: ").strip()
            if not raw:
                continue
            parsed_year, parsed_week = parse_date(raw)
            if parsed_year is not None and parsed_week is not None:
                return parsed_year, parsed_week
            if parsed_week is not None:
                # 只输入了周标签，补问年份
                year_raw = input("  年份 (如 2026): ").strip()
                if re.match(r"^\d{4}$", year_raw):
                    return int(year_raw), parsed_week
                print("  年份应为 4 位数字")
                continue
            if re.match(r"^\d{4}$", raw):
                year = int(raw)
                week_tag = input("  周标签 (如 0119-0125): ").strip()
                if re.match(r"^\d{4}-\d{4}$", week_tag):
                    return year, week_tag
                print("  周标签格式应为 xxxx-xxxx，如 0119-0125")
            else:
                print("  请输入 4 位年份或「年-周」如 2026-0119-0125")
        except EOFError:
            print("  输入已结束")
            sys.exit(1)


def _prompt_yn(msg: str, default: bool = True) -> bool:
    """提示 y/n，默认 default。"""
    d = "Y" if default else "N"
    try:
        s = input(f"  {msg} [{d}]: ").strip().upper() or d
        return s in ("Y", "YES")
    except EOFError:
        return default


def interactive_collect_phases(year: int, week_tag: str):
    """
    交互式收集要执行的阶段与第二步 API 选项。
    返回 (run_phase1, run_phase2, api_fetch_country, api_fetch_creatives, limit, target_source)。
    执行阶段 1 或 2 后会自动更新前端，无需单独选择。
    """
    print("\n请选择要执行的阶段（可多选，执行后自动更新前端）：")
    print("  1 = 制作数据监测表 + 获得目标产品表")
    print("  2 = 根据目标产品表调 API（稍后选择请求哪些数据）")
    print("  输入 1、2 或 1,2，逗号分隔。例如: 1、2、1,2")
    while True:
        try:
            raw = input("  阶段: ").strip()
            if not raw:
                print("  请至少选择一项，例如 1 或 2 或 1,2")
                continue
            phases = [int(x.strip()) for x in raw.replace(",", " ").split() if x.strip()]
            phases = sorted(set(p for p in phases if p in (1, 2)))
            if phases:
                break
        except ValueError:
            pass
        print("  请输入 1、2 或 1,2")
    run_phase1 = 1 in phases
    run_phase2 = 2 in phases

    api_fetch_country = False
    api_fetch_creatives = False
    limit = "all"
    target_source = "both"
    if run_phase2:
        print("\n第二步：请求策略目标、非策略目标、还是两者？")
        print("  1 = 仅策略目标  2 = 仅非策略目标  3 = 两者")
        while True:
            try:
                raw = input("  目标 [3]: ").strip() or "3"
                if raw in ("1", "2", "3"):
                    target_source = {"1": "strategy", "2": "non_strategy", "3": "both"}[raw]
                    break
                print("  请输入 1、2 或 3")
            except EOFError:
                target_source = "both"
                break
        print("\n第二步：请选择要请求的数据类型（可多选）")
        api_fetch_country = _prompt_yn("  请求地区数据？", default=True)
        api_fetch_creatives = _prompt_yn("  请求创意数据？", default=True)
        if not api_fetch_country and not api_fetch_creatives:
            print("  未选择任何 API，第二步将跳过")
        else:
            print("  处理数量：top1 / top5 / top10 / all")
            while True:
                raw = input("  数量 [all]: ").strip().lower() or "all"
                if raw in LIMIT_CHOICES:
                    limit = raw
                    break
                print(f"  请输入其中之一: {LIMIT_CHOICES}")

    return run_phase1, run_phase2, api_fetch_country, api_fetch_creatives, limit, target_source


def _parse_api_arg(s: str) -> tuple:
    """解析 --api 字符串，返回 (fetch_country: bool, fetch_creatives: bool)。"""
    if not s or not s.strip():
        return True, True
    parts = [x.strip().lower() for x in s.replace(",", " ").split() if x.strip()]
    country = "country" in parts or "地区" in s
    creatives = "creatives" in parts or "创意" in s
    if not parts:
        return True, True
    return country, creatives


def main():
    parser = argparse.ArgumentParser(
        description="SLG Monitor 完整数据流程：可任意选择要执行的阶段，不强制从第一步开始",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
阶段（执行 1 或 2 后会自动更新前端，无需单独选择）:
  1  制作数据监测表 + 获得目标产品表
  2  根据目标产品表调 API（地区数据/创意数据，可选数量；会询问策略/非策略目标；超过 100 个产品会二次确认）

  --target strategy|non_strategy|both  第二步请求策略目标、非策略目标或两者（默认 both）
  --yes  目标产品超过 100 个时不二次确认，直接执行第二步

示例（只执行第二步）:  --steps 2 --api creatives --limit top5 --target strategy
示例（1+2，超过 100 不确认）:  --steps 1,2 --api country,creatives --yes
        """,
    )
    parser.add_argument(
        "--date",
        help="时间段：年-周标签，如 2026-0119-0125（与 --year/--week 二选一）",
    )
    parser.add_argument("--year", type=int, help="年份，如 2026（与 --date 二选一）")
    parser.add_argument("--week", help="周标签，如 0119-0125（与 --date 二选一）")
    parser.add_argument(
        "--steps",
        help="要执行的阶段，逗号分隔：1、2 或 1,2。执行后自动更新前端",
    )
    parser.add_argument(
        "--api",
        help="第二步请求的数据类型：country,creatives 或 country 或 creatives。默认两者都请求",
    )
    parser.add_argument(
        "--limit",
        choices=LIMIT_CHOICES,
        default="all",
        help="第二步 API 处理数量：top1/top5/top10/all。默认 all",
    )
    parser.add_argument(
        "--target",
        choices=TARGET_SOURCE_CHOICES,
        default="both",
        help="第二步请求的目标：strategy=仅策略目标, non_strategy=仅非策略目标, both=两者。默认 both",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        dest="yes_over_100",
        help="目标产品超过 100 个时不二次确认，直接执行第二步",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="交互式：按提示选择阶段、第二步请求哪些数据及处理数量",
    )
    parser.add_argument("--target_type", choices=["old", "new"], default="old", help="目标类型（预留）")
    args = parser.parse_args()

    year = args.year
    week_tag = args.week
    if args.date:
        parsed_year, parsed_week = parse_date(args.date)
        if parsed_year is not None:
            year = parsed_year
        if parsed_week is not None:
            week_tag = parsed_week
    if year is None or week_tag is None:
        year, week_tag = prompt_for_timeframe()
        print(f"  已选择: {year} 年 / 周 {week_tag}")

    # 未传 --steps 时通过键盘输入选择阶段（与 --interactive 行为一致）
    if args.interactive or not (args.steps or "").strip():
        run_p1, run_p2, api_country, api_creatives, limit, target_src = interactive_collect_phases(year, week_tag)
        yes_over_100 = False
        interactive_confirm = True
    else:
        raw = args.steps.strip()
        try:
            phases = [int(x.strip()) for x in raw.replace(",", " ").split() if x.strip()]
            phases = sorted(set(p for p in phases if p in (1, 2)))
        except ValueError:
            print("--steps 应为 1、2 的逗号分隔组合，如 1、2、1,2（执行后自动更新前端）")
            sys.exit(1)
        if not phases:
            print("--steps 至少包含 1 或 2 中的一项")
            sys.exit(1)
        run_p1 = 1 in phases
        run_p2 = 2 in phases
        api_country, api_creatives = _parse_api_arg(args.api or "country,creatives")
        limit = args.limit or "all"
        target_src = (args.target or "both").lower()
        yes_over_100 = getattr(args, "yes_over_100", False)
        interactive_confirm = False

    ok = run_pipeline(
        week_tag=week_tag,
        year=year,
        run_phase1_flag=run_p1,
        run_phase2_flag=run_p2,
        api_fetch_country=api_country,
        api_fetch_creatives=api_creatives,
        limit=limit,
        target_source=target_src,
        yes_over_100=yes_over_100,
        interactive_confirm=interactive_confirm,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
