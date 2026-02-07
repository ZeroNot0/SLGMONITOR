#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
拉取国家维度下载/收入数据。仿照 advertisements，可写入 countiesdata/{年}/{周}/json 与 xlsx；
未指定年/周时写入 request/country_data/json 与 xlsx。

使用 Sensor Tower API：GET /v1/{os}/sales_report_estimates
- 路径：{os}/sales_report_estimates，用 unified（与接口文档一致）
- 参数：app_ids（必填）、start_date、end_date、date_granularity、countries（ALL=所有地区）
pipeline 传入 Unified ID 作为 app_id；指定 --year、--week 时写入 countiesdata/{年}/{周}/。
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ST API 网络不稳定：失败时延迟后重试，最多请求 3 次
RETRY_MAX = 3
RETRY_DELAY_SEC = 3

# 保证可导入 request.api_request（从项目根或 request 目录运行均可）
REQUEST_DIR = Path(__file__).resolve().parent
_root = REQUEST_DIR.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from request.api_request import load_token, get_session, get, ensure_dir

BASE_DIR = REQUEST_DIR.parent
# 默认写 request/country_data；指定 year/week 时在 run() 中改为 countiesdata/{年}/{周}/
OUTPUT_JSON_DIR = REQUEST_DIR / "country_data" / "json"
OUTPUT_XLSX_DIR = REQUEST_DIR / "country_data" / "xlsx"


def fetch_country_data_for_app(
    session,
    app_id,
    os_platform="unified",
    start_date=None,
    end_date=None,
    date_granularity="weekly",
    countries="ALL",
    data_model="DM_2025_Q2",
    debug=False,
):
    """
    请求单个 app 的下载/收入估算（按国家、日期）。
    API: GET /v1/{os}/sales_report_estimates
    参数：app_ids（必填）、start_date、end_date、date_granularity、countries（ALL=所有地区）、data_model。
    """
    path = f"{os_platform}/sales_report_estimates"
    params = {
        "app_ids": app_id,
        "date_granularity": date_granularity,
        "data_model": data_model,
        "countries": countries,
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    resp = get(session, path, params=params)
    resp.raise_for_status()
    result = resp.json()
    if debug:
        print(f"  [DEBUG] API 返回类型: {type(result)}")
        if isinstance(result, dict):
            print(f"  [DEBUG] 返回字典的 keys: {list(result.keys())}")
            if "data" in result:
                print(f"  [DEBUG] data 长度: {len(result['data']) if isinstance(result['data'], (list, dict)) else 'N/A'}")
        elif isinstance(result, list):
            print(f"  [DEBUG] 返回列表长度: {len(result)}")
            if result:
                print(f"  [DEBUG] 第一条 keys: {list(result[0].keys()) if isinstance(result[0], dict) else 'N/A'}")
    return result


def fetch_country_data_with_retry(
    session,
    app_id,
    os_platform="unified",
    start_date=None,
    end_date=None,
    date_granularity="weekly",
    countries="ALL",
    data_model="DM_2025_Q2",
    debug=False,
):
    """带重试的请求：失败时延迟 RETRY_DELAY_SEC 秒后重试，最多 RETRY_MAX 次。"""
    last_e = None
    for attempt in range(RETRY_MAX):
        try:
            return fetch_country_data_for_app(
                session,
                app_id,
                os_platform=os_platform,
                start_date=start_date,
                end_date=end_date,
                date_granularity=date_granularity,
                countries=countries,
                data_model=data_model,
                debug=debug,
            )
        except Exception as e:
            last_e = e
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_DELAY_SEC)
    raise last_e


def save_json(app_id, data, json_dir=None):
    d = json_dir or OUTPUT_JSON_DIR
    ensure_dir(d)
    path = d / f"{app_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  已写 JSON: {path}")


# 分地区数据表列顺序（与常见表格一致：app_id, country, date, android, iphone, unified, ipad）
COUNTRY_TABLE_COLUMN_ORDER = [
    "app_id",
    "country",
    "date",
    "android_units",
    "android_revenue",
    "iphone_units",
    "iphone_revenue",
    "unified_units",
    "unified_revenue",
    "ipad_units",
    "ipad_revenue",
]


def _format_date_for_excel(val):
    """将 ISO 日期转为 YYYY-MM-DD 字符串，便于 Excel 显示（避免 ######）。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if not s:
        return ""
    # 2026-01-19T00:00:00Z -> 2026-01-19
    if "T" in s:
        s = s.split("T")[0]
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s


def save_xlsx(app_id, data, xlsx_dir=None):
    """将 list[dict] 拆成表格写入 xlsx：固定列顺序、日期格式化为 YYYY-MM-DD。"""
    if not data or not isinstance(data, list):
        return
    try:
        import pandas as pd
        d = xlsx_dir or OUTPUT_XLSX_DIR
        ensure_dir(d)
        df = pd.DataFrame(data)
        # 日期列格式化为 YYYY-MM-DD，避免 Excel 显示 ######
        if "date" in df.columns:
            df["date"] = df["date"].apply(_format_date_for_excel)
        # 按约定列顺序排表（存在的列优先，其余列追加在后）
        ordered = [c for c in COUNTRY_TABLE_COLUMN_ORDER if c in df.columns]
        rest = [c for c in df.columns if c not in ordered]
        df = df[ordered + rest]
        path = d / f"{app_id}.xlsx"
        df.to_excel(path, index=False)
        print(f"  已写 XLSX: {path}")
    except Exception as e:
        print(f"  写 XLSX 跳过: {e}")


# 跳过时最多打印前几条详情，避免刷屏
MAX_SKIP_LOGS = 3

# 并发拉取时每个线程最多同时请求数（避免压垮 API 或触发限流）
DEFAULT_CONCURRENCY = 1  # API 有限额，串行请求


def _fetch_one(
    app_id,
    token,
    start_date,
    end_date,
    os_platform,
    date_granularity,
    countries,
    data_model,
    json_dir,
    xlsx_dir,
    save_xlsx_too,
    debug,
):
    """单个 app 拉取并落盘，供线程池调用。返回 (app_id, None) 成功，(app_id, exception) 失败。"""
    app_id = app_id.strip()
    if not app_id:
        return (app_id, None)
    try:
        session = get_session(token)
        if debug:
            print(f"  [请求] app_id={app_id}, start_date={start_date}, end_date={end_date}, countries={countries}")
        data = fetch_country_data_with_retry(
            session,
            app_id,
            os_platform=os_platform,
            start_date=start_date,
            end_date=end_date,
            date_granularity=date_granularity,
            countries=countries,
            data_model=data_model,
            debug=debug,
        )
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        save_json(app_id, data, json_dir=json_dir)
        if save_xlsx_too:
            save_xlsx(app_id, data, xlsx_dir=xlsx_dir)
        return (app_id, None)
    except Exception as e:
        return (app_id, e)


def run(
    app_ids,
    start_date=None,
    end_date=None,
    os_platform="unified",
    date_granularity="weekly",
    countries="ALL",
    data_model="DM_2025_Q2",
    save_xlsx_too=False,
    strict=False,
    year=None,
    week_tag=None,
    product_type=None,
    debug=False,
    concurrency=None,
):
    """
    strict=False：单个 app 请求失败时跳过并继续；strict=True 时任一失败即抛错。
    year、week_tag、product_type 若都提供，则写入 countiesdata/{年}/{周}/{product_type}/json（默认不写 xlsx，仅 json 供 build_final_join 与 MySQL 同步用）。
    concurrency：并发数，默认 1（串行）；设为 1 则退化为串行。
    """
    json_dir = OUTPUT_JSON_DIR
    xlsx_dir = OUTPUT_XLSX_DIR
    if year is not None and week_tag and product_type:
        json_dir = BASE_DIR / "countiesdata" / str(year) / week_tag / product_type / "json"
        xlsx_dir = BASE_DIR / "countiesdata" / str(year) / week_tag / product_type / "xlsx"
        print(f"  写入: countiesdata/{year}/{week_tag}/{product_type}/json" + (" 与 xlsx" if save_xlsx_too else ""))
    token = load_token()
    workers = int(concurrency) if concurrency is not None else DEFAULT_CONCURRENCY
    workers = max(1, min(workers, 20))
    app_ids = [a.strip() for a in app_ids if a and a.strip()]
    ok, fail = 0, 0
    if workers <= 1:
        total = len(app_ids)
        for idx, app_id in enumerate(app_ids, 1):
            print(f"  🔹 拉取地区数据：{idx}/{total} app_id={app_id}")
            _, exc = _fetch_one(
                app_id, token, start_date, end_date, os_platform,
                date_granularity, countries, data_model,
                json_dir, xlsx_dir, save_xlsx_too, debug,
            )
            if exc is None:
                ok += 1
            else:
                if fail < MAX_SKIP_LOGS:
                    print(f"  ⚠️ 跳过 {app_id}: {exc}")
                fail += 1
                if strict:
                    raise exc
    else:
        print(f"  地区数据并发数: {workers}")
        for idx, app_id in enumerate(app_ids, 1):
            print(f"  🔹 拉取地区数据：{idx}/{len(app_ids)} app_id={app_id}")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _fetch_one,
                    app_id, token, start_date, end_date, os_platform,
                    date_granularity, countries, data_model,
                    json_dir, xlsx_dir, save_xlsx_too, debug,
                ): app_id
                for app_id in app_ids
            }
            for fut in as_completed(futures):
                app_id, exc = fut.result()
                if exc is None:
                    ok += 1
                else:
                    if fail < MAX_SKIP_LOGS:
                        print(f"  ⚠️ 跳过 {app_id}: {exc}")
                    fail += 1
                    if strict:
                        raise exc
    if fail:
        if fail > MAX_SKIP_LOGS:
            print(f"  ... 其余 {fail - MAX_SKIP_LOGS} 个已跳过")
        print(f"  地区数据：成功 {ok}，跳过 {fail}")


def main():
    parser = argparse.ArgumentParser(
        description="拉取下载/收入估算（API: GET /v1/{os}/sales_report_estimates）"
    )
    parser.add_argument("--app_ids", nargs="+", help="一个或多个 app_id（Unified ID）")
    parser.add_argument("--app_ids_file", type=Path, help="每行一个 app_id 的文件")
    parser.add_argument("--start_date", help="开始日期 YYYY-MM-DD（必填建议）")
    parser.add_argument("--end_date", help="结束日期 YYYY-MM-DD（必填建议）")
    parser.add_argument("--os", dest="os_platform", default="unified", choices=["unified", "ios", "android"], help="平台，默认 unified（与接口一致）")
    parser.add_argument("--date_granularity", default="weekly", choices=["daily", "weekly", "monthly", "quarterly"], help="日期粒度，默认 weekly")
    parser.add_argument("--countries", default="ALL", help="国家：ALL=所有地区，或 WW/逗号分隔国家代码，默认 ALL")
    parser.add_argument("--data_model", default="DM_2025_Q2", choices=["DM_2025_Q1", "DM_2025_Q2"], help="数据模型，默认 DM_2025_Q2")
    parser.add_argument("--xlsx", action="store_true", help="同时写入 xlsx（默认仅写 json，供 build_final_join 与 MySQL 同步）")
    parser.add_argument("--strict", action="store_true", help="任一 app 请求失败即退出，默认跳过失败继续")
    parser.add_argument("--year", type=int, help="年份，与 --week、--product_type 一起指定时写入 countiesdata/{年}/{周}/{product_type}/")
    parser.add_argument("--week", dest="week_tag", help="周标签如 1201-1207")
    parser.add_argument("--product_type", choices=["strategy_old", "strategy_new"], help="与 --year、--week 一起时写入 countiesdata/{年}/{周}/strategy_old 或 strategy_new")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help=f"并发请求数，默认 {DEFAULT_CONCURRENCY}；设为 1 则串行")
    parser.add_argument("--test", action="store_true", help="测试模式：只请求第一个 app_id，打印详细调试信息")
    args = parser.parse_args()

    app_ids = []
    if args.app_ids:
        app_ids.extend(args.app_ids)
    if args.app_ids_file and args.app_ids_file.exists():
        app_ids.extend(args.app_ids_file.read_text(encoding="utf-8").strip().splitlines())
    if not app_ids:
        print("请提供 --app_ids 或 --app_ids_file")
        sys.exit(1)
    
    # 测试模式：只处理第一个 app_id
    if args.test:
        app_ids = app_ids[:1]
        print(f"  [TEST] 测试模式：只请求第一个 app_id: {app_ids[0]}")

    run(
        app_ids,
        start_date=args.start_date,
        end_date=args.end_date,
        os_platform=args.os_platform,
        date_granularity=args.date_granularity,
        countries=args.countries,
        data_model=args.data_model,
        save_xlsx_too=args.xlsx,
        strict=args.strict,
        year=args.year,
        week_tag=args.week_tag,
        product_type=args.product_type,
        debug=args.test,
        concurrency=args.concurrency,
    )


if __name__ == "__main__":
    main()
