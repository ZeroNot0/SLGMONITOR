#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
拉取广告创意数据，按 app_id + 地区写入 advertisements/{年}/{周}/ 对应日期文件夹下。

使用 Sensor Tower API：GET /v1/{os}/ad_intel/creatives
- 路径：{os} 为 ios、android 或 unified
- 参数：app_ids（必填，逗号分隔）、start_date（必填）、end_date、countries（必填，国家代码逗号分隔）
pipeline 传入 Unified ID 作为 app_id。
"""

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ST API 网络不稳定：失败时延迟后重试，最多请求 3 次
RETRY_MAX = 3
RETRY_DELAY_SEC = 3

REQUEST_DIR = Path(__file__).resolve().parent
BASE_DIR = REQUEST_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from request.api_request import load_token, get_session, get, ensure_dir

try:
    from app.app_paths import get_data_root
    ADS_ROOT = get_data_root() / "advertisements"
except Exception:
    ADS_ROOT = BASE_DIR / "advertisements"

# 未指定 --year/--week 时的兜底输出（request 下）
FALLBACK_JSON_DIR = REQUEST_DIR / "ad_creatives" / "json"
FALLBACK_XLSX_DIR = REQUEST_DIR / "ad_creatives" / "xlsx"

# 地区标签（与 mapping/市场T度 一致，亚洲 T1 排除 CN，未在表中的国家归 T3）
REGIONS = ["亚洲T1", "欧美T1", "T2", "T3"]

# 创意 API：只要 video 类型
AD_TYPES_VIDEO = ["video"]

# 创意 API：所有网络（与文档一致）
NETWORKS = [
    "Admob", "Applovin", "BidMachine", "Chartboost", "Digital Turbine",
    "Facebook", "InMobi", "Instagram", "Line", "Meta Audience Network",
    "Mintegral", "Moloco", "Pangle", "Pinterest", "Smaato", "Snapchat",
    "Supersonic", "TikTok", "Twitter", "Unity", "Verve", "Vungle", "Youtube",
]

# T3 市场国家代码：完整 50 国列表中未在 市场T度.csv（亚洲T1/欧美T1/T2）的国家，共 26 个
T3_COUNTRIES = [
    "AR", "BR", "CL", "CN", "CO", "CZ", "EC", "GR", "ID", "IN", "LU",
    "MX", "MY", "NG", "PA", "PE", "PH", "PL", "PT", "RO", "RU", "TH",
    "TR", "UA", "VN", "ZA",
]


def week_tag_to_dates(year: int, week_tag: str):
    """将 week_tag（如 0119-0125）和 year 转为 start_date、end_date（YYYY-MM-DD）。跨年周（如 1229-0104）时 end_date 用 year+1。"""
    if not year or not week_tag:
        return None, None
    s = (week_tag or "").strip()
    m = re.match(r"^(\d{2})(\d{2})-(\d{2})(\d{2})$", s)
    if not m:
        return None, None
    m1, d1, m2, d2 = m.group(1), m.group(2), m.group(3), m.group(4)
    try:
        start_date = f"{year}-{m1}-{d1}"
        # 跨年周：如 1229-0104，结束月在 01 小于开始月 12，则 end 用 year+1
        end_year = year if int(m2) >= int(m1) else year + 1
        end_date = f"{end_year}-{m2}-{d2}"
        return start_date, end_date
    except Exception:
        return None, None


def load_market_countries():
    """
    从 mapping/市场T度.csv 读取 亚洲T1 / 欧美T1 / T2 国家代码；
    T3 使用脚本内常量 T3_COUNTRIES。
    返回 dict: {"亚洲T1": "TW,HK,...", "欧美T1": "US,GB,...", "T2": "IT,ES,...", "T3": "BR,MX,..."}
    """
    csv_path = BASE_DIR / "mapping" / "市场T度.csv"
    by_region = {r: [] for r in REGIONS}
    if csv_path.exists():
        import csv
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                c = (row.get("country") or "").strip()
                t = (row.get("T度") or "").strip()
                if c and t and t in by_region:
                    by_region[t].append(c)
    by_region["T3"] = T3_COUNTRIES
    return {r: ",".join(by_region[r]) for r in REGIONS if by_region[r]}


def fetch_ad_creatives_for_app(
    session,
    app_id,
    os_platform="unified",
    start_date=None,
    end_date=None,
    countries="WW",
    ad_types=None,
    networks=None,
):
    """
    请求单个 app 的广告创意数据。
    API: GET /v1/{os}/ad_intel/creatives
    参数：app_ids、start_date、end_date、countries（必填）、ad_types、networks（必填）。
    """
    path = f"{os_platform}/ad_intel/creatives"
    params = {
        "app_ids": app_id,
        "countries": countries,
        "ad_types": ad_types or ",".join(AD_TYPES_VIDEO),
        "networks": networks or ",".join(NETWORKS),
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    resp = get(session, path, params=params)
    resp.raise_for_status()
    return resp.json()


def fetch_ad_creatives_with_retry(
    session,
    app_id,
    os_platform="unified",
    start_date=None,
    end_date=None,
    countries="WW",
    ad_types=None,
    networks=None,
):
    """带重试的请求：失败时延迟 RETRY_DELAY_SEC 秒后重试，最多 RETRY_MAX 次。"""
    last_e = None
    for attempt in range(RETRY_MAX):
        try:
            return fetch_ad_creatives_for_app(
                session,
                app_id,
                os_platform=os_platform,
                start_date=start_date,
                end_date=end_date,
                countries=countries,
                ad_types=ad_types,
                networks=networks,
            )
        except Exception as e:
            last_e = e
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_DELAY_SEC)
    raise last_e


def _safe_folder_name(s):
    """文件夹名中避免 Windows 非法字符。"""
    return re.sub(r'[<>:"/\\|?*]', "_", (s or "").strip()) or "unknown"


def get_output_dirs(year, week_tag, product_type, app_id, product_name):
    """
    返回 (json_dir, xlsx_dir) 用于 advertisements 下的日期文件夹。
    路径：advertisements/{year}/{week_tag}/{product_type}/{app_id}_{product_name}/json 与 xlsx
    """
    folder = f"{app_id}_{_safe_folder_name(product_name)}"
    base = ADS_ROOT / str(year) / week_tag / product_type / folder
    return base / "json", base / "xlsx"


def save_json(app_id, data, json_dir=None, suffix=""):
    """suffix 为空时保存为 {app_id}.json，否则为 {app_id}_{suffix}.json"""
    json_dir = json_dir or FALLBACK_JSON_DIR
    ensure_dir(json_dir)
    name = f"{app_id}_{suffix}.json" if suffix else f"{app_id}.json"
    path = json_dir / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  已写 JSON: {path}")


def save_xlsx(app_id, data, xlsx_dir=None, product_name=None, suffix=""):
    """若 data 为 dict 且含可表格化内容，则导出 xlsx。suffix 用于文件名区分（如 countries）。"""
    try:
        import pandas as pd
        if not isinstance(data, dict):
            return
        ad_units = data.get("ad_units") or data.get("creatives") or []
        if not ad_units:
            return
        xlsx_dir = xlsx_dir or FALLBACK_XLSX_DIR
        ensure_dir(xlsx_dir)
        df = pd.DataFrame(ad_units)
        name = _safe_folder_name(product_name) if product_name else app_id
        base = f"{app_id}_{name}"
        path = xlsx_dir / f"{base}_{suffix}.xlsx" if suffix else f"{base}.xlsx"
        df.to_excel(path, index=False)
        print(f"  已写 XLSX: {path}")
    except Exception as e:
        print(f"  写 XLSX 跳过: {e}")


# 跳过时最多打印前几条详情，避免刷屏
MAX_SKIP_LOGS = 3

# 创意数据并发数（每产品 4 个市场，并发可显著缩短总耗时）
DEFAULT_CONCURRENCY = 1  # API 有限额，串行请求


def _fetch_one_region(
    app_id,
    product_name,
    region,
    token,
    start_date,
    end_date,
    os_platform,
    market_countries,
    use_advertisements,
    year,
    week_tag,
    product_type,
    save_xlsx_too,
):
    """单次 (app_id, region) 拉取并落盘，供线程池调用。返回 (app_id, region, None) 成功，(app_id, region, exception) 失败。"""
    app_id = (app_id or "").strip()
    if not app_id:
        return (app_id, region, None)
    pname = (product_name or app_id).strip() or app_id
    region_countries = market_countries.get(region)
    if not region_countries:
        return (app_id, region, None)
    json_dir = xlsx_dir = None
    if use_advertisements and year and week_tag:
        json_dir, xlsx_dir = get_output_dirs(year, week_tag, product_type, app_id, pname)
    try:
        print(f"  🔹 拉取创意数据：app_id={app_id} 产品={pname} 地区={region}")
        session = get_session(token)
        data = fetch_ad_creatives_with_retry(
            session,
            app_id,
            os_platform=os_platform,
            start_date=start_date,
            end_date=end_date,
            countries=region_countries,
            ad_types=",".join(AD_TYPES_VIDEO),
            networks=",".join(NETWORKS),
        )
        save_json(app_id, data, json_dir=json_dir, suffix=region)
        if save_xlsx_too:
            save_xlsx(app_id, data, xlsx_dir=xlsx_dir, product_name=pname, suffix=region)
        return (app_id, region, None)
    except Exception as e:
        return (app_id, region, e)


def run(
    app_list,
    start_date=None,
    end_date=None,
    os_platform="unified",
    countries=None,
    save_xlsx_too=False,
    year=None,
    week_tag=None,
    product_type="strategy_old",
    strict=False,
    concurrency=None,
):
    """
    app_list: list of (app_id, product_name)。
    API: GET /v1/{os}/ad_intel/creatives。每个产品请求 4 次，分别对应 4 个市场（亚洲T1、欧美T1、T2、T3），
    只拉 video 类型、全部指定 networks；结果按市场存为 {app_id}_亚洲T1.json 等。
    year, week_tag 若都提供，则写入 advertisements/{year}/{week_tag}/{product_type}/ 下（默认仅 json，不写 xlsx）。
    strict=False：单个请求失败时跳过并继续；strict=True 时任一失败即抛错。
    concurrency：并发数，默认 1（串行）。
    """
    token = load_token()
    use_advertisements = year is not None and week_tag is not None
    market_countries = load_market_countries()
    workers = int(concurrency) if concurrency is not None else DEFAULT_CONCURRENCY
    workers = max(1, min(workers, 20))

    tasks = []
    for app_id, product_name in app_list:
        app_id = (app_id or "").strip()
        if not app_id:
            continue
        pname = (product_name or app_id).strip() or app_id
        for region in REGIONS:
            if market_countries.get(region):
                tasks.append((app_id, pname, region))

    ok, fail = 0, 0
    if workers <= 1:
        session = get_session(token)
        for app_id, pname, region in tasks:
            print(f"  🔹 拉取创意数据：app_id={app_id} 产品={pname} 地区={region}")
            json_dir = xlsx_dir = None
            if use_advertisements and year and week_tag:
                json_dir, xlsx_dir = get_output_dirs(year, week_tag, product_type, app_id, pname)
            region_countries = market_countries.get(region)
            try:
                data = fetch_ad_creatives_with_retry(
                    session, app_id, os_platform=os_platform,
                    start_date=start_date, end_date=end_date,
                    countries=region_countries,
                    ad_types=",".join(AD_TYPES_VIDEO),
                    networks=",".join(NETWORKS),
                )
                save_json(app_id, data, json_dir=json_dir, suffix=region)
                if save_xlsx_too:
                    save_xlsx(app_id, data, xlsx_dir=xlsx_dir, product_name=pname, suffix=region)
                ok += 1
            except Exception as e:
                if fail < MAX_SKIP_LOGS:
                    print(f"  ⚠️ 跳过 {app_id} {region}: {e}")
                fail += 1
                if strict:
                    raise
    else:
        print(f"  创意数据并发数: {workers}")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _fetch_one_region,
                    app_id, pname, region, token,
                    start_date, end_date, os_platform, market_countries,
                    use_advertisements, year, week_tag, product_type, save_xlsx_too,
                ): (app_id, region)
                for app_id, pname, region in tasks
            }
            for fut in as_completed(futures):
                app_id, region, exc = fut.result()
                if exc is None:
                    ok += 1
                else:
                    if fail < MAX_SKIP_LOGS:
                        print(f"  ⚠️ 跳过 {app_id} {region}: {exc}")
                    fail += 1
                    if strict:
                        raise exc
    if fail:
        if fail > MAX_SKIP_LOGS:
            print(f"  ... 其余 {fail - MAX_SKIP_LOGS} 次已跳过")
        print(f"  创意数据：成功 {ok}，跳过 {fail}")


def parse_app_list(app_ids=None, app_ids_file=None, app_list_file=None):
    """
    解析为 [(app_id, product_name), ...]。
    --app_ids / --app_ids_file：仅 app_id，product_name 用 app_id；
    --app_list_file：每行 app_id,产品名 或 app_id\\t产品名，逗号/制表符分隔。
    """
    out = []
    if app_list_file and app_list_file.exists():
        for line in app_list_file.read_text(encoding="utf-8").strip().splitlines():
            line = line.strip()
            if not line:
                continue
            parts = re.split(r"[\t,]", line, maxsplit=1)
            app_id = (parts[0] or "").strip()
            product_name = (parts[1] or "").strip() if len(parts) > 1 else app_id
            if app_id:
                out.append((app_id, product_name))
    if app_ids:
        for a in app_ids:
            a = (a or "").strip()
            if a:
                out.append((a, a))
    if app_ids_file and app_ids_file.exists():
        for line in app_ids_file.read_text(encoding="utf-8").strip().splitlines():
            a = (line or "").strip()
            if a:
                out.append((a, a))
    return out


def main():
    parser = argparse.ArgumentParser(
        description="拉取广告创意数据，写入 advertisements/{年}/{周}/ 或 request/ad_creatives/"
    )
    parser.add_argument("--app_ids", nargs="+", help="一个或多个 app_id（产品名用 app_id）")
    parser.add_argument("--app_ids_file", type=Path, help="每行一个 app_id 的文件")
    parser.add_argument(
        "--app_list_file",
        type=Path,
        help="每行 app_id,产品名 或 app_id\\t产品名，用于 advertisements 下文件夹名",
    )
    parser.add_argument("--year", type=int, help="年份，与 --week 一起指定时写入 advertisements/{年}/{周}/")
    parser.add_argument("--week", dest="week_tag", help="周标签如 0119-0125，与 --year 一起时写入 advertisements")
    parser.add_argument(
        "--product_type",
        default="strategy_old",
        help="产品类型子目录，默认 strategy_old（strategy_new 等）",
    )
    parser.add_argument("--os", dest="os_platform", default="unified", choices=["ios", "android", "unified"], help="平台，API 路径 /v1/{os}/ad_intel/creatives，默认 unified")
    parser.add_argument("--countries", default="WW", help="国家代码逗号分隔，默认 WW（全球）")
    parser.add_argument("--start_date", help="开始日期 YYYY-MM-DD（API 必填建议）")
    parser.add_argument("--end_date", help="结束日期 YYYY-MM-DD")
    parser.add_argument("--xlsx", action="store_true", help="同时写入 xlsx（默认仅写 json，供 MySQL 同步）")
    parser.add_argument("--strict", action="store_true", help="任一请求失败即退出，默认跳过失败继续")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help=f"并发请求数，默认 {DEFAULT_CONCURRENCY}；设为 1 则串行")
    args = parser.parse_args()

    app_list = parse_app_list(
        app_ids=args.app_ids,
        app_ids_file=args.app_ids_file,
        app_list_file=args.app_list_file,
    )
    if not app_list:
        print("请提供 --app_ids、--app_ids_file 或 --app_list_file（含 app_id[,产品名]）")
        sys.exit(1)

    # 有 --year 和 --week 时自动补全 start_date / end_date，确保创意 API 始终带 end_date
    start_date = args.start_date
    end_date = args.end_date
    if args.year and args.week_tag:
        derived_start, derived_end = week_tag_to_dates(int(args.year), args.week_tag)
        if derived_start and not start_date:
            start_date = derived_start
        if derived_end and not end_date:
            end_date = derived_end
    if start_date and not end_date:
        try:
            from datetime import datetime, timedelta
            dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = (dt + timedelta(days=6)).strftime("%Y-%m-%d")
        except Exception:
            pass

    run(
        app_list,
        start_date=start_date,
        end_date=end_date,
        os_platform=args.os_platform,
        countries=args.countries,
        save_xlsx_too=args.xlsx,
        year=args.year,
        week_tag=args.week_tag,
        product_type=args.product_type or "strategy_old",
        strict=args.strict,
        concurrency=args.concurrency,
    )


if __name__ == "__main__":
    main()
