# -*- coding: utf-8 -*-
"""从 MySQL 读取数据，返回与前端当前 JSON 一致的结构，供 start_server 的 /api/data/* 使用。"""
import json
import threading
import time

# 内存缓存，减少重复查库与解析大 JSON，缓解取数慢
_DATA_CACHE = {}
_CACHE_LOCK = threading.Lock()
_TTL_SHORT = 120   # 按周数据 2 分钟
_TTL_LONG = 300    # 周索引等 5 分钟

def _cache_get(key, ttl):
    with _CACHE_LOCK:
        ent = _DATA_CACHE.get(key)
        if ent is None:
            return None
        val, expire = ent
        if time.time() > expire:
            del _DATA_CACHE[key]
            return None
        return val

def _cache_set(key, value, ttl):
    with _CACHE_LOCK:
        _DATA_CACHE[key] = (value, time.time() + ttl)

def _get_conn():
    from .config import use_mysql
    if not use_mysql():
        return None
    from .connection import get_connection
    return get_connection()

def invalidate_weeks_index():
    """第一步或刷新周索引后调用，使下次 get_weeks_index 从 MySQL 重新读取，侧边栏能立即显示新周。"""
    with _CACHE_LOCK:
        _DATA_CACHE.pop(("weeks_index",), None)


def get_weeks_index():
    key = ("weeks_index",)
    v = _cache_get(key, _TTL_LONG)
    if v is not None:
        return v
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT config_value FROM app_config WHERE config_key = 'weeks_index'")
            row = cur.fetchone()
            if row:
                out = json.loads(row["config_value"])
                _cache_set(key, out, _TTL_LONG)
                return out
    except Exception:
        pass
    finally:
        conn.close()
    return None

def get_formatted(year, week_tag):
    key = ("formatted", str(year), str(week_tag))
    v = _cache_get(key, _TTL_SHORT)
    if v is not None:
        return v
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM formatted_data WHERE year = %s AND week_tag = %s", (int(year), week_tag))
            row = cur.fetchone()
            if row:
                out = json.loads(row["payload"])
                _cache_set(key, out, _TTL_SHORT)
                return out
    except Exception:
        pass
    finally:
        conn.close()
    return None

def get_product_strategy(year, week_tag, strategy_type):
    key = ("product_strategy", str(year), str(week_tag), strategy_type)
    v = _cache_get(key, _TTL_SHORT)
    if v is not None:
        return v
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM product_strategy WHERE year = %s AND week_tag = %s AND strategy_type = %s",
                (int(year), week_tag, strategy_type),
            )
            row = cur.fetchone()
            if row:
                out = json.loads(row["payload"])
                _cache_set(key, out, _TTL_SHORT)
                return out
    except Exception:
        pass
    finally:
        conn.close()
    return None

def get_creative_products(year, week_tag):
    key = ("creative_products", str(year), str(week_tag))
    v = _cache_get(key, _TTL_SHORT)
    if v is not None:
        return v
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM creative_products WHERE year = %s AND week_tag = %s", (int(year), week_tag))
            row = cur.fetchone()
            if row:
                out = json.loads(row["payload"])
                _cache_set(key, out, _TTL_SHORT)
                return out
    except Exception:
        pass
    finally:
        conn.close()
    return None

def _get_metrics_total_payload(year, week_tag):
    """内部：按年周取 metrics_total 整段 payload（供 get_metrics_total / get_metrics_total_product_names 复用并缓存）。"""
    key = ("metrics_total_payload", str(year), str(week_tag))
    v = _cache_get(key, _TTL_SHORT)
    if v is not None:
        return v
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM metrics_total WHERE year = %s AND week_tag = %s", (int(year), week_tag))
            row = cur.fetchone()
            if not row:
                return {"headers": [], "rows": []}
            out = json.loads(row["payload"])
            _cache_set(key, out, _TTL_SHORT)
            return out
    except Exception:
        pass
    finally:
        conn.close()
    return None

def get_metrics_total(year, week_tag, limit=1000, q=""):
    data = _get_metrics_total_payload(year, week_tag)
    if not data:
        return None
    headers = data.get("headers") or []
    rows = data.get("rows") or []
    if q:
        q_lower = q.lower()
        rows = [r for r in rows if any(str(c or "").lower().find(q_lower) >= 0 for c in (r or []))]
    total = len(rows)
    rows = rows[: limit]
    return {"headers": headers, "rows": rows, "total": total}

def get_metrics_total_product_names(year, week_tag):
    data = _get_metrics_total_payload(year, week_tag)
    if not data:
        return None
    headers = data.get("headers") or []
    rows = data.get("rows") or []
    name_idx = next((i for i, h in enumerate(headers) if (h or "").strip() == "Unified Name"), -1)
    id_idx = next((i for i, h in enumerate(headers) if (h or "").strip() == "Unified ID"), -1)
    product_names = []
    name_to_id = {}
    seen = set()
    for row in rows:
        if not row or name_idx < 0:
            continue
        name_val = (str(row[name_idx]).strip() if name_idx < len(row) and row[name_idx] is not None else "")
        if not name_val:
            continue
        if name_val not in seen:
            seen.add(name_val)
            product_names.append(name_val)
        if id_idx >= 0 and name_val and name_val not in name_to_id:
            id_val = (str(row[id_idx]).strip() if id_idx < len(row) and row[id_idx] is not None else "")
            if id_val:
                name_to_id[name_val] = id_val
    return {"productNames": product_names, "nameToUnifiedId": name_to_id}

def get_metrics_total_product_names_all():
    key = ("metrics_total_product_names_all",)
    v = _cache_get(key, _TTL_SHORT)
    if v is not None:
        return v
    wi = get_weeks_index()
    if not wi:
        return {"weeks": []}
    weeks_list = []
    for year_s, week_list in wi.items():
        if year_s == "data_range" or not isinstance(week_list, list):
            continue
        try:
            year = int(year_s)
        except ValueError:
            continue
        for week_tag in week_list:
            if not isinstance(week_tag, str):
                continue
            one = get_metrics_total_product_names(year, week_tag)
            if one:
                weeks_list.append({"year": year_s, "week": week_tag, "productNames": one["productNames"], "nameToUnifiedId": one["nameToUnifiedId"]})
    out = {"weeks": weeks_list}
    _cache_set(key, out, _TTL_SHORT)
    return out

def get_new_products():
    key = ("new_products",)
    v = _cache_get(key, _TTL_LONG)
    if v is not None:
        return v
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM new_products WHERE id = 1")
            row = cur.fetchone()
            if row:
                out = json.loads(row["payload"])
                _cache_set(key, out, _TTL_LONG)
                return out
    except Exception:
        pass
    finally:
        conn.close()
    return None

def get_product_theme_style_mapping():
    key = ("product_theme_style_mapping",)
    v = _cache_get(key, _TTL_LONG)
    if v is not None:
        return v
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM product_theme_style_mapping WHERE id = 1")
            row = cur.fetchone()
            if row:
                out = json.loads(row["payload"])
                _cache_set(key, out, _TTL_LONG)
                return out
    except Exception:
        pass
    finally:
        conn.close()
    return None

def _norm(s):
    return (s or "").strip()


def _product_name_match(name, cell):
    """产品名匹配：空视为不匹配；否则 strip 后相等或一方包含另一方。"""
    if not name:
        return False
    c = (cell or "").strip()
    if not c:
        return False
    return name == c or name in c or c in name


def get_product_detail_panels(year, week_tag, unified_id=None, product_name=None):
    """
    产品详情页「两数据面板」单请求取数：仅读 metrics_total（第一步产出）为主，辅以 product_strategy 判新/旧、mapping 取题材/画风。
    返回 None 表示该周无该产品；否则返回 { company, newOld, launch, install, rankInstall, revenue, rankRevenue, unifiedId, productName, theme, style }。
    """
    if not unified_id and not product_name:
        return None
    year, week_tag = str(year), str(week_tag)
    data = _get_metrics_total_payload(year, week_tag)
    if not data:
        return None
    headers = data.get("headers") or []
    rows = data.get("rows") or []
    if not headers or not rows:
        return None
    idx_uid = next((i for i, h in enumerate(headers) if _norm(h) == "Unified ID"), -1)
    idx_product = next((i for i, h in enumerate(headers) if _norm(h) == "产品归属"), -1)
    idx_company = next((i for i, h in enumerate(headers) if _norm(h) == "公司归属"), -1)
    idx_launch = next((i for i, h in enumerate(headers) if _norm(h) in ("第三方记录最早上线时间", "Earliest Release Date")), -1)
    idx_downloads = next((i for i, h in enumerate(headers) if _norm(h) == "All Time Downloads (WW)"), -1)
    idx_revenue = next((i for i, h in enumerate(headers) if _norm(h) == "All Time Revenue (WW)"), -1)
    idx_theme = next((i for i, h in enumerate(headers) if _norm(h) in ("题材标签", "题材")), -1)
    idx_style = next((i for i, h in enumerate(headers) if _norm(h) in ("画风标签", "画风")), -1)
    if idx_product < 0 and idx_uid < 0:
        return None
    target_uid = _norm(unified_id) if unified_id else None
    target_name = _norm(product_name) if product_name else None
    found_row = None
    for r in rows:
        if not r:
            continue
        uid_val = _norm(r[idx_uid]) if idx_uid >= 0 and idx_uid < len(r) else ""
        name_val = _norm(r[idx_product]) if idx_product >= 0 and idx_product < len(r) else ""
        if not uid_val and not name_val:
            continue
        if target_uid and uid_val == target_uid:
            found_row = r
            break
        if target_name and (name_val == target_name or _product_name_match(target_name, name_val)):
            found_row = r
            break
    if not found_row:
        return None
    unified_id_out = _norm(found_row[idx_uid]) if idx_uid >= 0 and idx_uid < len(found_row) else ""
    product_name_out = _norm(found_row[idx_product]) if idx_product >= 0 and idx_product < len(found_row) else ""
    company = _norm(found_row[idx_company]) if idx_company >= 0 and idx_company < len(found_row) else ""
    if company and "汇总" in company:
        company = ""
    launch = None
    if idx_launch >= 0 and idx_launch < len(found_row) and found_row[idx_launch] not in (None, ""):
        launch = found_row[idx_launch]
    install = found_row[idx_downloads] if idx_downloads >= 0 and idx_downloads < len(found_row) else None
    revenue = found_row[idx_revenue] if idx_revenue >= 0 and idx_revenue < len(found_row) else None
    def to_float(v):
        if v is None or v == "":
            return 0.0
        if isinstance(v, (int, float)):
            return float(v) if v == v else 0.0
        s = str(v).replace(",", "").replace("$", "").strip()
        try:
            return float(s) if s else 0.0
        except ValueError:
            return 0.0
    install_num = to_float(install)
    revenue_num = to_float(revenue)
    products_with_rank = []
    for r in rows:
        if not r or idx_uid < 0 or idx_product < 0:
            continue
        uid = _norm(r[idx_uid]) if idx_uid < len(r) else ""
        pname = _norm(r[idx_product]) if idx_product < len(r) else ""
        if not uid and not pname:
            continue
        products_with_rank.append({
            "uid": uid or pname,
            "downloads": to_float(r[idx_downloads] if idx_downloads >= 0 and idx_downloads < len(r) else None),
            "revenue": to_float(r[idx_revenue] if idx_revenue >= 0 and idx_revenue < len(r) else None),
        })
    by_downloads = sorted(products_with_rank, key=lambda x: x["downloads"], reverse=True)
    by_revenue = sorted(products_with_rank, key=lambda x: x["revenue"], reverse=True)
    key_uid = unified_id_out or product_name_out
    rank_install = next((i + 1 for i, p in enumerate(by_downloads) if p["uid"] == key_uid), 0)
    rank_revenue = next((i + 1 for i, p in enumerate(by_revenue) if p["uid"] == key_uid), 0)
    new_old = "old"
    for stype in ("old", "new"):
        strat = get_product_strategy(year, week_tag, stype)
        if not strat or not strat.get("rows"):
            continue
        h = strat.get("headers") or []
        pidxs = [i for i, hh in enumerate(h) if _norm(hh) in ("Unified ID", "产品归属")]
        for row in strat.get("rows") or []:
            if not row:
                continue
            for idx in pidxs:
                if idx < len(row) and _norm(row[idx]) in (unified_id_out, product_name_out):
                    new_old = stype
                    break
    theme = None
    style = None
    mapping = get_product_theme_style_mapping()
    if mapping:
        by_uid = (mapping.get("byUnifiedId") or {}) if isinstance(mapping, dict) else {}
        by_name = (mapping.get("byProductName") or {}) if isinstance(mapping, dict) else {}
        entry = None
        if unified_id_out and by_uid:
            entry = by_uid.get(unified_id_out)
        if not entry and product_name_out and by_name:
            entry = by_name.get(product_name_out)
        if not entry and by_name and product_name_out:
            for k, v in by_name.items():
                if k and (k == product_name_out or product_name_out in k or k in product_name_out):
                    entry = v
                    break
        if entry and isinstance(entry, dict):
            theme = entry.get("题材") or entry.get("theme") or (entry.get("题材标签") if isinstance(entry.get("题材标签"), str) else None)
            style = entry.get("画风") or entry.get("style") or (entry.get("画风标签") if isinstance(entry.get("画风标签"), str) else None)
    if theme is None and idx_theme >= 0 and idx_theme < len(found_row) and found_row[idx_theme] not in (None, ""):
        theme = found_row[idx_theme]
    if style is None and idx_style >= 0 and idx_style < len(found_row) and found_row[idx_style] not in (None, ""):
        style = found_row[idx_style]
    return {
        "company": company or None,
        "newOld": new_old,
        "launch": launch,
        "install": install,
        "rankInstall": rank_install or None,
        "revenue": revenue,
        "rankRevenue": rank_revenue or None,
        "unifiedId": unified_id_out or None,
        "productName": product_name_out or None,
        "theme": theme,
        "style": style,
    }


def get_company_detail_panels(year, week_tag, company_name):
    """
    公司详情页 4 卡片轻量取数：按周从 metrics_total 按公司归属汇总累计安装/流水并计算赛道排名。
    返回 None 表示无数据；否则返回 { sumInstall, sumRevenue, rankInstall, rankRevenue }（数值，前端做千分位）。
    """
    if not company_name or not (str(company_name or "").strip()):
        return None
    year, week_tag = str(year), str(week_tag)
    target_company = _norm(company_name)
    data = _get_metrics_total_payload(year, week_tag)
    if not data:
        return None
    headers = data.get("headers") or []
    rows = data.get("rows") or []
    if not headers or not rows:
        return None
    idx_company = next((i for i, h in enumerate(headers) if _norm(h) == "公司归属"), -1)
    idx_product = next((i for i, h in enumerate(headers) if _norm(h) == "产品归属"), -1)
    idx_uid = next((i for i, h in enumerate(headers) if _norm(h) == "Unified ID"), -1)
    idx_downloads = next((i for i, h in enumerate(headers) if _norm(h) == "All Time Downloads (WW)"), -1)
    idx_revenue = next((i for i, h in enumerate(headers) if _norm(h) == "All Time Revenue (WW)"), -1)
    if idx_company < 0 or (idx_downloads < 0 and idx_revenue < 0):
        return None

    def to_float(v):
        if v is None or v == "":
            return 0.0
        if isinstance(v, (int, float)):
            return float(v) if v == v else 0.0
        s = str(v).replace(",", "").replace("$", "").strip()
        try:
            return float(s) if s else 0.0
        except ValueError:
            return 0.0

    company_totals = {}
    seen_per_company = {}
    for r in rows:
        if not r:
            continue
        company = _norm(r[idx_company]) if idx_company >= 0 and idx_company < len(r) else ""
        if not company or "汇总" in company:
            continue
        uid = _norm(r[idx_uid]) if idx_uid >= 0 and idx_uid < len(r) else ""
        product = _norm(r[idx_product]) if idx_product >= 0 and idx_product < len(r) else ""
        key = uid or product
        if not key:
            continue
        if company not in seen_per_company:
            seen_per_company[company] = set()
            company_totals[company] = {"install": 0.0, "revenue": 0.0}
        if key in seen_per_company[company]:
            continue
        seen_per_company[company].add(key)
        company_totals[company]["install"] += to_float(r[idx_downloads] if idx_downloads >= 0 and idx_downloads < len(r) else None)
        company_totals[company]["revenue"] += to_float(r[idx_revenue] if idx_revenue >= 0 and idx_revenue < len(r) else None)

    if target_company not in company_totals:
        return None
    tot = company_totals[target_company]
    sum_install = tot["install"]
    sum_revenue = tot["revenue"]
    by_install = sorted(company_totals.keys(), key=lambda c: company_totals[c]["install"], reverse=True)
    by_revenue = sorted(company_totals.keys(), key=lambda c: company_totals[c]["revenue"], reverse=True)
    rank_install = next((i + 1 for i, c in enumerate(by_install) if _norm(c) == target_company), 0)
    rank_revenue = next((i + 1 for i, c in enumerate(by_revenue) if _norm(c) == target_company), 0)
    return {
        "sumInstall": sum_install,
        "sumRevenue": sum_revenue,
        "rankInstall": rank_install or None,
        "rankRevenue": rank_revenue or None,
    }


def get_basetable(name):
    key = ("basetable", str(name))
    v = _cache_get(key, _TTL_SHORT)
    if v is not None:
        return v
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT headers, `rows` FROM basetable WHERE name = %s", (name,))
            row = cur.fetchone()
            if row:
                out = {"headers": json.loads(row["headers"]), "rows": json.loads(row["rows"])}
                _cache_set(key, out, _TTL_SHORT)
                return out
    except Exception:
        pass
    finally:
        conn.close()
    return None
