# -*- coding: utf-8 -*-
"""
上传维护类接口成功后，将对应文件同步到 MySQL（USE_MYSQL 时生效）。
- sync_basetable_from_files: 归属表/标签表 Excel → basetable
- sync_new_products_from_file: frontend/data/new_products.json → new_products
"""
import json
from pathlib import Path

try:
    import pymysql
except ImportError:
    pymysql = None

def _get_base_dir():
    from .config import BASE_DIR
    return BASE_DIR

def _excel_to_headers_rows(path):
    if not path or not path.is_file():
        return [], []
    try:
        import pandas as pd
        df = pd.read_excel(path)
    except Exception:
        return [], []
    if df.empty:
        return [], []
    headers = [str(c).strip() if c is not None else "" for c in df.columns]
    rows = []
    isnan = getattr(pd, "isna", lambda x: x != x)
    for _, r in df.iterrows():
        row = []
        for v in r:
            if isnan(v):
                row.append("")
            elif isinstance(v, (int, float)) and not isinstance(v, bool):
                row.append(int(v) if v == int(v) else v)
            else:
                row.append(str(v).strip() if v is not None else "")
        rows.append(row)
    return headers, rows


def sync_basetable_from_files(conn, base_dir: Path = None) -> bool:
    """将 mapping/、labels/ 下 Excel 同步到 basetable（产品/公司归属、题材/玩法/画风标签）。"""
    if not conn or not pymysql:
        return False
    base_dir = base_dir or _get_base_dir()
    mapping_dir = base_dir / "mapping"
    labels_dir = base_dir / "labels"
    sources = {
        "product_mapping": mapping_dir / "产品归属.xlsx",
        "company_mapping": mapping_dir / "公司归属.xlsx",
        "theme_label": labels_dir / "题材标签表.xlsx",
        "gameplay_label": labels_dir / "玩法标签表.xlsx",
        "art_style_label": labels_dir / "画风标签表.xlsx",
    }
    # 标签表缺省表头（文件缺失时仍写入一条空底表记录，保证数据底表 5 张在库中均存在）
    default_label_headers = ["序号", "标签名", "备注"]
    try:
        with conn.cursor() as cur:
            for name, xlsx_path in sources.items():
                headers, rows = _excel_to_headers_rows(xlsx_path)
                if not headers and not rows and name in ("theme_label", "gameplay_label", "art_style_label"):
                    headers, rows = default_label_headers, []
                if headers or rows:
                    h_val = json.dumps(headers, ensure_ascii=False)
                    r_val = json.dumps(rows, ensure_ascii=False)
                    cur.execute(
                        """INSERT INTO basetable (name, headers, `rows`) VALUES (%s, %s, %s)
                           ON DUPLICATE KEY UPDATE headers = VALUES(headers), `rows` = VALUES(`rows`)""",
                        (name, h_val, r_val),
                    )
        conn.commit()
        return True
    except Exception:
        if conn:
            conn.rollback()
        return False


def _merge_company_mapping_from_rows(conn, rows: list) -> None:
    """
    将 product 行中的 (发行商, 公司归属) 合并进 basetable 的 company_mapping（仅当两者均非空时）。
    rows: list of [产品名, Unified ID, 产品归属, 题材, 画风, 发行商, 公司归属]，索引 5=发行商，6=公司归属。
    """
    if not conn or not pymysql or not rows:
        return
    comp_headers = ["序号", "发行商", "公司归属"]
    idx_pub, idx_comp = 5, 6
    pairs = []
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) <= max(idx_pub, idx_comp):
            continue
        pub = str(r[idx_pub]).strip() if r[idx_pub] is not None else ""
        comp = str(r[idx_comp]).strip() if r[idx_comp] is not None else ""
        if pub and comp:
            pairs.append((pub, comp))
    if not pairs:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT headers, `rows` FROM basetable WHERE name = %s", ("company_mapping",))
            row = cur.fetchone()
            existing_headers = comp_headers
            existing_rows = []
            if row and row.get("rows"):
                try:
                    existing_rows = json.loads(row["rows"])
                    if row.get("headers"):
                        existing_headers = json.loads(row["headers"])
                except Exception:
                    pass
            pub_idx = existing_headers.index("发行商") if "发行商" in existing_headers else 1
            comp_idx = existing_headers.index("公司归属") if "公司归属" in existing_headers else 2
            by_pub = {}
            for r in existing_rows:
                if isinstance(r, (list, tuple)) and len(r) > max(pub_idx, comp_idx):
                    p = str(r[pub_idx]).strip() if r[pub_idx] is not None else ""
                    c = str(r[comp_idx]).strip() if r[comp_idx] is not None else ""
                    if p:
                        by_pub[p] = c
            for pub, comp in pairs:
                if pub:
                    by_pub[pub] = comp
            merged_rows = [[i, pub, comp] for i, (pub, comp) in enumerate(by_pub.items(), 1)]
            cur.execute(
                """INSERT INTO basetable (name, headers, `rows`) VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE headers = VALUES(headers), `rows` = VALUES(`rows`)""",
                ("company_mapping", json.dumps(comp_headers, ensure_ascii=False), json.dumps(merged_rows, ensure_ascii=False)),
            )
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()


def append_product_mapping_rows(conn, new_rows: list) -> int:
    """
    将新行追加到 basetable 的 product_mapping（仅追加 产品归属 不在表中的行）。
    若不空的 发行商/公司归属 存在，会一并合并进 basetable 的 company_mapping。
    new_rows: list of [产品名, Unified ID, 产品归属, 题材, 画风, 发行商, 公司归属]（与 OUT_COLS 顺序一致）。
    返回实际追加条数。
    """
    if not conn or not pymysql or not new_rows:
        return 0
    headers = ["产品名（实时更新中）", "Unified ID", "产品归属", "题材", "画风", "发行商", "公司归属"]
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT headers, `rows` FROM basetable WHERE name = %s", ("product_mapping",))
            row = cur.fetchone()
            existing_headers = headers
            existing_rows = []
            if row and row.get("rows"):
                try:
                    existing_rows = json.loads(row["rows"])
                    if row.get("headers"):
                        existing_headers = json.loads(row["headers"])
                except Exception:
                    pass
            existing_belong = set()
            for r in existing_rows:
                if isinstance(r, (list, tuple)) and len(r) >= 3:
                    existing_belong.add(str(r[2]).strip() if r[2] is not None else "")
            to_append = []
            for r in new_rows:
                if not isinstance(r, (list, tuple)) or len(r) < 7:
                    continue
                belong = str(r[2]).strip() if r[2] is not None else ""
                if not belong or belong in existing_belong:
                    continue
                existing_belong.add(belong)
                to_append.append([str(x).strip() if x is not None else "" for x in r[:7]])
            if not to_append:
                return 0
            merged_rows = existing_rows + to_append
            cur.execute(
                """INSERT INTO basetable (name, headers, `rows`) VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE headers = VALUES(headers), `rows` = VALUES(`rows`)""",
                ("product_mapping", json.dumps(existing_headers, ensure_ascii=False), json.dumps(merged_rows, ensure_ascii=False)),
            )
        conn.commit()
        if to_append:
            _merge_company_mapping_from_rows(conn, to_append)
        return len(to_append)
    except Exception:
        if conn:
            conn.rollback()
        return 0


def sync_new_products_from_file(conn, base_dir: Path = None) -> bool:
    """将 frontend/data/new_products.json 同步到 new_products 表。"""
    if not conn or not pymysql:
        return False
    base_dir = base_dir or _get_base_dir()
    json_path = base_dir / "frontend" / "data" / "new_products.json"
    if not json_path.is_file():
        return False
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        val = json.dumps(payload, ensure_ascii=False)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO new_products (id, payload) VALUES (1, %s) ON DUPLICATE KEY UPDATE payload = VALUES(payload)",
                (val,),
            )
        conn.commit()
        return True
    except Exception:
        if conn:
            conn.rollback()
        return False
