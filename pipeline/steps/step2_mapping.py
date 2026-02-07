import pandas as pd
from pathlib import Path
import argparse

def run_step2(week_tag: str = None, year: int = None):

    BASE_DIR = Path(__file__).resolve().parent.parent.parent   # 项目根目录
    
    # === 构建输入文件路径 ===
    if week_tag and year:
        INPUT_FILE = BASE_DIR / "intermediate" / str(year) / week_tag / "merged_deduplicated.xlsx"
    else:
        # 向后兼容：查找最新的merged_deduplicated.xlsx
        intermediate_dir = BASE_DIR / "intermediate"
        if week_tag and not year:
            # 只有week_tag，查找匹配的文件
            pattern = f"*/{week_tag}/merged_deduplicated.xlsx"
            files = list(intermediate_dir.glob(pattern))
            if files:
                INPUT_FILE = sorted(files)[-1]  # 取最新的
            else:
                INPUT_FILE = BASE_DIR / "intermediate" / "merged_deduplicated.xlsx"
        else:
            INPUT_FILE = BASE_DIR / "intermediate" / "merged_deduplicated.xlsx"
    
    # === 构建输出文件路径 ===
    if week_tag and year:
        OUTPUT_DIR = BASE_DIR / "intermediate" / str(year) / week_tag
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_FILE = OUTPUT_DIR / "mapped_total.xlsx"
    else:
        OUTPUT_FILE = BASE_DIR / "intermediate" / "mapped_total.xlsx"

    # === 读取 STEP1 总表 ===
    df = pd.read_excel(INPUT_FILE)

    # === 将 "Earliest Release Date" 重命名为 "第三方记录最早上线时间"（如果存在）===
    if "Earliest Release Date" in df.columns and "第三方记录最早上线时间" not in df.columns:
        df = df.rename(columns={"Earliest Release Date": "第三方记录最早上线时间"})

    # === 读取映射表 ===
    prod_map_raw = pd.read_excel(BASE_DIR / "mapping" / "产品归属.xlsx")
    comp_map = pd.read_excel(BASE_DIR / "mapping" / "公司归属.xlsx")
    coef_map = pd.read_excel(BASE_DIR / "mapping" / "流水系数.xlsx")

    # ---------------------------------------------------
    # 1. 产品归属（按列名取「产品名」与「产品归属」，避免列顺序变化导致匹配错）
    # 总表用 Unified Name 与映射表的产品名列匹配，得到 产品归属
    # ---------------------------------------------------
    def _norm_col(s):
        return (s or "").strip()

    prod_cols = {_norm_col(c): c for c in prod_map_raw.columns}
    # 用作匹配的列：与总表 Unified Name 对应
    name_candidates = ["产品名（实时更新中）", "Unified Name", "产品名", "Unified name"]
    prod_name_col = None
    for cand in name_candidates:
        if cand in prod_cols:
            prod_name_col = prod_cols[cand]
            break
    # 产品归属列
    belong_cand = "产品归属"
    prod_belong_col = prod_cols.get(belong_cand)

    if prod_name_col and prod_belong_col:
        prod_map = prod_map_raw[[prod_name_col, prod_belong_col]].copy()
        prod_map = prod_map.rename(columns={prod_name_col: "Unified Name", prod_belong_col: "产品归属"})
        prod_map["Unified Name"] = prod_map["Unified Name"].astype(str).str.strip()
        prod_map = prod_map.dropna(subset=["Unified Name"])
        prod_map = prod_map.drop_duplicates(subset=["Unified Name"], keep="first")
    else:
        # 兼容旧表：无上述列名时仍用 B 列、E 列（索引 1、4）
        prod_map = prod_map_raw.iloc[:, [1, 4]].copy()
        prod_map.columns = ["Unified Name", "产品归属"]

    df["Unified Name"] = df["Unified Name"].astype(str).str.strip()
    df = df.merge(prod_map, on="Unified Name", how="left")

    # 若按名称匹配后仍有空，且总表与映射表都有 Unified ID，则用 Unified ID 回填 产品归属
    uid_in_df = "Unified ID" in df.columns
    uid_in_map = next((c for c in prod_map_raw.columns if _norm_col(c) in ("Unified ID", "Unified id", "unified id")), None)
    belong_in_map = prod_belong_col or prod_cols.get("产品归属")
    if uid_in_df and uid_in_map and belong_in_map:
        fallback = prod_map_raw[[uid_in_map, belong_in_map]].copy()
        fallback.columns = ["_uid", "产品归属"]
        fallback["_uid"] = fallback["_uid"].astype(str).str.strip()
        fallback = fallback.dropna(subset=["_uid"]).drop_duplicates(subset=["_uid"], keep="first")
        if df["产品归属"].isna().any():
            df["_uid"] = df["Unified ID"].astype(str).str.strip()
            filled = df["产品归属"].isna()
            df.loc[filled, "产品归属"] = df.loc[filled, "_uid"].map(fallback.set_index("_uid")["产品归属"])
            df = df.drop(columns=["_uid"])

    # ---------------------------------------------------
    # 2. 公司归属
    # 总表: Unified Publisher Name → 公司归属表: B列 → 返回 C列
    # ---------------------------------------------------
    comp_map = comp_map.iloc[:, [1, 2]]
    comp_map.columns = ["Unified Publisher Name", "公司归属"]
    df = df.merge(comp_map, on="Unified Publisher Name", how="left")

    # ---------------------------------------------------
    # 3. 流水系数
    # 总表: Unified Name → 流水系数表: A列 → 返回 B列
    # ---------------------------------------------------
    coef_map = coef_map.iloc[:, [0, 1]]
    coef_map.columns = ["Unified Name", "流水系数"]
    df = df.merge(coef_map, on="Unified Name", how="left")

    # === 输出 ===
    df.to_excel(OUTPUT_FILE, index=False)

    print("\n✅ STEP2 完成")
    print(f"输出文件: {OUTPUT_FILE}\n")


# 允许单独运行测试
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", help="例如 0105-0111（可选）")
    parser.add_argument("--year", type=int, help="年份，例如 2025（可选）")
    args = parser.parse_args()

    run_step2(args.week, args.year)
