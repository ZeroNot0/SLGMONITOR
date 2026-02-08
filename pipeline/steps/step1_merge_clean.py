import pandas as pd
from pathlib import Path
import argparse

# =================================================
# Sensor Tower CSV 读取函数
# =================================================
def read_sensor_tower_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path,
            encoding="utf-16",
            sep="\t",
            engine="c",
            low_memory=False,
        )
    except Exception:
        return pd.read_csv(
            path,
            encoding="utf-16",
            sep="\t",
            engine="python",
        )

# =================================================
# STEP1 主流程
# =================================================
def run_step1(week_tag: str, year: int = None, write_normalized: bool = True):

    # === 项目根目录 SLG Monitor ===
    BASE_DIR = Path(__file__).resolve().parent.parent.parent

    # === 自动检测年份（如果未提供） ===
    if year is None:
        # 尝试从week_tag推断年份，或查找存在的年份文件夹
        week_year = None
        if week_tag and len(week_tag) >= 7:
            # 如果week_tag是1229-0104这种跨年格式，需要判断
            # 简单策略：查找所有存在的年份文件夹
            for y in range(2020, 2030):
                year_dir = BASE_DIR / f"{y}_raw_csv"
                if year_dir.exists():
                    week_dir = year_dir / week_tag
                    if week_dir.exists() and week_dir.is_dir():
                        week_year = y
                        break
        
        if week_year is None:
            # 默认使用当前年份，或查找最新的年份文件夹
            from datetime import datetime
            current_year = datetime.now().year
            for y in range(current_year, current_year - 5, -1):
                year_dir = BASE_DIR / f"{y}_raw_csv"
                if year_dir.exists():
                    week_year = y
                    break
        
        if week_year is None:
            raise ValueError(f"❌ 无法自动检测年份，请手动指定 --year 参数")
        
        year = week_year
        print(f"🔍 自动检测到年份: {year}")

    # === 原始 CSV 所在目录 {year}_raw_csv/0105-0111 ===
    RAW_DIR = BASE_DIR / f"{year}_raw_csv" / week_tag

    if not RAW_DIR.exists():
        try:
            from app.app_paths import get_data_root
            data_root = get_data_root()
            alt_dir = data_root / "raw_csv" / str(year) / week_tag
            if alt_dir.exists():
                RAW_DIR = alt_dir
        except Exception:
            pass

    if not RAW_DIR.exists():
        raise ValueError(f"❌ 未找到目录: {RAW_DIR}")

    # === 标准化后 CSV 输出目录 ===
    NORMALIZED_DIR = RAW_DIR / "normalized"

    # === STEP1 最终合并输出目录：intermediate/{year}/{week_tag}/ ===
    OUTPUT_DIR = BASE_DIR / "intermediate" / str(year) / week_tag

    # ✅ 关键修正：允许自动创建所有父目录
    if write_normalized:
        NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    FINAL_OUTPUT_PATH = OUTPUT_DIR / "merged_deduplicated.xlsx"

    # =================================================
    # 1. 找出本周 CSV
    # =================================================
    csv_files = sorted(RAW_DIR.glob(f"{week_tag}-*.csv"))

    print(f"\n📂 检测到 {len(csv_files)} 个原始 CSV")

    if len(csv_files) == 0:
        raise ValueError(f"❌ 未找到 {week_tag}-*.csv 文件")

    # =================================================
    # 2. 标准化 CSV (utf-16/tab → utf-8)
    # =================================================
    df_list = []

    print("\n🔹 Step 1.1: 读取 CSV 并合并")
    for f in csv_files:
        print(f"读取: {f.name}")
        df = read_sensor_tower_csv(f)
        if write_normalized:
            out_path = NORMALIZED_DIR / f.name
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"  ✅ 输出: normalized/{out_path.name}")
        df_list.append(df)

    merged_df = pd.concat(df_list, ignore_index=True)

    print(f"   合并后总行数: {len(merged_df)}")

    # =================================================
    # 4. 查找 Unified ID 列
    # =================================================
    unified_id_candidates = ["Unified ID", "Unified_ID", "unified_id"]
    unified_col = next((c for c in unified_id_candidates if c in merged_df.columns), None)

    if unified_col is None:
        raise ValueError("❌ 未找到 Unified ID 列")

    print(f"🔹 使用去重列: {unified_col}")

    # =================================================
    # 5. Step A: 按 Unified ID 去重（仅对非空 Unified ID 生效）
    # =================================================
    before = len(merged_df)
    uid_series = merged_df[unified_col].astype(str).str.strip()
    has_uid = uid_series.notna() & (uid_series != "") & (uid_series.str.lower() != "nan")
    df_with_uid = merged_df[has_uid].copy()
    df_no_uid = merged_df[~has_uid].copy()
    df_with_uid = df_with_uid.drop_duplicates(subset=[unified_col], keep="first")
    df_uid = pd.concat([df_with_uid, df_no_uid], ignore_index=True)
    after = len(df_uid)

    print("\n🔹 Step 1.3: Unified ID 去重")
    print(f"   去重前行数: {before}")
    print(f"   去重后行数: {after}")
    print(f"   删除行数:   {before - after}")

    # =================================================
    # 6. Step B: Excel-like Revenue + Name 去重逻辑
    # =================================================
    revenue_col = "Revenue (Absolute)"
    name_col = "Unified Name"

    missing_cols = [c for c in [revenue_col, name_col] if c not in df_uid.columns]
    if missing_cols:
        raise ValueError(f"❌ 缺少列，无法执行 Step B: {missing_cols}")

    before = len(df_uid)

    # B1: Revenue 是否重复拆分
    rev_dup_mask = df_uid.duplicated(subset=[revenue_col], keep=False)
    df_rev_dup = df_uid[rev_dup_mask]
    df_rev_unique = df_uid[~rev_dup_mask]

    print("\n🔹 Step 1.4: Revenue 重复性拆分")
    print(f"   Revenue 不重复行数: {len(df_rev_unique)}")
    print(f"   Revenue 重复行数:   {len(df_rev_dup)}")

    # B2: 重复部分 → 按 Revenue 降序 → Unified Name 忽略大小写去重
    df_rev_dup_sorted = df_rev_dup.sort_values(by=revenue_col, ascending=False)
    name_lower = df_rev_dup_sorted[name_col].astype(str).str.lower()
    df_rev_dup_dedup = df_rev_dup_sorted.loc[
        ~name_lower.duplicated(keep="first")
    ]

    print("\n🔹 Step 1.5: Unified Name 忽略大小写去重")
    print(f"   去重前(重复部分): {len(df_rev_dup)}")
    print(f"   去重后(重复部分): {len(df_rev_dup_dedup)}")
    print(f"   删除: {len(df_rev_dup) - len(df_rev_dup_dedup)}")

    # B3: 合并回最终结果
    df_final = pd.concat([df_rev_unique, df_rev_dup_dedup], ignore_index=True)

    after = len(df_final)

    print("\n🔹 Step 1.6: 最终合并结果")
    print(f"   StepB 去重前行数: {before}")
    print(f"   StepB 去重后行数: {after}")
    print(f"   StepB 删除行数:   {before - after}")

    # =================================================
    # 7. 输出最终 XLSX
    # =================================================
    df_final.to_excel(FINAL_OUTPUT_PATH, index=False)

    print("\n==============================")
    print("✅ STEP 1 全流程完成")
    print(f"   最终剩余行数: {len(df_final)}")
    print(f"   输出文件: {FINAL_OUTPUT_PATH}")
    print("==============================\n")


# =================================================
# 命令行入口
# =================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", required=True, help="例如 0105-0111")
    parser.add_argument("--year", type=int, help="年份，例如 2025（可选，会自动检测）")
    parser.add_argument("--no-normalize", action="store_true", help="不输出 normalized 目录（更快）")
    args = parser.parse_args()

    run_step1(args.week, args.year, write_normalized=not args.no_normalize)
