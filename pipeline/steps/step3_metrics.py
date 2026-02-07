import pandas as pd
from pathlib import Path

def run_step3(week_tag: str = None, year: int = None):

    BASE_DIR = Path(__file__).resolve().parent.parent.parent

    # === 构建输入文件路径 ===
    if week_tag and year:
        INPUT_FILE = BASE_DIR / "intermediate" / str(year) / week_tag / "mapped_total.xlsx"
    else:
        # 向后兼容：查找最新的mapped_total.xlsx
        intermediate_dir = BASE_DIR / "intermediate"
        if week_tag and not year:
            # 只有week_tag，查找匹配的文件
            pattern = f"*/{week_tag}/mapped_total.xlsx"
            files = list(intermediate_dir.glob(pattern))
            if files:
                INPUT_FILE = sorted(files)[-1]  # 取最新的
            else:
                INPUT_FILE = BASE_DIR / "intermediate" / "mapped_total.xlsx"
        else:
            INPUT_FILE = BASE_DIR / "intermediate" / "mapped_total.xlsx"
    
    # === 构建输出文件路径 ===
    if week_tag and year:
        OUTPUT_DIR = BASE_DIR / "intermediate" / str(year) / week_tag
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_FILE = OUTPUT_DIR / "metrics_total.xlsx"
    else:
        OUTPUT_FILE = BASE_DIR / "intermediate" / "metrics_total.xlsx"

    # === 读取 STEP2 表 ===
    df = pd.read_excel(INPUT_FILE)

    # === 必要列名 ===
    downloads_abs = "Downloads (Absolute)"
    downloads_pop = "Downloads (PoP Growth)"
    revenue_abs = "Revenue (Absolute)"
    revenue_pop = "Revenue (PoP Growth)"
    coef_col = "流水系数"

    # === 检查列是否存在 ===
    required_cols = [downloads_abs, downloads_pop, revenue_abs, revenue_pop, coef_col]
    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        raise ValueError(f"❌ STEP3 缺少必要列: {missing}")

    # === 流水系数空值 → 默认 0.63 ===
    df[coef_col] = df[coef_col].fillna(0.63)

    # === 计算四个新指标列 ===
    df["当周周安装"] = df[downloads_abs]
    df["上周周安装"] = df[downloads_abs] - df[downloads_pop]

    df["当周周流水"] = df[revenue_abs] / df[coef_col]
    df["上周周流水"] = (df[revenue_abs] - df[revenue_pop]) / df[coef_col]

    # === 输出 ===
    df.to_excel(OUTPUT_FILE, index=False)

    print("\n✅ STEP3 完成")
    print(f"输出文件: {OUTPUT_FILE}\n")


# 允许单独运行测试
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", help="例如 0105-0111（可选）")
    parser.add_argument("--year", type=int, help="年份，例如 2025（可选）")
    args = parser.parse_args()
    
    run_step3(args.week, args.year)

