import pandas as pd
from pathlib import Path

def run_step4(week_tag: str = None, year: int = None):

    BASE_DIR = Path(__file__).parent.parent

    # === 构建输入文件路径 ===
    if week_tag and year:
        INPUT_FILE = BASE_DIR / "intermediate" / str(year) / week_tag / "metrics_total.xlsx"
    else:
        # 向后兼容：查找最新的metrics_total.xlsx
        intermediate_dir = BASE_DIR / "intermediate"
        if week_tag and not year:
            # 只有week_tag，查找匹配的文件
            pattern = f"*/{week_tag}/metrics_total.xlsx"
            files = list(intermediate_dir.glob(pattern))
            if files:
                INPUT_FILE = sorted(files)[-1]  # 取最新的
            else:
                INPUT_FILE = BASE_DIR / "intermediate" / "metrics_total.xlsx"
        else:
            INPUT_FILE = BASE_DIR / "intermediate" / "metrics_total.xlsx"
    
    # === 构建输出文件路径 ===
    if week_tag and year:
        OUTPUT_DIR = BASE_DIR / "intermediate" / str(year) / week_tag
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_FILE = OUTPUT_DIR / "pivot_table.xlsx"
    else:
        OUTPUT_FILE = BASE_DIR / "intermediate" / "pivot_table.xlsx"

    df = pd.read_excel(INPUT_FILE)

    # === 列名定义 ===
    col_company = "公司归属"
    col_product = "产品归属"
    col_date = "第三方记录最早上线时间"

    val_cols = ["当周周安装", "上周周安装", "当周周流水", "上周周流水"]

    # === 基础校验 ===
    required = [col_company, col_product, col_date] + val_cols
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"❌ STEP4 缺少必要列: {missing}")

    # === 过滤公司 / 产品为空的行 ===
    df = df[df[col_company].notna()]
    df = df[df[col_product].notna()]

    # === 生成基础透视数据 ===
    pivot = pd.pivot_table(
        df,
        index=[col_company, col_product, col_date],
        values=val_cols,
        aggfunc="sum"
    ).reset_index()

    # === 保留 Unified ID（每组取第一个），供后续 API 请求使用 ===
    if "Unified ID" in df.columns:
        first_id = df.groupby([col_company, col_product, col_date])["Unified ID"].first().reset_index()
        pivot = pivot.merge(first_id, on=[col_company, col_product, col_date], how="left")

    # === 计算公司层汇总 ===
    company_summary = pivot.groupby(col_company)[val_cols].sum().reset_index()

    # 👇 生成 “xxx 汇总”
    company_summary[col_company] = company_summary[col_company].astype(str) + " 汇总"
    company_summary[col_product] = ""
    company_summary[col_date] = ""
    if "Unified ID" in pivot.columns:
        company_summary["Unified ID"] = ""

    # === 公司排序（按当周周流水总额倒序） ===
    company_order = (
        pivot.groupby(col_company)["当周周流水"]
        .sum()
        .sort_values(ascending=False)
        .index
        .tolist()
    )

    # === 给 pivot 加入排序标记 ===
    pivot["__company_order"] = pivot[col_company].map(
        {name: i for i, name in enumerate(company_order)}
    )

    # === 公司内部产品按当周周流水倒序 ===
    pivot = pivot.sort_values(
        by=["__company_order", "当周周流水"],
        ascending=[True, False]
    )

    # === 重新拼接：每个公司 → 产品明细 → 汇总行 ===
    final_blocks = []

    for comp in company_order:
        block = pivot[pivot[col_company] == comp].copy()
        final_blocks.append(block)

        summary_row = company_summary[
            company_summary[col_company] == comp + " 汇总"
        ]
        final_blocks.append(summary_row)

    final = pd.concat(final_blocks, ignore_index=True)

    # === 删除辅助列 ===
    if "__company_order" in final.columns:
        final = final.drop(columns="__company_order")

    # === 最终列顺序 ===
    out_cols = [col_company, col_product, col_date]
    if "Unified ID" in final.columns:
        out_cols.append("Unified ID")
    final = final[out_cols + val_cols]

    # === 输出 ===
    final.to_excel(OUTPUT_FILE, index=False)

    print("\n✅ STEP4 完成")
    print(f"输出文件: {OUTPUT_FILE}\n")


# 允许单独运行
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", help="例如 0105-0111（可选）")
    parser.add_argument("--year", type=int, help="年份，例如 2025（可选）")
    args = parser.parse_args()
    
    run_step4(args.week, args.year)

