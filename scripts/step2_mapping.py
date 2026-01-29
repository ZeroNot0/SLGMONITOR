import pandas as pd
from pathlib import Path
import argparse

def run_step2(week_tag: str = None, year: int = None):

    BASE_DIR = Path(__file__).parent.parent   # 项目根目录
    
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
    prod_map = pd.read_excel(BASE_DIR / "mapping" / "产品归属.xlsx")
    comp_map = pd.read_excel(BASE_DIR / "mapping" / "公司归属.xlsx")
    coef_map = pd.read_excel(BASE_DIR / "mapping" / "流水系数.xlsx")

    # ---------------------------------------------------
    # 1. 产品归属
    # 总表: Unified Name  → 产品归属表: B列 → 返回 E列
    # ---------------------------------------------------
    prod_map = prod_map.iloc[:, [1, 4]]
    prod_map.columns = ["Unified Name", "产品归属"]
    df = df.merge(prod_map, on="Unified Name", how="left")

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
