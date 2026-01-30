# SLG Monitor 3.0

基于 Sensor Tower 数据的 **SLG 竞对数据监测** 系统：从原始 CSV 制作数据监测表、产出目标产品，并支持分地区数据与广告创意拉取；前端按公司维度、产品维度、素材维度展示监测表与目标产品数据。

---

## 快速开始

### 1. 运行完整流程（数据 + 前端更新）

指定**年**和**周**，一键生成数据监测表、目标产品，并更新前端展示（新周会自动出现在侧栏）：

```bash
# 默认执行步骤 1,2,5：数据监测表 + 目标产品 + 前端更新
python run_full_pipeline.py --year 2026 --week 0119-0125

# 或使用 --date（年-周）
python run_full_pipeline.py --date 2026-0119-0125
```

### 2. 启动前端服务

```bash
python start_server.py
```

浏览器访问：**http://localhost:8000/frontend/**

---

## 目录结构

```
SLG Monitor 3.0/
├── run_full_pipeline.py     # 完整流程入口（步骤 1,2,3,4,5）
├── start_server.py          # 静态资源服务（frontend + data）
├── README.md                # 本文件
├── README_SCHEDULE.md       # 定期更新与 cron 说明
│
├── raw_csv/                 # 原始 CSV（按年/周）
│   └── {年}/{周}/*.csv
├── intermediate/           # 中间表（step1→step4 产出）
│   └── {年}/{周}/merged_deduplicated.xlsx, mapped_total.xlsx, metrics_total.xlsx, pivot_table.xlsx
├── output/                 # 最终数据监测表（step5 产出）
│   └── {年}/{周}_SLG数据监测表.xlsx
├── target/                 # 目标产品表（步骤 2 产出）
│   └── {年}/{周}/strategy_target/, non_strategy_target/
├── final_join/             # 目标产品 + 各 T 度获量（供产品维度展示）
│   └── {年}/{周}/target_strategy_old_with_ads_all.xlsx, target_strategy_new_with_ads_all.xlsx
├── advertisements/         # 广告创意数据（按年/周/产品类型/产品目录）
├── request/                # API 请求脚本（拉地区数据、广告创意）
│   ├── fetch_country_data.py
│   └── fetch_ad_creatives.py
├── mapping/                # 映射表（产品归属、公司归属、流水系数、市场 T 度）
├── scripts/                # 数据监测表与目标产品脚本
│   ├── step1_merge_clean.py … step5_5_fix_arrow_color.py
│   └── generate_target.py
├── frontend/                # 前端静态页与数据生成脚本
│   ├── index.html, css/, js/
│   ├── data/               # 前端用 JSON（weeks_index、各周 formatted、产品/素材维度）
│   ├── convert_excel_with_format.py   # output → data/{年}/{周}_formatted.json
│   ├── convert_final_join_to_json.py  # final_join → data/{年}/{周}/product_strategy_*.json
│   ├── build_creative_products_index.py  # advertisements → data/{年}/{周}/creative_products.json
│   ├── build_weeks_index.py            # 扫描 data 生成 weeks_index.json
│   └── README.md           # 前端使用说明
└── docs/                   # 功能说明与数据流文档
```

---

## 流程步骤说明

| 步骤 | 说明 | 产出 |
|------|------|------|
| **1** | 制作数据监测表（step1 合并去重 → step2 映射 → step3 指标 → step4 透视 → step5 报告 → step5_5 箭头颜色） | `output/{年}/{周}_SLG数据监测表.xlsx`、`intermediate/{年}/{周}/` |
| **2** | 获得目标产品（策略目标=产品归属表；非策略=P50+周安装变动>20%） | `target/{年}/{周}/strategy_target/`、`non_strategy_target/` |
| **3** | 拉取地区数据（预留，需接 request/fetch_country_data） | request/country_data、最终可产出 final_join |
| **4** | 拉取广告数据（预留，需接 request/fetch_ad_creatives） | advertisements/{年}/{周}/ |
| **5** | 前端数据更新 | `frontend/data/` 下 formatted JSON、产品维度 JSON、素材索引、`weeks_index.json` |

### 常用命令

```bash
# 只做数据监测表 + 目标产品（不更新前端）
python run_full_pipeline.py --year 2026 --week 0119-0125 --steps 1,2

# 只更新前端（已有 output / final_join / advertisements 时）
python run_full_pipeline.py --year 2026 --week 0119-0125 --steps 5

# 查看帮助
python run_full_pipeline.py --help
```

**加入新一周数据时**：对新的 `--year` / `--week` 执行一次 `run_full_pipeline.py`（默认含步骤 5），前端侧栏会通过 `weeks_index.json` 自动出现该周。

---

## 前端功能

- **公司维度**：数据监测表（来自 `frontend/data/{年}/{周}_formatted.json`），支持搜索、排序、下载 CSV；目标产品行黄底蓝字，可点击跳转素材维度。
- **产品维度**：爆量产品地区数据（来自 `data/{年}/{周}/product_strategy_old.json`、`product_strategy_new.json`）。
- **素材维度**：广告创意列表与视频（来自 `advertisements/` 与 `data/{年}/{周}/creative_products.json`）。
- **组合分析**：每周趋势（静态图表）。

左侧边栏按 **年 / 周** 选择周期，数据来自 `frontend/data/weeks_index.json`（由步骤 5 中的 `build_weeks_index.py` 生成）。

详见 **frontend/README.md**。

---

## 环境与依赖

- **Python 3**
- 脚本依赖：`pandas`、`openpyxl`（如运行 `convert_excel_with_format.py`、各 step 脚本）

```bash
pip install pandas openpyxl
```

---

## 定期更新

若需定时跑流程（如每周一更新），可使用 `schedule_update.py` 并配置 cron 或任务计划程序。详见 **README_SCHEDULE.md**。

---

## 文档

- **docs/从GitHub克隆项目说明.md** — 同事从 GitHub 克隆、依赖安装与复刻验证
- **docs/系统功能与目标.md** — 系统定位、步骤与数据流
- **docs/功能一_数据监测表与目标产品.md** — 数据监测表与目标产品规则
- **docs/final_join说明.md** — final_join 与产品维度数据来源
- **docs/市场T度划分说明.md** — 市场 T 度划分
- **frontend/README.md** — 前端启动、数据生成与排错
