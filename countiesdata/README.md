# countiesdata（地区数据）

仿照 `advertisements` 的目录结构（年/周/strategy_old|strategy_new），存放按国家维度的下载/收入数据（JSON 与 xlsx）。

## 目录结构

- **countiesdata/{年}/{周}/strategy_old/json/**、**xlsx/** — 对应 target_strategy_old.xlsx 的产品地区数据，每个 app 一个 `{app_id}.json` / `{app_id}.xlsx`
- **countiesdata/{年}/{周}/strategy_new/json/**、**xlsx/** — 对应 target_strategy_new.xlsx 的产品地区数据

层级：年（如 2025、2026）→ 时间段（如 1201-1207）→ 策略老/新（strategy_old、strategy_new）。

## 数据来源

- 步骤 3（拉取地区数据）会按 **strategy_old** 与 **strategy_new** 分别拉取，写入 **countiesdata/{年}/{周}/strategy_old/** 与 **strategy_new/**，并自动把该周 json 转成 xlsx
- 迁移存量：`python scripts/convert_country_json_to_xlsx.py --migrate --year 2025 --week 1201-1207` 会把 `request/country_data/json` 下 json 复制到该周的 strategy_old 与 strategy_new 并转 xlsx

## 表结构（xlsx）

列顺序：`app_id`, `country`, `date`, `android_units`, `android_revenue`, `iphone_units`, `iphone_revenue`, `unified_units`, `unified_revenue`, `ipad_units`, `ipad_revenue`。日期格式为 YYYY-MM-DD。

## 使用

- **build_final_join** 会从 countiesdata 读地区数据：处理 target_strategy_old 时读 `countiesdata/{年}/{周}/strategy_old`，处理 target_strategy_new 时读 `strategy_new`，若无则回退到 `request/country_data`。
