# 分地区数据 API 请求逻辑确认

## 一、触发时机

- **步骤 3**：拉取地区数据  
- 在 pipeline 中执行步骤 3 时，会调用 `request/fetch_country_data.py`，对「目标产品」按 **Unified ID** 逐个请求国家维度数据。

---

## 二、app_id 来源（请求谁）

1. 从 **target/{年}/{周}/** 下读取目标产品表：
   - `strategy_target/*.xlsx`（策略目标 old/new）
   - `non_strategy_target/*.xlsx`（非策略目标 old/new）
2. 每个产品取 **Unified ID** 作为 `app_id`；若表内无「Unified ID」列，则用 **产品归属** 作为 app_id。
3. 按 `(app_id, 产品归属)` 去重、保留顺序后，再按 **limit** 截断：
   - `top1` / `top5` / `top10`：只请求前 1 / 5 / 10 个产品
   - `all`：不截断，请求全部目标产品
4. 仅把 **app_id 列表** 写入临时文件，用 `--app_ids_file` 传给 `fetch_country_data.py`。

**结论**：请求对象 = 当前所选「年 + 周」对应的 target 表里的产品，且以 **Unified ID** 为准（无则退化为产品名）。

---

## 三、请求参数（API 层）

脚本内部对 **每个 app_id** 调用一次 Sensor Tower API：

- **接口**：`GET /v1/{os}/sales_report_estimates`
- **路径**：`{os}` 来自 `--os`，默认 **`ios`**（可选 `android`）。
- **查询参数**：
  - `app_ids`：当前这一个 app 的 Unified ID（单次请求一个 app）
  - `start_date` / `end_date`：由 pipeline 根据 **周标签** 换算成 YYYY-MM-DD 传入（如 1208-1214 → 2025-12-08 ~ 2025-12-14）
  - `date_granularity`：默认 **`daily`**（可按日聚合）
  - `countries`：默认 **`all`**（请求所有国家）
  - `data_model`：默认 **`DM_2025_Q2`**

**结论**：  
- 国家维度：用 **`all`** 请求所有国家。  
- 时间维度：用当前周的 **start_date ~ end_date**。  
- 平台：默认 **ios**，如需 android 需传 `--os android`。

---

## 四、pipeline 实际传入的参数

步骤 3 调用方式（节选）：

```text
python request/fetch_country_data.py --app_ids_file <临时文件> [--start_date YYYY-MM-DD] [--end_date YYYY-MM-DD]
```

- **传入**：`--app_ids_file`、`--start_date`、`--end_date`（由 `week_tag_to_dates(year, week_tag)` 算出）。
- **未传入**（使用脚本默认）：
  - `--countries` → 脚本默认 **`all`**
  - `--os` → 默认 **`ios`**
  - `--date_granularity` → 默认 **`daily`**
  - `--data_model` → 默认 **`DM_2025_Q2`**

若需改「国家」或「平台」，需在 pipeline 里为步骤 3 增加对应命令行参数并传给 `fetch_country_data.py`。

---

## 五、落盘与后续使用

- **写入路径**：
  - `request/country_data/json/{app_id}.json`
  - `request/country_data/xlsx/{app_id}.xlsx`（若未关掉 xlsx）
- **目录结构**：按 **app_id** 存文件，**不按年/周分子目录**；同一 app 多次跑步骤 3 会覆盖原文件。
- **后续使用**：  
  步骤 5 会跑 `scripts/build_final_join.py`，读取 **target 表** + **上述 json/xlsx**，按 `mapping/市场T度.csv` 把国家归到亚洲 T1 / 欧美 T1 / T2 / T3，汇总安装与流水，写入 **final_join**，再转成前端「产品维度」用的 JSON。

---

## 六、小结表

| 项目         | 当前逻辑 |
|--------------|----------|
| 请求谁       | target 表中目标产品的 **Unified ID**（无则产品归属），可被 limit 截断 |
| 国家范围     | **all**（所有国家） |
| 时间范围     | 当前周的 **start_date ~ end_date**（由周标签换算） |
| 平台         | 默认 **ios** |
| 日期粒度     | **daily** |
| 数据模型     | **DM_2025_Q2** |
| 存盘         | `request/country_data/json/{app_id}.json`（及可选 xlsx），不按年/周分目录 |
| 与 final_join | 用同一批 json 按 T 度汇总 → final_join → 产品维度页 |

如需改「国家」「平台」或「按周分目录存」，可以说明目标行为，再对 pipeline / 脚本做对应改动。
