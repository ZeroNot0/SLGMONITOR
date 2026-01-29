# final_join 文件夹说明

## 功能

**final_join** 存放的是**目标产品表 + 各 T 度市场获量**的结果，**不含广告列**，供**前端展示**或分析使用。

- **目录结构**：`final_join/{年}/{周}/`
  - 如：`target_strategy_old_with_ads_all.xlsx`、`target_strategy_new_with_ads_all.xlsx`（文件名沿用历史，实际内容为「目标产品 + 各 T 度获量」）
- **数据来源**：
  - 基础行 = **target 表**（step 2 产出的目标产品，如 `target/{年}/{周}/strategy_target/target_strategy_old.xlsx` 等）
  - 合并列 = **各 T 度市场获量**：由**国家维度数据**（如 `request/country_data/` 下按 app 拉取的分国家安装/收入）按 **mapping/市场T度** 归入亚洲 T1、欧美 T1、T2、T3 后汇总得到的列（如「亚洲T1_安装」「欧美T1_流水」「T2_安装」等）
- **用途**：前端或分析时读取 final_join 下对应周、对应类型的表，可同时看到目标产品基础指标与各 T 度市场的获量/流水，**不包含广告创意相关列**。

## 市场 T 度划分

- 国家按 **mapping/市场T度.csv**（或 `docs/市场T度划分说明.md`）划分为：**亚洲 T1**（不含 CN）、**欧美 T1**、**T2**、**T3**。
- 国家维度数据汇总为 T 度时，以该映射表为准；未在表中的国家默认归为 T3。

## 与 target、country_data、advertisements 的关系

| 目录 | 内容 | 用途 |
|------|------|------|
| **target/** | 目标产品表（仅基础列：产品归属、公司、周安装、周流水等） | 供 request 拉数、供 join 脚本作为左表 |
| **request/country_data/** | 按目标产品拉取的国家维度下载/收入数据（json/xlsx） | 按 T 度汇总后合并到目标表，产出 final_join |
| **advertisements/** | 按目标产品 + 地区存放的广告创意原始数据（json/xlsx） | 与 final_join 无关；广告展示由前端另行读取 advertisements 或其它带广告列的表 |
| **final_join/** | 目标表 + **各 T 度市场获量列**（无广告列） | **供前端展示**：产品维度查看目标产品 + 各 T 度获量 |

## 生成方式

- 由**目标表与地区数据（按 T 度汇总）join** 的脚本生成：
  1. 读取 `target/{年}/{周}/strategy_target/` 下 target_strategy_old.xlsx、target_strategy_new.xlsx；
  2. 读取 `request/country_data/` 下对应目标产品的国家维度数据；
  3. 按 **mapping/市场T度** 将国家归入亚洲 T1、欧美 T1、T2、T3，汇总得到各 T 度的安装、流水等；
  4. 按产品归属（或 app_id）与目标表 join，输出到 `final_join/{年}/{周}/`，文件名如 `target_strategy_old_with_ads_all.xlsx`、`target_strategy_new_with_ads_all.xlsx`。
- 该步骤应在「拉取地区数据」（步骤 3）之后执行，并可作为 pipeline 中的一步接入 run_full_pipeline。

## 小结

- **final_join** = 目标产品表 + **各 T 度市场获量**（亚洲 T1 / 欧美 T1 / T2 / T3 的安装、流水等），**无广告列**。
- 前端展示产品维度时，可读 final_join 下的表查看目标产品 + 各 T 度获量；广告创意需从 advertisements 等渠道另行获取。
