# 新游戏表（上线新游）

本目录用于存放**新游戏**数据，结构与产品归属表一致。

- 将 Excel 表（.xlsx）放在此目录下，列需包含：产品名（实时更新中）、Unified ID、产品归属、题材、画风、发行商、公司归属。
- 运行 `python frontend/convert_newproducts_to_json.py` 将 xlsx 转为 `frontend/data/new_products.json`，供【产品维度】-【上线新游】页面展示。
- 前端统一通过 JSON 展示，不直接读 xlsx。
