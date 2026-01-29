# 前端Demo使用说明

## 快速开始

### 方法1：从项目根目录启动（推荐）

```bash
# 在项目根目录下
python start_server.py
```

然后访问：`http://localhost:8000/frontend/`

### 方法2：从frontend目录启动

```bash
cd frontend
python -m http.server 8000
```

然后访问：`http://localhost:8000`

**注意**：如果使用方法2，需要修改 `app.js` 中的路径为 `../output/...`

## 功能说明

- **顶部**: 标题「SLG竞对数据监测演示DEMO」+ 本机时间（实时）
- **主导航**: 公司维度、产品维度、素材维度、组合分析（公司维度 + 产品维度已有数据）
- **左侧边栏**: 按年 / 月 / 周选择周期（数据来自 `data/weeks_index.json`）
- **公司维度**（大盘数据）:
  - 标题：`{年}年, {周}, SLG竞对数据监测表 (大盘数据)`
  - 子导航：大盘数据、公司分地区获量、公司素材投放情况、公司数据趋势（当前仅大盘数据有表格）
  - 搜索框 +「下载表格」按钮
  - **数据监测表**：脚本第一步产出，由 **JSON** 展示（`data/{年}/{周}_formatted.json`）
  - 周安装/流水变动：▲ 绿色、▼ 红色；汇总行加粗；支持搜索、排序、固定表头
- **产品维度**（爆量产品地区数据）:
  - 标题：`{年}年, {周}, 爆量产品地区数据`
  - 产品选择：SLG爆量旧产品 / SLG爆量新产品；时间段（与左侧周一致）；搜索 +「下载表格」
  - **表格**：来自 **final_join** 表转 JSON（`data/{年}/{周}/product_strategy_old.json`、`product_strategy_new.json`）
  - 列：产品归属（蓝链）、公司归属、第三方记录最早上线时间、当周/上周周安装、周安装变动、亚洲 T1 / 欧美 T1 / T2 / T3 市场获量

## 工作原理

前端优先加载带格式的JSON文件（保留颜色、字体等格式），如果不存在则回退到直接读取Excel文件。

### 一键全量更新前端（推荐）

在项目根目录运行完整流程（含步骤 5「前端数据更新」），可自动生成/更新所有前端数据与新周侧栏：

```bash
# 默认执行步骤 1,2,5：数据监测表 + 目标产品 + 前端更新（新周会自动出现在侧栏）
python run_full_pipeline.py --year 2026 --week 0119-0125

# 或只更新前端（已有 output / final_join / advertisements 时）
python run_full_pipeline.py --year 2026 --week 0119-0125 --steps 5
```

步骤 5 会依次执行：`convert_excel_with_format`（公司维度）、`convert_final_join_to_json`（产品维度，需 final_join）、`build_creative_products_index`（素材维度，需 advertisements）、`build_weeks_index`（周索引）。**加入新一周数据后，运行一次 pipeline 即可在前端自动看到新周。**

### 生成带格式的数据（单周手动）

```bash
# 转换单个文件
python frontend/convert_excel_with_format.py --year 2025 --week 1201-1207
```

转换后的JSON文件保存在：`frontend/data/{year}/{week_tag}_formatted.json`

### 生成产品维度 JSON（产品维度页）

产品维度页数据来自 **final_join** 文件夹下的表，需先转为 JSON：

```bash
# 转换单个周期
python frontend/convert_final_join_to_json.py --year 2025 --week 1201-1207
```

转换后生成：`frontend/data/{year}/{week_tag}/product_strategy_old.json`、`product_strategy_new.json`。前端「产品维度」切换「SLG爆量旧产品」/「SLG爆量新产品」时读取对应 JSON。

### 直接读取Excel（备用）

如果JSON文件不存在，前端会自动尝试直接读取Excel文件（但不会保留格式）。

数据文件路径：`output/{year}/{week_tag}_SLG数据监测表.xlsx`

## 文件结构

```
frontend/
├── index.html              # 主页面（公司维度 - 大盘数据）
├── css/
│   └── style.css           # 样式
├── js/
│   └── app.js              # 前端逻辑（读 weeks_index + 各周 formatted JSON、表格、搜索、排序、下载 CSV）
├── data/                   # 全部表格数据为 JSON
│   ├── weeks_index.json    # 年 -> [周] 索引（由 build_weeks_index.py 生成）
│   ├── {year}/{week_tag}_formatted.json   # 数据监测表（步骤一产出）带格式 JSON
│   └── {year}/{week_tag}/  # 产品/素材维度
│       ├── product_strategy_old.json
│       ├── product_strategy_new.json
│       └── creative_products.json
├── convert_excel_with_format.py   # 从 output 表生成 data 下 formatted JSON
├── convert_final_join_to_json.py  # 从 final_join 表生成 data/{年}/{周}/product_strategy_*.json
├── build_creative_products_index.py  # 从 advertisements 生成 data/{年}/{周}/creative_products.json
├── build_weeks_index.py    # 扫描 data 目录生成 weeks_index.json（新周自动出现在侧栏）
└── README.md               # 本文件
```

## 故障排除

### 如果加载数据失败：

1. **检查文件是否存在**：
   ```bash
   ls -la output/2025/1201-1207_SLG数据监测表.xlsx
   ```

2. **检查服务器启动位置**：
   - 如果从根目录启动，路径应该是 `output/...`
   - 如果从frontend目录启动，路径应该是 `../output/...`

3. **查看浏览器控制台**：
   - 按 F12 打开开发者工具
   - 查看 Console 标签页的错误信息
   - 查看 Network 标签页，检查文件请求是否成功

4. **尝试直接访问Excel文件**：
   - 在浏览器中访问：`http://localhost:8000/output/2025/1201-1207_SLG数据监测表.xlsx`
   - 如果无法访问，说明路径有问题

## 注意事项

- 确保 `output/` 目录下有对应的Excel文件
- 建议使用Chrome或Firefox浏览器
- 如果遇到CORS错误，使用 `start_server.py` 启动（已包含CORS支持）
