# 标签表目录

本目录用于存放【数据底表】页面展示的三张标签表（Excel .xlsx）：

- **题材标签表.xlsx** — 题材标签表（建议列：序号、标签名、备注）
- **玩法标签表.xlsx** — 玩法标签表（建议列：序号、标签名、备注）
- **画风标签表.xlsx** — 画风标签表（建议列：序号、标签名、备注）

**初始化**：若文件不存在，可在项目根目录执行：

```bash
python scripts/init_label_tables.py
```

（需安装 openpyxl：`pip install openpyxl`）会创建带表头的空 Excel。将对应 Excel 放入此目录或由脚本创建后，在「数据维护」上传归属表时会同步到 MySQL basetable，前端「数据底表」→ 题材/玩法/画风标签表 中可查看。
