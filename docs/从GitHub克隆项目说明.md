# 从 GitHub 克隆项目说明

供同事从 GitHub 复刻/克隆 **SLG Monitor 3.0** 并在本机运行。按顺序完成以下步骤即可。

---

## 一、环境要求

- **Git**：用于克隆仓库  
- **Python 3**：建议 3.8 及以上  
- （可选）若需调用 Sensor Tower API（拉取地区数据、广告创意），需配置 Token，见下文「必要配置」

---

## 二、克隆仓库

### 方式一：HTTPS（推荐，无需配置 SSH）

```bash
git clone https://github.com/ZeroNot0/SLGMONITOR.git
cd SLGMONITOR
```

若仓库为私有，会提示输入 GitHub 用户名和密码（或 **Personal Access Token**）。

### 方式二：SSH（已配置 SSH 密钥时）

```bash
git clone git@github.com:ZeroNot0/SLGMONITOR.git
cd SLGMONITOR
```

克隆完成后，进入项目目录 `SLGMONITOR`（或你重命名后的文件夹）。

---

## 三、安装依赖

在项目根目录下执行：

```bash
pip install pandas openpyxl
```

如需使用虚拟环境（推荐）：

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS / Linux:
source .venv/bin/activate
pip install pandas openpyxl
```

---

## 四、必要配置

### 1. API Token（仅在使用「地区数据 / 广告创意」时需要）

- 项目会从 **`request/token.txt`** 读取 Sensor Tower API Token。  
- 该文件**未随仓库提交**（已在 `.gitignore` 中），需要本地新建。  
- 向项目负责人/有权限的同事**索取 Token**，然后在项目根目录下执行：

```bash
# 在 request 目录下创建 token.txt，写入一行 Token（无引号、无换行）
# Windows（PowerShell）示例：
echo 你的Token字符串 > request\token.txt

# macOS / Linux 示例：
echo "你的Token字符串" > request/token.txt
```

- 仅做「数据监测表 + 目标产品 + 前端展示」且不拉取地区/创意数据时，**可以不配置** Token。

### 2. 映射表与数据（一般已随仓库）

- **mapping/**：产品归属、公司归属、市场 T 度、流水系数等映射表，通常已包含在仓库中，无需额外操作。  
- **raw_csv/**：原始 CSV 按 `raw_csv/{年}/{周}/` 放置。若仓库中未包含（被 .gitignore 排除），需从内网/同事处获取对应周数据，放到相同目录结构下，否则无法跑「步骤 1：制作数据监测表」。

---

## 五、验证是否复刻成功

### 1. 仅启动前端（不依赖 API、不依赖 raw_csv）

在项目根目录执行：

```bash
python start_server.py
```

浏览器访问：**http://localhost:8000/frontend/**  
若能看到页面且侧栏有周选项，说明前端与静态资源正常。  
（若无任何周数据，侧栏可能为空，属正常，需先跑一次 pipeline 并生成 `frontend/data/weeks_index.json`。）

### 2. 跑一次完整流程（数据监测表 + 目标产品 + 前端更新）

需确保 **raw_csv/** 下已有对应周的数据，例如：

```bash
python run_full_pipeline.py --year 2026 --week 0119-0125
```

默认会执行：步骤 1（数据监测表）、步骤 2（目标产品）、步骤 5（前端更新）。  
若成功，会在 `output/`、`target/`、`frontend/data/` 下生成对应文件，刷新前端即可看到该周。

### 3. 拉取地区数据 / 广告创意（需 Token）

在已配置 **request/token.txt** 的前提下，可执行：

```bash
# 示例：拉取地区数据 + 广告创意（需先有 target 表，即至少跑过步骤 1+2）
python run_full_pipeline.py --year 2026 --week 0119-0125 --steps 1,2 --api country,creatives --limit top5
```

详见 **README.md** 与 **docs/Sensor_Tower_API使用文档.md**。

---

## 六、目录结构速览

克隆后主要目录含义：

| 目录 | 说明 |
|------|------|
| **run_full_pipeline.py** | 一键流程入口（数据监测表 → 目标产品 → 可选 API → 前端更新） |
| **start_server.py** | 启动前端静态服务 |
| **raw_csv/** | 原始 CSV（按年/周），需自行准备或从同事处获取 |
| **mapping/** | 映射表（产品归属、公司归属、市场 T 度等） |
| **request/** | API 脚本；**request/token.txt** 需本地新建并填入 Token |
| **frontend/** | 前端页面与数据生成脚本 |
| **docs/** | 功能与 API 说明文档 |

更多结构说明见项目根目录 **README.md**。

---

## 七、常见问题

| 问题 | 处理 |
|------|------|
| 端口 8000 已被占用 | 服务会自动尝试 8001；或手动指定：`python start_server.py --port 8001` |
| 提示未找到 `request/token.txt` | 仅在使用地区/创意 API 时需要；不用 API 可跳过；若要用，请按上文「必要配置」创建并填入 Token |
| 克隆后没有 raw_csv 或没有某周数据 | 若仓库未包含这些数据，需从内网或同事处拷贝到对应 `raw_csv/{年}/{周}/` 下 |
| 前端打开是空白或没有周选项 | 先跑一次 `run_full_pipeline.py --year ... --week ...`（至少步骤 1,2,5），生成 `frontend/data/weeks_index.json` 与各周数据 |

---

## 八、相关文档

- **README.md** — 项目概述、快速开始、流程步骤  
- **docs/Sensor_Tower_API使用文档.md** — Sensor Tower API 参数与代码示例  
- **docs/同网共享.md** — 同网同事访问你本机 demo 的说明  
- **frontend/README.md** — 前端数据与排错  

克隆或运行中若有问题，可联系项目维护人或查看上述文档。
