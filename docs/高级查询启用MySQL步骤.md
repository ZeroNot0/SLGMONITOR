# 高级查询：启用 MySQL 操作步骤

「高级查询」页依赖 MySQL。按下面顺序操作即可使用。

---

## 一、确认 MySQL 已安装并运行

在终端执行：

```bash
mysql --version
```

- **有版本号**：说明已安装，继续下一步。
- **未安装（Mac）**：`brew install mysql`，然后 `brew services start mysql`。

---

## 二、建库并导入表结构

在**项目根目录**执行（把 `你的root密码` 换成实际密码；若无密码，`-p` 后直接回车）：

```bash
cd "/Users/codfz1/Desktop/Tuyoo Internship/SLG Monitor 3.0"

# 创建数据库
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS slg_monitor DEFAULT CHARSET utf8mb4;"

# 导入表结构
mysql -u root -p slg_monitor < backend/db/schema.sql
```

执行成功不会有明显输出，无报错即可。

---

## 三、启动服务时开启 MySQL

**必须**在启动 `server/start_server.py` 时加上 `USE_MYSQL=1` 和数据库账号信息。

### 方式 1：本机测试（推荐先这样试）

在项目根目录执行（`你的root密码` 换成实际密码，无密码可留空）：

```bash
cd "/Users/codfz1/Desktop/Tuyoo Internship/SLG Monitor 3.0"

USE_MYSQL=1 MYSQL_USER=root MYSQL_PASSWORD=你的root密码 MYSQL_DATABASE=slg_monitor python server/start_server.py --port 8000
```

无密码示例：

```bash
USE_MYSQL=1 MYSQL_USER=root MYSQL_PASSWORD= MYSQL_DATABASE=slg_monitor python server/start_server.py --port 8000
```

### 方式 2：已用 systemd / 宝塔部署

在对应的「环境变量」或 service 里增加：

- `USE_MYSQL` = `1`
- `MYSQL_HOST` = `127.0.0.1`
- `MYSQL_USER` = `root`（或你创建的 `slg_monitor` 用户）
- `MYSQL_PASSWORD` = 你的 MySQL 密码
- `MYSQL_DATABASE` = `slg_monitor`

保存后**重启服务**。

---

## 四、验证

1. 浏览器访问：`http://127.0.0.1:8000/frontend/`（端口按你实际用的改）。
2. 登录后点击顶部 **「高级查询」**。
3. 左侧「数据库表」下应出现表名列表（如 `app_config`、`formatted_data` 等），不再显示「高级查询需启用 MySQL」。

若仍显示「高级查询需启用 MySQL」，说明当前进程没有带 `USE_MYSQL=1` 或连不上 MySQL，请检查：

- 启动命令里是否包含 `USE_MYSQL=1`；
- MySQL 是否在运行（`brew services list` 或 `mysql -u root -p -e "SELECT 1"`）；
- 用户名、密码、库名是否正确。

更完整的迁移与部署说明见：`docs/第二步_数据迁移与启用MySQL.md`。
