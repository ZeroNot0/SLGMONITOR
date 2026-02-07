# 第二步：数据迁移与启用 MySQL

按顺序完成下面 3 步后，前端会从 MySQL 读数据（或未设置 USE_MYSQL 时仍从文件读）。

---

## 本地先跑一遍（确认无误再迁到云）

在 Mac 上建议先在本机跑通，再在服务器上按「步骤 1～3」操作。

### 0. 本机准备 MySQL

- **已装 MySQL**：在终端执行 `mysql --version` 能输出版本即可。
- **未装**：可用 Homebrew：`brew install mysql`，安装后 `brew services start mysql`。

### 1. 建库并导入表结构

在**项目根目录**执行（把 `你的root密码` 换成实际密码，无密码可留空 `-p` 后直接回车）：

```bash
cd "/Users/codfz1/Desktop/Tuyoo Internship/SLG Monitor 3.0"
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS slg_monitor DEFAULT CHARSET utf8mb4;"
mysql -u root -p slg_monitor < backend/db/schema.sql
```

### 2. 安装依赖并执行数据迁移

```bash
pip install pymysql   # 若未装
MYSQL_USER=root MYSQL_PASSWORD=你的root密码 MYSQL_DATABASE=slg_monitor python -m backend.db.migrate_data
```

看到多行 `[OK] ...` 和「迁移完成。」即表示成功。

### 3. 启动服务并用 MySQL 读数据

```bash
USE_MYSQL=1 MYSQL_USER=root MYSQL_PASSWORD=你的root密码 MYSQL_DATABASE=slg_monitor python start_server.py --port 8000
```

浏览器访问：`http://127.0.0.1:8000/frontend/`，登录后选年/周，能正常看到公司维度、产品维度等数据即表示本地 MySQL 模式正常。确认无误后，再在云服务器上按下面「步骤 1～3」做迁移与启用。

---

## 步骤 1：运行数据迁移脚本（把现有 JSON/Excel 导入 MySQL）

在**服务器上**、**项目根目录**下执行（把密码换成你的 MySQL 密码）：

```bash
cd /www/wwwroot/slg-monitor
source venv/bin/activate
pip install pymysql   # 若未装
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=root
export MYSQL_PASSWORD=你的MySQL的root密码
export MYSQL_DATABASE=slg_monitor
python -m backend.db.migrate_data
```

或一行命令（推荐用 root 执行迁移）：

```bash
MYSQL_USER=root MYSQL_PASSWORD=你的root密码 python -m backend.db.migrate_data
```

看到多行 `[OK] ...` 和最后的「迁移完成。」即表示导入成功。

---

## 步骤 2：启用从 MySQL 读数据并重启服务

让后端从 MySQL 读数据需要设置环境变量 **USE_MYSQL=1**，并保证 **MYSQL_PASSWORD** 等已配置。

### 方式 A：用 systemd（你之前用的 slg-monitor.service）

编辑服务文件：

```bash
sudo nano /etc/systemd/system/slg-monitor.service
```

在 `[Service]` 里增加环境变量（注意路径和 ExecStart 保持你当前配置）：

```ini
[Service]
Environment="USE_MYSQL=1"
Environment="MYSQL_HOST=127.0.0.1"
Environment="MYSQL_USER=slg_monitor"
Environment="MYSQL_PASSWORD=你创建的slg_monitor用户密码"
Environment="MYSQL_DATABASE=slg_monitor"
```

保存后执行：

```bash
sudo systemctl daemon-reload
sudo systemctl restart slg-monitor
sudo systemctl status slg-monitor
```

### 方式 B：用宝塔「Python 项目管理器」

在项目的「设置」或「环境变量」里添加：

- `USE_MYSQL` = `1`
- `MYSQL_HOST` = `127.0.0.1`
- `MYSQL_USER` = `slg_monitor`
- `MYSQL_PASSWORD` = 你的数据库密码
- `MYSQL_DATABASE` = `slg_monitor`

保存后重启该项目。

### 方式 C：命令行临时测试

```bash
cd /www/wwwroot/slg-monitor
source venv/bin/activate
USE_MYSQL=1 MYSQL_PASSWORD=你的密码 python start_server.py --port 8000
```

---

## 步骤 3：确认前端已用 /api/data/*

前端已改为请求 **/api/data/weeks_index**、**/api/data/formatted**、**/api/data/product_strategy** 等，不再直接请求 `/frontend/data/*.json`。

- **USE_MYSQL=1** 时：后端从 MySQL 读并返回相同结构的 JSON。
- **未设置 USE_MYSQL** 时：后端从 `frontend/data/` 等文件读并返回，行为与之前一致。

在浏览器打开站点，登录后选年/周，能正常看到公司维度、产品维度等数据即表示第二步完成。

---

## 常见问题

1. **迁移报错 No module named 'backend'**  
   必须在**项目根目录**执行：`cd /www/wwwroot/slg-monitor` 后再运行 `python -m backend.db.migrate_data`。

2. **迁移报错 Access denied / 2003**  
   检查 MYSQL_USER、MYSQL_PASSWORD、MYSQL_DATABASE 是否正确；迁移建议用 **root**，运行服务可用 **slg_monitor** 用户。

3. **页面空白或 500**  
   看后端日志（`journalctl -u slg-monitor -f` 或 Python 项目管理器日志）。若报错 pymysql、backend.db 等，确认已 `pip install pymysql` 且 USE_MYSQL=1 时 MYSQL_PASSWORD 已设置。

4. **以后更新了 frontend/data 或 mapping，要再入 MySQL 吗？**  
   需要。可再次执行一次 `python -m backend.db.migrate_data`（会覆盖同主键数据）。或后续在流水线步骤 5 末尾增加「同步到 MySQL」的调用，使每次跑完脚本自动更新库。
