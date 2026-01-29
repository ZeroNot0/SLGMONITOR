# 定期更新数据说明

## 概述

前端只负责**展示数据**，不调用任何API。数据更新由后端脚本定期运行完成。

## 数据更新脚本

### 主要脚本

1. **`run_full_pipeline.py`** - 完整数据流程
   ```bash
   python run_full_pipeline.py --week 1201-1207 --year 2025 --target_type old --limit 10
   ```

2. **`schedule_update.py`** - 定期更新脚本（推荐）
   ```bash
   # 更新当前周的数据
   python schedule_update.py
   
   # 更新指定周的数据
   python schedule_update.py --week 1201-1207 --year 2025
   ```

### 已废弃的脚本

以下脚本在清理时已被删除，请使用新的脚本：

- ❌ `request/advertisements_requests.py` → ✅ 使用 `request/fetch_ad_creatives.py`
- ❌ `request/downloads_request.py` → ✅ 使用 `request/fetch_country_data.py`

## 设置定期任务

### Linux/Mac (cron)

编辑crontab：
```bash
crontab -e
```

添加任务（例如：每周一早上8点运行）：
```bash
0 8 * * 1 cd /Users/codfz1/Desktop/Tuyoo\ Internship/SLG\ Monitor\ 3.0 && /opt/anaconda3/envs/deeppython/bin/python schedule_update.py >> logs/update.log 2>&1
```

### Windows (任务计划程序)

1. 打开"任务计划程序"
2. 创建基本任务
3. 触发器：每周一早上8点
4. 操作：启动程序
   - 程序：`C:\path\to\python.exe`
   - 参数：`schedule_update.py`
   - 起始于：项目目录路径

## 数据流程

1. **后端定期运行** `schedule_update.py` 或 `run_full_pipeline.py`
2. **生成Excel文件** 到 `output/{year}/` 目录
3. **前端自动读取** Excel文件并展示（无需重启）

## 注意事项

- 确保Python环境正确（`/opt/anaconda3/envs/deeppython/bin/python`）
- 确保有足够的API配额
- 建议在非工作时间运行，避免影响前端访问
- 可以设置日志文件记录运行情况

## 日志

建议将输出重定向到日志文件：
```bash
python schedule_update.py >> logs/update_$(date +%Y%m%d).log 2>&1
```
