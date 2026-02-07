#!/usr/bin/env bash
# 一键启动 MySQL（无密码） + SLG Monitor 服务
#
# 用法（在项目根目录）：
#   ./start_with_mysql.sh           # 默认端口 8000
#   PORT=8080 ./start_with_mysql.sh # 指定端口
#
# 若 MySQL root 有密码，请先设置再执行：
#   export MYSQL_PASSWORD=你的密码
#   ./start_with_mysql.sh

set -e
cd "$(dirname "$0")"

PORT="${PORT:-8000}"

echo "=========================================="
echo "  SLG Monitor 3.0 - 启动 MySQL + 服务"
echo "=========================================="

# 1. 启动 MySQL
echo ""
echo "[1/3] 检查并启动 MySQL..."
if command -v brew &>/dev/null; then
  # Mac: Homebrew
  if brew services list 2>/dev/null | grep -q mysql; then
    brew services start mysql 2>/dev/null || true
  else
    # 未通过 brew 安装，尝试 mysql.server
    (mysql.server start 2>/dev/null) || true
  fi
elif command -v systemctl &>/dev/null && systemctl list-units --type=service 2>/dev/null | grep -q mysql; then
  sudo systemctl start mysql 2>/dev/null || true
else
  echo "  未检测到 brew 或 systemctl 下的 MySQL，请确保 MySQL 已安装并手动启动。"
fi

# 2. 等待 MySQL 就绪
echo ""
echo "[2/3] 等待 MySQL 就绪..."
for i in 1 2 3 4 5 6 7 8 9 10; do
  if mysql -u root -e "SELECT 1" &>/dev/null; then
    echo "  MySQL 已就绪。"
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "  警告：MySQL 可能尚未启动或 root 需要密码，将继续启动服务（未启用 MySQL 时不影响）。"
  else
    sleep 1
  fi
done

# 3. 启动 SLG Monitor 服务（无密码）
echo ""
echo "[3/3] 启动 SLG Monitor 服务（端口 ${PORT}，已启用 MySQL，root 无密码）..."
echo "  访问: http://localhost:${PORT}/frontend/"
echo "  按 Ctrl+C 停止服务"
echo ""

export USE_MYSQL=1
export MYSQL_USER=root
export MYSQL_PASSWORD=
export MYSQL_DATABASE=slg_monitor
export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-3306}"

exec python server/start_server.py --port "$PORT"
