#!/bin/bash
# 在项目根目录执行，将当前项目打包为 zip（不排除任何文件）
# 用法：在项目根目录执行 bash deploy/pack.sh
cd "$(dirname "$0")/.."
OUT="SLG-Monitor-$(date +%Y%m%d-%H%M).zip"
zip -r "$OUT" . -x "*.git*"
echo "已生成: $(pwd)/$OUT"
