#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性创建 5 个超级管理员账号，写入 deploy/auth_users.json（与现有用户合并）。
默认用户名：super_admin_1 .. super_admin_5，默认密码：SuperAdmin123!
可设置环境变量 SLG_SUPER_ADMIN_PASSWORD 覆盖默认密码。
"""
import os
import sys
from pathlib import Path

# 使用 create_admin 的逻辑（保证从项目根或 deploy 目录运行都能找到模块）
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
from deploy.create_admin import load_users, save_users, hash_password

DEFAULT_PASSWORD = "SuperAdmin123!"
USERNAMES = ["super_admin_1", "super_admin_2", "super_admin_3", "super_admin_4", "super_admin_5"]


def main():
    password = os.environ.get("SLG_SUPER_ADMIN_PASSWORD", DEFAULT_PASSWORD).strip()
    if not password or len(password) < 6:
        print("请设置环境变量 SLG_SUPER_ADMIN_PASSWORD（至少 6 位）或使用默认密码")
        password = DEFAULT_PASSWORD

    users = load_users()
    seen = {u.get("username", "").strip() for u in users}

    for username in USERNAMES:
        if username in seen:
            print("已存在用户 %s，跳过" % username)
            continue
        salt, h = hash_password(password)
        users.append({
            "username": username,
            "salt": salt,
            "hash": h,
            "role": "super_admin",
            "status": "approved",
        })
        seen.add(username)
        print("已添加超级管理员: %s" % username)

    save_users(users)
    print("共 5 个超级管理员账号，默认密码: %s" % (password if password == DEFAULT_PASSWORD else "(已使用 SLG_SUPER_ADMIN_PASSWORD)"))


if __name__ == "__main__":
    main()
