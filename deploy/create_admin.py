#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建或重置管理员账号，写入 deploy/auth_users.json。
用法:
  python deploy/create_admin.py                    # 交互输入用户名和密码
  python deploy/create_admin.py admin MyPass123    # 命令行指定
  SLG_ADMIN_PASSWORD=MyPass python deploy/create_admin.py admin  # 密码从环境变量读取
"""
import hashlib
import json
import os
import secrets
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
AUTH_USERS_PATH = BASE_DIR / "deploy" / "auth_users.json"


def hash_password(password: str):
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return salt, h


def main():
    if len(sys.argv) >= 3:
        username = sys.argv[1].strip()
        password = sys.argv[2]
    elif len(sys.argv) == 2:
        username = sys.argv[1].strip()
        password = os.environ.get("SLG_ADMIN_PASSWORD", "").strip()
        if not password:
            print("请设置环境变量 SLG_ADMIN_PASSWORD 或使用: python create_admin.py <用户名> <密码>")
            sys.exit(1)
    else:
        username = input("用户名: ").strip()
        if not username:
            print("用户名不能为空")
            sys.exit(1)
        import getpass
        password = getpass.getpass("密码: ")
        if not password:
            print("密码不能为空")
            sys.exit(1)

    salt, h = hash_password(password)
    AUTH_USERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {"users": [{"username": username, "salt": salt, "hash": h}]}
    AUTH_USERS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("已写入 %s，用户: %s" % (AUTH_USERS_PATH, username))


if __name__ == "__main__":
    main()
