#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建或追加管理员/超级管理员账号，写入 deploy/auth_users.json。
用法:
  python deploy/create_admin.py                         # 交互输入用户名和密码（默认 role=user）
  python deploy/create_admin.py admin MyPass123         # 创建单个用户（覆盖文件，role=user）
  python deploy/create_admin.py --add --role super_admin u1 Pass123   # 追加超级管理员，不覆盖已有用户
  SLG_ADMIN_PASSWORD=MyPass python deploy/create_admin.py admin       # 密码从环境变量读取
"""
import argparse
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


def load_users():
    if not AUTH_USERS_PATH.is_file():
        return []
    try:
        data = json.loads(AUTH_USERS_PATH.read_text(encoding="utf-8"))
        return data.get("users") or []
    except Exception:
        return []


def save_users(users: list):
    AUTH_USERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUTH_USERS_PATH.write_text(
        json.dumps({"users": users}, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser(description="创建或追加用户到 auth_users.json")
    parser.add_argument("username", nargs="?", help="用户名")
    parser.add_argument("password", nargs="?", help="密码（可选，可从 SLG_ADMIN_PASSWORD 读取）")
    parser.add_argument("--add", action="store_true", help="追加用户而不覆盖现有文件")
    parser.add_argument("--role", choices=["user", "super_admin"], default="user", help="角色，默认 user")
    args = parser.parse_args()

    if args.username:
        username = args.username.strip()
        password = (args.password or os.environ.get("SLG_ADMIN_PASSWORD", "")).strip()
        if not password and not args.password:
            print("请提供密码或设置环境变量 SLG_ADMIN_PASSWORD")
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
    role = args.role
    new_user = {"username": username, "salt": salt, "hash": h, "role": role, "status": "approved"}

    if args.add:
        users = load_users()
        for u in users:
            if (u.get("username") or "").strip() == username:
                u["salt"] = salt
                u["hash"] = h
                u["role"] = role
                u["status"] = "approved"
                save_users(users)
                print("已更新用户 %s，角色: %s" % (username, role))
                return
        users.append(new_user)
        save_users(users)
        print("已追加用户 %s，角色: %s" % (username, role))
    else:
        save_users([new_user])
        print("已写入 %s，用户: %s，角色: %s" % (AUTH_USERS_PATH, username, role))


if __name__ == "__main__":
    main()
