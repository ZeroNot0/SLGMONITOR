# 将项目推送到 GitHub

仓库地址：<https://github.com/ZeroNot0/SLGMONITOR.git>

---

## 一、首次推送（本机未初始化过 Git）

在项目根目录（`SLG Monitor 3.0`）下执行：

```bash
cd "/Users/codfz1/Desktop/Tuyoo Internship/SLG Monitor 3.0"

# 1. 初始化仓库
git init

# 2. 添加远程仓库
git remote add origin https://github.com/ZeroNot0/SLGMONITOR.git

# 3. 添加所有文件（.gitignore 已排除 token、__pycache__ 等）
git add .

# 4. 首次提交
git commit -m "Initial commit: SLG Monitor 3.0"

# 5. 推送到 GitHub（主分支名一般为 main 或 master，按你仓库默认）
git branch -M main
git push -u origin main
```

若 GitHub 仓库默认分支是 `master`，把上面最后两行改成：

```bash
git push -u origin master
```

---

## 二、已在别处克隆过 / 已有 .git

若项目里已经有 `.git`（例如从别处克隆的），只需添加你的远程并推送：

```bash
cd "/Users/codfz1/Desktop/Tuyoo Internship/SLG Monitor 3.0"
git remote add origin https://github.com/ZeroNot0/SLGMONITOR.git
git add .
git commit -m "Initial commit: SLG Monitor 3.0"
git branch -M main
git push -u origin main
```

若提示 `origin` 已存在，可先查看再决定是否替换：

```bash
git remote -v
# 若要改成你的仓库：
git remote set-url origin https://github.com/ZeroNot0/SLGMONITOR.git
```

---

## 三、重要说明

1. **不要提交 API 密钥**  
   已用 `.gitignore` 排除 `request/token.txt`。请在本地自己创建该文件并填入 token，不要提交到 GitHub。

2. **首次推送可能很大**  
   若未在 `.gitignore` 中排除 `advertisements`、`raw_csv`、`countiesdata` 等数据目录，首次 `git push` 会较慢；若以后希望仓库只放代码、不放数据，可编辑根目录 `.gitignore`，取消注释“可选”部分后再提交。

3. **推送需登录 GitHub**  
   使用 HTTPS 时，会提示输入 GitHub 用户名和密码（或 Personal Access Token）。若已配置 SSH，可改用地址：  
   `git@github.com:ZeroNot0/SLGMONITOR.git`  
   并执行：  
   `git remote set-url origin git@github.com:ZeroNot0/SLGMONITOR.git`  
   再 `git push -u origin main`。

---

## 四、之后日常更新

改完代码后：

```bash
git add .
git commit -m "简短说明本次修改"
git push
```
