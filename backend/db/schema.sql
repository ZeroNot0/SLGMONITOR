-- SLG Monitor 3.0 数据库表结构
-- 使用前请在宝塔「数据库」中先创建数据库（如 slg_monitor），再导入本文件。
-- MySQL 5.7+ / 8.0 均可。
--
-- 【数据底表】与物理表对应（表结构以数据底表为准，共 7 张）：
--   1. 产品总表       -> metrics_total (按 year, week_tag)
--   2. 产品归属表     -> basetable (name=product_mapping)
--   3. 公司归属表     -> basetable (name=company_mapping)
--   4. 新产品监测表   -> new_products
--   5. 题材标签表     -> basetable (name=theme_label)，源文件 labels/题材标签表.xlsx
--   6. 玩法标签表     -> basetable (name=gameplay_label)，源文件 labels/玩法标签表.xlsx
--   7. 画风标签表     -> basetable (name=art_style_label)，源文件 labels/画风标签表.xlsx

-- 1. 全局配置（周索引、data_range）
CREATE TABLE IF NOT EXISTS app_config (
  config_key   VARCHAR(64) PRIMARY KEY COMMENT 'weeks_index, data_range',
  config_value JSON NOT NULL COMMENT 'JSON 体',
  updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. 按年周索引（可选，便于按年/周查）
CREATE TABLE IF NOT EXISTS year_weeks (
  year      SMALLINT UNSIGNED NOT NULL,
  week_tag  VARCHAR(16) NOT NULL COMMENT '如 0119-0125',
  PRIMARY KEY (year, week_tag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. 公司维度大盘（formatted）
CREATE TABLE IF NOT EXISTS formatted_data (
  year       SMALLINT UNSIGNED NOT NULL,
  week_tag   VARCHAR(16) NOT NULL,
  payload    JSON NOT NULL COMMENT '{"headers":[...],"rows":[...]}',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (year, week_tag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. 产品维度爆量表（旧/新）
CREATE TABLE IF NOT EXISTS product_strategy (
  year          SMALLINT UNSIGNED NOT NULL,
  week_tag      VARCHAR(16) NOT NULL,
  strategy_type ENUM('old','new') NOT NULL COMMENT 'old=爆量旧产品, new=爆量新产品',
  payload       JSON NOT NULL COMMENT '{"headers":[...],"rows":[...]}',
  updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (year, week_tag, strategy_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. 素材维度产品索引
CREATE TABLE IF NOT EXISTS creative_products (
  year       SMALLINT UNSIGNED NOT NULL,
  week_tag   VARCHAR(16) NOT NULL,
  payload    JSON NOT NULL COMMENT '{"week_tag","strategy_old",[...],"strategy_new",[...]}',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (year, week_tag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6. 产品总表（metrics_total）
CREATE TABLE IF NOT EXISTS metrics_total (
  year       SMALLINT UNSIGNED NOT NULL,
  week_tag   VARCHAR(16) NOT NULL,
  payload    JSON NOT NULL COMMENT '{"headers":[...],"rows":[...]}',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (year, week_tag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 7. 产品赛道排名（可选）
CREATE TABLE IF NOT EXISTS metrics_rank (
  id         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  payload    JSON NOT NULL COMMENT '{"unified_id":{"rankInstall","rankRevenue"},...}',
  scope      VARCHAR(32) DEFAULT 'global' COMMENT 'global 或 year_week',
  year       SMALLINT UNSIGNED NULL,
  week_tag   VARCHAR(16) NULL,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 8. 上线新游表（全局一份）
CREATE TABLE IF NOT EXISTS new_products (
  id         TINYINT UNSIGNED PRIMARY KEY DEFAULT 1,
  payload    JSON NOT NULL COMMENT '{"headers":[...],"rows":[...]}',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 9. 题材/画风映射（全局一份）
CREATE TABLE IF NOT EXISTS product_theme_style_mapping (
  id         TINYINT UNSIGNED PRIMARY KEY DEFAULT 1,
  payload    JSON NOT NULL COMMENT '{"byUnifiedId":{...},"byProductName":{...}}',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 10. 数据底表（与前端「数据底表」页 7 张表对应，此处 5 张存 basetable；产品总表、新产品监测表见上）
-- basetable.name 枚举：product_mapping(产品归属表), company_mapping(公司归属表),
--   theme_label(题材标签表), gameplay_label(玩法标签表), art_style_label(画风标签表)
-- 题材/玩法/画风 Excel 位于 labels/题材标签表.xlsx、labels/玩法标签表.xlsx、labels/画风标签表.xlsx，可由 scripts/init_label_tables.py 初始化
CREATE TABLE IF NOT EXISTS basetable (
  name       VARCHAR(32) PRIMARY KEY COMMENT 'product_mapping|company_mapping|theme_label|gameplay_label|art_style_label',
  headers    JSON NOT NULL COMMENT '["列名1",...]',
  `rows`     JSON NOT NULL COMMENT '[[...],...]',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 11. 用户表（替代 auth_users.json）
CREATE TABLE IF NOT EXISTS users (
  id            INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  username      VARCHAR(64) NOT NULL UNIQUE,
  salt          VARCHAR(64) NOT NULL,
  password_hash VARCHAR(128) NOT NULL,
  role          VARCHAR(32) NOT NULL DEFAULT 'user' COMMENT 'user|super_admin',
  status        VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT 'pending|approved',
  created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 12. 会话表（替代内存 session）
CREATE TABLE IF NOT EXISTS sessions (
  session_id   VARCHAR(64) PRIMARY KEY,
  user_id      INT UNSIGNED NOT NULL,
  expires_at   DATETIME NOT NULL,
  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
