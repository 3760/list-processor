-- ================================================================
-- 数据库迁移脚本: 001_initial.sql
-- 描述: 初始化数据库，创建历史记录表和处理日志表
-- ================================================================

-- 1. 处理历史记录表
CREATE TABLE IF NOT EXISTS processing_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT    NOT NULL UNIQUE,   -- 唯一运行ID（UUID）
    start_time      TEXT    NOT NULL,           -- ISO 8601 格式时间
    end_time        TEXT,                       -- 结束时间
    status          TEXT    NOT NULL DEFAULT 'running',  -- running / completed / failed
    input_yixian    TEXT,                       -- 一线名单文件路径
    input_sanfang   TEXT,                       -- 三方名单文件路径
    input_hw        TEXT,                       -- HW名单文件路径
    total_records   INTEGER DEFAULT 0,          -- 输入总记录数
    output_records  INTEGER DEFAULT 0,          -- 输出有效记录数
    error_records   INTEGER DEFAULT 0,          -- 错误记录数
    duplicate_count INTEGER DEFAULT 0,          -- 重复记录数
    summary         TEXT,                       -- JSON 格式的处理摘要
    output_dir      TEXT,                       -- 输出目录路径（v1.0.2+）
    dict_file       TEXT,                       -- 字典文件路径（v1.0.3+）
    spec_file       TEXT,                       -- 字段规范文件路径（v1.0.3+）
    created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_history_status   ON processing_history(status);
CREATE INDEX IF NOT EXISTS idx_history_start    ON processing_history(start_time);

-- 2. 处理日志表（记录模块级执行明细）
CREATE TABLE IF NOT EXISTS processing_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT    NOT NULL,
    module      TEXT    NOT NULL,               -- 模块名称：F1/F2/F3/F4/F5/F6/F7
    phase       TEXT    NOT NULL,               -- 处理阶段描述
    status      TEXT    NOT NULL,               -- running / completed / failed / skipped
    message     TEXT,                           -- 日志信息
    detail      TEXT,                           -- 详细错误信息（如有）
    elapsed_ms  INTEGER,                        -- 耗时（毫秒）
    created_at  TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
    FOREIGN KEY (run_id) REFERENCES processing_history(run_id)
);

CREATE INDEX IF NOT EXISTS idx_log_run_id   ON processing_log(run_id);
CREATE INDEX IF NOT EXISTS idx_log_module   ON processing_log(module);

-- 3. 字典版本表（记录字典文件 MD5 版本）
CREATE TABLE IF NOT EXISTS dict_version (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT    NOT NULL,
    dict_file   TEXT    NOT NULL,           -- 字典文件路径
    file_name   TEXT    NOT NULL,           -- 文件名
    md5_hash    TEXT    NOT NULL,           -- MD5 哈希值
    recorded_at TEXT    NOT NULL            -- 记录时间
);

CREATE INDEX IF NOT EXISTS idx_dv_run_id   ON dict_version(run_id);
CREATE INDEX IF NOT EXISTS idx_dv_file     ON dict_version(dict_file);

-- 4. 应用配置表（KV 存储）
CREATE TABLE IF NOT EXISTS app_config (
    key         TEXT PRIMARY KEY,
    value       TEXT,
    description TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);
