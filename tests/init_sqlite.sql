-- SQLite compatible init script with base schema simulation
-- SQLite doesn't support PostgreSQL-style schemas, so we use ATTACH DATABASE
-- The main database is attached as 'base' to match the SeaORM entity schema_name

-- First attach an in-memory database as 'base' schema
ATTACH DATABASE ':memory:' AS base;

-- Drop existing tables in reverse dependency order
DROP TABLE IF EXISTS base.log;
DROP TABLE IF EXISTS base.task_result;
DROP TABLE IF EXISTS base.rel_account_platform;
DROP TABLE IF EXISTS base.rel_module_account;
DROP TABLE IF EXISTS base.rel_module_platform;
DROP TABLE IF EXISTS base.rel_module_data_middleware;
DROP TABLE IF EXISTS base.rel_module_download_middleware;
DROP TABLE IF EXISTS base.data_middleware;
DROP TABLE IF EXISTS base.download_middleware;
DROP TABLE IF EXISTS base.account;
DROP TABLE IF EXISTS base.platform;
DROP TABLE IF EXISTS base.module;

-- Create module table
CREATE TABLE base.module (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version INTEGER NOT NULL DEFAULT 1
);

-- Create data_middleware table
CREATE TABLE base.data_middleware (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create rel_module_data_middleware junction table
CREATE TABLE base.rel_module_data_middleware (
    module_id INTEGER NOT NULL REFERENCES base.module(id) ON DELETE CASCADE,
    data_middleware_id INTEGER NOT NULL REFERENCES base.data_middleware(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (module_id, data_middleware_id)
);

-- Create download_middleware table
CREATE TABLE base.download_middleware (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create rel_module_download_middleware junction table
CREATE TABLE base.rel_module_download_middleware (
    module_id INTEGER NOT NULL REFERENCES base.module(id) ON DELETE CASCADE,
    download_middleware_id INTEGER NOT NULL REFERENCES base.download_middleware(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (module_id, download_middleware_id)
);

-- Create account table
CREATE TABLE base.account (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    modules TEXT NOT NULL DEFAULT '{}',
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create platform table
CREATE TABLE base.platform (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    base_url VARCHAR(255),
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create rel_account_platform junction table
CREATE TABLE base.rel_account_platform (
    account_id INTEGER NOT NULL REFERENCES base.account(id) ON DELETE CASCADE,
    platform_id INTEGER NOT NULL REFERENCES base.platform(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, platform_id)
);

-- Create rel_module_account junction table
CREATE TABLE base.rel_module_account (
    module_id INTEGER NOT NULL REFERENCES base.module(id) ON DELETE CASCADE,
    account_id INTEGER NOT NULL REFERENCES base.account(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (module_id, account_id)
);

-- Create rel_module_platform junction table
CREATE TABLE base.rel_module_platform (
    module_id INTEGER NOT NULL REFERENCES base.module(id) ON DELETE CASCADE,
    platform_id INTEGER NOT NULL REFERENCES base.platform(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (module_id, platform_id)
);

-- Create indexes for junction tables
CREATE INDEX base.idx_rel_module_account_account_id ON base.rel_module_account(account_id);
CREATE INDEX base.idx_rel_account_platform_platform_id ON base.rel_account_platform(platform_id);
CREATE INDEX base.idx_rel_module_platform_platform_id ON base.rel_module_platform(platform_id);

-- Create log table
CREATE TABLE base.log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id VARCHAR(255) NOT NULL,
    request_id TEXT,
    status VARCHAR(50) NOT NULL,
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    traceback TEXT
);

CREATE INDEX base.idx_log_task_id ON base.log(task_id);
CREATE INDEX base.idx_log_timestamp ON base.log(timestamp);

-- Create task_result table
CREATE TABLE base.task_result (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    result TEXT,
    error TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX base.idx_task_result_task_id ON base.task_result(task_id);
