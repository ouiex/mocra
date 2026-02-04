CREATE SCHEMA IF NOT EXISTS base;

DROP TABLE IF EXISTS base.log CASCADE;
DROP TABLE IF EXISTS base.task_result CASCADE;
DROP TABLE IF EXISTS base.rel_account_platform CASCADE;
DROP TABLE IF EXISTS base.rel_module_account CASCADE;
DROP TABLE IF EXISTS base.rel_module_platform CASCADE;
DROP TABLE IF EXISTS base.rel_module_data_middleware CASCADE;
DROP TABLE IF EXISTS base.data_middleware CASCADE;
DROP TABLE IF EXISTS base.rel_module_download_middleware CASCADE;
DROP TABLE IF EXISTS base.download_middleware CASCADE;
DROP TABLE IF EXISTS base.account CASCADE;
DROP TABLE IF EXISTS base.platform CASCADE;
DROP TABLE IF EXISTS base.module CASCADE;

CREATE TABLE base.module (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE base.data_middleware (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE base.rel_module_data_middleware (
    module_id INTEGER NOT NULL REFERENCES base.module(id) ON DELETE CASCADE,
    data_middleware_id INTEGER NOT NULL REFERENCES base.data_middleware(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (module_id, data_middleware_id)
);

CREATE TABLE base.download_middleware (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE base.rel_module_download_middleware (
    module_id INTEGER NOT NULL REFERENCES base.module(id) ON DELETE CASCADE,
    download_middleware_id INTEGER NOT NULL REFERENCES base.download_middleware(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (module_id, download_middleware_id)
);

CREATE TABLE base.account (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    modules TEXT[] NOT NULL DEFAULT '{}',
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE base.platform (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    base_url VARCHAR(255),
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE base.rel_account_platform (
    account_id INTEGER NOT NULL REFERENCES base.account(id) ON DELETE CASCADE,
    platform_id INTEGER NOT NULL REFERENCES base.platform(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, platform_id)
);

CREATE TABLE base.rel_module_account (
    module_id INTEGER NOT NULL REFERENCES base.module(id) ON DELETE CASCADE,
    account_id INTEGER NOT NULL REFERENCES base.account(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (module_id, account_id)
);

CREATE TABLE base.rel_module_platform (
    module_id INTEGER NOT NULL REFERENCES base.module(id) ON DELETE CASCADE,
    platform_id INTEGER NOT NULL REFERENCES base.platform(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (module_id, platform_id)
);

CREATE INDEX idx_rel_module_account_account_id ON base.rel_module_account(account_id);
CREATE INDEX idx_rel_account_platform_platform_id ON base.rel_account_platform(platform_id);
CREATE INDEX idx_rel_module_platform_platform_id ON base.rel_module_platform(platform_id);

CREATE TABLE base.log (
    id BIGSERIAL PRIMARY KEY,
    task_id VARCHAR(255) NOT NULL,
    request_id UUID,
    status VARCHAR(50) NOT NULL,
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    traceback TEXT
);

CREATE INDEX idx_log_task_id ON base.log(task_id);
CREATE INDEX idx_log_timestamp ON base.log(timestamp);

CREATE TABLE base.task_result (
    id BIGSERIAL PRIMARY KEY,
    task_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    result TEXT,
    error TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_task_result_task_id ON base.task_result(task_id);

