-- Seed data for moc.dev test module (benchmark test)

-- 1. Insert Module with name matching the registered module
INSERT INTO base.module (name, version, priority, enabled)
VALUES ('moc.dev', 1, 5, true)
ON CONFLICT (name) DO NOTHING;

-- 2. Insert Account with name matching TaskModel.account
INSERT INTO base.account (name, enabled, priority)
VALUES ('benchmark', true, 5)
ON CONFLICT (name) DO NOTHING;

-- 3. Insert Platform with name matching TaskModel.platform
INSERT INTO base.platform (name, description, enabled)
VALUES ('test', 'Test Platform for Benchmark', true)
ON CONFLICT (name) DO NOTHING;

-- 4. Link Module to Account
INSERT INTO base.rel_module_account (module_id, account_id, priority, enabled)
SELECT m.id, a.id, 5, true
FROM base.module m, base.account a
WHERE m.name = 'moc.dev' AND a.name = 'benchmark'
ON CONFLICT DO NOTHING;

-- 5. Link Module to Platform
INSERT INTO base.rel_module_platform (module_id, platform_id, priority, enabled)
SELECT m.id, p.id, 5, true
FROM base.module m, base.platform p
WHERE m.name = 'moc.dev' AND p.name = 'test'
ON CONFLICT DO NOTHING;

-- 6. Link Account to Platform
INSERT INTO base.rel_account_platform (account_id, platform_id, enabled)
SELECT a.id, p.id, true
FROM base.account a, base.platform p
WHERE a.name = 'benchmark' AND p.name = 'test'
ON CONFLICT DO NOTHING;
