-- KEYS[1] = ptm_dedup_key (string state: pending|processing|done|failed)
-- KEYS[2] = exec_state_key (hash)
-- ARGV[1] = expected_step
-- ARGV[2] = lease_owner
-- ARGV[3] = lease_ttl_secs
-- ARGV[4] = now_epoch_secs

local dedup_key = KEYS[1]
local exec_key = KEYS[2]

local expected_step = tonumber(ARGV[1]) or 0
local lease_owner = ARGV[2] or ''
local lease_ttl_secs = tonumber(ARGV[3]) or 300
local now_epoch_secs = tonumber(ARGV[4]) or 0

local dedup_state = redis.call('GET', dedup_key)
if dedup_state == 'done' then
	return {1, 'DUPLICATE_DONE', '{}'}
end

if dedup_state == 'processing' then
	return {4, 'LOCKED_BY_OTHER', '{}'}
end

local committed_step = tonumber(redis.call('HGET', exec_key, 'committed_step') or '0')

if expected_step <= committed_step then
	return {2, 'STALE_STEP', '{}'}
end

if expected_step > committed_step + 1 then
	return {3, 'OUT_OF_ORDER', '{}'}
end

local version = redis.call('HINCRBY', exec_key, 'version', 1)
redis.call('HSET', exec_key,
	'inflight_step', expected_step,
	'lease_owner', lease_owner,
	'lease_expire_at', now_epoch_secs + lease_ttl_secs,
	'updated_at', now_epoch_secs
)
redis.call('EXPIRE', exec_key, lease_ttl_secs)

redis.call('SET', dedup_key, 'processing', 'EX', lease_ttl_secs)

local payload = cjson.encode({
	committed_step = committed_step,
	inflight_step = expected_step,
	version = version
})

return {0, 'EXECUTE', payload}
