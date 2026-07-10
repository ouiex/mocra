-- KEYS[1] = ptm_dedup_key
-- KEYS[2] = exec_state_key
-- ARGV[1] = expected_step
-- ARGV[2] = lease_owner
-- ARGV[3] = now_epoch_secs
-- ARGV[4] = done_ttl_secs

local dedup_key = KEYS[1]
local exec_key = KEYS[2]

local expected_step = tonumber(ARGV[1]) or 0
local lease_owner = ARGV[2] or ''
local now_epoch_secs = tonumber(ARGV[3]) or 0
local done_ttl_secs = tonumber(ARGV[4]) or 86400

local dedup_state = redis.call('GET', dedup_key)
if dedup_state == 'done' then
	return {3, 'ALREADY_COMMITTED', '{}'}
end

local owner = redis.call('HGET', exec_key, 'lease_owner')
if owner and owner ~= lease_owner then
	return {1, 'FENCING_REJECT', '{}'}
end

local inflight_step = tonumber(redis.call('HGET', exec_key, 'inflight_step') or '-1')
if inflight_step ~= expected_step then
	return {2, 'CAS_CONFLICT', '{}'}
end

redis.call('HSET', exec_key,
	'committed_step', expected_step,
	'inflight_step', expected_step,
	'updated_at', now_epoch_secs
)

redis.call('SET', dedup_key, 'done', 'EX', done_ttl_secs)

local payload = cjson.encode({
	committed_step = expected_step,
	done_ttl_secs = done_ttl_secs
})

return {0, 'COMMIT_OK', payload}
