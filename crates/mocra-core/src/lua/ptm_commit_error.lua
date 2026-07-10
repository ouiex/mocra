-- KEYS[1] = ptm_dedup_key
-- KEYS[2] = exec_state_key
-- KEYS[3] = error_emit_key
-- ARGV[1] = lease_owner
-- ARGV[2] = fail_ttl_secs
-- ARGV[3] = emit_ttl_secs

local dedup_key = KEYS[1]
local exec_key = KEYS[2]
local error_emit_key = KEYS[3]

local lease_owner = ARGV[1] or ''
local fail_ttl_secs = tonumber(ARGV[2]) or 300
local emit_ttl_secs = tonumber(ARGV[3]) or 86400

local owner = redis.call('HGET', exec_key, 'lease_owner')
if owner and owner ~= lease_owner then
	return {2, 'FENCING_REJECT', '{}'}
end

if redis.call('SETNX', error_emit_key, '1') == 0 then
	return {1, 'ERROR_ALREADY_EMITTED', '{}'}
end

redis.call('EXPIRE', error_emit_key, emit_ttl_secs)
redis.call('SET', dedup_key, 'failed', 'EX', fail_ttl_secs)

local payload = cjson.encode({
	fail_ttl_secs = fail_ttl_secs,
	emit_ttl_secs = emit_ttl_secs
})

return {0, 'ERROR_EMIT_OK', payload}
