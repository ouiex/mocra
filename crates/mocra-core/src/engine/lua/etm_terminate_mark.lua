-- KEYS[1] = terminate_key
-- ARGV[1] = reason
-- ARGV[2] = now_ms
-- ARGV[3] = ttl_secs(optional)

local terminate_key = KEYS[1]

local reason = ARGV[1] or "TERMINATED"
local now_ms = tonumber(ARGV[2]) or 0
local ttl_secs = tonumber(ARGV[3])

if redis.call('EXISTS', terminate_key) == 1 then
	return {1, 'ALREADY_TERMINATED', '{}'}
end

local payload_obj = {
	reason = reason,
	terminated_at_ms = now_ms
}
local payload = cjson.encode(payload_obj)
redis.call('SET', terminate_key, payload)
if ttl_secs and ttl_secs > 0 then
	redis.call('EXPIRE', terminate_key, ttl_secs)
end

return {0, 'TERMINATED', payload}
