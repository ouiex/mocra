-- KEYS[1] = retry_schedule_zset_key
-- ARGV[1] = member
-- ARGV[2] = retry_after_ms
-- ARGV[3] = now_ms
-- ARGV[4] = ttl_secs (optional)

local retry_key = KEYS[1]

local member = ARGV[1] or ""
local retry_after_ms = tonumber(ARGV[2]) or 1000
local now_ms = tonumber(ARGV[3]) or 0
local ttl_secs = tonumber(ARGV[4])

if member == "" then
	return {1, 'RETRY_NOT_ALLOWED', '{}'}
end

local schedule_at = now_ms + retry_after_ms
redis.call('ZADD', retry_key, schedule_at, member)

if ttl_secs and ttl_secs > 0 then
	redis.call('EXPIRE', retry_key, ttl_secs)
end

local payload = cjson.encode({
	retry_key = retry_key,
	member = member,
	schedule_at = schedule_at,
	retry_after_ms = retry_after_ms
})

return {0, 'RETRY_SCHEDULED', payload}
