-- KEYS[1] = module_error_counter_key
-- KEYS[2] = task_error_counter_key
-- ARGV[1] = module_threshold
-- ARGV[2] = task_threshold
-- ARGV[3] = retry_after_ms
-- ARGV[4] = ttl_secs (optional)

local module_key = KEYS[1]
local task_key = KEYS[2]

local module_threshold = tonumber(ARGV[1]) or 10
local task_threshold = tonumber(ARGV[2]) or 100
local retry_after_ms = tonumber(ARGV[3]) or 1000
local ttl_secs = tonumber(ARGV[4])

local module_count = redis.call('INCR', module_key)
local task_count = redis.call('INCR', task_key)

if ttl_secs and ttl_secs > 0 then
	redis.call('EXPIRE', module_key, ttl_secs)
	redis.call('EXPIRE', task_key, ttl_secs)
end

local payload = cjson.encode({
	module_count = module_count,
	task_count = task_count,
	module_threshold = module_threshold,
	task_threshold = task_threshold,
	retry_after_ms = retry_after_ms
})

if module_count >= module_threshold then
	return {2, 'TERMINATE_MODULE', payload}
end

if task_count >= task_threshold then
	return {3, 'TERMINATE_TASK', payload}
end

if retry_after_ms > 0 then
	return {1, 'RETRY_AFTER', payload}
end

return {0, 'CONTINUE', payload}
