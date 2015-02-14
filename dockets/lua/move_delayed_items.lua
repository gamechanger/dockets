local delayedQueueKey = KEYS[1]
local payloadKey = KEYS[2]
local queueKey = KEYS[3]

-- We're forced to pass this in because Redis forces our
-- scripts to be deterministic
local currentTime = ARGV[1]
local FIFO = ARGV[2]

local toMove = redis.call('zrangebyscore', delayedQueueKey, 0, currentTime, 'limit', 0, 50)

for index, moveKey in ipairs(toMove) do
   local movePayload = redis.call('hget', payloadKey, moveKey)
   if FIFO then
      redis.call('lpush', queueKey, movePayload)
   else
      redis.call('rpush', queueKey, movePayload)
   end
   redis.call('zrem', delayedQueueKey, moveKey)
   redis.call('hdel', payloadKey, moveKey)
end

return #toMove
