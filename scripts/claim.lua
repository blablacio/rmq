-- retry_claim.lua

-- KEYS[1]: Stream name
-- ARGV[1]: Consumer group name
-- ARGV[2]: Consumer name
-- ARGV[3]: Pending timeout in milliseconds (or 'nil' if not provided)

local stream = KEYS[1]
local group = ARGV[1]
local consumer = ARGV[2]
local pending_timeout = ARGV[3]

local result = {}

if pending_timeout ~= 'nil' then
    -- Step 1: Attempt to claim pending messages that have exceeded the pending_timeout
    local auto_claim = redis.call('XAUTOCLAIM', stream, group, consumer, pending_timeout, '0-0', 'COUNT', 1)
    local claimed_messages = auto_claim[2]

    if #claimed_messages > 0 then
        for _, message in ipairs(claimed_messages) do
            local msg_id = message[1]
            local fields = message[2]
            
            -- Retrieve the pending info for the message to get retry_count
            local pending_info = redis.call('XPENDING', stream, group, msg_id, msg_id, 1)
            local retry_count = 0

            for _, entry in ipairs(pending_info) do
                if type(entry) == "table" and #entry >= 4 then
                    retry_count = tonumber(entry[4]) or 0
                end
            end

            -- Append the message ID, fields, and retry_count to the result
            table.insert(result, {msg_id, fields, retry_count - 1})
        end

        return result
    end
end

-- Step 2: Attempt to read new messages
local read = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', 1, 'STREAMS', stream, '>')

if read then
    for _, stream_data in ipairs(read) do
        local msgs = stream_data[2]

        for _, msg in ipairs(msgs) do
            local msg_id = msg[1]
            local fields = msg[2]
            
            -- Retrieve the pending info for the message to get retry_count
            local pending_info = redis.call('XPENDING', stream, group, msg_id, msg_id, 1)
            local retry_count = 0

            for _, entry in ipairs(pending_info) do
                if type(entry) == "table" and #entry >= 4 then
                    retry_count = tonumber(entry[4]) or 0
                end
            end

            -- Append the message ID, fields, and retry_count to the result
            table.insert(result, {msg_id, fields, retry_count - 1})
        end
    end
end

return result