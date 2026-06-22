package wallet

// All Redis operations that touch both balance and accumulator must be
// atomic. Lua scripts execute as a single unit inside Redis — no other
// command can interleave between the check and the deduct, even at 10k TPS.
//
// Balances are integer message credits (1 credit ≈ 1 SMS unit), not
// currency — so every script uses INCRBY/DECRBY rather than float ops.

// luaDeduct atomically:
//  1. Checks if the balance key exists. If not, returns -1 (Cold Cache).
//  2. Reads the client's hot balance (credits).
//  3. If balance >= cost: deducts cost, increments the pending accumulator
//     and message counter, returns {1, "new_balance"}.
//  4. If balance < cost: returns {0, "current_balance"} — no state changed.
//
// KEYS[1] = wallet:{client_id}:balance
// KEYS[2] = wallet:{client_id}:pending_deduction
// KEYS[3] = wallet:{client_id}:pending_count
// ARGV[1] = cost (integer credits, string representation)
const luaDeduct = `
local balanceStr = redis.call('GET', KEYS[1])
if balanceStr == false then
  return -1 -- COLD CACHE SIGNAL
end

local balance = tonumber(balanceStr)
local cost = tonumber(ARGV[1])

if balance < cost then
  return {0, tostring(balance)}
end

local new_balance = balance - cost
redis.call('SET', KEYS[1], tostring(new_balance))
redis.call('INCRBY', KEYS[2], cost)
redis.call('INCR', KEYS[3])

return {1, tostring(new_balance)}
`

// luaRefund atomically adds credits back to the hot balance and decrements
// the accumulator. Called by the DLR reconciler on FAILED delivery when
// RefundOnFailedDelivery is enabled.
//
// KEYS[1] = wallet:{client_id}:balance
// KEYS[2] = wallet:{client_id}:pending_deduction
// KEYS[3] = wallet:{client_id}:pending_count
// ARGV[1] = refund amount (integer credits, string representation)
const luaRefund = `
local amount = tonumber(ARGV[1])
local current = tonumber(redis.call('GET', KEYS[1]))
if current == nil then current = 0 end

local new_balance = current + amount
redis.call('SET', KEYS[1], tostring(new_balance))

local pending = tonumber(redis.call('GET', KEYS[2]))
if pending ~= nil and pending >= amount then
  redis.call('DECRBY', KEYS[2], amount)
  local count = tonumber(redis.call('GET', KEYS[3]))
  if count ~= nil and count > 0 then
    redis.call('DECR', KEYS[3])
  end
end

return {1, tostring(new_balance)}
`

// luaFlushAccumulator atomically reads the pending deduction + count for a
// client and resets both to zero. The flusher calls this per client before
// sending the batch to the Core Wallet Service.
//
// KEYS[1] = wallet:{client_id}:pending_deduction
// KEYS[2] = wallet:{client_id}:pending_count
const luaFlushAccumulator = `
local amount = redis.call('GETSET', KEYS[1], '0')
local count  = redis.call('GETSET', KEYS[2], '0')
if amount == false then amount = '0' end
if count  == false then count  = '0' end
return {amount, count}
`

// luaSeedBalance sets the hot wallet balance (credits) for a client.
// Called when the Core Wallet Service returns a fresh balance from the
// internal balance endpoint, or on initial cache warm-up.
//
// KEYS[1] = wallet:{client_id}:balance
// ARGV[1] = amount (integer credits)
// ARGV[2] = force ("0" = only set if missing, "1" = always overwrite)
const luaSeedBalance = `
local force = ARGV[2]
if force == '1' then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
local exists = redis.call('EXISTS', KEYS[1])
if exists == 0 then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
return 0
`
