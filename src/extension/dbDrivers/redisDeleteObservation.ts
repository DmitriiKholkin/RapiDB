import { REDIS_READ_BUDGET } from "../../shared/safetyContracts";

export const REDIS_DELETE_EVIDENCE_BYTES = 64 * 1024;

// Redis 7/8 object.c uses the actual backing allocation (zmalloc_size), not
// a sample estimate, for these compact encodings. They are uncompressed;
// integer decoding and score formatting add only constant bytes per element.
// Other encodings (including compressed quicklists) cannot use this proof.
// Both the encoding check and the read must run in the same script.
const helpers = `local key = KEYS[1]
local limit = ${REDIS_DELETE_EVIDENCE_BYTES}
local typeName = redis.call('TYPE', key).ok
local lengths = { hash = 'HLEN', list = 'LLEN', set = 'SCARD', zset = 'ZCARD' }
local function bounded()
  if typeName == 'string' then
    return redis.call('STRLEN', key) <= limit
  end
  local len = lengths[typeName]
  if not len then return false end
  local encoding = redis.call('OBJECT', 'ENCODING', key)
  if encoding ~= 'listpack' and not (typeName == 'set' and encoding == 'intset')
    and not (typeName == 'hash' and encoding == 'listpackex') then return false end
  local bytes = redis.call('MEMORY', 'USAGE', key, 'SAMPLES', 1)
  return bytes and bytes <= limit and redis.call(len, key) <= ${REDIS_READ_BUDGET.maxValueEntries}
end
local function snapshot(call)
  if typeName == 'string' then
    local value = call('GET', key)
    return type(value) == 'string' and value or nil
  end
  local value = call('DUMP', key)
  return type(value) == 'string' and #value <= limit and value or nil
end
local function expiry(call)
  local at = call('PEXPIRETIME', key)
  if type(at) ~= 'number' or at == -2 or at >= 9007199254740992 then return nil end
  return at
end
local function hashEvidence(fields)
  local parts = {}
  for i = 1, #fields, 2 do
    local name, value = fields[i], fields[i+1]
    parts[#parts+1] = #name .. ':' .. name .. #value .. ':' .. value
  end
  table.sort(parts)
  return table.concat(parts)
end`;

// EVAL_RO starts in RESP2: all member/score strings remain raw bulk bytes,
// and the hashes and WITHSCORES replies are flat arrays. DUMP is optional for
// browsing, but if requested and permitted it uses the same bounded object.
export const REDIS_OBSERVE_DELETE = `${helpers}
if typeName ~= ARGV[1] then return false end
if typeName == 'string' then
  local cap = tonumber(ARGV[3])
  if not cap or cap < 0 or cap > limit or redis.call('STRLEN', key) > cap then return false end
elseif not bounded() then return false end
local value
if typeName == 'string' then value = snapshot(redis.pcall) end
if typeName == 'hash' then value = redis.call('HGETALL', key) end
if typeName == 'list' then value = redis.call('LRANGE', key, 0, -1) end
if typeName == 'set' then value = redis.call('SMEMBERS', key) end
if typeName == 'zset' then value = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES') end
if not value then return false end
local observed, at, visibleHash = false, false, false
if ARGV[2] == '1' then
  local absolute = expiry(redis.pcall)
  local raw = absolute and (typeName == 'string' or snapshot(redis.pcall))
  if raw then
    observed, at = raw, absolute
    if typeName == 'hash' then visibleHash = hashEvidence(value) end
  end
end
if typeName == 'string' then observed = false end
return {typeName, at, redis.call('TTL', key), observed, value, visibleHash}`;

export const REDIS_COMPARE_DELETE = `${helpers}
if typeName ~= ARGV[1] or expiry(redis.call) ~= tonumber(ARGV[2]) then return 0 end
if not bounded() then return 0 end
if typeName == 'hash' then
  if hashEvidence(redis.call('HGETALL', key)) ~= ARGV[4] then return 0 end
end
if snapshot(redis.call) ~= ARGV[3] then return 0 end
return redis.call('DEL', key)`;
