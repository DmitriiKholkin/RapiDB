import { isUtf8 } from "node:buffer";
import { randomUUID } from "node:crypto";
import { createClient, ErrorReply, RESP_TYPES } from "redis";
import { noSqlDeleteObservationError } from "../../shared/noSqlDeleteObservation";
import {
  isRedisBinaryCollectionValueSnapshot,
  isRedisBinaryValueSnapshot,
  isRedisNotLoadedValueSnapshot,
  isRedisOversizedValueSnapshot,
  isRedisUnavailableValueSnapshot,
  type RedisBinaryCollectionValueSnapshot,
  type RedisBinaryValueSnapshot,
  type RedisOversizedValueSnapshot,
  redisOversizedValueSnapshot,
  redisUnavailableValueSnapshot,
} from "../../shared/redisValueSnapshot";
import {
  type OperationCancellationContext,
  REDIS_READ_BUDGET,
} from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import {
  getSshTcpForwardTransport,
  getTlsServername,
} from "../driverRuntimeConfig";
import {
  type ResolvedConnectionTlsSettings,
  resolveConnectionTlsSettings,
} from "../services/connectionTls";
import { pMapWithLimit } from "../utils/concurrency";
import {
  isRoundTripJsonNumberToken,
  parseJsonForWrite,
} from "../utils/jsonWriteSafety";
import { logger } from "../utils/logger";
import { allowReadOnlyQuery, denyReadOnlyQuery } from "../utils/readOnlyGuards";
import { deleteRowsSequentially, prepareDeleteBatch } from "./deleteOutcomes";
import { getMetadataContext } from "./metadataCancellation";
import { MutationNotExecutedError } from "./mutationExecutionState";
import {
  applyFilters,
  applySort,
  flattenRootRecord,
  inferColumnsFromRows,
  pageRows,
  stringifyCommandPayload,
  unsupported,
} from "./nosqlUtils";
import {
  REDIS_COMPARE_DELETE,
  REDIS_DELETE_EVIDENCE_BYTES,
  REDIS_OBSERVE_DELETE,
} from "./redisDeleteObservation";
import {
  isRedisUnprefixedKeysTable,
  REDIS_ALL_KEYS_TABLE,
  REDIS_EMPTY_PREFIX_TABLE,
  REDIS_UNPREFIXED_KEYS_TABLE,
  redisKeyspacePattern,
} from "./redisKeyspace";
import {
  DriverTimeoutError,
  type DriverTimeoutSettingsProvider,
  getDefaultDriverTimeoutSettings,
  throwIfQueryCancelled,
  throwIfTransactionCancelled,
} from "./timeout";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseExecutionScope,
  DatabaseInfo,
  DriverDeleteRowsRequest,
  DriverEntityManifest,
  DriverInsertRowRequest,
  DriverMutationResult,
  DriverOperationContext,
  DriverTableExportChunk,
  DriverTablePageRequest,
  DriverTablePageResult,
  DriverUpdateRowOutcome,
  DriverUpdateRowsRequest,
  ForeignKeyMeta,
  IDBDriver,
  IndexMeta,
  NoSqlDeleteObservation,
  PaginationResult,
  PreparedObservedDelete,
  QueryExecutionOptions,
  QueryResult,
  SchemaInfo,
  TableConstraintMeta,
  TableInfoList,
  TransactionContext,
  TransactionOperation,
  TriggerMeta,
  TypeCategory,
} from "./types";
import { resolveFilterOperators } from "./types";
import {
  completeDriverUpdateRowsResult,
  failedDriverUpdateRows,
} from "./updateRowOutcomes";

const REDIS_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "not_applicable",
    triggers: "not_applicable",
  },
};

const REDIS_VALUE_TYPE_ORDER = [
  "string",
  "hash",
  "list",
  "set",
  "zset",
  "stream",
] as const;

const REDIS_READ_ONLY_QUERY_REASON =
  "[RapiDB] Read-only Redis connections allow only read commands.";

const REDIS_MAX_TTL_SECONDS = Math.floor(Number.MAX_SAFE_INTEGER / 1000);

const REDIS_SCAN_COUNT = 1000;

const REDIS_EDIT_NOT_APPLIED_REPLY = /^(?:ERR )?RAPIDB_NOT_APPLIED (.*)$/s;

const REDIS_ATOMIC_EDIT_SCRIPT = `
local source = KEYS[1]
local target = KEYS[2]
local staging = KEYS[3]
if redis.call('EXISTS', source) == 0 then return 0 end
if source ~= target and redis.call('EXISTS', target) ~= 0 then return -1 end
local kind = redis.call('TYPE', source)['ok']
if ARGV[1] ~= '' and kind ~= ARGV[1] then return -2 end
if ARGV[7] == '1' then
  if kind == 'string' then
    if redis.call('GET', source) ~= ARGV[2] then return -3 end
  elseif kind == 'hash' then
    local expected = cjson.decode(ARGV[2])
    local count = 0
    for field, entry in pairs(expected) do
      count = count + 1
      if redis.call('HGET', source, field) ~= tostring(entry) then return -3 end
    end
    if redis.call('HLEN', source) ~= count then return -3 end
  elseif kind == 'list' then
    local expected = cjson.decode(ARGV[2])
    if redis.call('LLEN', source) ~= #expected then return -3 end
    for index, entry in ipairs(expected) do
      if redis.call('LINDEX', source, index - 1) ~= tostring(entry) then return -3 end
    end
  elseif kind == 'set' then
    local expected = cjson.decode(ARGV[2])
    if redis.call('SCARD', source) ~= #expected then return -3 end
    for _, entry in ipairs(expected) do
      if redis.call('SISMEMBER', source, tostring(entry)) ~= 1 then return -3 end
    end
  elseif kind == 'zset' then
    local expected = cjson.decode(ARGV[2])
    if redis.call('ZCARD', source) ~= #expected then return -3 end
    for _, entry in ipairs(expected) do
      local score = redis.call('ZSCORE', source, tostring(entry.value))
      if score == false or tonumber(score) ~= tonumber(entry.score) then return -3 end
    end
  end
end
local ttl = redis.call('PTTL', source)
local expiry = nil
if ARGV[5] == 'set' then
  local seconds = tonumber(ARGV[6])
  if not string.match(ARGV[6], '^%d+$') or not seconds or seconds < 1 or seconds > ${REDIS_MAX_TTL_SECONDS} or seconds ~= math.floor(seconds) then
    return redis.error_reply('RAPIDB_NOT_APPLIED Redis edit TTL is outside the supported integer range.')
  end
  expiry = string.format('%.0f', seconds * 1000)
elseif ARGV[5] ~= 'keep' and ARGV[5] ~= 'persist' then
  return redis.error_reply('RAPIDB_NOT_APPLIED Redis edit TTL mode is invalid.')
end
local denied = nil
local function requirePermission(command, ...)
  if not denied and not redis.acl_check_cmd(command, ...) then
    denied = 'RAPIDB_NOT_APPLIED Redis edit requires ' .. command .. ' permission on its keys.'
  end
end
if ARGV[3] == '1' then
  local value = cjson.decode(ARGV[4])
  if kind ~= 'string' and kind ~= 'hash' and kind ~= 'list' and kind ~= 'set' and kind ~= 'zset' then return -2 end
  if staging == source or staging == target or redis.call('EXISTS', staging) ~= 0 then
    return redis.error_reply('RAPIDB_NOT_APPLIED Redis edit staging key already exists.')
  end
  if kind ~= 'string' and next(value) == nil then
    redis.call('DEL', source)
    return 1
  end
  if not redis.acl_check_cmd then
    return redis.error_reply('RAPIDB_NOT_APPLIED Redis value replacements require Redis 7 or newer for ACL-safe edits.')
  end
  if ARGV[5] == 'keep' and ttl >= 0 then
    if ttl > 9007199254740991 then
      return redis.error_reply('RAPIDB_NOT_APPLIED Redis edit TTL is too large to preserve safely.')
    end
    expiry = string.format('%.0f', ttl)
  end
  local fieldExpiries = {}
  if kind == 'hash' then
    local supported, allowed = pcall(redis.acl_check_cmd, 'HPEXPIRETIME', source, 'FIELDS', '1', 'field')
    if supported then
      if not allowed then
        return redis.error_reply('RAPIDB_NOT_APPLIED Redis hash edits require HPEXPIRETIME permission to preserve field TTLs.')
      end
      for field in pairs(value) do
        local at = redis.call('HPEXPIRETIME', source, 'FIELDS', 1, field)[1]
        if at > 0 then table.insert(fieldExpiries, { field, string.format('%.0f', at) }) end
      end
    elseif pcall(redis.acl_check_cmd, 'HPEXPIRE', source, '1', 'FIELDS', '1', 'field') then
      return redis.error_reply('RAPIDB_NOT_APPLIED Redis hash edits cannot preserve field TTLs because HPEXPIRETIME is unavailable.')
    end
  end
  local function buildReplacement(call)
    if kind == 'string' then call('SET', staging, value)
    elseif kind == 'hash' then for field, entry in pairs(value) do call('HSET', staging, field, entry) end
    elseif kind == 'list' then for _, entry in ipairs(value) do call('RPUSH', staging, entry) end
    elseif kind == 'set' then for _, entry in ipairs(value) do call('SADD', staging, entry) end
    elseif kind == 'zset' then for _, entry in ipairs(value) do call('ZADD', staging, entry.score, entry.value) end
    end
    for _, entry in ipairs(fieldExpiries) do call('HPEXPIREAT', staging, entry[2], 'FIELDS', '1', entry[1]) end
  end
  requirePermission('DEL', staging)
  if source ~= target then requirePermission('DEL', source) end
  requirePermission('RENAME', staging, target)
  buildReplacement(requirePermission)
  if expiry then requirePermission('PEXPIRE', staging, expiry) end
  if denied then return redis.error_reply(denied) end
  local ok, err = pcall(function()
    buildReplacement(redis.call)
    if expiry then redis.call('PEXPIRE', staging, expiry) end
    redis.call('RENAME', staging, target)
  end)
  if not ok then
    redis.call('DEL', staging)
    return redis.error_reply(tostring(err))
  end
  if source ~= target then redis.call('DEL', source) end
  return 1
end
if source ~= target and ARGV[5] ~= 'keep' and not redis.acl_check_cmd then
  return redis.error_reply('RAPIDB_NOT_APPLIED Combined Redis rename and TTL edits require Redis 7 or newer for ACL-safe edits.')
end
if redis.acl_check_cmd then
  if source ~= target then requirePermission('RENAME', source, target) end
  if expiry then requirePermission('PEXPIRE', target, expiry)
  elseif ARGV[5] == 'persist' then requirePermission('PERSIST', target) end
  if denied then return redis.error_reply(denied) end
end
if source ~= target then redis.call('RENAME', source, target) end
if ARGV[5] == 'set' then
  redis.call('PEXPIRE', target, expiry)
elseif ARGV[5] == 'persist' then
  redis.call('PERSIST', target)
end
return 1
`;

const READ_ONLY_REDIS_COMMANDS = new Set([
  "DBSIZE",
  "EXISTS",
  "GET",
  "GETRANGE",
  "HGET",
  "HGETALL",
  "HEXISTS",
  "HKEYS",
  "HLEN",
  "HMGET",
  "HSCAN",
  "HVALS",
  "KEYS",
  "LINDEX",
  "LLEN",
  "LRANGE",
  "MGET",
  "PING",
  "PTTL",
  "SCAN",
  "SCARD",
  "SISMEMBER",
  "SMEMBERS",
  "SRANDMEMBER",
  "SSCAN",
  "STRLEN",
  "TTL",
  "TYPE",
  "XLEN",
  "XRANGE",
  "XREVRANGE",
  "ZCARD",
  "ZRANGE",
  "ZRANK",
  "ZREVRANGE",
  "ZREVRANK",
  "ZSCORE",
  "ZSCAN",
]);

interface RedisKeyMetadata {
  type: string;
  size: number | undefined;
  ttl: number;
}

interface RedisSampleRow {
  redisType: string;
  row: Record<string, unknown>;
}

interface RedisLoadedValue {
  value:
    | string
    | RedisOversizedValueSnapshot
    | RedisBinaryValueSnapshot
    | RedisBinaryCollectionValueSnapshot;
  ttl?: number;
  deleteState?: RedisDeleteState;
}

const REDIS_TABLE_VALUE_COMMANDS = 4;
// Paged fallback for collections that the atomic compact read cannot bound
// (Redis < 7, non-compact encodings, ACLs without EVAL_RO). Before any member
// is transferred, MEMORY USAGE ... SAMPLES 0 must bound the whole value: it
// walks every element and counts the allocations, which are never smaller
// than the uncompressed member bytes. Each page reply is then counted and the
// read stops (no partial value) once the members exceed the raw limit.
export const REDIS_PAGED_PREFLIGHT_BYTES = 1024 * 1024;
export const REDIS_PAGED_COLLECTION_BYTES = 256 * 1024;
export const REDIS_PAGED_COUNT = 64;
const REDIS_PAGED_MAX_COMMANDS = 1024;
// A compact collection has <=64 KiB backing storage and <=1000 entries; a
// paged one is refused past 256 KiB of members, and one reply is bounded by
// the 1 MiB preflight. At most two bulk strings per entry; adding 32 bytes per
// string for integer/score rendering gives <320 KiB raw. Sixfold JSON
// escaping plus labels/structure stays under 2 MiB. Leave room for the
// in-flight reply, DUMP (64 KiB) and the visible hash proof.
const REDIS_COLLECTION_READ_RESERVATION = 3 * 1024 * 1024;

interface RedisDeleteState {
  client: RedisClient;
  database: number;
  table: string;
  key: string;
  type: string;
  expiry: number;
  bytes: Buffer;
  visibleHash?: Buffer;
}

type RedisDeleteReadScope = Pick<
  RedisDeleteState,
  "client" | "database" | "table"
>;

type RedisHashEntries = Record<string, string>;

type RedisClient = ReturnType<typeof createClient>;

type RedisKeyLimitPurpose = "paging" | "filtering";

type RedisInsertPlan = {
  command: "set";
  args: [key: string, value: string];
  options: { NX: true; EX?: number };
};
interface RedisClientEntry {
  client: RedisClient;
  promise: Promise<RedisClient>;
  cancelled: boolean;
  openingSettled: boolean;
  abort: AbortController;
}

interface RedisConnectionAttempt {
  abort: AbortController;
  entry?: RedisClientEntry;
}

type RedisSortedSetEntry = {
  score: number | string;
  value: string;
};

function assertRedisLosslessUtf8Value(value: unknown): void {
  if (typeof value === "string") {
    if (Buffer.from(value, "utf8").toString("utf8") !== value) {
      throw new Error(
        "Redis value must be losslessly UTF-8 encodable text; it contains an unpaired UTF-16 surrogate.",
      );
    }
    return;
  }
  if (Array.isArray(value)) {
    for (const entry of value) assertRedisLosslessUtf8Value(entry);
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const [field, entry] of Object.entries(value)) {
      assertRedisLosslessUtf8Value(field);
      assertRedisLosslessUtf8Value(entry);
    }
  }
}

function isDisplayableRedisText(bytes: Buffer): boolean {
  if (!isUtf8(bytes)) return false;
  // Each disallowed code point is ASCII and cannot occur inside a valid
  // UTF-8 multibyte sequence. No decoded/re-encoded scratch buffers needed.
  for (const byte of bytes) {
    if (
      byte <= 0x08 ||
      byte === 0x0b ||
      byte === 0x0c ||
      (byte >= 0x0e && byte <= 0x1f) ||
      byte === 0x7f
    ) {
      return false;
    }
  }
  return true;
}

function formatRedisBlobString(bytes: Buffer): string {
  if (isDisplayableRedisText(bytes)) return bytes.toString("utf8");
  return `RedisBinary(0x${bytes.toString("hex")})`;
}

function formatRedisSortedSetScore(score: unknown): number | string {
  const text = String(score).trim();
  const parsed = /^[+-]?inf(?:inity)?$/i.test(text)
    ? text.startsWith("-")
      ? Number.NEGATIVE_INFINITY
      : Number.POSITIVE_INFINITY
    : Number(text);
  return parsed === Number.POSITIVE_INFINITY
    ? "+inf"
    : parsed === Number.NEGATIVE_INFINITY
      ? "-inf"
      : parsed;
}

class RedisCollectionDecoder {
  binary = false;

  text(value: Buffer): string {
    if (isDisplayableRedisText(value)) return value.toString("utf8");
    this.binary = true;
    return `RedisBinary(0x${value.toString("hex")})`;
  }

  record(reply: Buffer[]): Record<string, string> {
    const record: Record<string, string> = Object.create(null);
    for (let i = 0; i < reply.length; i += 2) {
      record[this.text(reply[i])] = this.text(reply[i + 1]);
    }
    return record;
  }
}

function redisStringValue(bytes: Buffer): RedisLoadedValue["value"] {
  if (isDisplayableRedisText(bytes)) return bytes.toString("utf8");
  const binaryHex = bytes.toString("hex");
  return {
    __rapidbRedisBinary: 1,
    displayValue: `RedisBinary(0x${binaryHex})`,
    binaryHex,
  };
}

function formatRedisCollection(
  type: string,
  rawValue: unknown,
): RedisLoadedValue["value"] | undefined {
  const paired = type === "hash" || type === "zset";
  if (
    !Array.isArray(rawValue) ||
    rawValue.length > REDIS_READ_BUDGET.maxValueEntries * (paired ? 2 : 1) ||
    (paired && rawValue.length % 2 !== 0) ||
    !rawValue.every(Buffer.isBuffer)
  )
    return undefined;
  const decoder = new RedisCollectionDecoder();
  let value: unknown;
  if (type === "hash") {
    value = decoder.record(rawValue);
    // A literal "RedisBinary(0x...)" field can share the display label
    // of a binary field. The grid would display only one of them.
    if (Object.keys(value as object).length * 2 !== rawValue.length) {
      return undefined;
    }
  } else if (type === "zset") {
    value = rawValue.flatMap((entry, index) =>
      index % 2 === 0
        ? [
            {
              value: decoder.text(entry),
              score: formatRedisSortedSetScore(
                rawValue[index + 1].toString("ascii"),
              ),
            },
          ]
        : [],
    );
  } else if (type === "list" || type === "set") {
    value = rawValue.map((entry) => decoder.text(entry));
  } else {
    return undefined;
  }
  const displayValue = JSON.stringify(value);
  return decoder.binary
    ? { __rapidbRedisBinaryCollection: 1, displayValue }
    : displayValue;
}

// ZSCAN order is arbitrary. Present members in Redis rank order: by score,
// then by member bytes.
function sortRedisSortedSetPairs(pairs: Buffer[]): void {
  const score = (value: Buffer) => {
    const parsed = formatRedisSortedSetScore(value.toString("ascii"));
    return parsed === "+inf"
      ? Number.POSITIVE_INFINITY
      : parsed === "-inf"
        ? Number.NEGATIVE_INFINITY
        : Number(parsed);
  };
  const entries: Array<[Buffer, Buffer]> = [];
  for (let i = 0; i < pairs.length; i += 2)
    entries.push([pairs[i], pairs[i + 1]]);
  entries.sort(
    ([a, as], [b, bs]) => score(as) - score(bs) || Buffer.compare(a, b),
  );
  pairs.splice(0, pairs.length, ...entries.flat());
}

function redisRowValueBytes(value: unknown): number {
  if (typeof value === "string") return Buffer.byteLength(value);
  if (isRedisBinaryCollectionValueSnapshot(value))
    return Buffer.byteLength(value.displayValue);
  return isRedisBinaryValueSnapshot(value)
    ? Buffer.byteLength(value.displayValue) * 2
    : 0;
}

function redisComparisonRow(
  row: Record<string, unknown>,
): Record<string, unknown> {
  return isRedisBinaryValueSnapshot(row.value) ||
    isRedisBinaryCollectionValueSnapshot(row.value) ||
    isRedisOversizedValueSnapshot(row.value)
    ? { ...row, value: row.value.displayValue }
    : row;
}

function formatRedisReplyValue(value: unknown): unknown {
  if (Buffer.isBuffer(value)) return formatRedisBlobString(value);
  if (value instanceof ErrorReply) return `(error) ${value.message}`;
  // Wire strings arrive as buffers; raw numeric tokens arrive as strings.
  // Only promote canonical safe integers, never padded text.
  if (typeof value === "string" && /^-?(?:0|[1-9]\d*)$/.test(value)) {
    const integer = Number(value);
    return Number.isSafeInteger(integer) ? integer : value;
  }
  if (Array.isArray(value)) return value.map(formatRedisReplyValue);
  if (value instanceof Map) {
    return Object.fromEntries(
      [...value].map(([key, entry]) => [
        Buffer.isBuffer(key) ? formatRedisBlobString(key) : String(key),
        formatRedisReplyValue(entry),
      ]),
    );
  }
  if (
    value !== null &&
    typeof value === "object" &&
    Object.getPrototypeOf(value) === Object.prototype
  ) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        formatRedisReplyValue(entry),
      ]),
    );
  }
  return value;
}

function compareRedisValueTypes(left: string, right: string): number {
  const leftIndex = REDIS_VALUE_TYPE_ORDER.indexOf(
    left as (typeof REDIS_VALUE_TYPE_ORDER)[number],
  );
  const rightIndex = REDIS_VALUE_TYPE_ORDER.indexOf(
    right as (typeof REDIS_VALUE_TYPE_ORDER)[number],
  );
  if (leftIndex === -1 && rightIndex === -1) {
    return left.localeCompare(right);
  }
  if (leftIndex === -1) {
    return 1;
  }
  if (rightIndex === -1) {
    return -1;
  }
  return leftIndex - rightIndex;
}

function compareRedisKeys(left: string, right: string): number {
  return (
    left.localeCompare(right) || (left < right ? -1 : left > right ? 1 : 0)
  );
}

function formatRedisValueTypeLabel(
  entries: readonly RedisSampleRow[],
): string | null {
  const valueTypes = [
    ...new Set(
      entries
        .map((entry) => entry.redisType)
        .filter(
          (value): value is string =>
            typeof value === "string" && value.length > 0 && value !== "none",
        ),
    ),
  ].sort(compareRedisValueTypes);
  if (valueTypes.length === 0) {
    return null;
  }
  if (valueTypes.length === 1) {
    return valueTypes[0];
  }
  return `mixed(${valueTypes.join(", ")})`;
}

function redisKeyspaceColumn(
  name: string,
  type: string,
  nativeType: string,
  category: TypeCategory,
  {
    nullable = false,
    filterable = true,
    isReadOnly = false,
    isPrimaryKey = false,
  }: {
    nullable?: boolean;
    filterable?: boolean;
    isReadOnly?: boolean;
    isPrimaryKey?: boolean;
  } = {},
): ColumnTypeMeta {
  return {
    name,
    type,
    nativeType,
    category,
    nullable,
    isPrimaryKey,
    ...(isPrimaryKey ? { primaryKeyOrdinal: 1 } : {}),
    ...(isReadOnly ? { isReadOnly: true } : {}),
    isForeignKey: false,
    filterable,
    filterOperators: resolveFilterOperators(category, { filterable, nullable }),
    valueSemantics: "plain",
  };
}

function splitRedisStatements(input: string): string[] {
  const statements: string[] = [];
  let current = "";
  let quote: '"' | "'" | null = null;

  const pushCurrent = () => {
    const trimmed = current.trim();
    if (trimmed.length > 0) {
      statements.push(trimmed);
    }
    current = "";
  };

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    if (
      char === "\\" &&
      index + 1 < input.length &&
      (quote === '"' || (quote === "'" && input[index + 1] === "'"))
    ) {
      current += char + input[index + 1];
      index += 1;
      continue;
    }

    if (quote) {
      current += char;
      if (char === quote) {
        quote = null;
      }
      continue;
    }

    if (char === '"' || char === "'") {
      current += char;
      quote = char;
      continue;
    }

    if (char === ";" || char === "\n" || char === "\r") {
      pushCurrent();
      continue;
    }

    current += char;
  }

  if (quote) {
    throw new Error("Redis query has an unterminated quoted argument.");
  }

  pushCurrent();
  return statements;
}

function stripTrailingPeriod(message: string): string {
  return message.trimEnd().replace(/\.+$/, "");
}

interface RedisClosedTransaction {
  multiIndex: number;
  closeIndex: number;
  outcome:
    | "applied"
    | "partial"
    | "watchAborted"
    | "discarded"
    | "reset"
    | "quit";
}

// RESET de-authenticates the connection and QUIT closes it, so scripts using
// them never run on the shared client.
const REDIS_CONNECTION_RESETTING_COMMANDS = new Set(["QUIT", "RESET"]);

function capitalize(text: string): string {
  return `${text[0].toUpperCase()}${text.slice(1)}`;
}

function describeClosedTransaction({
  multiIndex,
  closeIndex,
  outcome,
}: RedisClosedTransaction): string | undefined {
  const queued = `the commands queued after MULTI at statement ${multiIndex + 1}`;
  switch (outcome) {
    case "watchAborted":
      return `${queued} were not applied because EXEC at statement ${closeIndex + 1} returned nil (a WATCHed key changed)`;
    case "discarded":
      return `${queued} were not applied because DISCARD at statement ${closeIndex + 1} discarded them`;
    case "reset":
      return `${queued} were not applied because RESET at statement ${closeIndex + 1} discarded them`;
    case "quit":
      return `${queued} were not applied because QUIT at statement ${closeIndex + 1} closed the connection`;
    case "partial":
      return `some of ${queued} returned errors in the EXEC result at statement ${closeIndex + 1}`;
    default:
      return undefined;
  }
}

function describeRedisTransactionWarning(
  transaction: RedisClosedTransaction,
): string | undefined {
  const { multiIndex, closeIndex, outcome } = transaction;
  if (outcome === "partial") {
    return `EXEC at statement ${closeIndex + 1} returned errors for one or more commands queued after MULTI at statement ${multiIndex + 1}. Redis does not roll back successful sibling commands.`;
  }
  if (outcome === "watchAborted") {
    const description = describeClosedTransaction(transaction);
    if (description) return `${capitalize(description)}.`;
  }
  return undefined;
}

function describeRedisScriptProgress(
  statement: string,
  index: number,
  multiIndex: number | undefined,
  closedTransactions: readonly RedisClosedTransaction[],
  failure: "notRun" | "rejected" | "unknown",
  isExec: boolean,
): string {
  const completedCount = multiIndex ?? index;
  const notes = closedTransactions.flatMap(
    (transaction) => describeClosedTransaction(transaction) ?? [],
  );
  const subject =
    completedCount === 1 ? "Statement 1" : `Statements 1 to ${completedCount}`;
  const writes =
    completedCount === 1 ? "any writes it made" : "any writes they made";
  const completed =
    completedCount === 0
      ? ""
      : notes.length === 0
        ? `${subject} completed and ${writes} were applied`
        : `${subject} completed. ${notes.map(capitalize).join(". ")}. ${capitalize(writes.replace("any", "any other"))} were applied`;
  if (multiIndex === undefined) {
    return failure === "unknown"
      ? `${completed}; the outcome of ${statement} is unknown and later statements were not run.`
      : `${completed}; later statements were not run.`;
  }
  const transaction = `the transaction started by MULTI at statement ${multiIndex + 1}`;
  const outcome =
    isExec && failure === "unknown"
      ? `the outcome of ${transaction} is unknown`
      : isExec && failure === "rejected"
        ? `${transaction} was discarded and its queued commands were not applied`
        : `the commands queued after MULTI at statement ${multiIndex + 1} were not applied because EXEC did not run`;
  const sentence = `${capitalize(outcome)}; later statements were not run.`;
  return completed ? `${completed}. ${sentence}` : sentence;
}

function formatRedisPreviewCommand(
  command: string,
  args: ReadonlyArray<string | number>,
): string {
  return [command, ...args.map((arg) => JSON.stringify(String(arg)))].join(" ");
}

function tokenizeRedisCommand(input: string): Array<string | Buffer> {
  const tokens: Array<string | Buffer> = [];
  let current = "";
  let bytes: Buffer[] | null = null;
  let quote: '"' | "'" | null = null;
  let tokenStarted = false;

  const encodeCurrent = () => {
    const encoded = Buffer.from(current, "utf8");
    if (encoded.toString("utf8") !== current) {
      throw new Error(
        "Redis query has invalid Unicode that cannot be encoded as UTF-8 without data loss.",
      );
    }
    current = "";
    return encoded;
  };

  const pushCurrent = () => {
    if (tokenStarted) {
      if (bytes) {
        bytes.push(encodeCurrent());
        tokens.push(Buffer.concat(bytes));
      } else {
        const text = current;
        encodeCurrent();
        tokens.push(text);
      }
    }
    current = "";
    bytes = null;
    tokenStarted = false;
  };

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    const next = input[index + 1];
    if (char === "\\" && quote === "'" && next === "'") {
      current += "'";
      index += 1;
      continue;
    }

    if (char === "\\" && quote === '"' && next !== undefined) {
      index += 1;
      if (next === "u") {
        const hex = input.slice(index + 1, index + 5);
        if (!/^[\da-fA-F]{4}$/.test(hex)) {
          throw new Error("Redis query has an invalid Unicode escape.");
        }
        current += String.fromCharCode(Number.parseInt(hex, 16));
        index += 4;
        continue;
      }
      const hex = input.slice(index + 1, index + 3);
      if (next === "x" && /^[\da-fA-F]{2}$/.test(hex)) {
        const byte = Number.parseInt(hex, 16);
        if (byte < 0x80) {
          current += String.fromCharCode(byte);
        } else {
          bytes ??= [];
          bytes.push(encodeCurrent(), Buffer.from([byte]));
        }
        index += 2;
        continue;
      }
      switch (next) {
        case "n":
          current += "\n";
          break;
        case "r":
          current += "\r";
          break;
        case "t":
          current += "\t";
          break;
        case "b":
          current += "\b";
          break;
        case "a":
          current += "\x07";
          break;
        case "f":
          current += "\f";
          break;
        default:
          current += next;
      }
      continue;
    }

    if (quote) {
      if (char === quote) {
        quote = null;
        tokenStarted = true;
        continue;
      }
      current += char;
      tokenStarted = true;
      continue;
    }

    if (char === '"' || char === "'") {
      quote = char;
      tokenStarted = true;
      continue;
    }

    if (/\s/.test(char)) {
      pushCurrent();
      continue;
    }

    current += char;
    tokenStarted = true;
  }

  if (quote) {
    throw new Error("Redis query has an unterminated quoted argument.");
  }

  pushCurrent();
  return tokens;
}

function decideRedisReadOnlyQuery(queryText: string) {
  const trimmed = queryText.trim();
  if (!trimmed) {
    return denyReadOnlyQuery(REDIS_READ_ONLY_QUERY_REASON);
  }

  try {
    const statements = splitRedisStatements(trimmed);
    if (statements.length === 0) {
      return denyReadOnlyQuery(REDIS_READ_ONLY_QUERY_REASON);
    }

    return statements.every((statement) => {
      const command = tokenizeRedisCommand(statement)[0];
      return typeof command === "string" && command
        ? READ_ONLY_REDIS_COMMANDS.has(command.toUpperCase())
        : false;
    })
      ? allowReadOnlyQuery()
      : denyReadOnlyQuery(REDIS_READ_ONLY_QUERY_REASON);
  } catch (error: unknown) {
    return denyReadOnlyQuery(
      error instanceof Error ? error.message : String(error),
    );
  }
}

export class RedisDriver implements IDBDriver {
  private client: RedisClient | null = null;
  private connected = false;
  private tlsSettings: ResolvedConnectionTlsSettings | undefined;
  private epoch = 0;
  private connecting?: Promise<void>;
  private connectionAbortController?: AbortController;
  private readonly connectionAttempts = new WeakMap<
    Promise<unknown>,
    RedisConnectionAttempt
  >();
  private readonly databaseClients = new Map<number, RedisClientEntry>();
  private readonly clients = new Set<RedisClientEntry>();
  private readonly queries = new Map<number, AbortController>();

  constructor(
    private readonly config: ConnectionConfig,
    private readonly timeoutSettingsProvider: DriverTimeoutSettingsProvider = getDefaultDriverTimeoutSettings,
  ) {}

  connect(): Promise<void> {
    if (this.connected) {
      return Promise.resolve();
    }
    if (this.connecting) return this.connecting;
    const epoch = this.epoch;
    const state: RedisConnectionAttempt = { abort: new AbortController() };
    this.connectionAbortController = state.abort;
    let attempt: Promise<void>;
    attempt = Promise.resolve()
      .then(async () => {
        const tlsSettings = await resolveConnectionTlsSettings(
          this.config,
          state.abort.signal,
        );
        if (epoch !== this.epoch) {
          throw new DOMException(
            "Redis connection attempt was cancelled.",
            "AbortError",
          );
        }
        this.tlsSettings = tlsSettings;
        const entry = this.openClient(this.resolveDbIndex());
        state.entry = entry;
        const client = await entry.promise;
        if (epoch !== this.epoch || entry.cancelled) {
          this.cancelClientEntry(entry);
          throw new Error("Redis connection attempt was cancelled.");
        }
        this.client = client;
        this.connected = true;
      })
      .finally(() => {
        if (this.connecting === attempt) this.connecting = undefined;
        if (this.connectionAbortController === state.abort) {
          this.connectionAbortController = undefined;
        }
      });
    this.connectionAttempts.set(attempt, state);
    this.connecting = attempt;
    return attempt;
  }

  cancelConnectionAttempt(attempt: Promise<unknown>): void {
    const state = this.connectionAttempts.get(attempt);
    if (!state) return;
    state.abort.abort();
    if (state.entry) this.cancelClientEntry(state.entry);
    if (this.connecting === attempt) {
      this.epoch += 1;
      this.connecting = undefined;
      this.tlsSettings = undefined;
    }
    if (this.connectionAbortController === state.abort) {
      this.connectionAbortController = undefined;
    }
    if (state.entry && this.client === state.entry.client) {
      this.client = null;
      this.connected = false;
    }
  }

  private createDatabaseClient(
    database: number,
    signal: AbortSignal,
  ): RedisClient {
    const forwardedTransport = getSshTcpForwardTransport(this.config);
    const tlsSettings = this.tlsSettings;
    const tlsServername =
      tlsSettings?.servername ??
      (forwardedTransport ? getTlsServername(this.config) : undefined);
    const uri = this.config.connectionUri
      ? new URL(this.config.connectionUri)
      : undefined;
    if (uri) {
      if (uri.protocol !== "redis:" && uri.protocol !== "rediss:") {
        throw new Error("Redis connection URI must use redis:// or rediss://.");
      }
      uri.pathname = `/${database}`;
      if (forwardedTransport) {
        uri.hostname = forwardedTransport.localHost;
        uri.port = String(forwardedTransport.localPort);
      }
      if (tlsSettings) uri.protocol = "rediss:";
    }
    const socket = tlsSettings
      ? {
          host:
            (forwardedTransport?.localHost ?? this.config.host) || "127.0.0.1",
          port: forwardedTransport?.localPort ?? this.config.port ?? 6379,
          tls: true as const,
          rejectUnauthorized: tlsSettings.rejectUnauthorized,
          servername: tlsSettings.servername,
          ca: tlsSettings.ca,
          cert: tlsSettings.cert,
          key: tlsSettings.key,
          passphrase: tlsSettings.passphrase,
          checkServerIdentity: tlsSettings.checkServerIdentity,
        }
      : {
          host:
            (forwardedTransport?.localHost ?? this.config.host) || "127.0.0.1",
          port: forwardedTransport?.localPort ?? this.config.port ?? 6379,
          ...(uri?.protocol === "rediss:" && forwardedTransport && tlsServername
            ? { servername: tlsServername }
            : {}),
        };
    let hasBeenReady = false;
    const socketOptions = {
      ...socket,
      signal,
      reconnectStrategy: (retries: number) =>
        signal.aborted || !hasBeenReady
          ? false
          : Math.min(2 ** retries * 50, 2000) + Math.floor(Math.random() * 200),
    };
    const username = this.config.username?.trim();
    const client: RedisClient = createClient({
      url: uri?.toString(),
      socket: socketOptions,
      database,
      username: username || undefined,
      password: this.config.password,
    });
    client.on("ready", () => {
      hasBeenReady = true;
    });
    client.on("error", (error: unknown) => {
      logger.error("Redis client error", error);
    });
    return client;
  }

  private destroyClient(client: RedisClient): unknown {
    try {
      if (client.isOpen) client.destroy();
    } catch (error) {
      logger.error("Redis client cleanup error", error);
      return error;
    }
  }

  private releaseClientEntry(entry: RedisClientEntry): void {
    if (
      entry.cancelled &&
      entry.openingSettled &&
      !entry.client.isOpen &&
      !entry.client.isReady
    ) {
      this.clients.delete(entry);
    }
  }

  private cancelClientEntry(
    entry: RedisClientEntry,
    reason: unknown = new Error("Redis connection attempt was cancelled."),
  ): unknown {
    entry.cancelled = true;
    entry.abort.abort(reason);
    const error = this.destroyClient(entry.client);
    this.releaseClientEntry(entry);
    return error;
  }

  private openClient(database: number): RedisClientEntry {
    const abort = new AbortController();
    const client = this.createDatabaseClient(database, abort.signal);
    const entry: RedisClientEntry = {
      client,
      cancelled: false,
      openingSettled: false,
      abort,
      promise: Promise.resolve(client),
    };
    this.clients.add(entry);
    client.on("end", () => {
      if (this.client === client) {
        this.client = null;
        this.connected = false;
      }
      this.releaseClientEntry(entry);
    });
    client.on("error", () => {
      queueMicrotask(() => this.releaseClientEntry(entry));
    });
    const epoch = this.epoch;
    entry.promise = (async () => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const opening = client
          .connect()
          .then(() => {
            if (entry.cancelled || epoch !== this.epoch) {
              this.cancelClientEntry(entry);
              throw new Error("Redis connection attempt was cancelled.");
            }
            return client;
          })
          .finally(() => {
            entry.openingSettled = true;
            this.releaseClientEntry(entry);
          });
        const timeoutMs = this.timeoutSettingsProvider().connectionTimeoutMs;
        if (timeoutMs > 0) {
          timer = setTimeout(() => {
            this.cancelClientEntry(
              entry,
              new DriverTimeoutError("connection", "connect", timeoutMs),
            );
          }, timeoutMs);
        }
        await this.waitForResult(opening, entry.abort.signal);
        if (entry.cancelled || epoch !== this.epoch) {
          throw new Error("Redis connection attempt was cancelled.");
        }
        return client;
      } catch (error) {
        this.cancelClientEntry(entry, error);
        throw error;
      } finally {
        if (timer) clearTimeout(timer);
      }
    })();
    return entry;
  }

  async disconnect(): Promise<void> {
    const client = this.client;
    this.epoch += 1;
    this.connectionAbortController?.abort();
    this.connectionAbortController = undefined;
    this.client = null;
    this.connected = false;
    this.connecting = undefined;
    this.tlsSettings = undefined;
    this.databaseClients.clear();
    for (const abort of this.queries.values()) abort.abort();
    this.queries.clear();
    const entries = [...this.clients];
    const errors: unknown[] = [];
    for (const entry of entries) {
      const error = this.cancelClientEntry(entry);
      if (error !== undefined) errors.push(error);
    }
    if (client && !entries.some((entry) => entry.client === client)) {
      const error = this.destroyClient(client);
      if (error !== undefined) errors.push(error);
    }
    if (errors.length) throw errors[0];
  }

  isConnected(): boolean {
    return this.connected && this.client?.isReady === true;
  }

  getEntityManifest(): DriverEntityManifest {
    return REDIS_ENTITY_MANIFEST;
  }

  getCapabilities() {
    return {
      tabularRead: "nosql" as const,
      schemaNamespaces: "none" as const,
      queryMode: "text" as const,
      supportsMutations: true,
      readOnlyQueryGuard: decideRedisReadOnlyQuery,
      editorPresentation: {
        formatOnOpen: false,
        editorLanguage: "plaintext" as const,
      },
    };
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    try {
      const client = getMetadataContext()
        ? await this.getDatabaseClient()
        : this.requireClient();
      const info = await client.info("keyspace");
      const names = info
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => /^db\d+:/.test(line))
        .map((line) => line.split(":")[0]);
      if (names.length === 0) {
        names.push(`db${this.resolveDbIndex()}`);
      }
      return names.map((name) => ({ name, schemas: [] }));
    } catch {
      return [{ name: `db${this.resolveDbIndex()}`, schemas: [] }];
    }
  }

  async listSchemas(): Promise<SchemaInfo[]> {
    return [];
  }

  async listObjects(
    database?: string,
    _schema?: string,
  ): Promise<TableInfoList> {
    const client = await this.getDatabaseClient(database);
    let truncated = false;
    const keys = await this.scanKeys(
      client,
      "*",
      REDIS_READ_BUDGET.maxScanKeys,
      false,
      undefined,
      {
        skipBinaryKeys: true,
        onTruncated: () => {
          truncated = true;
          logger.warn(
            `Redis key prefix discovery stopped after ${REDIS_READ_BUDGET.maxScanKeys} keys; some prefixes may be missing from the object list. Open "All keys" to browse the full keyspace.`,
          );
        },
      },
    );
    const prefixedNames = new Set<string>();
    let hasUnprefixedKeys = false;
    for (const key of keys) {
      const separator = key.indexOf(":");
      if (separator < 0) {
        hasUnprefixedKeys = true;
      } else if (separator === 0) {
        prefixedNames.add(REDIS_EMPTY_PREFIX_TABLE);
      } else {
        const prefix = key.slice(0, separator);
        prefixedNames.add(prefix === "default" ? "default:" : prefix);
      }
    }
    const names = [
      REDIS_ALL_KEYS_TABLE,
      ...(hasUnprefixedKeys ? [REDIS_UNPREFIXED_KEYS_TABLE] : []),
      ...[...prefixedNames].sort((left, right) => left.localeCompare(right)),
    ];
    const objects: TableInfoList = names.map((name) => ({
      schema: "",
      name,
      type: "table",
    }));
    if (truncated) objects.truncated = true;
    return objects;
  }

  async describeTable(
    database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const rows = await this.readRows(
      await this.getDatabaseClient(database),
      table,
      200,
    );
    return this.inferRedisColumns(rows).map((column) => ({
      name: column.name,
      type: column.nativeType,
      nullable: column.nullable,
      defaultValue: undefined,
      ...(column.isReadOnly ? { isReadOnly: true } : {}),
      isPrimaryKey: column.isPrimaryKey,
      primaryKeyOrdinal: column.primaryKeyOrdinal,
      isForeignKey: false,
    }));
  }

  async describeColumns(
    database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const rows = await this.readRows(
      await this.getDatabaseClient(database),
      table,
      200,
    );
    return this.inferRedisColumns(rows);
  }

  async getIndexes(): Promise<IndexMeta[]> {
    return [];
  }

  async getForeignKeys(): Promise<ForeignKeyMeta[]> {
    return [];
  }

  async getConstraints(): Promise<TableConstraintMeta[]> {
    return [];
  }

  async getTriggers(): Promise<TriggerMeta[] | null> {
    return null;
  }

  async getConstraintDDL(): Promise<string> {
    unsupported("Redis constraints DDL");
  }

  async getIndexDDL(): Promise<string> {
    unsupported("Redis index DDL");
  }

  async getTriggerDDL(): Promise<string> {
    unsupported("Redis trigger DDL");
  }

  async getCreateTableDDL(): Promise<string> {
    unsupported("Redis DDL");
  }

  async getObjectDefinition(): Promise<string | null> {
    return null;
  }

  async getRoutineDefinition(
    _database?: string,
    _schema?: string,
    _name?: string,
    _kind?: "function" | "procedure",
    _routineIdentity?: string,
  ): Promise<string> {
    unsupported("Redis routine definition");
  }

  query(
    sql: string,
    _params?: unknown[],
    options?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    return this.executeQuery(sql, options, options?.signal);
  }

  private async executeQuery(
    sql: string,
    options?: QueryExecutionOptions,
    signal?: AbortSignal,
  ): Promise<QueryResult> {
    const statements = splitRedisStatements(sql.trim());
    if (statements.length === 0) {
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      };
    }

    const startedAt = Date.now();
    const commands = statements.map(tokenizeRedisCommand);
    const results: unknown[] = [];
    const abort = new AbortController();
    const externalSignal = signal ?? options?.signal;
    const querySignal = externalSignal
      ? AbortSignal.any([externalSignal, abort.signal])
      : abort.signal;
    const epoch = this.epoch;
    if (options?.requestToken !== undefined)
      this.queries.set(options.requestToken, abort);
    let entry: RedisClientEntry | undefined;
    let onAbort: (() => void) | undefined;
    try {
      querySignal.throwIfAborted();
      this.requireClient();
      if (
        options?.database ||
        options?.requestToken !== undefined ||
        externalSignal !== undefined ||
        commands.some(([command]) =>
          REDIS_CONNECTION_RESETTING_COMMANDS.has(
            String(command ?? "").toUpperCase(),
          ),
        )
      ) {
        entry = this.openClient(this.resolveDbIndex(options?.database));
        const owned = entry;
        onAbort = () => {
          this.cancelClientEntry(owned, querySignal.reason);
        };
        querySignal.addEventListener("abort", onAbort, { once: true });
      }
      const baseClient = entry
        ? await this.waitForResult(entry.promise, querySignal)
        : this.requireClient();
      const client =
        options?.requestToken !== undefined || externalSignal
          ? baseClient.withAbortSignal(querySignal)
          : baseClient;
      let partialError: string | undefined;
      let partialErrorReportedByServer = false;
      let multiIndex: number | undefined;
      const closedTransactions: RedisClosedTransaction[] = [];
      for (const [index, parts] of commands.entries()) {
        const statement = `statement ${index + 1} of ${commands.length}`;
        const [command, ...args] = parts;
        const commandName = String(command ?? "").toUpperCase();
        const closesTransaction =
          commandName === "EXEC" || commandName === "DISCARD";
        const progress = (failure: "notRun" | "rejected" | "unknown") =>
          describeRedisScriptProgress(
            statement,
            index,
            multiIndex,
            closedTransactions,
            failure,
            commandName === "EXEC",
          );
        try {
          querySignal.throwIfAborted();
          if (epoch !== this.epoch)
            throw new Error("Redis query was cancelled by disconnect.");
        } catch (error) {
          if (results.length === 0) throw error;
          partialError = `Redis script was cancelled before ${statement}. ${progress("notRun")}`;
          break;
        }
        let result: unknown;
        try {
          result = await this.waitForResult(
            client.sendCommand(
              [
                typeof command === "string" ? command.toUpperCase() : command,
                ...args,
              ],
              {
                typeMapping: {
                  [RESP_TYPES.SIMPLE_STRING]: Buffer,
                  [RESP_TYPES.BLOB_STRING]: Buffer,
                  [RESP_TYPES.VERBATIM_STRING]: Buffer,
                  [RESP_TYPES.DOUBLE]: String,
                  // node-redis supports String, but not BigInt, for RESP NUMBER.
                  [RESP_TYPES.NUMBER]: String,
                  [RESP_TYPES.BIG_NUMBER]: String,
                  [RESP_TYPES.MAP]: Map,
                },
              },
            ),
            querySignal,
          );
        } catch (error) {
          if (results.length === 0) throw error;
          if (error instanceof ErrorReply) {
            partialError = `Redis ${statement} failed: ${stripTrailingPeriod(error.message)}. ${progress("rejected")}`;
            partialErrorReportedByServer = true;
            results.push(formatRedisReplyValue(error));
            // Only EXECABORT ends MULTI; other rejected EXEC/DISCARD calls
            // (arity errors) leave it open, so the cleanup DISCARD must run.
            if (commandName === "EXEC" && error.message.startsWith("EXECABORT"))
              multiIndex = undefined;
          } else {
            const failure =
              querySignal.aborted || epoch !== this.epoch
                ? "was cancelled"
                : `failed: ${stripTrailingPeriod(error instanceof Error ? error.message : String(error))}`;
            partialError = `Redis ${statement} ${failure}. ${progress("unknown")}`;
          }
          break;
        }
        results.push(formatRedisReplyValue(result));
        if (closesTransaction && multiIndex !== undefined) {
          closedTransactions.push({
            multiIndex,
            closeIndex: index,
            outcome:
              commandName === "DISCARD"
                ? "discarded"
                : result === null
                  ? "watchAborted"
                  : Array.isArray(result) &&
                      result.some((reply) => reply instanceof ErrorReply)
                    ? "partial"
                    : "applied",
          });
          multiIndex = undefined;
        } else if (commandName === "RESET" || commandName === "QUIT") {
          // Neither is queued inside MULTI; both end the transaction at once.
          if (multiIndex !== undefined)
            closedTransactions.push({
              multiIndex,
              closeIndex: index,
              outcome: commandName === "RESET" ? "reset" : "quit",
            });
          multiIndex = undefined;
        } else if (commandName === "MULTI" && multiIndex === undefined)
          multiIndex = index;
      }
      if (!entry && multiIndex !== undefined && epoch === this.epoch) {
        void baseClient.sendCommand(["DISCARD"]).catch((error: unknown) => {
          logger.error("Redis DISCARD after interrupted script failed", error);
        });
      }
      const queryValue = statements.length === 1 ? results[0] : results;
      const row = flattenRootRecord(
        statements.length === 1
          ? { result: queryValue }
          : { results: queryValue },
      );
      const columns = Object.keys(row);
      // If execution stopped early, partialError already includes the
      // completed transaction outcomes. Otherwise all replies are available.
      const warnings = partialError
        ? []
        : closedTransactions.flatMap(
            (transaction) => describeRedisTransactionWarning(transaction) ?? [],
          );
      return {
        columns,
        columnMeta: [
          {
            category:
              queryValue !== null && typeof queryValue === "object"
                ? "json"
                : null,
          },
        ],
        rows: [this.mapRowToQueryRow(row, columns)],
        rowCount: 1,
        executionTimeMs: Date.now() - startedAt,
        ...(partialError ? { error: partialError } : {}),
        ...(partialError && partialErrorReportedByServer
          ? { serverReportedError: true }
          : {}),
        ...(warnings.length > 0 ? { warnings } : {}),
      };
    } finally {
      if (onAbort) querySignal.removeEventListener("abort", onAbort);
      if (
        options?.requestToken !== undefined &&
        this.queries.get(options.requestToken) === abort
      ) {
        this.queries.delete(options.requestToken);
      }
      if (entry) {
        this.cancelClientEntry(entry);
      }
    }
  }

  cancelCurrentOperation(context?: OperationCancellationContext): void {
    if (context?.requestToken !== undefined)
      this.queries.get(context.requestToken)?.abort();
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    const startTime = performance.now();
    const scope =
      request.observeNoSqlDelete && !request.omitRowIdentity
        ? {
            client: this.requireClient(),
            database: this.resolveDbIndex(request.database),
            table: request.table,
          }
        : undefined;
    const observations = (rows: Record<string, unknown>[]) => {
      if (!scope || scope.client !== this.client) return {};
      let remaining = 4 * 1024 * 1024;
      return {
        noSqlDeleteObservations: rows.map((row) => {
          const observation = this.observedDeleteRows.get(row);
          const state =
            observation && this.deleteObservationStates.get(observation);
          if (!state) return null;
          const cost =
            4 * (state.bytes.length + (state.visibleHash?.length ?? 0)) +
            Buffer.byteLength(state.key) +
            REDIS_COMPARE_DELETE.length;
          if (cost > remaining) return null;
          remaining -= cost;
          return observation;
        }),
      };
    };
    const plan = await this.planTablePage(request, scope);
    if ("rows" in plan) {
      return {
        columns: plan.columns,
        rows: plan.rows,
        ...observations(plan.rows),
        totalCount: request.skipCount ? 0 : plan.totalCount,
        executionTimeMs: Math.round(performance.now() - startTime),
      };
    }
    const rows = await this.readRowsForKeys(plan.client, plan.keys, request, {
      deleteReadScope: scope,
    });
    throwIfQueryCancelled(request);
    const pageRows = rows.map((entry) => entry.row);
    return {
      columns: this.inferRedisColumns(rows),
      rows: pageRows,
      ...observations(pageRows),
      totalCount: request.skipCount ? 0 : plan.totalCount,
      executionTimeMs: Math.round(performance.now() - startTime),
    };
  }

  async *readTablePageChunks(
    request: DriverTablePageRequest,
  ): AsyncGenerator<DriverTableExportChunk> {
    const plan = await this.planTablePage(request);
    if ("rows" in plan) {
      yield { columns: plan.columns, rows: plan.rows };
      return;
    }
    const { keys } = plan;
    if (keys.length === 0) {
      yield { columns: this.inferRedisColumns([]), rows: [] };
      return;
    }
    const metadata = new Map<string, RedisKeyMetadata>();
    const held = new Map<string, RedisSampleRow>();
    let heldBytes = 0;
    let columns: ColumnTypeMeta[] | undefined;
    let start = 0;
    while (start < keys.length) {
      const pending = keys.slice(start).filter((key) => !held.has(key));
      const rows = await this.readRowsForKeys(plan.client, pending, request, {
        metadata,
        initialBytes: heldBytes,
        stopWhenBudgetSpent: true,
      });
      throwIfQueryCancelled(request);
      columns ??= this.inferRedisColumns(rows);
      rows.forEach((entry, index) => {
        if (isRedisNotLoadedValueSnapshot(entry.row.value)) return;
        held.set(pending[index], entry);
        heldBytes += redisRowValueBytes(entry.row.value);
      });
      let end = start;
      while (end < keys.length && held.has(keys[end])) end += 1;
      if (end === start) {
        held.clear();
        const [single] = await this.readRowsForKeys(
          plan.client,
          [keys[start]],
          request,
          { metadata },
        );
        throwIfQueryCancelled(request);
        held.set(keys[start], single);
        heldBytes = redisRowValueBytes(single.row.value);
        end = start + 1;
      }
      const ready = keys.slice(start, end).map((key) => {
        const entry = held.get(key) as RedisSampleRow;
        held.delete(key);
        heldBytes -= redisRowValueBytes(entry.row.value);
        return entry.row;
      });
      yield { columns, rows: ready };
      start = end;
    }
  }

  private async planTablePage(
    request: DriverTablePageRequest,
    deleteReadScope?: RedisDeleteReadScope,
  ): Promise<
    | { client: RedisClient; keys: string[]; totalCount: number }
    | {
        columns: ColumnTypeMeta[];
        rows: Record<string, unknown>[];
        totalCount: number;
      }
  > {
    throwIfQueryCancelled(request);
    const baseClient = await this.getDatabaseClient(
      request.database,
      request.signal,
    );
    throwIfQueryCancelled(request);
    const client = request.signal
      ? baseClient.withAbortSignal(request.signal)
      : baseClient;
    if (this.canUseKeyOnlyPaging(request)) {
      const offset = Math.max(0, (request.page - 1) * request.pageSize);
      const keys = (
        await this.scanTableKeys(
          client,
          request.table,
          REDIS_READ_BUDGET.maxScanKeys,
          "paging",
          request,
        )
      ).sort(compareRedisKeys);
      if (request.sort?.direction === "desc") keys.reverse();
      return {
        client,
        keys: keys.slice(offset, offset + request.pageSize),
        totalCount: keys.length,
      };
    }

    const boundedReadLimit = REDIS_READ_BUDGET.maxValueReads;
    const valueQuery =
      request.sort?.column === "value" ||
      request.filters.some((filter) => filter.column === "value");
    if (!valueQuery) {
      const keys = (
        await this.scanTableKeys(
          client,
          request.table,
          boundedReadLimit,
          "filtering",
          request,
        )
      ).sort(compareRedisKeys);
      const metadata = await this.readRowMetadata(client, keys, request);
      const metadataRows = metadata.map((entry) => entry.row);
      const filtered = applyFilters(
        metadataRows,
        request.filters,
        this.inferRedisColumns(metadata),
      );
      const sorted = applySort(
        filtered,
        request.sort,
        this.inferRedisColumns(metadata),
      );
      return {
        client,
        keys: pageRows(sorted, request.page, request.pageSize).map((row) =>
          String(row.key),
        ),
        totalCount: sorted.length,
      };
    }
    const rows = await this.readRows(
      client,
      request.table,
      boundedReadLimit,
      "filtering",
      request,
      deleteReadScope,
    );
    const displayToOriginal = new Map(
      rows.map((entry) => [redisComparisonRow(entry.row), entry.row]),
    );
    const omittedValueRow = rows.find((entry) =>
      isRedisOversizedValueSnapshot(entry.row.value),
    );
    if (omittedValueRow) {
      const reason = isRedisUnavailableValueSnapshot(omittedValueRow.row.value)
        ? "could not be loaded safely"
        : `has a value beyond the table viewer read limit (${REDIS_READ_BUDGET.maxValueEntries} entries or ${REDIS_READ_BUDGET.maxValueBytes} bytes per value, ${REDIS_READ_BUDGET.maxTotalValueBytes} bytes per read)`;
      throw new Error(
        `Redis key '${String(omittedValueRow.row.key)}' ${reason}, so filtering or sorting by value is unavailable. Narrow the keyspace or filter by key, type, or TTL.`,
      );
    }
    const rowRecords = [...displayToOriginal.keys()];
    const columns = this.inferRedisColumns(rows);
    const filtered = applyFilters(rowRecords, request.filters, columns);
    const sorted = applySort(filtered, request.sort, columns);
    const paged = pageRows(sorted, request.page, request.pageSize);
    return {
      columns,
      rows: paged.map((row) => displayToOriginal.get(row) ?? row),
      totalCount: sorted.length,
    };
  }

  private canUseKeyOnlyPaging(request: DriverTablePageRequest): boolean {
    if (request.filters.length > 0) {
      return false;
    }

    if (!request.sort) {
      return true;
    }

    return request.sort.column === "key";
  }

  async updateRows(
    request: DriverUpdateRowsRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    context?.signal.throwIfAborted();
    const baseClient = await this.getDatabaseClient(
      request.database,
      context?.signal,
    );
    const client = context
      ? baseClient.withAbortSignal(context.signal)
      : baseClient;
    let affectedRows = 0;
    const updateRowOutcomes: DriverUpdateRowOutcome[] = [];
    for (const [rowIndex, update] of request.updates.entries()) {
      let writeMayHaveApplied = false;
      try {
        context?.signal.throwIfAborted();
        const sourceKey = this.resolveStoredKey(update.primaryKeys.key);
        if (sourceKey === null) {
          updateRowOutcomes.push({ rowIndex, status: "not_applied" });
          continue;
        }

        const hasKeyChange = Object.hasOwn(update.changes, "key");
        const targetKey = hasKeyChange
          ? this.resolveStoredKey(update.changes.key)
          : sourceKey;
        if (targetKey === null) {
          throw new Error("Redis key updates require a string 'key' value.");
        }

        const keyChanged = sourceKey !== targetKey;

        const hasValueChange =
          Object.hasOwn(update.changes, "value") ||
          Object.hasOwn(update.changes, "json") ||
          Object.hasOwn(update.changes, "text");
        const hasTtlChange = Object.hasOwn(update.changes, "ttl");
        if (!keyChanged && !hasValueChange && !hasTtlChange) {
          updateRowOutcomes.push({ rowIndex, status: "not_applied" });
          continue;
        }

        const currentType = await client.type(sourceKey);
        if (currentType === "stream" && hasValueChange) {
          throw new Error(
            "Redis stream values are read-only in the table viewer.",
          );
        }
        const originalSnapshot = update.originalValues?.value;
        if (
          hasValueChange &&
          isRedisBinaryCollectionValueSnapshot(originalSnapshot)
        ) {
          throw new Error(
            `Redis ${currentType} values with binary members are read-only in the table viewer. Rename, TTL changes, and deletion remain available.`,
          );
        }
        if (hasValueChange && isRedisNotLoadedValueSnapshot(originalSnapshot)) {
          throw new Error(
            "This Redis value was not loaded because the page reached its total read limit. Reload with a smaller page size or a narrower filter before editing it.",
          );
        }
        if (
          hasValueChange &&
          isRedisUnavailableValueSnapshot(originalSnapshot)
        ) {
          throw new Error(
            "This Redis value was not safely loaded and is read-only in the table viewer. Rename and TTL changes remain available.",
          );
        }
        if (
          hasValueChange &&
          (isRedisOversizedValueSnapshot(originalSnapshot) ||
            this.exceedsRedisValueLimit(
              currentType,
              await this.readRedisValueSize(client, sourceKey, currentType),
            ))
        ) {
          throw new Error(
            "Redis values larger than the table viewer read limit are read-only. Rename, TTL changes, and deletion remain available.",
          );
        }
        if (currentType === "string" && hasValueChange) {
          const currentBytes = await client
            .withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer })
            .get(sourceKey);
          if (
            Buffer.isBuffer(currentBytes) &&
            !isDisplayableRedisText(currentBytes)
          ) {
            throw new Error(
              "Binary-backed Redis string values are read-only in the table viewer.",
            );
          }
        }
        const value =
          update.changes.value ?? update.changes.json ?? update.changes.text;
        const encodedValue = hasValueChange
          ? this.encodeRedisLuaValue(currentType, value)
          : "null";
        const ttlSeconds = hasTtlChange
          ? this.parseRedisTtlInput(update.changes.ttl, "Redis TTL updates")
          : undefined;
        const hasOriginalValue =
          Object.hasOwn(update.originalValues ?? {}, "value") &&
          !isRedisOversizedValueSnapshot(update.originalValues?.value);
        const originalValue = hasOriginalValue
          ? currentType === "string"
            ? isRedisBinaryValueSnapshot(update.originalValues?.value)
              ? Buffer.from(update.originalValues.value.binaryHex, "hex")
              : this.normalizeStoredValue(update.originalValues?.value)
            : this.encodeRedisLuaValue(
                currentType,
                update.originalValues?.value,
                false,
              )
          : "";
        writeMayHaveApplied = true;
        const result = Number(
          await this.sendRedisEdit(client, [
            "EVAL",
            REDIS_ATOMIC_EDIT_SCRIPT,
            "3",
            sourceKey,
            targetKey,
            `${sourceKey}:rapidb-edit:${randomUUID()}`,
            currentType,
            originalValue,
            hasValueChange ? "1" : "0",
            encodedValue,
            ttlSeconds === undefined
              ? "keep"
              : ttlSeconds === null
                ? "persist"
                : "set",
            ttlSeconds === undefined || ttlSeconds === null
              ? "0"
              : String(ttlSeconds),
            hasOriginalValue ? "1" : "0",
          ]),
        );
        if (result === -1) {
          writeMayHaveApplied = false;
          throw new Error(`Redis key '${targetKey}' already exists.`);
        }
        if (result === -2) {
          writeMayHaveApplied = false;
          updateRowOutcomes.push({
            rowIndex,
            status: "not_applied",
            conflict: true,
          });
          throw new Error("Redis key type changed after the row was loaded.");
        }
        if (result === -3) {
          writeMayHaveApplied = false;
          updateRowOutcomes.push({
            rowIndex,
            status: "not_applied",
            conflict: true,
          });
          throw new Error("Redis value changed after the row was loaded.");
        }
        if (result === 1) {
          writeMayHaveApplied = false;
          affectedRows++;
          updateRowOutcomes.push({ rowIndex, status: "applied" });
        } else if (result === 0) {
          writeMayHaveApplied = false;
          updateRowOutcomes.push({
            rowIndex,
            status: "not_applied",
            conflict: true,
          });
        } else {
          updateRowOutcomes.push({ rowIndex, status: "unknown" });
        }
      } catch (error: unknown) {
        throw failedDriverUpdateRows(
          error,
          request.updates.length,
          updateRowOutcomes,
          rowIndex,
          affectedRows,
          writeMayHaveApplied,
        );
      }
    }
    return completeDriverUpdateRowsResult(affectedRows, updateRowOutcomes);
  }

  private async sendRedisEdit(
    client: RedisClient,
    args: Array<string | Buffer>,
  ): Promise<unknown> {
    try {
      return await client.sendCommand(args);
    } catch (error) {
      const notApplied =
        error instanceof ErrorReply
          ? REDIS_EDIT_NOT_APPLIED_REPLY.exec(error.message)
          : null;
      if (notApplied) throw new MutationNotExecutedError(notApplied[1], error);
      throw error;
    }
  }

  async insertRow(
    request: DriverInsertRowRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    context?.signal.throwIfAborted();
    const plan = this.buildRedisInsertPlan(request.values);
    const baseClient = await this.getDatabaseClient(
      request.database,
      context?.signal,
    );
    const client = context
      ? baseClient.withAbortSignal(context.signal)
      : baseClient;
    let result: string | null;
    try {
      result = await client[plan.command](...plan.args, plan.options);
    } catch (error) {
      if (error instanceof ErrorReply) {
        throw new MutationNotExecutedError(error.message, error);
      }
      throw error;
    }

    return { affectedRows: result === "OK" ? 1 : 0 };
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    const { keys, baseClient } = await prepareDeleteBatch(
      request.primaryKeyValuesList,
      context,
      async () => ({
        keys: this.resolveDeleteKeys(request.primaryKeyValuesList),
        baseClient: await this.getDatabaseClient(
          request.database,
          context?.signal,
        ),
      }),
    );
    const client = context
      ? baseClient.withAbortSignal(context.signal)
      : baseClient;
    return deleteRowsSequentially(
      request.primaryKeyValuesList,
      context,
      (_entry, index) => async () =>
        (await client.del(keys[index])) > 0 ? "deleted" : "notfound",
    );
  }

  private resolveDeleteKeys(
    identities: readonly Record<string, unknown>[],
  ): string[] {
    return identities.map(({ key }) => {
      const resolved = this.resolveStoredKey(key);
      if (resolved === null)
        throw new Error("Redis delete requires a string key.");
      return resolved;
    });
  }

  private buildRedisDeletePreview(data: {
    primaryKeys?: Record<string, unknown>;
    primaryKeyValuesList?: Array<Record<string, unknown>>;
  }): string[] {
    return this.resolveDeleteKeys(
      data.primaryKeyValuesList ?? (data.primaryKeys ? [data.primaryKeys] : []),
    ).map((key) => formatRedisPreviewCommand("DEL", [key]));
  }

  private readonly observedDeleteRows = new WeakMap<
    Record<string, unknown>,
    NoSqlDeleteObservation
  >();
  private readonly deleteObservationStates = new WeakMap<
    NoSqlDeleteObservation,
    RedisDeleteState
  >();

  prepareObservedDelete(
    request: DriverDeleteRowsRequest,
    observations: readonly NoSqlDeleteObservation[],
  ): PreparedObservedDelete {
    const owner = this.requireClient();
    const database = this.resolveDbIndex(request.database);
    const reject = () =>
      new MutationNotExecutedError(noSqlDeleteObservationError.redis);
    if (observations.length !== request.primaryKeyValuesList.length)
      throw reject();
    const states = request.primaryKeyValuesList.map((keys, index) => {
      const state = this.deleteObservationStates.get(observations[index]);
      if (
        !state ||
        state.client !== owner ||
        state.database !== database ||
        state.table !== request.table ||
        Object.keys(keys).length !== 1 ||
        state.key !== this.resolveStoredKey(keys.key)
      )
        throw reject();
      return state;
    });
    return {
      previewStatements: states.map(
        ({ key, type, expiry, bytes, visibleHash }) => {
          const binaryArg = (value: Buffer) =>
            `"${value.toString("hex").replace(/../g, "\\x$&")}"`;
          return [
            formatRedisPreviewCommand("EVAL", [
              REDIS_COMPARE_DELETE,
              1,
              key,
              type,
              expiry,
            ]),
            binaryArg(bytes),
            ...(visibleHash ? [binaryArg(visibleHash)] : []),
          ].join(" ");
        },
      ),
      execute: async (context) => {
        const baseClient = await prepareDeleteBatch(
          request.primaryKeyValuesList,
          context,
          async () => {
            if (owner !== this.requireClient()) throw reject();
            const client = await this.getDatabaseClient(
              request.database,
              context?.signal,
            );
            if (owner !== this.requireClient()) throw reject();
            return client;
          },
        );
        const client = context
          ? baseClient.withAbortSignal(context.signal)
          : baseClient;
        return deleteRowsSequentially(
          request.primaryKeyValuesList,
          context,
          (_identity, index) => {
            if (owner !== this.requireClient()) throw reject();
            const { key, type, expiry, bytes, visibleHash } = states[index];
            return async () => {
              try {
                const result = await client.sendCommand<unknown>([
                  "EVAL",
                  REDIS_COMPARE_DELETE,
                  "1",
                  key,
                  type,
                  String(expiry),
                  bytes,
                  ...(visibleHash ? [visibleHash] : []),
                ]);
                return result === 1
                  ? "deleted"
                  : result === 0
                    ? "conflict"
                    : "unknown";
              } catch (error) {
                // DEL is the script's only write and its final operation.
                if (error instanceof ErrorReply) {
                  throw new MutationNotExecutedError(error.message, error);
                }
                throw error;
              }
            };
          },
        );
      },
    };
  }

  buildMutationPreviewStatement(
    operation: "insert" | "update" | "delete",
    _database: string,
    _schema: string,
    _table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
  ): string {
    if (operation === "insert") {
      return this.buildRedisInsertPreview(
        this.buildRedisInsertPlan(data.values),
      );
    }
    if (operation === "update") {
      return this.buildRedisUpdatePreview(data);
    }
    return this.buildRedisDeletePreview(data).join("\n");
  }

  async buildMutationPreviewStatements(
    operation: "insert" | "update" | "delete",
    database: string,
    _schema: string,
    _table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
  ): Promise<string[]> {
    if (operation === "delete") {
      return this.buildRedisDeletePreview(data);
    }

    if (operation === "insert") {
      return [
        this.buildRedisInsertPreview(this.buildRedisInsertPlan(data.values)),
      ];
    }

    const changes = data.changes ?? {};
    const hasValueChange = ["value", "json", "text"].some((key) =>
      Object.hasOwn(changes, key),
    );
    const sourceKey = this.resolveStoredKey(data.primaryKeys?.key);
    if (sourceKey !== null && hasValueChange) {
      const redisType = await (await this.getDatabaseClient(database)).type(
        sourceKey,
      );
      if (redisType === "stream") {
        throw new Error(
          "Redis stream values are read-only in the table viewer.",
        );
      }
      this.encodeRedisLuaValue(
        redisType,
        changes.value ?? changes.json ?? changes.text,
      );
    }
    return [this.buildRedisUpdatePreview(data)];
  }

  async runTransaction(
    operations: TransactionOperation[],
    context?: TransactionContext,
    scope?: DatabaseExecutionScope,
  ): Promise<void> {
    if (operations.length > 1) {
      throw new Error(
        "[RapiDB] Redis driver does not support atomic multi-operation transactions. Apply one row at a time.",
      );
    }
    for (const operation of operations) {
      throwIfTransactionCancelled(context);
      if (context)
        await this.executeQuery(operation.sql, scope, context.signal);
      else if (scope) await this.query(operation.sql, operation.params, scope);
      else await this.query(operation.sql, operation.params);
    }
  }

  quoteIdentifier(name: string): string {
    return name;
  }

  qualifiedTableName(
    _database: string,
    _schema: string,
    table: string,
  ): string {
    return table;
  }

  buildPagination(
    offset: number,
    limit: number,
    _paramIndex: number,
  ): PaginationResult {
    return {
      sql: "LIMIT ? OFFSET ?",
      params: [limit, offset],
    };
  }

  buildOrderByDefault(_cols: ColumnTypeMeta[]): string {
    return "ORDER BY key";
  }

  coerceInputValue(value: unknown, _column: ColumnTypeMeta): unknown {
    return value;
  }

  coerceOriginalValue(value: unknown, _column: ColumnTypeMeta): unknown {
    return value;
  }

  formatOutputValue(value: unknown, _column: ColumnTypeMeta): unknown {
    return value;
  }

  checkPersistedEdit(
    _column: ColumnTypeMeta,
    _expectedValue: unknown,
    _options?: { persistedValue: unknown },
  ) {
    return null;
  }

  normalizeFilterValue(
    _column: ColumnTypeMeta,
    _operator: never,
    value: string | [string, string] | undefined,
  ) {
    return value;
  }

  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: never,
    value: string | [string, string] | undefined,
    _paramIndex: number,
  ) {
    return {
      sql: `${column.name}:${String(operator)}`,
      params: value === undefined ? [] : Array.isArray(value) ? value : [value],
    };
  }

  buildInsertDefaultValuesSql(qualifiedTableName: string): string {
    return stringifyCommandPayload("redis_insert", {
      table: qualifiedTableName,
    });
  }

  buildInsertValueExpr(_column: ColumnTypeMeta, _paramIndex: number): string {
    return "?";
  }

  buildSetExpr(column: ColumnTypeMeta): string {
    return `${column.name} = ?`;
  }

  materializePreviewSql(sql: string): string {
    return sql;
  }

  private requireClient(): ReturnType<typeof createClient> {
    if (!this.client || !this.connected) {
      throw new Error("Redis is not connected.");
    }
    return this.client;
  }

  private resolveDbIndex(database?: string): number {
    let selected = database?.trim() || this.config.database?.trim();
    if (!selected && this.config.connectionUri) {
      selected = new URL(this.config.connectionUri).pathname.slice(1);
    }
    if (!selected) return 0;
    const match = /^(?:db)?(\d+)$/.exec(selected);
    const index = match ? Number(match[1]) : NaN;
    if (!Number.isSafeInteger(index)) {
      throw new Error(
        `Invalid Redis logical database '${selected}': expected a numeric index or dbN.`,
      );
    }
    return index;
  }

  private readonly metadataClients = new WeakMap<
    object,
    Map<number, RedisClientEntry>
  >();

  private async getDatabaseClient(
    database?: string,
    signal?: AbortSignal,
  ): Promise<RedisClient> {
    this.requireClient();
    const metadata = getMetadataContext();
    if (metadata) {
      const index = this.resolveDbIndex(database);
      let clients = this.metadataClients.get(metadata);
      if (!clients) {
        clients = new Map();
        this.metadataClients.set(metadata, clients);
      }
      let entry = clients.get(index);
      if (!entry) {
        entry = this.openClient(index);
        clients.set(index, entry);
        const owned = entry;
        metadata.cleanups.add(() => {
          this.cancelClientEntry(owned);
        });
      }
      const client = await this.waitForResult(entry.promise, metadata.signal);
      metadata.signal.throwIfAborted();
      return client;
    }
    signal?.throwIfAborted();
    const index = this.resolveDbIndex(database);
    let entry = this.databaseClients.get(index);
    if (!entry) {
      entry = this.openClient(index);
      this.databaseClients.set(index, entry);
      const pending = entry;
      void entry.promise.catch(() => {
        if (this.databaseClients.get(index) === pending)
          this.databaseClients.delete(index);
      });
    }
    return this.waitForResult(entry.promise, signal);
  }

  private async waitForResult<T>(
    promise: Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    if (!signal) return promise;
    signal.throwIfAborted();
    let onAbort: () => void = () => undefined;
    try {
      return await Promise.race([
        promise,
        new Promise<never>((_resolve, reject) => {
          onAbort = () => reject(signal.reason);
          signal.addEventListener("abort", onAbort, { once: true });
        }),
      ]);
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  private normalizeStoredValue(value: unknown): string {
    if (typeof value === "string") {
      return value;
    }
    if (value === null || value === undefined) {
      return "";
    }
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  private async readRows(
    client: RedisClient,
    table: string,
    maxRows: number,
    failOnTruncation: RedisKeyLimitPurpose | false = false,
    context?: QueryExecutionOptions,
    deleteReadScope?: RedisDeleteReadScope,
  ): Promise<RedisSampleRow[]> {
    const readLimit = Math.min(maxRows, REDIS_READ_BUDGET.maxValueReads);
    const keys = (
      await this.scanTableKeys(
        client,
        table,
        readLimit,
        failOnTruncation,
        context,
      )
    ).sort(compareRedisKeys);
    return await this.readRowsForKeys(client, keys, context, {
      deleteReadScope,
    });
  }

  private async scanTableKeys(
    client: RedisClient,
    table: string,
    limit: number,
    failOnTruncation: RedisKeyLimitPurpose | false = false,
    context?: QueryExecutionOptions,
  ): Promise<string[]> {
    return this.scanKeys(
      client,
      redisKeyspacePattern(table),
      limit,
      failOnTruncation,
      context,
      isRedisUnprefixedKeysTable(table)
        ? { matches: (bytes) => !bytes.includes(0x3a) }
        : {},
    );
  }

  private async readRowsForKeys(
    client: RedisClient,
    keys: readonly string[],
    context?: QueryExecutionOptions,
    options: {
      metadata?: Map<string, RedisKeyMetadata>;
      initialBytes?: number;
      stopWhenBudgetSpent?: boolean;
      deleteReadScope?: RedisDeleteReadScope;
    } = {},
  ): Promise<RedisSampleRow[]> {
    const read = async <T>(operation: () => Promise<T>): Promise<T> => {
      throwIfQueryCancelled(context);
      const result = await this.waitForResult(operation(), context?.signal);
      throwIfQueryCancelled(context);
      return result;
    };
    const { maxTotalValueBytes } = REDIS_READ_BUDGET;
    let spentBytes = options.initialBytes ?? 0;
    let inFlightReads = 0;
    let budgetSpent = false;
    const budgetWaiters: Array<() => void> = [];
    return pMapWithLimit(
      [...keys],
      REDIS_READ_BUDGET.parallelValueReads,
      async (key) => {
        let metadata = options.metadata?.get(key);
        if (!metadata) {
          const type = await read(() => client.type(key));
          const [size, ttl] = await read(() =>
            Promise.all([
              this.readRedisValueSize(client, key, type).catch(
                (error: unknown) => {
                  if (error instanceof ErrorReply) return undefined;
                  throw error;
                },
              ),
              typeof client.ttl === "function"
                ? client.ttl(key)
                : Promise.resolve(-1),
            ]),
          );
          metadata = { type, size, ttl };
          options.metadata?.set(key, metadata);
        }
        const { type, size, ttl } = metadata;
        const loadedRow = (
          value: unknown,
          currentTtl = ttl,
        ): RedisSampleRow => ({
          redisType: type,
          row: { key, value, ttl: currentTtl >= 0 ? currentTtl : null, type },
        });
        if (type === "none") return loadedRow(null);
        const isString = type === "string";
        const canReadCompact = ["hash", "list", "set", "zset"].includes(type);
        if (size !== undefined && this.exceedsRedisValueLimit(type, size)) {
          return loadedRow(redisOversizedValueSnapshot(type, size));
        }
        if (!isString && !canReadCompact) {
          return loadedRow(
            redisUnavailableValueSnapshot(type, "no byte-bounded read"),
          );
        }
        if (size === undefined || !Number.isSafeInteger(size) || size < 0) {
          return loadedRow(
            redisUnavailableValueSnapshot(
              type,
              "bounded read denied or unsupported",
            ),
          );
        }

        // GETRANGE's inclusive end allows exactly size+1 bytes, not a guess
        // about the size when Redis runs the command. Reserve raw bytes and
        // worst-case display + hex before dispatch; include any retained proof.
        const reserved = isString
          ? 5 * size + 2 * "RedisBinary(0x)".length + 1
          : REDIS_COLLECTION_READ_RESERVATION;
        const stopped = () => options.stopWhenBudgetSpent && budgetSpent;
        while (
          !stopped() &&
          spentBytes + reserved > maxTotalValueBytes &&
          inFlightReads > 0
        ) {
          await new Promise<void>((resolve) => budgetWaiters.push(resolve));
          throwIfQueryCancelled(context);
        }
        if (stopped() || spentBytes + reserved > maxTotalValueBytes) {
          budgetSpent = true;
          return loadedRow(
            redisOversizedValueSnapshot(type, size, undefined, true),
          );
        }

        spentBytes += reserved;
        inFlightReads += 1;
        try {
          const {
            value,
            ttl: currentTtl,
            deleteState,
          } = await this.readBoundedRedisValue(
            client,
            key,
            type,
            size,
            ttl,
            options.deleteReadScope,
            context,
          );
          throwIfQueryCancelled(context);
          const result = loadedRow(value, currentTtl);
          spentBytes += redisRowValueBytes(value);
          if (deleteState && deleteState.client === this.client) {
            const observation = { backend: "redis" } as const;
            this.observedDeleteRows.set(result.row, observation);
            this.deleteObservationStates.set(observation, deleteState);
            spentBytes +=
              deleteState.bytes.length + (deleteState.visibleHash?.length ?? 0);
          }
          return result;
        } finally {
          spentBytes -= reserved;
          inFlightReads -= 1;
          for (const resolve of budgetWaiters.splice(0)) resolve();
        }
      },
      context?.signal,
    );
  }

  // Keep the slot until node-redis settles the command, not just until the
  // caller stops waiting. An already sent reply can arrive after cancellation.
  private tableValueCommands = 0;
  private readonly tableValueWaiters = new Set<() => void>();

  private async runTableValueCommand<T>(
    operation: () => Promise<T>,
    context?: QueryExecutionOptions,
  ): Promise<T> {
    while (this.tableValueCommands >= REDIS_TABLE_VALUE_COMMANDS) {
      throwIfQueryCancelled(context);
      let notify!: () => void;
      const ready = new Promise<void>((resolve) => {
        notify = resolve;
        this.tableValueWaiters.add(notify);
      });
      try {
        await this.waitForResult(ready, context?.signal);
      } finally {
        this.tableValueWaiters.delete(notify);
      }
    }
    throwIfQueryCancelled(context);
    this.tableValueCommands += 1;
    const pending = (async () => {
      try {
        return await operation();
      } finally {
        this.tableValueCommands -= 1;
        for (const notify of this.tableValueWaiters) notify();
        this.tableValueWaiters.clear();
      }
    })();
    const result = await this.waitForResult(pending, context?.signal);
    throwIfQueryCancelled(context);
    return result;
  }

  private async readRedisCompactValue(
    client: RedisClient,
    key: string,
    type: string,
    size: number,
    scope?: RedisDeleteReadScope,
    context?: QueryExecutionOptions,
  ): Promise<RedisLoadedValue | false | undefined> {
    const reader = client.withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer });
    if (typeof reader.sendCommand !== "function") return undefined;
    let result: unknown;
    try {
      result = await this.runTableValueCommand(
        () =>
          reader.sendCommand([
            "EVAL_RO",
            REDIS_OBSERVE_DELETE,
            "1",
            key,
            type,
            scope ? "1" : "0",
            String(size),
          ]),
        context,
      );
    } catch (error) {
      // Redis < 7 or ACLs rejecting the observation must not prevent browsing.
      if (error instanceof ErrorReply) return undefined;
      throw error;
    }
    if (result === null || result === false) return false;
    if (!Array.isArray(result) || result.length !== 6) return undefined;
    const [rawType, expiry, ttl, dump, rawValue, visibleHash] = result;
    if (
      !Buffer.isBuffer(rawType) ||
      rawType.toString("ascii") !== type ||
      !Number.isSafeInteger(ttl) ||
      ttl < -1
    )
      return undefined;
    const bytes = type === "string" ? rawValue : dump;
    const deleteState =
      scope &&
      Number.isSafeInteger(expiry) &&
      expiry >= -1 &&
      Buffer.isBuffer(bytes) &&
      bytes.length <= REDIS_DELETE_EVIDENCE_BYTES &&
      (type !== "hash" || Buffer.isBuffer(visibleHash))
        ? {
            ...scope,
            key,
            type,
            expiry,
            bytes,
            ...(type === "hash" ? { visibleHash } : {}),
          }
        : undefined;
    if (type === "string") {
      if (!Buffer.isBuffer(rawValue) || rawValue.length > size)
        return undefined;
      return { deleteState, ttl, value: redisStringValue(rawValue) };
    }
    const value = formatRedisCollection(type, rawValue);
    return value === undefined ? undefined : { deleteState, ttl, value };
  }

  // Reads a collection in pages without an atomic snapshot. The result is
  // display data only and never carries delete evidence.
  private async readPagedRedisCollection(
    client: RedisClient,
    key: string,
    type: string,
    context?: QueryExecutionOptions,
  ): Promise<RedisLoadedValue> {
    const unavailable = (
      reason:
        | "no byte-bounded read"
        | "bounded read denied or unsupported"
        | "value changed during read",
    ) => ({ value: redisUnavailableValueSnapshot(type, reason) });
    if (!["hash", "list", "set", "zset"].includes(type)) {
      return unavailable("no byte-bounded read");
    }
    const reader = client.withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer });
    if (typeof reader.sendCommand !== "function") {
      return unavailable("bounded read denied or unsupported");
    }
    const send = (args: string[]) =>
      this.runTableValueCommand(() => reader.sendCommand(args), context);
    const text = (value: unknown) =>
      Buffer.isBuffer(value) ? value.toString("latin1") : value;
    try {
      if (type === "list") {
        // Compressed quicklist nodes make MEMORY USAGE smaller than the data.
        const encoding = text(await send(["OBJECT", "ENCODING", key]));
        if (encoding === null) return unavailable("value changed during read");
        if (encoding === "quicklist") {
          const config = await send(["CONFIG", "GET", "list-compress-depth"]);
          if (
            !Array.isArray(config) ||
            config.length !== 2 ||
            text(config[1]) !== "0"
          ) {
            return unavailable("no byte-bounded read");
          }
        } else if (
          !["listpack", "ziplist", "linkedlist"].includes(String(encoding))
        ) {
          return unavailable("no byte-bounded read");
        }
      }
      const usage: unknown = await send([
        "MEMORY",
        "USAGE",
        key,
        "SAMPLES",
        "0",
      ]);
      if (usage === null) return unavailable("value changed during read");
      if (!Number.isSafeInteger(usage) || (usage as number) < 0) {
        return unavailable("bounded read denied or unsupported");
      }
      if ((usage as number) > REDIS_PAGED_PREFLIGHT_BYTES) {
        return {
          value: redisOversizedValueSnapshot(type, usage as number, "bytes"),
        };
      }
      const { maxValueEntries } = REDIS_READ_BUDGET;
      const paired = type === "hash" || type === "zset";
      const collected: Buffer[] = [];
      const seen = new Set<string>();
      let bytes = 0;
      const tooLarge = () => ({
        value:
          bytes > REDIS_PAGED_COLLECTION_BYTES
            ? redisOversizedValueSnapshot(type, bytes, "bytes")
            : redisOversizedValueSnapshot(
                type,
                collected.length / (paired ? 2 : 1),
              ),
      });
      const accept = (reply: unknown[]): boolean => {
        for (const item of reply) bytes += (item as Buffer).length;
        if (type === "list") {
          collected.push(...(reply as Buffer[]));
        } else {
          const step = paired ? 2 : 1;
          for (let i = 0; i < reply.length; i += step) {
            const member = (reply[i] as Buffer).toString("latin1");
            if (seen.has(member)) continue;
            seen.add(member);
            collected.push(...(reply.slice(i, i + step) as Buffer[]));
          }
        }
        return (
          bytes <= REDIS_PAGED_COLLECTION_BYTES &&
          collected.length <= maxValueEntries * (paired ? 2 : 1)
        );
      };
      const isBuffers = (value: unknown): value is Buffer[] =>
        Array.isArray(value) && value.every(Buffer.isBuffer);
      if (type === "list") {
        for (let start = 0, commands = 0; ; start += REDIS_PAGED_COUNT) {
          if (++commands > REDIS_PAGED_MAX_COMMANDS) return tooLarge();
          const reply = await send([
            "LRANGE",
            key,
            String(start),
            String(start + REDIS_PAGED_COUNT - 1),
          ]);
          if (!isBuffers(reply) || reply.length > REDIS_PAGED_COUNT) {
            return unavailable("bounded read denied or unsupported");
          }
          if (!accept(reply)) return tooLarge();
          if (reply.length < REDIS_PAGED_COUNT) break;
        }
      } else {
        const command = { hash: "HSCAN", set: "SSCAN", zset: "ZSCAN" }[
          type as "hash" | "set" | "zset"
        ];
        const cursors = new Set<string>();
        let cursor = "0";
        do {
          if (cursors.size >= REDIS_PAGED_MAX_COMMANDS) return tooLarge();
          cursors.add(cursor);
          const reply = await send([
            command,
            key,
            cursor,
            "COUNT",
            String(REDIS_PAGED_COUNT),
          ]);
          if (
            !Array.isArray(reply) ||
            reply.length !== 2 ||
            !Buffer.isBuffer(reply[0]) ||
            !isBuffers(reply[1]) ||
            (paired && reply[1].length % 2 !== 0)
          ) {
            return unavailable("bounded read denied or unsupported");
          }
          if (!accept(reply[1])) return tooLarge();
          cursor = reply[0].toString("latin1");
          if (cursor !== "0" && cursors.has(cursor)) {
            return unavailable("value changed during read");
          }
        } while (cursor !== "0");
      }
      if (type === "zset") sortRedisSortedSetPairs(collected);
      const value = formatRedisCollection(type, collected);
      return value === undefined
        ? unavailable("bounded read denied or unsupported")
        : { value };
    } catch (error) {
      if (!(error instanceof ErrorReply)) throw error;
      return error.message.startsWith("WRONGTYPE")
        ? unavailable("value changed during read")
        : unavailable("bounded read denied or unsupported");
    }
  }

  private async readBoundedRedisValue(
    client: RedisClient,
    key: string,
    type: string,
    size: number,
    ttl: number,
    scope?: RedisDeleteReadScope,
    context?: QueryExecutionOptions,
  ): Promise<RedisLoadedValue> {
    if (type !== "string" || (scope && size <= REDIS_DELETE_EVIDENCE_BYTES)) {
      const checked = await this.readRedisCompactValue(
        client,
        key,
        type,
        size,
        scope,
        context,
      );
      if (checked) return checked;
    }
    // Never fall back to an unbounded collection read (even for one member):
    // the paged reader requires a byte preflight and counts every reply.
    if (type !== "string") {
      return this.readPagedRedisCollection(client, key, type, context);
    }
    const unavailable = {
      value: redisUnavailableValueSnapshot(
        type,
        "bounded read denied or unsupported",
      ),
    };
    const reader = client.withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer });
    if (typeof reader.getRange !== "function") return unavailable;
    const changed = {
      value: redisUnavailableValueSnapshot(type, "value changed during read"),
    };
    try {
      const bytes = await this.runTableValueCommand(
        () => reader.getRange(key, 0, size),
        context,
      );
      if (!Buffer.isBuffer(bytes)) return unavailable;
      if (bytes.length > REDIS_READ_BUDGET.maxValueBytes) {
        return { value: redisOversizedValueSnapshot(type, bytes.length) };
      }
      if (
        bytes.length > size ||
        (bytes.length === 0 && (size > 0 || ttl === -2))
      ) {
        return changed;
      }
      return { value: redisStringValue(bytes) };
    } catch (error) {
      if (!(error instanceof ErrorReply)) throw error;
      return error.message.startsWith("WRONGTYPE") ? changed : unavailable;
    }
  }

  private async readRowMetadata(
    client: RedisClient,
    keys: readonly string[],
    context?: QueryExecutionOptions,
  ): Promise<RedisSampleRow[]> {
    return pMapWithLimit(
      [...keys],
      REDIS_READ_BUDGET.parallelValueReads,
      async (key) => {
        throwIfQueryCancelled(context);
        const [type, ttl] = await this.waitForResult(
          Promise.all([
            client.type(key),
            typeof client.ttl === "function"
              ? client.ttl(key)
              : Promise.resolve(-1),
          ]),
          context?.signal,
        );
        throwIfQueryCancelled(context);
        return {
          redisType: type,
          row: { key, value: null, ttl: ttl >= 0 ? ttl : null, type },
        };
      },
      context?.signal,
    );
  }

  private inferRedisColumns(rows: readonly RedisSampleRow[]): ColumnTypeMeta[] {
    // The grid schema exists even without keys. Only value comparisons and
    // the native value label are inferred from samples; inserts still use SET.
    const valueTypeLabel = formatRedisValueTypeLabel(rows);
    const [sampledValue] = inferColumnsFromRows(
      rows.map(({ row }) => ({
        value: isRedisOversizedValueSnapshot(row.value)
          ? null
          : redisComparisonRow(row).value,
      })),
      "key",
      { nullableMode: "schemaLess", consistentCategories: true },
    );
    const valueType = valueTypeLabel ?? sampledValue?.nativeType ?? "string";
    return [
      redisKeyspaceColumn("key", "string", "string", "text", {
        isPrimaryKey: true,
      }),
      {
        ...(sampledValue ??
          redisKeyspaceColumn("value", "string", "string", "text", {
            nullable: true,
          })),
        type: valueType,
        nativeType: valueType,
        redisValue: true,
      },
      redisKeyspaceColumn("ttl", "integer", "ttl_seconds", "integer", {
        nullable: true,
      }),
      redisKeyspaceColumn("type", "string", "redis_type", "text", {
        isReadOnly: true,
      }),
    ];
  }

  private parseRedisTtlInput(
    value: unknown,
    source: "Redis TTL inserts" | "Redis TTL updates",
  ): number | null {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === "number") {
      if (!Number.isInteger(value)) {
        throw new Error(`${source} require an integer TTL in seconds.`);
      }
      if (!Number.isSafeInteger(value)) {
        throw new Error(`${source} require a safe integer TTL value.`);
      }
      if (value === -1) {
        return null;
      }
      if (value >= 1) {
        if (value > REDIS_MAX_TTL_SECONDS) {
          throw new Error(
            `${source} require a TTL no greater than ${REDIS_MAX_TTL_SECONDS} seconds.`,
          );
        }
        return value;
      }
      throw new Error(`${source} require a positive TTL or -1 to persist.`);
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        return null;
      }
      if (!/^-?\d+$/.test(trimmed)) {
        throw new Error(`${source} require an integer TTL in seconds.`);
      }
      const parsed = Number(trimmed);
      if (!Number.isSafeInteger(parsed)) {
        throw new Error(`${source} require a safe integer TTL value.`);
      }
      if (parsed === -1) {
        return null;
      }
      if (parsed >= 1) {
        if (parsed > REDIS_MAX_TTL_SECONDS) {
          throw new Error(
            `${source} require a TTL no greater than ${REDIS_MAX_TTL_SECONDS} seconds.`,
          );
        }
        return parsed;
      }
      throw new Error(`${source} require a positive TTL or -1 to persist.`);
    }

    throw new Error(`${source} require an integer TTL in seconds.`);
  }

  private buildRedisUpdatePreview(data: {
    primaryKeys?: Record<string, unknown>;
    changes?: Record<string, unknown>;
  }): string {
    const changes = data.changes ?? {};
    const source = this.resolveStoredKey(data.primaryKeys?.key);
    const target = Object.hasOwn(changes, "key")
      ? this.resolveStoredKey(changes.key)
      : source;
    if (source !== null && target === null) {
      throw new Error("Redis key updates require a string 'key' value.");
    }
    const hasValue = ["value", "json", "text"].some((key) =>
      Object.hasOwn(changes, key),
    );
    const ttl = Object.hasOwn(changes, "ttl")
      ? this.parseRedisTtlInput(changes.ttl, "Redis TTL updates")
      : undefined;
    return JSON.stringify({
      preview: "Redis update description only; not an executable command",
      source: source ?? "<missing key>",
      target: target ?? "<missing key>",
      conditions:
        "Source must exist; a different target must not exist; the type must match the type read during execution. When an eligible original value is supplied, string, hash, list, set and zset values are checked for changes. Stream contents are not checked for changes. Other safety checks may reject the edit.",
      value: hasValue
        ? {
            action:
              "replace using the existing Redis type (empty collections delete the source)",
            input: changes.value ?? changes.json ?? changes.text ?? null,
          }
        : { action: "unchanged" },
      ttl:
        ttl === undefined
          ? "keep existing TTL"
          : ttl === null
            ? "persist"
            : `set to ${ttl} seconds`,
      execution:
        "The application executes a conditional Lua script, not this description.",
    });
  }

  private buildRedisInsertPlan(
    values: Record<string, unknown> = {},
  ): RedisInsertPlan {
    const key = this.resolveStoredKey(values.key);
    if (key === null) {
      throw new Error("Redis insert requires a 'key' field.");
    }
    const ttlSeconds = Object.hasOwn(values, "ttl")
      ? this.parseRedisTtlInput(values.ttl, "Redis TTL inserts")
      : undefined;
    const value = this.normalizeStoredValue(
      values.value ?? values.json ?? values.text,
    );
    assertRedisLosslessUtf8Value(value);
    return {
      command: "set",
      args: [key, value],
      options:
        ttlSeconds !== undefined && ttlSeconds !== null
          ? { NX: true, EX: ttlSeconds }
          : { NX: true },
    };
  }

  private buildRedisInsertPreview(plan: RedisInsertPlan): string {
    const args: Array<string | number> = [...plan.args];
    if (plan.options.EX !== undefined) {
      args.push("EX", plan.options.EX);
    }
    if (plan.options.NX) {
      args.push("NX");
    }
    return formatRedisPreviewCommand(plan.command.toUpperCase(), args);
  }

  private encodeRedisLuaValue(
    redisType: string,
    value: unknown,
    validatePrecision = true,
  ): string {
    if (["hash", "list", "set", "zset"].includes(redisType)) {
      value = this.parseRedisJsonValue(
        value,
        `${redisType} value`,
        validatePrecision,
      );
    }
    const encoded =
      redisType === "hash"
        ? this.parseRedisHashEntries(value)
        : redisType === "list" || redisType === "set"
          ? this.parseRedisSequenceElements(value, redisType)
          : redisType === "zset"
            ? this.parseRedisSortedSetEntries(value)
            : this.normalizeStoredValue(value);
    assertRedisLosslessUtf8Value(encoded);
    return JSON.stringify(encoded);
  }

  private parseRedisSequenceElements(
    value: unknown,
    redisType: "list" | "set",
  ): string[] {
    const parsed = this.parseRedisJsonValue(value, `${redisType} value`);
    if (!Array.isArray(parsed)) {
      throw new Error(
        `Redis ${redisType} values must be edited as a JSON array.`,
      );
    }
    return parsed.map((entry) => this.stringifyRedisNestedValue(entry));
  }

  private parseRedisHashEntries(value: unknown): RedisHashEntries {
    const parsed = this.parseRedisJsonValue(value, "hash value");
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("Redis hash values must be edited as a JSON object.");
    }
    return Object.fromEntries(
      Object.entries(parsed).map(([field, fieldValue]) => [
        field,
        this.stringifyRedisNestedValue(fieldValue),
      ]),
    );
  }

  private parseRedisSortedSetEntries(value: unknown): RedisSortedSetEntry[] {
    const parsed = this.parseRedisJsonValue(value, "sorted set value");
    if (!Array.isArray(parsed)) {
      throw new Error(
        "Redis sorted set values must be edited as a JSON array of { value, score } objects.",
      );
    }
    return parsed.map((entry) => {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
        throw new Error(
          "Redis sorted set values must be edited as a JSON array of { value, score } objects.",
        );
      }
      const rawScore = (entry as { score?: unknown }).score;
      const score =
        typeof rawScore === "number"
          ? rawScore
          : typeof rawScore === "string"
            ? /^[+-]?inf$/i.test(rawScore.trim())
              ? formatRedisSortedSetScore(rawScore)
              : Number(rawScore)
            : Number.NaN;
      if (typeof score === "number" && !Number.isFinite(score)) {
        throw new Error(
          'Redis sorted set scores must be finite numbers, "+inf", or "-inf".',
        );
      }
      if (!("value" in entry)) {
        throw new Error(
          "Redis sorted set entries must include both value and score fields.",
        );
      }
      return {
        score,
        value: this.stringifyRedisNestedValue(
          (entry as { value: unknown }).value,
        ),
      };
    });
  }

  private parseRedisJsonValue(
    value: unknown,
    label: string,
    validatePrecision = true,
  ): unknown {
    if (typeof value !== "string") {
      return value;
    }
    try {
      return validatePrecision
        ? parseJsonForWrite(value, isRoundTripJsonNumberToken)
        : JSON.parse(value);
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
      throw new Error(`Redis ${label} must be valid JSON.`);
    }
  }

  private stringifyRedisNestedValue(value: unknown): string {
    if (typeof value === "string") {
      return value;
    }
    if (value === undefined) {
      return "";
    }
    return JSON.stringify(value);
  }

  private async readRedisValueSize(
    client: RedisClient,
    key: string,
    type: string,
  ): Promise<number> {
    switch (type) {
      case "string":
        return client.strLen(key);
      case "hash":
        return client.hLen(key);
      case "list":
        return client.lLen(key);
      case "set":
        return client.sCard(key);
      case "zset":
        return client.zCard(key);
      case "stream":
        return client.xLen(key);
      default:
        return 0;
    }
  }

  private exceedsRedisValueLimit(type: string, size: number): boolean {
    return type === "string"
      ? size > REDIS_READ_BUDGET.maxValueBytes
      : size > REDIS_READ_BUDGET.maxValueEntries;
  }

  private resolveStoredKey(rawKey: unknown): string | null {
    if (typeof rawKey !== "string") return null;
    if (Buffer.from(rawKey, "utf8").toString("utf8") !== rawKey) {
      throw new Error("Redis key must be a losslessly UTF-8 encodable string.");
    }
    return rawKey;
  }

  private async scanKeys(
    client: RedisClient,
    pattern: string,
    limit = Number.POSITIVE_INFINITY,
    failOnTruncation: RedisKeyLimitPurpose | false = false,
    context?: QueryExecutionOptions,
    options: {
      matches?: (bytes: Buffer) => boolean;
      skipBinaryKeys?: boolean;
      onTruncated?: () => void;
    } = {},
  ): Promise<string[]> {
    const keys = new Set<string>();
    const seenCursors = new Set<string>();
    let cursor = "0";
    const canScan = () =>
      !seenCursors.has(cursor) &&
      seenCursors.size < REDIS_READ_BUDGET.maxScanIterations;
    const scanBatch = async () => {
      throwIfQueryCancelled(context);
      if (!canScan()) {
        throw new Error(
          `Redis SCAN did not complete within its safety budget of ${REDIS_READ_BUDGET.maxScanIterations} iterations (about ${REDIS_READ_BUDGET.maxScanIterations * REDIS_SCAN_COUNT} keys examined). The keyspace may be very large or changing; narrow the keyspace and retry.`,
        );
      }
      seenCursors.add(cursor);
      const response = await this.waitForResult(
        client
          .withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer })
          .scan(cursor, {
            MATCH: pattern,
            COUNT: REDIS_SCAN_COUNT,
          }),
        context?.signal,
      );
      throwIfQueryCancelled(context);
      cursor = response.cursor.toString();
      for (const bytes of response.keys) {
        if (options.matches && !options.matches(bytes)) continue;
        const key = bytes.toString("utf8");
        if (!Buffer.from(key, "utf8").equals(bytes)) {
          if (options.skipBinaryKeys) continue;
          throw new Error(
            "Redis keyspace contains a non-UTF-8 key. The table viewer cannot safely display or edit binary keys. Use a binary-safe Redis client or narrow the keyspace.",
          );
        }
        keys.add(key);
      }
    };
    do {
      await scanBatch();
    } while (
      cursor !== "0" &&
      (failOnTruncation ? keys.size <= limit : keys.size < limit)
    );
    if (failOnTruncation && keys.size > limit) {
      throw new Error(
        failOnTruncation === "filtering"
          ? `Redis filtering and non-key sorting read at most ${limit} keys (the ${limit}-key safety limit), and this keyspace has more. Open a narrower key prefix, or clear filters and non-key sorting to page through all keys.`
          : `Redis key paging reads at most ${limit} keys (the ${limit}-key safety limit), and this keyspace has more. Open a narrower key prefix before continuing or exporting.`,
      );
    }
    if (
      options.onTruncated &&
      keys.size === limit &&
      cursor !== "0" &&
      canScan()
    ) {
      await scanBatch();
    }
    if (keys.size > limit || (cursor !== "0" && keys.size >= limit))
      options.onTruncated?.();
    return Number.isFinite(limit) ? [...keys].slice(0, limit) : [...keys];
  }

  private mapRowToQueryRow(
    row: Record<string, unknown>,
    columns: string[],
  ): Record<string, unknown> {
    const mapped: Record<string, unknown> = {};
    columns.forEach((columnName, index) => {
      mapped[`__col_${index}`] = row[columnName];
    });
    return mapped;
  }
}
