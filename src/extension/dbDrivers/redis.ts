import { randomUUID } from "node:crypto";
import { createClient, ErrorReply, RESP_TYPES } from "redis";
import {
  isRedisBinaryCollectionValueSnapshot,
  isRedisBinaryValueSnapshot,
  isRedisNotLoadedValueSnapshot,
  isRedisOversizedValueSnapshot,
  type RedisOversizedValueSnapshot,
  redisOversizedValueSnapshot,
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
  PaginationResult,
  QueryExecutionOptions,
  QueryResult,
  SchemaInfo,
  TableConstraintMeta,
  TableInfoList,
  TransactionContext,
  TransactionOperation,
  TriggerMeta,
} from "./types";
import { NULL_SENTINEL, resolveFilterOperators } from "./types";
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
  size: number;
  memoryBytes: number | null;
  ttl: number;
}

interface RedisSampleRow {
  redisType: string;
  row: Record<string, unknown>;
}

type RedisHashEntries = Record<string, string>;

type RedisClient = ReturnType<typeof createClient>;

type RedisKeyLimitPurpose = "paging" | "filtering";

function redisMemberClient(client: RedisClient) {
  return client.withTypeMapping({
    [RESP_TYPES.BLOB_STRING]: Buffer,
    [RESP_TYPES.MAP]: Array,
    [RESP_TYPES.DOUBLE]: String,
  });
}

type RedisMemberClient = ReturnType<typeof redisMemberClient>;

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
  const text = bytes.toString("utf8");
  if (!Buffer.from(text, "utf8").equals(bytes)) return false;
  for (const character of text) {
    const codePoint = character.codePointAt(0) ?? 0;
    if (
      codePoint <= 0x08 ||
      codePoint === 0x0b ||
      codePoint === 0x0c ||
      (codePoint >= 0x0e && codePoint <= 0x1f) ||
      codePoint === 0x7f
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

function redisReplyPairs(reply: unknown): Array<[unknown, unknown]> {
  if (Array.isArray(reply)) {
    const pairs: Array<[unknown, unknown]> = [];
    for (let index = 0; index + 1 < reply.length; index += 2) {
      pairs.push([reply[index], reply[index + 1]]);
    }
    return pairs;
  }
  if (reply instanceof Map) return [...reply];
  return reply && typeof reply === "object" ? Object.entries(reply) : [];
}

class RedisCollectionDecoder {
  binary = false;

  text(value: unknown): string {
    if (!Buffer.isBuffer(value)) return String(value);
    if (isDisplayableRedisText(value)) return value.toString("utf8");
    this.binary = true;
    return formatRedisBlobString(value);
  }

  record(reply: unknown): Record<string, string> {
    return Object.fromEntries(
      redisReplyPairs(reply).map(([field, entry]) => [
        this.text(field),
        this.text(entry),
      ]),
    );
  }
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
  if (typeof value === "string" && /^-?\d+$/.test(value)) {
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
    const plan = await this.planTablePage(request);
    if ("rows" in plan) {
      return {
        columns: plan.columns,
        rows: plan.rows,
        totalCount: request.skipCount ? 0 : plan.totalCount,
        executionTimeMs: Math.round(performance.now() - startTime),
      };
    }
    const rows = await this.readRowsForKeys(plan.client, plan.keys, request);
    throwIfQueryCancelled(request);
    return {
      columns: this.inferRedisColumns(rows),
      rows: rows.map((entry) => entry.row),
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

  private async planTablePage(request: DriverTablePageRequest): Promise<
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
    );
    const displayToOriginal = new Map(
      rows.map((entry) => [redisComparisonRow(entry.row), entry.row]),
    );
    const omittedValueRow = rows.find((entry) =>
      isRedisOversizedValueSnapshot(entry.row.value),
    );
    if (omittedValueRow) {
      throw new Error(
        `Redis key '${String(omittedValueRow.row.key)}' has a value beyond the table viewer read limit (${REDIS_READ_BUDGET.maxValueEntries} entries or ${REDIS_READ_BUDGET.maxValueBytes} bytes per value, ${REDIS_READ_BUDGET.maxTotalValueBytes} bytes per read), so filtering or sorting by value is unavailable. Narrow the keyspace or filter by key, type, or TTL.`,
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
          throw new Error("Redis key type changed after the row was loaded.");
        }
        if (result === -3) {
          writeMayHaveApplied = false;
          throw new Error("Redis value changed after the row was loaded.");
        }
        if (result === 1) {
          writeMayHaveApplied = false;
          affectedRows++;
          updateRowOutcomes.push({ rowIndex, status: "applied" });
        } else if (result === 0) {
          writeMayHaveApplied = false;
          updateRowOutcomes.push({ rowIndex, status: "not_applied" });
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
    const baseClient = await prepareDeleteBatch(
      request.primaryKeyValuesList,
      context,
      () => this.getDatabaseClient(request.database, context?.signal),
    );
    const client = context
      ? baseClient.withAbortSignal(context.signal)
      : baseClient;
    return deleteRowsSequentially(
      request.primaryKeyValuesList,
      context,
      (entry) => {
        const key = this.resolveStoredKey(entry.key);
        if (key === null)
          throw new Error("Redis delete requires a string key.");
        return async () =>
          (await client.del(key)) > 0 ? "deleted" : "notfound";
      },
    );
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
    const keys = (
      data.primaryKeyValuesList ?? (data.primaryKeys ? [data.primaryKeys] : [])
    )
      .map((entry) => entry.key)
      .filter((k) => k !== undefined)
      .map((key) => {
        const resolved = this.resolveStoredKey(key);
        if (resolved === null)
          throw new Error("Redis delete requires a string key.");
        return resolved;
      });
    return keys.length > 0
      ? formatRedisPreviewCommand("DEL", keys)
      : "DEL <key>";
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
      const keys = (
        data.primaryKeyValuesList ??
        (data.primaryKeys ? [data.primaryKeys] : [])
      )
        .map((entry) => entry.key)
        .filter((key): key is unknown => key !== undefined)
        .map((key) => {
          const resolved = this.resolveStoredKey(key);
          if (resolved === null)
            throw new Error("Redis delete requires a string key.");
          return resolved;
        });
      return keys.length > 0
        ? [formatRedisPreviewCommand("DEL", keys)]
        : ["DEL <key>"];
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
    return value === NULL_SENTINEL ? null : value;
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
    return await this.readRowsForKeys(client, keys, context);
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
    } = {},
  ): Promise<RedisSampleRow[]> {
    const read = async <T>(operation: () => Promise<T>): Promise<T> => {
      throwIfQueryCancelled(context);
      const result = await this.waitForResult(operation(), context?.signal);
      throwIfQueryCancelled(context);
      return result;
    };
    const { maxValueBytes, maxTotalValueBytes } = REDIS_READ_BUDGET;
    let spentBytes = options.initialBytes ?? 0;
    let inFlightReads = 0;
    let budgetSpent = false;
    const budgetWaiters: Array<() => void> = [];
    return pMapWithLimit(
      [...keys],
      REDIS_READ_BUDGET.parallelValueReads,
      async (key) => {
        let metadata = options.metadata?.get(key);
        const refreshSize =
          metadata?.type === "hash" || metadata?.type === "set";
        if (!metadata) {
          const type = await read(() => client.type(key));
          const [size, memoryBytes, ttl] = await read(() =>
            Promise.all([
              this.readRedisValueSize(client, key, type),
              this.readRedisMemoryUsage(client, key, type),
              typeof client.ttl === "function"
                ? client.ttl(key)
                : Promise.resolve(-1),
            ]),
          );
          metadata = { type, size, memoryBytes, ttl };
          options.metadata?.set(key, metadata);
        }
        const { type, size, memoryBytes, ttl } = metadata;
        const stopped = () => options.stopWhenBudgetSpent && budgetSpent;
        const estimatedBytes =
          type === "string"
            ? 4 * size + 2 * "RedisBinary(0x)".length
            : (memoryBytes ?? maxValueBytes);
        let omitted: RedisOversizedValueSnapshot | undefined =
          this.exceedsRedisValueLimit(type, size)
            ? redisOversizedValueSnapshot(type, size)
            : memoryBytes !== null && memoryBytes > maxValueBytes
              ? redisOversizedValueSnapshot(type, memoryBytes, "bytes")
              : undefined;
        if (!omitted) {
          while (
            !stopped() &&
            spentBytes + estimatedBytes > maxTotalValueBytes &&
            inFlightReads > 0
          ) {
            await new Promise<void>((resolve) => budgetWaiters.push(resolve));
            throwIfQueryCancelled(context);
          }
          if (stopped() || spentBytes + estimatedBytes > maxTotalValueBytes) {
            budgetSpent = true;
            omitted = redisOversizedValueSnapshot(type, size, undefined, true);
          }
        }
        let value: unknown = null;
        let binarySnapshot: unknown;
        if (!omitted) {
          spentBytes += estimatedBytes;
          inFlightReads += 1;
          try {
            const currentSize = refreshSize
              ? await read(() => this.readRedisValueSize(client, key, type))
              : size;
            ({ value, binarySnapshot, omitted } = this.exceedsRedisValueLimit(
              type,
              currentSize,
            )
              ? {
                  value: null,
                  binarySnapshot: undefined,
                  omitted: redisOversizedValueSnapshot(type, currentSize),
                }
              : await this.readBoundedRedisValue(
                  client,
                  key,
                  type,
                  currentSize,
                  read,
                ));
          } finally {
            spentBytes -= estimatedBytes;
            inFlightReads -= 1;
            for (const resolve of budgetWaiters.splice(0)) resolve();
          }
        }

        const row = flattenRootRecord({
          key,
          value: omitted ? null : value,
          ttl: ttl >= 0 ? ttl : null,
          type,
        });
        if (!omitted) {
          const actualBytes =
            typeof row.value === "string"
              ? Buffer.byteLength(row.value) *
                (isRedisBinaryValueSnapshot(binarySnapshot) ? 2 : 1)
              : 0;
          if (type !== "string" && actualBytes > maxValueBytes) {
            omitted = redisOversizedValueSnapshot(type, actualBytes, "bytes");
          } else if (spentBytes + actualBytes > maxTotalValueBytes) {
            budgetSpent = true;
            omitted = redisOversizedValueSnapshot(type, size, undefined, true);
          } else {
            spentBytes += actualBytes;
          }
        }
        return {
          redisType: type,
          row: {
            ...row,
            ...(omitted
              ? { value: omitted }
              : binarySnapshot
                ? { value: binarySnapshot }
                : {}),
          },
        };
      },
      context?.signal,
    );
  }

  private async readBoundedRedisValue(
    client: RedisClient,
    key: string,
    type: string,
    size: number,
    read: <T>(operation: () => Promise<T>) => Promise<T>,
  ): Promise<{
    value: unknown;
    binarySnapshot?: unknown;
    omitted?: RedisOversizedValueSnapshot;
  }> {
    const { maxValueEntries, maxValueBytes } = REDIS_READ_BUDGET;
    let value: unknown = null;
    let binarySnapshot: unknown;
    const decoder = new RedisCollectionDecoder();
    const members = redisMemberClient(client);
    switch (type) {
      case "string":
        {
          const bytes = await read(() =>
            client
              .withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer })
              .getRange(key, 0, maxValueBytes),
          );
          if (Buffer.isBuffer(bytes) && bytes.length > maxValueBytes) {
            return {
              value: null,
              omitted: redisOversizedValueSnapshot(
                type,
                Math.max(size, bytes.length),
              ),
            };
          }
          value = Buffer.isBuffer(bytes) ? formatRedisBlobString(bytes) : null;
          if (Buffer.isBuffer(bytes) && !isDisplayableRedisText(bytes)) {
            binarySnapshot = {
              __rapidbRedisBinary: 1,
              displayValue: value,
              binaryHex: bytes.toString("hex"),
            };
          }
        }
        break;
      case "hash":
        value = decoder.record(await read(() => members.hGetAll(key)));
        break;
      case "list":
        value = (await read(() => members.lRange(key, 0, maxValueEntries))).map(
          (entry) => decoder.text(entry),
        );
        break;
      case "set":
        value = (await read(() => members.sMembers(key))).map((entry) =>
          decoder.text(entry),
        );
        break;
      case "zset":
        value = (
          await read(() => members.zRangeWithScores(key, 0, maxValueEntries))
        ).map((entry) => ({
          value: decoder.text(entry.value),
          score: formatRedisSortedSetScore(entry.score),
        }));
        break;
      case "stream": {
        const entries = await read(() =>
          this.readStreamEntries(members, key, maxValueEntries + 1),
        );
        value = Array.isArray(entries)
          ? entries.map((entry) => {
              const [id, message] = Array.isArray(entry)
                ? entry
                : [entry?.id, entry?.message];
              return { id: decoder.text(id), message: decoder.record(message) };
            })
          : entries;
        break;
      }
      default:
        value = null;
    }
    const entries = Array.isArray(value)
      ? value.length
      : type === "hash" && value
        ? Object.keys(value).length
        : 0;
    if (type !== "string" && entries > maxValueEntries) {
      return {
        value: null,
        omitted: redisOversizedValueSnapshot(type, Math.max(size, entries)),
      };
    }
    if (decoder.binary) {
      binarySnapshot = {
        __rapidbRedisBinaryCollection: 1,
        displayValue: JSON.stringify(value),
      };
    }
    return { value, binarySnapshot };
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
    const columns = inferColumnsFromRows(
      rows.map((entry) =>
        isRedisOversizedValueSnapshot(entry.row.value)
          ? { ...entry.row, value: null }
          : redisComparisonRow(entry.row),
      ),
      "key",
      {
        nullableMode: "schemaLess",
        consistentCategories: true,
      },
    );
    const valueTypeLabel = formatRedisValueTypeLabel(rows);
    return columns.map((column) =>
      column.name === "key"
        ? {
            ...column,
            type: "string",
            nativeType: "string",
            category: "text",
            filterOperators: resolveFilterOperators("text", {
              filterable: true,
              nullable: false,
            }),
          }
        : column.name === "value" && valueTypeLabel
          ? {
              ...column,
              type: valueTypeLabel,
              nativeType: valueTypeLabel,
            }
          : column.name === "type"
            ? {
                ...column,
                type: "string",
                nativeType: "redis_type",
                category: "text",
                isReadOnly: true,
                filterable: true,
                filterOperators: resolveFilterOperators("text", {
                  filterable: true,
                  nullable: false,
                }),
                valueSemantics: "plain",
              }
            : column.name === "ttl"
              ? {
                  ...column,
                  type: "integer",
                  nativeType: "ttl_seconds",
                  category: "integer",
                  filterable: true,
                  filterOperators: resolveFilterOperators("integer", {
                    filterable: true,
                    nullable: true,
                  }),
                  valueSemantics: "plain",
                }
              : column,
    );
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

  private async readRedisMemoryUsage(
    client: RedisClient,
    key: string,
    type: string,
  ): Promise<number | null> {
    if (
      !["hash", "list", "set", "zset", "stream"].includes(type) ||
      typeof client.memoryUsage !== "function"
    ) {
      return null;
    }
    try {
      return await client.memoryUsage(key, {
        SAMPLES: REDIS_READ_BUDGET.maxValueEntries,
      });
    } catch (error) {
      if (error instanceof ErrorReply) return null;
      throw error;
    }
  }

  private exceedsRedisValueLimit(type: string, size: number): boolean {
    return type === "string"
      ? size > REDIS_READ_BUDGET.maxValueBytes
      : size > REDIS_READ_BUDGET.maxValueEntries;
  }

  private async readStreamEntries(
    client: RedisMemberClient,
    key: string,
    count: number,
  ): Promise<unknown> {
    if (typeof client.xRange === "function") {
      return client.xRange(key, "-", "+", { COUNT: count });
    }
    return client.sendCommand([
      "XRANGE",
      key,
      "-",
      "+",
      "COUNT",
      String(count),
    ]);
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
