import { createClient } from "redis";
import {
  type OperationCancellationContext,
  REDIS_READ_BUDGET,
} from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import { getSshTcpForwardTransport } from "../driverRuntimeConfig";
import { resolveConnectionTlsSettings } from "../services/connectionTls";
import { pMapWithLimit } from "../utils/concurrency";
import { logger } from "../utils/logger";
import { allowReadOnlyQuery, denyReadOnlyQuery } from "../utils/readOnlyGuards";
import { deleteRowsSequentially, prepareDeleteBatch } from "./deleteOutcomes";
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
  DriverTimeoutError,
  type DriverTimeoutSettingsProvider,
  getDefaultDriverTimeoutSettings,
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
  DriverTablePageRequest,
  DriverTablePageResult,
  DriverUpdateRowsRequest,
  ForeignKeyMeta,
  IDBDriver,
  IndexMeta,
  PaginationResult,
  QueryExecutionOptions,
  QueryResult,
  SchemaInfo,
  TableConstraintMeta,
  TableInfo,
  TransactionContext,
  TransactionOperation,
  TriggerMeta,
} from "./types";
import { NULL_SENTINEL, resolveFilterOperators } from "./types";

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

const REDIS_ATOMIC_EDIT_SCRIPT = `
local source = KEYS[1]
local target = KEYS[2]
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
if ARGV[3] == '1' then
  redis.call('DEL', source)
  local value = cjson.decode(ARGV[4])
  if kind == 'string' then redis.call('SET', source, value)
  elseif kind == 'hash' then for field, entry in pairs(value) do redis.call('HSET', source, field, entry) end
  elseif kind == 'list' then for _, entry in ipairs(value) do redis.call('RPUSH', source, entry) end
  elseif kind == 'set' then for _, entry in ipairs(value) do redis.call('SADD', source, entry) end
  elseif kind == 'zset' then for _, entry in ipairs(value) do redis.call('ZADD', source, entry.score, entry.value) end
  else return -2 end
end
if source ~= target and redis.call('EXISTS', source) ~= 0 then redis.call('RENAME', source, target) end
local finalKey = target
if ARGV[5] == 'set' then
  if redis.call('EXISTS', finalKey) ~= 0 then redis.call('PEXPIRE', finalKey, tonumber(ARGV[6]) * 1000) end
elseif ARGV[5] == 'persist' then
  if redis.call('EXISTS', finalKey) ~= 0 then redis.call('PERSIST', finalKey) end
elseif ttl >= 0 and redis.call('EXISTS', finalKey) ~= 0 then redis.call('PEXPIRE', finalKey, ttl) end
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

interface RedisSampleRow {
  redisType: string;
  row: Record<string, unknown>;
}

type RedisHashEntries = Record<string, string>;

type RedisClient = ReturnType<typeof createClient>;

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

type RedisSortedSetEntry = {
  score: number;
  value: string;
};

// Keep the historical all-keys identity for saved queries. A real first
// prefix cannot contain ':', so 'default:' is an unambiguous navigation ID.
function keyspacePattern(table: string): string {
  if (table === "default") return "*";
  return table === "default:" ? "default:*" : `${table}:*`;
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
  let escaping = false;

  const pushCurrent = () => {
    const trimmed = current.trim();
    if (trimmed.length > 0) {
      statements.push(trimmed);
    }
    current = "";
  };

  for (const char of input) {
    if (escaping) {
      current += char;
      escaping = false;
      continue;
    }

    if (char === "\\") {
      current += char;
      escaping = true;
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

  if (escaping) {
    current += "\\";
  }

  pushCurrent();
  return statements;
}

function formatRedisPreviewCommand(
  command: string,
  args: ReadonlyArray<string | number>,
): string {
  return [command, ...args.map((arg) => JSON.stringify(String(arg)))].join(" ");
}

function tokenizeRedisCommand(input: string): string[] {
  const tokens: string[] = [];
  let current = "";
  let quote: '"' | "'" | null = null;
  let escaping = false;
  let tokenStarted = false;

  const pushCurrent = () => {
    if (tokenStarted) {
      tokens.push(current);
    }
    current = "";
    tokenStarted = false;
  };

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    if (escaping) {
      // Preview arguments are JSON strings. Decode their escapes only inside
      // double quotes; single-quoted and unquoted queries keep shell escaping.
      if (quote === '"' && char === "u") {
        const hex = input.slice(index + 1, index + 5);
        if (!/^[\da-fA-F]{4}$/.test(hex)) {
          throw new Error("Redis query has an invalid Unicode escape.");
        }
        current += String.fromCharCode(Number.parseInt(hex, 16));
        index += 4;
      } else if (quote === '"') {
        switch (char) {
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
          case "f":
            current += "\f";
            break;
          default:
            current += char;
        }
      } else {
        current += char;
      }
      tokenStarted = true;
      escaping = false;
      continue;
    }

    if (char === "\\") {
      escaping = true;
      tokenStarted = true;
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

  if (escaping) {
    current += "\\";
    tokenStarted = true;
  }

  if (quote) {
    throw new Error("Redis query has an unterminated quoted argument.");
  }

  pushCurrent();
  return tokens;
}

function decideRedisReadOnlyQuery(queryText: string) {
  const trimmed = queryText.trim().replace(/;+$/, "");
  if (!trimmed) {
    return denyReadOnlyQuery(REDIS_READ_ONLY_QUERY_REASON);
  }

  try {
    const statements = splitRedisStatements(trimmed);
    if (statements.length === 0) {
      return denyReadOnlyQuery(REDIS_READ_ONLY_QUERY_REASON);
    }

    return statements.every((statement) => {
      const command = tokenizeRedisCommand(statement)[0]?.toUpperCase();
      return command ? READ_ONLY_REDIS_COMMANDS.has(command) : false;
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
  private epoch = 0;
  private connecting?: Promise<void>;
  private readonly connectionAttempts = new WeakMap<
    Promise<unknown>,
    RedisClientEntry
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
    let entry: RedisClientEntry;
    try {
      entry = this.openClient(this.resolveDbIndex());
    } catch (error) {
      return Promise.reject(error);
    }
    const attempt = entry.promise
      .then((client) => {
        if (epoch !== this.epoch || entry.cancelled) {
          this.cancelClientEntry(entry);
          throw new Error("Redis connection attempt was cancelled.");
        }
        this.client = client;
        this.connected = true;
      })
      .finally(() => {
        if (this.connecting === attempt) this.connecting = undefined;
      });
    this.connecting = attempt;
    this.connectionAttempts.set(attempt, entry);
    return attempt;
  }

  cancelConnectionAttempt(attempt: Promise<unknown>): void {
    const entry = this.connectionAttempts.get(attempt);
    if (!entry) return;
    this.cancelClientEntry(entry);
    if (this.connecting === attempt) this.connecting = undefined;
    if (this.client === entry.client) {
      this.client = null;
      this.connected = false;
    }
  }

  private createDatabaseClient(
    database: number,
    signal: AbortSignal,
  ): RedisClient {
    const forwardedTransport = getSshTcpForwardTransport(this.config);
    const tlsSettings = resolveConnectionTlsSettings(this.config);
    // node-redis gives URI fields priority over explicit options. Rewrite both
    // the DB and forwarded endpoint, retaining URI authentication and TLS.
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
        };
    // destroy() cannot reach a TCP/TLS socket still inside createSocket().
    // Node's TLS transport supports signal too (its declaration omits it).
    // Stop reconnecting on abort, retaining node-redis's default retry backoff.
    const socketOptions = {
      ...socket,
      signal,
      reconnectStrategy: (retries: number) =>
        signal.aborted
          ? false
          : Math.min(2 ** retries * 50, 2000) + Math.floor(Math.random() * 200),
    };
    const client: RedisClient = createClient({
      url: uri?.toString(),
      socket: socketOptions,
      database,
      username: this.config.username,
      password: this.config.password,
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
    client.on("end", () => this.releaseClientEntry(entry));
    client.on("error", () => {
      // node-redis changes isOpen after emitting an error during reconnect.
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
    this.client = null;
    this.connected = false;
    this.connecting = undefined;
    this.databaseClients.clear();
    for (const abort of this.queries.values()) abort.abort();
    this.queries.clear();
    const entries = [...this.clients];
    const errors: unknown[] = [];
    // close() waits for outstanding replies indefinitely. Force physical
    // transport teardown and keep opening/failed entries tracked until retired.
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
    return this.connected;
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
      const info = await this.requireClient().info("keyspace");
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

  async listObjects(database?: string, _schema?: string): Promise<TableInfo[]> {
    const client = await this.getDatabaseClient(database);
    const keys = await this.scanKeys(
      client,
      "*",
      REDIS_READ_BUDGET.maxScanKeys,
    );
    const names = new Set<string>();
    for (const key of keys) {
      const firstPrefix = key.includes(":") ? key.split(":")[0] : null;
      const prefix = firstPrefix === "default" ? "default:" : firstPrefix;
      names.add(prefix || "default");
    }
    if (names.size === 0) {
      names.add("default");
    }
    return [...names].sort().map((name) => ({
      schema: "",
      name,
      type: "table",
    }));
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
    return this.executeQuery(sql, options);
  }

  private async executeQuery(
    sql: string,
    options?: QueryExecutionOptions,
    signal?: AbortSignal,
  ): Promise<QueryResult> {
    const trimmed = sql.trim().replace(/;+$/, "");
    if (!trimmed) {
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      };
    }

    const startedAt = Date.now();
    const statements = splitRedisStatements(trimmed);
    const results: unknown[] = [];
    const abort = new AbortController();
    const querySignal = signal
      ? AbortSignal.any([signal, abort.signal])
      : abort.signal;
    const epoch = this.epoch;
    if (options?.requestToken !== undefined)
      this.queries.set(options.requestToken, abort);
    let entry: RedisClientEntry | undefined;
    let onAbort: (() => void) | undefined;
    try {
      // Explicit query scopes use a disposable session: SELECT in query text must
      // never change either the editor's base session or cached native DB clients.
      querySignal.throwIfAborted();
      this.requireClient();
      if (options?.database) {
        entry = this.openClient(this.resolveDbIndex(options.database));
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
        options?.requestToken !== undefined || signal
          ? baseClient.withAbortSignal(querySignal)
          : baseClient;
      for (const statement of statements) {
        querySignal.throwIfAborted();
        if (epoch !== this.epoch)
          throw new Error("Redis query was cancelled by disconnect.");
        const parts = tokenizeRedisCommand(statement);
        const [command, ...args] = parts;
        // node-redis removes the command's abort listener when it is written.
        // Race sent commands explicitly; only owned sessions are destroyed.
        results.push(
          await this.waitForResult(
            client.sendCommand([command.toUpperCase(), ...args]),
            querySignal,
          ),
        );
      }
      const row = flattenRootRecord(
        statements.length === 1 ? { result: results[0] } : { results },
      );
      const columns = Object.keys(row);
      return {
        columns,
        rows: [this.mapRowToQueryRow(row, columns)],
        rowCount: 1,
        executionTimeMs: Date.now() - startedAt,
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
    const client = await this.getDatabaseClient(request.database);
    if (this.canUseKeyOnlyPaging(request)) {
      const pattern = keyspacePattern(request.table);
      const offset = Math.max(0, (request.page - 1) * request.pageSize);
      const keys = (
        await this.scanKeys(
          client,
          pattern,
          REDIS_READ_BUDGET.maxScanKeys,
          true,
        )
      ).sort((left, right) => left.localeCompare(right));
      if (request.sort?.direction === "desc") keys.reverse();
      const pageKeys = keys.slice(offset, offset + request.pageSize);
      const rows = await this.readRowsForKeys(client, pageKeys);
      return {
        columns: this.inferRedisColumns(rows),
        rows: rows.map((entry) => entry.row),
        totalCount: request.skipCount ? 0 : keys.length,
        executionTimeMs: Math.round(performance.now() - startTime),
      };
    }

    const fallbackReadLimit = Math.max(
      request.page * request.pageSize * 2,
      request.pageSize * 10,
    );
    const boundedReadLimit = Math.min(
      REDIS_READ_BUDGET.maxValueReads,
      fallbackReadLimit,
    );
    const rows = await this.readRows(
      client,
      request.table,
      boundedReadLimit,
      true,
    );
    const rowRecords = rows.map((entry) => entry.row);
    const columns = this.inferRedisColumns(rows);
    const filtered = applyFilters(rowRecords, request.filters, columns);
    const sorted = applySort(filtered, request.sort, columns);
    const paged = pageRows(sorted, request.page, request.pageSize);
    return {
      columns,
      rows: paged,
      totalCount: request.skipCount ? 0 : sorted.length,
      executionTimeMs: Math.round(performance.now() - startTime),
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
    for (const update of request.updates) {
      context?.signal.throwIfAborted();
      const sourceKey = this.resolveStoredKey(update.primaryKeys.key);
      if (!sourceKey) {
        continue;
      }

      const hasKeyChange = Object.hasOwn(update.changes, "key");
      const targetKey = hasKeyChange
        ? this.resolveStoredKey(update.changes.key)
        : sourceKey;
      if (!targetKey) {
        throw new Error("Redis key updates require a non-empty 'key' value.");
      }

      const keyChanged = sourceKey !== targetKey;

      const hasValueChange =
        Object.hasOwn(update.changes, "value") ||
        Object.hasOwn(update.changes, "json") ||
        Object.hasOwn(update.changes, "text");
      const hasTtlChange = Object.hasOwn(update.changes, "ttl");
      if (!keyChanged && !hasValueChange && !hasTtlChange) {
        continue;
      }

      const currentType = await client.type(sourceKey);
      if (currentType === "stream" && hasValueChange) {
        throw new Error(
          "Redis stream values are read-only in the table viewer.",
        );
      }
      const value =
        update.changes.value ?? update.changes.json ?? update.changes.text;
      const encodedValue = hasValueChange
        ? this.encodeRedisLuaValue(currentType, value)
        : "null";
      const ttlSeconds = hasTtlChange
        ? this.parseRedisTtlInput(update.changes.ttl, "Redis TTL updates")
        : undefined;
      const hasOriginalValue = Object.hasOwn(
        update.originalValues ?? {},
        "value",
      );
      const originalValue = hasOriginalValue
        ? currentType === "string"
          ? this.normalizeStoredValue(update.originalValues?.value)
          : this.encodeRedisLuaValue(currentType, update.originalValues?.value)
        : "";
      const result = Number(
        await client.sendCommand([
          "EVAL",
          REDIS_ATOMIC_EDIT_SCRIPT,
          "2",
          sourceKey,
          targetKey,
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
        throw new Error(`Redis key '${targetKey}' already exists.`);
      }
      if (result === -2) {
        throw new Error("Redis key type changed after the row was loaded.");
      }
      if (result === -3) {
        throw new Error("Redis value changed after the row was loaded.");
      }
      affectedRows += result === 1 ? 1 : 0;
    }
    return { affectedRows };
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
    const result = await client[plan.command](...plan.args, plan.options);

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
        if (!key) throw new Error("Redis delete requires a non-empty key.");
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
      const sourceKey = this.resolveStoredKey(data.primaryKeys?.key);
      if (!sourceKey) {
        return "GET <key>";
      }

      const hasKeyChange = Object.hasOwn(data.changes ?? {}, "key");
      const targetKey = hasKeyChange
        ? this.resolveStoredKey(data.changes?.key)
        : sourceKey;
      if (!targetKey) {
        throw new Error("Redis key updates require a non-empty 'key' value.");
      }
      const keyChanged = sourceKey !== targetKey;

      const ttlSeconds = Object.hasOwn(data.changes ?? {}, "ttl")
        ? this.parseRedisTtlInput(data.changes?.ttl, "Redis TTL updates")
        : undefined;
      const newValue =
        data.changes?.value ?? data.changes?.json ?? data.changes?.text;
      if (newValue !== undefined) {
        const setPreview = formatRedisPreviewCommand("SET", [
          targetKey,
          this.normalizeStoredValue(newValue),
        ]);
        const commands: string[] = [];
        if (keyChanged) {
          commands.push(
            formatRedisPreviewCommand("RENAME", [sourceKey, targetKey]),
          );
        }
        commands.push(setPreview);
        if (ttlSeconds !== undefined) {
          commands.push(
            this.buildRedisTtlPreviewStatement(targetKey, ttlSeconds),
          );
        }
        return commands.join("; ");
      }

      const commands: string[] = [];
      if (keyChanged) {
        commands.push(
          formatRedisPreviewCommand("RENAME", [sourceKey, targetKey]),
        );
      }
      if (ttlSeconds !== undefined) {
        commands.push(
          this.buildRedisTtlPreviewStatement(targetKey, ttlSeconds),
        );
      }

      return commands.length > 0
        ? commands.join("; ")
        : formatRedisPreviewCommand("GET", [sourceKey]);
    }
    // delete
    const keys = (
      data.primaryKeyValuesList ?? (data.primaryKeys ? [data.primaryKeys] : [])
    )
      .map((entry) => entry.key)
      .filter((k) => k !== undefined)
      .map((key) => String(key));
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
        .map((key) => String(key));
      return keys.length > 0
        ? [formatRedisPreviewCommand("DEL", keys)]
        : ["DEL <key>"];
    }

    if (operation === "insert") {
      return [
        this.buildRedisInsertPreview(this.buildRedisInsertPlan(data.values)),
      ];
    }

    const key = data.primaryKeys?.key;
    if (key === undefined) {
      return ["GET <key>"];
    }
    const sourceKey = this.resolveStoredKey(key);
    if (!sourceKey) {
      return ["GET <key>"];
    }
    const hasKeyChange = Object.hasOwn(data.changes ?? {}, "key");
    const targetKey = hasKeyChange
      ? this.resolveStoredKey(data.changes?.key)
      : sourceKey;
    if (!targetKey) {
      throw new Error("Redis key updates require a non-empty 'key' value.");
    }
    const keyChanged = sourceKey !== targetKey;

    const value =
      data.changes?.value ?? data.changes?.json ?? data.changes?.text;
    const ttlSeconds = Object.hasOwn(data.changes ?? {}, "ttl")
      ? this.parseRedisTtlInput(data.changes?.ttl, "Redis TTL updates")
      : undefined;
    const statements: string[] = [];
    if (keyChanged) {
      statements.push(
        formatRedisPreviewCommand("RENAME", [sourceKey, targetKey]),
      );
    }

    if (value === undefined) {
      if (ttlSeconds !== undefined) {
        statements.push(
          this.buildRedisTtlPreviewStatement(targetKey, ttlSeconds),
        );
        return statements;
      }
      return statements.length > 0
        ? statements
        : [formatRedisPreviewCommand("GET", [sourceKey])];
    }
    const redisType = await (await this.getDatabaseClient(database)).type(
      sourceKey,
    );
    statements.push(
      ...this.buildRedisPreviewStatementsForType(targetKey, redisType, value),
    );
    if (ttlSeconds !== undefined) {
      statements.push(
        this.buildRedisTtlPreviewStatement(targetKey, ttlSeconds),
      );
    }
    return statements;
  }

  async runTransaction(
    operations: TransactionOperation[],
    context?: TransactionContext,
    scope?: DatabaseExecutionScope,
  ): Promise<void> {
    // No MULTI/EXEC wrapping here: sequential queries are not atomic.
    // Fail closed on multi-op instead of risking a partial apply.
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
    // Persisted bytes may equal the UI NULL marker; keep them for the
    // atomic edit's original-value comparison.
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

  private async getDatabaseClient(
    database?: string,
    signal?: AbortSignal,
  ): Promise<RedisClient> {
    this.requireClient();
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
    failOnTruncation = false,
  ): Promise<RedisSampleRow[]> {
    const pattern = keyspacePattern(table);
    const readLimit = Math.min(maxRows, REDIS_READ_BUDGET.maxValueReads);
    const keys = (
      await this.scanKeys(client, pattern, readLimit, failOnTruncation)
    ).sort((left, right) => left.localeCompare(right));
    return await this.readRowsForKeys(client, keys);
  }

  private async readRowsForKeys(
    client: RedisClient,
    keys: readonly string[],
  ): Promise<RedisSampleRow[]> {
    return pMapWithLimit(
      [...keys],
      REDIS_READ_BUDGET.parallelValueReads,
      async (key) => {
        const type = await client.type(key);
        let value: unknown = null;
        switch (type) {
          case "string":
            value = await client.get(key);
            break;
          case "hash":
            value = await client.hGetAll(key);
            break;
          case "list":
            value = await client.lRange(key, 0, -1);
            break;
          case "set":
            value = await client.sMembers(key);
            break;
          case "zset":
            value = await client.zRangeWithScores(key, 0, -1);
            break;
          case "stream":
            value = await this.readStreamEntries(client, key);
            break;
          default:
            value = null;
        }

        const ttl =
          typeof client.ttl === "function" ? await client.ttl(key) : -1;

        return {
          redisType: type,
          row: flattenRootRecord({
            key,
            value,
            ttl: ttl >= 0 ? ttl : null,
          }),
        };
      },
    );
  }

  private inferRedisColumns(rows: readonly RedisSampleRow[]): ColumnTypeMeta[] {
    const columns = inferColumnsFromRows(
      rows.map((entry) => entry.row),
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
      if (value === -1) {
        return null;
      }
      if (value >= 1) {
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
        return parsed;
      }
      throw new Error(`${source} require a positive TTL or -1 to persist.`);
    }

    throw new Error(`${source} require an integer TTL in seconds.`);
  }

  private buildRedisTtlPreviewStatement(
    key: string,
    ttlSeconds: number | null,
  ): string {
    return ttlSeconds === null
      ? formatRedisPreviewCommand("PERSIST", [key])
      : formatRedisPreviewCommand("EXPIRE", [key, ttlSeconds]);
  }

  private buildRedisInsertPlan(
    values: Record<string, unknown> = {},
  ): RedisInsertPlan {
    const key = this.resolveStoredKey(values.key);
    if (!key) {
      throw new Error("Redis insert requires a 'key' field.");
    }
    const ttlSeconds = Object.hasOwn(values, "ttl")
      ? this.parseRedisTtlInput(values.ttl, "Redis TTL inserts")
      : undefined;
    return {
      command: "set",
      args: [
        key,
        this.normalizeStoredValue(values.value ?? values.json ?? values.text),
      ],
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

  private encodeRedisLuaValue(redisType: string, value: unknown): string {
    switch (redisType) {
      case "hash":
        return JSON.stringify(this.parseRedisHashEntries(value));
      case "list":
        return JSON.stringify(this.parseRedisSequenceElements(value, "list"));
      case "set":
        return JSON.stringify(this.parseRedisSequenceElements(value, "set"));
      case "zset":
        return JSON.stringify(this.parseRedisSortedSetEntries(value));
      default:
        return JSON.stringify(this.normalizeStoredValue(value));
    }
  }

  private buildRedisPreviewStatementsForType(
    key: string,
    redisType: string,
    value: unknown,
  ): string[] {
    switch (redisType) {
      case "hash": {
        const entries = this.parseRedisHashEntries(value);
        const preview = [formatRedisPreviewCommand("DEL", [key])];
        if (Object.keys(entries).length > 0) {
          preview.push(
            formatRedisPreviewCommand("HSET", [
              key,
              ...Object.entries(entries).flatMap(([field, fieldValue]) => [
                field,
                fieldValue,
              ]),
            ]),
          );
        }
        return preview;
      }
      case "list": {
        const elements = this.parseRedisSequenceElements(value, "list");
        const preview = [formatRedisPreviewCommand("DEL", [key])];
        if (elements.length > 0) {
          preview.push(formatRedisPreviewCommand("RPUSH", [key, ...elements]));
        }
        return preview;
      }
      case "set": {
        const elements = this.parseRedisSequenceElements(value, "set");
        const preview = [formatRedisPreviewCommand("DEL", [key])];
        if (elements.length > 0) {
          preview.push(formatRedisPreviewCommand("SADD", [key, ...elements]));
        }
        return preview;
      }
      case "zset": {
        const entries = this.parseRedisSortedSetEntries(value);
        const preview = [formatRedisPreviewCommand("DEL", [key])];
        if (entries.length > 0) {
          preview.push(
            formatRedisPreviewCommand("ZADD", [
              key,
              ...entries.flatMap((entry) => [entry.score, entry.value]),
            ]),
          );
        }
        return preview;
      }
      case "stream":
        throw new Error(
          "Redis stream values are read-only in the table viewer.",
        );
      default:
        return [
          formatRedisPreviewCommand("SET", [
            key,
            this.normalizeStoredValue(value),
          ]),
        ];
    }
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
            ? Number(rawScore)
            : Number.NaN;
      if (!Number.isFinite(score)) {
        throw new Error("Redis sorted set scores must be finite numbers.");
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

  private parseRedisJsonValue(value: unknown, label: string): unknown {
    if (typeof value !== "string") {
      return value;
    }
    try {
      return JSON.parse(value);
    } catch {
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

  private async readStreamEntries(
    client: RedisClient,
    key: string,
  ): Promise<unknown> {
    if (typeof client.xRange === "function") {
      return client.xRange(key, "-", "+");
    }
    return client.sendCommand(["XRANGE", key, "-", "+"]);
  }

  private resolveStoredKey(rawKey: unknown): string {
    if (typeof rawKey !== "string" || rawKey.length === 0) {
      return "";
    }
    return rawKey;
  }

  private async scanKeys(
    client: RedisClient,
    pattern: string,
    limit = Number.POSITIVE_INFINITY,
    failOnTruncation = false,
  ): Promise<string[]> {
    const keys: string[] = [];
    let cursor = "0";
    do {
      const count = Number.isFinite(limit)
        ? Math.min(500, Math.max(1, limit - keys.length))
        : 500;
      const response = await client.scan(cursor, {
        MATCH: pattern,
        COUNT: count,
      });
      cursor = response.cursor;
      keys.push(...response.keys);
    } while (cursor !== "0" && keys.length < limit);
    if (failOnTruncation && (cursor !== "0" || keys.length > limit)) {
      throw new Error(
        `Redis data exceeds the ${limit}-key safety limit. Narrow the filter before continuing or exporting.`,
      );
    }
    return Number.isFinite(limit) ? keys.slice(0, limit) : keys;
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
