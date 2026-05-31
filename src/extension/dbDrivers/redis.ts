import { createClient } from "redis";
import { REDIS_READ_BUDGET } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import {
  getSshTcpForwardTransport,
  getTlsServername,
} from "../driverRuntimeConfig";
import { pMapWithLimit } from "../utils/concurrency";
import { allowReadOnlyQuery, denyReadOnlyQuery } from "../utils/readOnlyGuards";
import {
  applyFilters,
  applySort,
  flattenRootRecord,
  inferColumnsFromRows,
  pageRows,
  stringifyCommandPayload,
  unsupported,
} from "./nosqlUtils";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverDeleteRowsRequest,
  DriverEntityManifest,
  DriverInsertRowRequest,
  DriverMutationResult,
  DriverTablePageRequest,
  DriverTablePageResult,
  DriverUpdateRowsRequest,
  ForeignKeyMeta,
  IDBDriver,
  IndexMeta,
  PaginationResult,
  QueryResult,
  SchemaInfo,
  TableConstraintMeta,
  TableInfo,
  TransactionOperation,
  TriggerMeta,
} from "./types";
import { resolveFilterOperators } from "./types";

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

type RedisSortedSetEntry = {
  score: number;
  value: string;
};

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

  for (const char of input) {
    if (escaping) {
      current += char;
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
  private client: ReturnType<typeof createClient> | null = null;
  private connected = false;

  constructor(private readonly config: ConnectionConfig) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const forwardedTransport = getSshTcpForwardTransport(this.config);
    const socket = this.config.ssl
      ? {
          host:
            (forwardedTransport?.localHost ?? this.config.host) || "127.0.0.1",
          port: forwardedTransport?.localPort ?? this.config.port ?? 6379,
          tls: true as const,
          rejectUnauthorized: this.config.rejectUnauthorized,
          servername:
            this.config.rejectUnauthorized !== false
              ? getTlsServername(this.config)
              : undefined,
        }
      : {
          host:
            (forwardedTransport?.localHost ?? this.config.host) || "127.0.0.1",
          port: forwardedTransport?.localPort ?? this.config.port ?? 6379,
        };
    const client: ReturnType<typeof createClient> = createClient({
      url: this.config.connectionUri,
      socket,
      username: this.config.username,
      password: this.config.password,
    });
    client.on("error", (error) => {
      console.error(
        "[RapiDB] Redis client error:",
        error instanceof Error ? error.message : error,
      );
    });
    try {
      await client.connect();
      const dbIndex = this.resolveDbIndex();
      if (dbIndex > 0) {
        await client.select(dbIndex);
      }
    } catch (error) {
      if (client.isOpen) {
        client.destroy();
      }
      throw error;
    }
    this.client = client;
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    const client = this.client;
    this.client = null;
    this.connected = false;
    if (client?.isOpen) {
      await client.close();
    }
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

  async listObjects(): Promise<TableInfo[]> {
    try {
      const keys = await this.scanKeys("*", REDIS_READ_BUDGET.maxScanKeys);
      const names = new Set<string>();
      for (const key of keys) {
        const prefix = key.includes(":") ? key.split(":")[0] : "default";
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
    } catch {
      return [];
    }
  }

  async describeTable(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const rows = await this.readRows(table, 200);
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
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const rows = await this.readRows(table, 200);
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

  async query(sql: string, _params?: unknown[]): Promise<QueryResult> {
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
    for (const statement of statements) {
      const parts = tokenizeRedisCommand(statement);
      const [command, ...args] = parts;
      results.push(
        await this.requireClient().sendCommand([
          command.toUpperCase(),
          ...args,
        ]),
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
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    if (this.canUseKeyOnlyPaging(request)) {
      const pattern = request.table === "default" ? "*" : `${request.table}:*`;
      const offset = Math.max(0, (request.page - 1) * request.pageSize);
      const scanLimit = request.skipCount
        ? Math.min(REDIS_READ_BUDGET.maxScanKeys, offset + request.pageSize)
        : REDIS_READ_BUDGET.maxScanKeys;
      const keys = (await this.scanKeys(pattern, scanLimit)).sort(
        (left, right) => left.localeCompare(right),
      );
      const pageKeys = keys.slice(offset, offset + request.pageSize);
      const rows = await this.readRowsForKeys(pageKeys);
      return {
        columns: this.inferRedisColumns(rows),
        rows: rows.map((entry) => entry.row),
        totalCount: request.skipCount ? 0 : keys.length,
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
    const rows = await this.readRows(request.table, boundedReadLimit);
    const rowRecords = rows.map((entry) => entry.row);
    const filtered = applyFilters(rowRecords, request.filters);
    const sorted = applySort(filtered, request.sort);
    const paged = pageRows(sorted, request.page, request.pageSize);
    const redisTypeByKey = new Map(
      rows.map((entry) => [String(entry.row.key ?? ""), entry.redisType]),
    );
    return {
      columns: this.inferRedisColumns(
        sorted.map((row) => ({
          redisType: redisTypeByKey.get(String(row.key ?? "")) ?? "none",
          row,
        })),
      ),
      rows: paged,
      totalCount: request.skipCount ? 0 : sorted.length,
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
  ): Promise<DriverMutationResult> {
    const client = this.requireClient();
    let affectedRows = 0;
    for (const update of request.updates) {
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

      if ((await client.exists(sourceKey)) === 0) {
        continue;
      }

      const keyChanged = sourceKey !== targetKey;
      if (keyChanged && (await client.exists(targetKey)) !== 0) {
        throw new Error(`Redis key '${targetKey}' already exists.`);
      }

      const hasValueChange =
        Object.hasOwn(update.changes, "value") ||
        Object.hasOwn(update.changes, "json") ||
        Object.hasOwn(update.changes, "text");
      const hasTtlChange = Object.hasOwn(update.changes, "ttl");
      if (!keyChanged && !hasValueChange && !hasTtlChange) {
        continue;
      }

      if (keyChanged) {
        await this.renameRedisKey(sourceKey, targetKey);
      }

      if (hasValueChange) {
        const value =
          update.changes.value ?? update.changes.json ?? update.changes.text;
        const currentType = await client.type(targetKey);
        await this.writeRedisValueByType(targetKey, currentType, value);
      }

      if (hasTtlChange) {
        const ttlSeconds = this.parseRedisTtlInput(
          update.changes.ttl,
          "Redis TTL updates",
        );
        await this.applyRedisTtl(targetKey, ttlSeconds);
      }

      affectedRows += 1;
    }
    return { affectedRows };
  }

  async insertRow(
    request: DriverInsertRowRequest,
  ): Promise<DriverMutationResult> {
    const key = this.resolveStoredKey(request.values.key);
    if (!key) {
      throw new Error("Redis insert requires a 'key' field.");
    }
    const hasTtl = Object.hasOwn(request.values, "ttl");
    const ttlSeconds = hasTtl
      ? this.parseRedisTtlInput(request.values.ttl, "Redis TTL inserts")
      : undefined;
    const value =
      request.values.value ?? request.values.json ?? request.values.text;
    const result = await this.requireClient().set(
      key,
      this.normalizeStoredValue(value),
      ttlSeconds !== undefined && ttlSeconds !== null
        ? { NX: true, EX: ttlSeconds }
        : { NX: true },
    );

    if (result === "OK" && ttlSeconds === null) {
      await this.applyRedisTtl(key, null);
    }

    return { affectedRows: result === "OK" ? 1 : 0 };
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
  ): Promise<DriverMutationResult> {
    const client = this.requireClient();
    let affectedRows = 0;
    for (const entry of request.primaryKeyValuesList) {
      const key = this.resolveStoredKey(entry.key);
      if (!key) {
        continue;
      }
      const deleted = await client.del(key);
      affectedRows += deleted;
    }
    return { affectedRows };
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
      const key = data.values?.key ?? "<key>";
      const ttlSeconds = Object.hasOwn(data.values ?? {}, "ttl")
        ? this.parseRedisTtlInput(data.values?.ttl, "Redis TTL inserts")
        : undefined;
      const value =
        data.values?.value ??
        data.values?.json ??
        data.values?.text ??
        JSON.stringify(data.values ?? {});
      const setArgs: Array<string | number> = [
        String(key),
        this.normalizeStoredValue(value),
      ];
      if (ttlSeconds !== undefined && ttlSeconds !== null) {
        setArgs.push("EX", ttlSeconds);
      }
      const setPreview = formatRedisPreviewCommand("SET", setArgs);
      if (ttlSeconds === null) {
        return `${setPreview}; ${formatRedisPreviewCommand("PERSIST", [String(key)])}`;
      }
      return setPreview;
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
    _database: string,
    _schema: string,
    table: string,
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
      const key = data.values?.key;
      if (key === undefined) {
        return ['SET "<key>" ""'];
      }
      const ttlSeconds = Object.hasOwn(data.values ?? {}, "ttl")
        ? this.parseRedisTtlInput(data.values?.ttl, "Redis TTL inserts")
        : undefined;
      const value =
        data.values?.value ?? data.values?.json ?? data.values?.text ?? "";
      const inferredType = await this.inferInsertRedisType(table);
      const statements = this.buildRedisPreviewStatementsForType(
        String(key),
        inferredType,
        value,
      );
      if (ttlSeconds !== undefined) {
        statements.push(
          this.buildRedisTtlPreviewStatement(String(key), ttlSeconds),
        );
      }
      return statements;
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
    const redisType = await this.requireClient().type(sourceKey);
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

  private async renameRedisKey(sourceKey: string, targetKey: string) {
    if (sourceKey === targetKey) {
      return;
    }
    const client = this.requireClient() as ReturnType<typeof createClient> & {
      rename?: (source: string, target: string) => Promise<unknown>;
      sendCommand: (args: string[]) => Promise<unknown>;
    };
    if (typeof client.rename === "function") {
      await client.rename(sourceKey, targetKey);
      return;
    }
    await client.sendCommand(["RENAME", sourceKey, targetKey]);
  }

  async runTransaction(operations: TransactionOperation[]): Promise<void> {
    for (const operation of operations) {
      await this.query(operation.sql, operation.params);
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

  private resolveDbIndex(): number {
    if (
      typeof this.config.database === "string" &&
      /^\d+$/.test(this.config.database)
    ) {
      return Number(this.config.database);
    }
    return 0;
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
    table: string,
    maxRows: number,
  ): Promise<RedisSampleRow[]> {
    try {
      const pattern = table === "default" ? "*" : `${table}:*`;
      const readLimit = Math.min(maxRows, REDIS_READ_BUDGET.maxValueReads);
      const keys = (await this.scanKeys(pattern, readLimit))
        .slice(0, readLimit)
        .sort((left, right) => left.localeCompare(right));
      return await this.readRowsForKeys(keys);
    } catch {
      return [];
    }
  }

  private async readRowsForKeys(
    keys: readonly string[],
  ): Promise<RedisSampleRow[]> {
    return pMapWithLimit(
      [...keys],
      REDIS_READ_BUDGET.parallelValueReads,
      async (key) => {
        const client = this.requireClient() as ReturnType<
          typeof createClient
        > & {
          ttl?: (key: string) => Promise<number>;
        };
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
            value = await this.readStreamEntries(key);
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
      },
    );
    const valueTypeLabel = formatRedisValueTypeLabel(rows);
    return columns.map((column) =>
      column.name === "key"
        ? {
            ...column,
            type: "string",
            nativeType: "string",
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

  private async applyRedisTtl(key: string, ttlSeconds: number | null) {
    const client = this.requireClient() as ReturnType<typeof createClient> & {
      expire?: (key: string, seconds: number) => Promise<number>;
      persist?: (key: string) => Promise<number>;
    };

    if (ttlSeconds === null) {
      if (typeof client.persist !== "function") {
        throw new Error("Redis client does not support PERSIST.");
      }
      await client.persist(key);
      return;
    }

    if (typeof client.expire !== "function") {
      throw new Error("Redis client does not support EXPIRE.");
    }
    await client.expire(key, ttlSeconds);
  }

  private buildRedisTtlPreviewStatement(
    key: string,
    ttlSeconds: number | null,
  ): string {
    return ttlSeconds === null
      ? formatRedisPreviewCommand("PERSIST", [key])
      : formatRedisPreviewCommand("EXPIRE", [key, ttlSeconds]);
  }

  private async inferInsertRedisType(table: string): Promise<string> {
    const pattern = table === "default" ? "*" : `${table}:*`;
    const keys = (await this.scanKeys(pattern, 25)).slice(0, 25);
    const valueTypes = [
      ...new Set(
        await Promise.all(keys.map((key) => this.requireClient().type(key))),
      ),
    ].filter((type) => type !== "none");
    if (valueTypes.length !== 1) {
      return "string";
    }
    return valueTypes[0] ?? "string";
  }

  private async writeRedisValueByType(
    key: string,
    redisType: string,
    value: unknown,
  ): Promise<void> {
    const client = this.requireClient();
    switch (redisType) {
      case "hash": {
        const entries = this.parseRedisHashEntries(value);
        await client.del(key);
        if (Object.keys(entries).length > 0) {
          await client.hSet(key, entries);
        }
        return;
      }
      case "list": {
        const elements = this.parseRedisSequenceElements(value, "list");
        await client.del(key);
        if (elements.length > 0) {
          await client.rPush(key, elements);
        }
        return;
      }
      case "set": {
        const elements = this.parseRedisSequenceElements(value, "set");
        await client.del(key);
        if (elements.length > 0) {
          await client.sAdd(key, elements);
        }
        return;
      }
      case "zset": {
        const entries = this.parseRedisSortedSetEntries(value);
        await client.del(key);
        if (entries.length > 0) {
          await client.zAdd(key, entries);
        }
        return;
      }
      case "stream":
        throw new Error(
          "Redis stream values are read-only in the table viewer.",
        );
      default:
        await client.set(key, this.normalizeStoredValue(value));
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

  private async readStreamEntries(key: string): Promise<unknown> {
    const client = this.requireClient() as ReturnType<typeof createClient> & {
      xRange?: (key: string, start: string, end: string) => Promise<unknown>;
      sendCommand: (args: string[]) => Promise<unknown>;
    };
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
    pattern: string,
    limit = Number.POSITIVE_INFINITY,
  ): Promise<string[]> {
    const client = this.requireClient();
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
    return keys;
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
