import { createClient } from "redis";
import type { ConnectionConfig } from "../connectionManager";
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

export class RedisDriver implements IDBDriver {
  private client: ReturnType<typeof createClient> | null = null;
  private connected = false;

  constructor(private readonly config: ConnectionConfig) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const socket = this.config.ssl
      ? {
          host: this.config.host || "127.0.0.1",
          port: this.config.port ?? 6379,
          tls: true as const,
          rejectUnauthorized: this.config.rejectUnauthorized,
        }
      : {
          host: this.config.host || "127.0.0.1",
          port: this.config.port ?? 6379,
        };
    const client = createClient({
      url: this.config.connectionUri,
      socket,
      username: this.config.redisUsername ?? this.config.username,
      password: this.config.password,
    });
    await client.connect();
    const dbIndex = this.resolveDbIndex();
    if (dbIndex > 0) {
      await client.select(dbIndex);
    }
    this.client = client;
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.quit();
    }
    this.client = null;
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  getCapabilities() {
    return {
      tabularRead: "nosql" as const,
      queryMode: "text" as const,
      supportsMutations: true,
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
    return [{ name: "default" }];
  }

  async listObjects(): Promise<TableInfo[]> {
    try {
      const keys = await this.scanKeys(this.prefixedPattern("*"));
      const names = new Set<string>();
      for (const key of keys) {
        const logicalKey = this.stripKeyPrefix(key);
        const prefix = logicalKey.includes(":")
          ? logicalKey.split(":")[0]
          : "default";
        names.add(prefix || "default");
      }
      if (names.size === 0) {
        names.add("default");
      }
      return [...names].sort().map((name) => ({
        schema: "default",
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
    return inferColumnsFromRows(rows, "key").map((column) => ({
      name: column.name,
      type: column.nativeType,
      nullable: column.name !== "key" && column.nullable,
      defaultValue: undefined,
      isPrimaryKey: column.name === "key",
      primaryKeyOrdinal: column.name === "key" ? 1 : undefined,
      isForeignKey: false,
    }));
  }

  async describeColumns(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const rows = await this.readRows(table, 200);
    return inferColumnsFromRows(rows, "key").map((column) => ({
      ...column,
      nullable: column.name !== "key" && column.nullable,
    }));
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

  async getRoutineDefinition(): Promise<string> {
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
    const parts = trimmed.split(/\s+/);
    const [command, ...args] = parts;
    const result = await this.requireClient().sendCommand([
      command.toUpperCase(),
      ...args,
    ]);
    const row = flattenRootRecord({ result });
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
    const rows = await this.readRows(request.table, 2000);
    const filtered = applyFilters(rows, request.filters);
    const sorted = applySort(filtered, request.sort);
    const paged = pageRows(sorted, request.page, request.pageSize);
    return {
      columns: inferColumnsFromRows(sorted, "key"),
      rows: paged,
      totalCount: request.skipCount ? 0 : sorted.length,
    };
  }

  async updateRows(
    request: DriverUpdateRowsRequest,
  ): Promise<DriverMutationResult> {
    const client = this.requireClient();
    let affectedRows = 0;
    for (const update of request.updates) {
      const key = this.resolveStoredKey(update.primaryKeys.key);
      if (!key) {
        continue;
      }
      if (
        Object.hasOwn(update.changes, "key") &&
        update.changes.key !== update.primaryKeys.key
      ) {
        throw new Error("Redis does not support updating the key field.");
      }
      if ((await client.exists(key)) === 0) {
        continue;
      }
      const value =
        update.changes.value ?? update.changes.json ?? update.changes.text;
      await client.set(key, this.normalizeStoredValue(value));
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
    const value =
      request.values.value ?? request.values.json ?? request.values.text;
    const result = await this.requireClient().set(
      key,
      this.normalizeStoredValue(value),
      { NX: true },
    );
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
      const value =
        data.values?.value ??
        data.values?.json ??
        data.values?.text ??
        JSON.stringify(data.values ?? {});
      return `SET ${String(key)} ${String(value)}`;
    }
    if (operation === "update") {
      const key = data.primaryKeys?.key;
      const newValue =
        data.changes?.value ?? data.changes?.json ?? data.changes?.text;
      if (key !== undefined && newValue !== undefined) {
        return `SET ${String(key)} ${String(newValue)}`;
      }
      return key !== undefined ? `GET ${String(key)}` : "GET <key>";
    }
    // delete
    const keys = (
      data.primaryKeyValuesList ?? (data.primaryKeys ? [data.primaryKeys] : [])
    )
      .map((entry) => entry.key)
      .filter((k) => k !== undefined)
      .join(" ");
    return keys ? `DEL ${keys}` : "DEL <key>";
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
    if (typeof this.config.redisDb === "number") {
      return this.config.redisDb;
    }
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
  ): Promise<Record<string, unknown>[]> {
    try {
      const pattern = table === "default" ? "*" : `${table}:*`;
      const keys = (
        await this.scanKeys(this.prefixedPattern(pattern), maxRows)
      ).slice(0, maxRows);
      const rows: Record<string, unknown>[] = [];
      for (const key of keys) {
        const type = await this.requireClient().type(key);
        let value: unknown = null;
        switch (type) {
          case "string":
            value = await this.requireClient().get(key);
            break;
          case "hash":
            value = await this.requireClient().hGetAll(key);
            break;
          case "list":
            value = await this.requireClient().lRange(key, 0, -1);
            break;
          case "set":
            value = await this.requireClient().sMembers(key);
            break;
          case "zset":
            value = await this.requireClient().zRangeWithScores(key, 0, -1);
            break;
          default:
            value = null;
        }
        rows.push(
          flattenRootRecord({
            key: this.stripKeyPrefix(key),
            type,
            value,
          }),
        );
      }
      return rows;
    } catch {
      return [];
    }
  }

  private resolveStoredKey(rawKey: unknown): string {
    if (typeof rawKey !== "string" || rawKey.length === 0) {
      return "";
    }
    return `${this.config.keyPrefix ?? ""}${rawKey}`;
  }

  private stripKeyPrefix(key: string): string {
    const prefix = this.config.keyPrefix ?? "";
    return prefix && key.startsWith(prefix) ? key.slice(prefix.length) : key;
  }

  private prefixedPattern(pattern: string): string {
    const prefix = this.config.keyPrefix ?? "";
    return `${prefix}${pattern}`;
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
        ? Math.min(500, Math.max(100, limit - keys.length))
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
