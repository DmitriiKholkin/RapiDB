import { MongoClient, ObjectId } from "mongodb";
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

export class MongoDBDriver implements IDBDriver {
  private client: MongoClient | null = null;
  private connected = false;

  constructor(private readonly config: ConnectionConfig) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const uri = this.config.connectionUri ?? this.config.uri ?? this.buildUri();
    this.client = new MongoClient(uri, {
      tls: this.config.ssl,
      tlsAllowInvalidCertificates:
        this.config.ssl && this.config.rejectUnauthorized === false,
      authSource: this.config.authDatabase ?? this.config.authSource,
      replicaSet: this.config.replicaSet,
      directConnection: this.config.directConnection,
    });
    await this.client.connect();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    await this.client?.close();
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
      const admin = this.requireClient().db().admin();
      const dbs = await admin.listDatabases();
      return dbs.databases.map((database) => ({
        name: database.name,
        schemas: [],
      }));
    } catch {
      return [{ name: this.defaultDatabaseName(), schemas: [] }];
    }
  }

  async listSchemas(database: string): Promise<SchemaInfo[]> {
    return [{ name: database || this.defaultDatabaseName() }];
  }

  async listObjects(database: string): Promise<TableInfo[]> {
    try {
      const db = this.requireDb(database);
      const collections = await db
        .listCollections({}, { nameOnly: true })
        .toArray();
      return collections.map((collection) => ({
        schema: database || this.defaultDatabaseName(),
        name: collection.name,
        type: "table",
      }));
    } catch {
      return [];
    }
  }

  async describeTable(
    database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const rows = await this.readRows(database, table, 50);
    return inferColumnsFromRows(rows, "_id").map((column) => ({
      name: column.name,
      type: column.nativeType,
      nullable: column.name !== "_id" && column.nullable,
      defaultValue: column.name === "_id" ? "ObjectId()" : undefined,
      isPrimaryKey: column.name === "_id",
      primaryKeyOrdinal: column.name === "_id" ? 1 : undefined,
      isForeignKey: false,
    }));
  }

  async describeColumns(
    database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const rows = await this.readRows(database, table, 50);
    return inferColumnsFromRows(rows, "_id").map((column) => ({
      ...column,
      nullable: column.name !== "_id" && column.nullable,
      defaultValue: column.name === "_id" ? "ObjectId()" : undefined,
    }));
  }

  async getIndexes(
    database: string,
    _schema: string,
    table: string,
  ): Promise<IndexMeta[]> {
    try {
      const indexes = await this.requireDb(database)
        .collection(table)
        .indexes();
      return indexes.map((index) => ({
        name: index.name ?? "index",
        columns: Object.keys(index.key),
        unique: Boolean(index.unique),
        primary: index.name === "_id_",
      }));
    } catch {
      return [];
    }
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
    unsupported("MongoDB constraints DDL");
  }

  async getIndexDDL(): Promise<string> {
    unsupported("MongoDB index DDL");
  }

  async getTriggerDDL(): Promise<string> {
    unsupported("MongoDB trigger DDL");
  }

  async getCreateTableDDL(): Promise<string> {
    unsupported("MongoDB create collection DDL");
  }

  async getObjectDefinition(): Promise<string | null> {
    return null;
  }

  async getRoutineDefinition(): Promise<string> {
    unsupported("MongoDB routine definition");
  }

  async query(sql: string, _params?: unknown[]): Promise<QueryResult> {
    const trimmed = sql.trim().replace(/;+$/, "");
    if (trimmed.length === 0) {
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      };
    }

    const startedAt = Date.now();
    if (trimmed.startsWith("find ")) {
      const payload = JSON.parse(trimmed.slice("find ".length)) as {
        database?: string;
        collection: string;
        filter?: Record<string, unknown>;
        limit?: number;
      };
      const docs = await this.requireDb(payload.database)
        .collection(payload.collection)
        .find(this.normalizeFilterCriteria(payload.filter ?? {}))
        .limit(payload.limit ?? 100)
        .toArray();
      const rows = docs.map((doc) =>
        this.toRow(doc as Record<string, unknown>),
      );
      const columns = inferColumnsFromRows(rows, "_id").map(
        (column) => column.name,
      );
      return {
        columns,
        rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
        rowCount: rows.length,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (trimmed.startsWith("update ")) {
      const payload = JSON.parse(trimmed.slice("update ".length)) as {
        database?: string;
        collection: string;
        filter: Record<string, unknown>;
        set: Record<string, unknown>;
      };
      const result = await this.requireDb(payload.database)
        .collection(payload.collection)
        .updateMany(this.normalizeFilterCriteria(payload.filter), {
          $set: payload.set,
        });
      const row = {
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
      };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: result.modifiedCount,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (trimmed.startsWith("delete ")) {
      const payload = JSON.parse(trimmed.slice("delete ".length)) as {
        database?: string;
        collection: string;
        filter: Record<string, unknown>;
      };
      const result = await this.requireDb(payload.database)
        .collection(payload.collection)
        .deleteMany(this.normalizeFilterCriteria(payload.filter));
      const row = { deletedCount: result.deletedCount };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: result.deletedCount,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (trimmed.startsWith("command ")) {
      const payload = JSON.parse(trimmed.slice("command ".length)) as {
        database?: string;
        command: Record<string, unknown>;
      };
      const result = await this.requireDb(payload.database).command(
        payload.command,
      );
      const row = flattenRootRecord(result as Record<string, unknown>);
      const columns = Object.keys(row);
      return {
        columns,
        rows: [this.mapRowToQueryRow(row, columns)],
        rowCount: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    throw new Error(
      'MongoDB query mode expects commands like:\n  find {"collection":"users","filter":{}}\n  update {"collection":"users","filter":{"_id":"..."},"set":{"field":"value"}}\n  delete {"collection":"users","filter":{"_id":"..."}}\n  command {"command":{"ping":1}}',
    );
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    const rows = await this.readRows(request.database, request.table, 2000);
    const filtered = applyFilters(rows, request.filters);
    const sorted = applySort(filtered, request.sort);
    const paged = pageRows(sorted, request.page, request.pageSize);
    return {
      columns: inferColumnsFromRows(sorted, "_id"),
      rows: paged,
      totalCount: request.skipCount ? 0 : sorted.length,
    };
  }

  async updateRows(
    request: DriverUpdateRowsRequest,
  ): Promise<DriverMutationResult> {
    const collection = this.requireDb(request.database).collection(
      request.table,
    );
    let affectedRows = 0;
    for (const update of request.updates) {
      if (
        Object.hasOwn(update.changes, "_id") &&
        update.changes._id !== update.primaryKeys._id
      ) {
        throw new Error("MongoDB does not support updating the _id field.");
      }
      const criteria = this.normalizeCriteria(update.primaryKeys);
      const result = await collection.updateOne(criteria, {
        $set: update.changes,
      });
      affectedRows += result.matchedCount;
    }
    return { affectedRows };
  }

  async insertRow(
    request: DriverInsertRowRequest,
  ): Promise<DriverMutationResult> {
    const result = await this.requireDb(request.database)
      .collection(request.table)
      .insertOne(request.values);
    return { affectedRows: result.acknowledged ? 1 : 0 };
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
  ): Promise<DriverMutationResult> {
    const collection = this.requireDb(request.database).collection(
      request.table,
    );
    const criteria = request.primaryKeyValuesList.map((entry) =>
      this.normalizeCriteria(entry),
    );
    if (criteria.length === 0) {
      return { affectedRows: 0 };
    }
    const result = await collection.deleteMany({ $or: criteria });
    return { affectedRows: result.deletedCount };
  }

  buildMutationPreviewStatement(
    operation: "insert" | "update" | "delete",
    database: string,
    _schema: string,
    table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
  ): string {
    if (operation === "insert") {
      return stringifyCommandPayload("command", {
        database,
        command: { insert: table, documents: [data.values ?? {}] },
      });
    }
    if (operation === "update") {
      return stringifyCommandPayload("update", {
        collection: table,
        database,
        filter: data.primaryKeys ?? {},
        set: data.changes ?? {},
      });
    }
    // delete
    const filter = data.primaryKeyValuesList?.length
      ? data.primaryKeyValuesList.length === 1
        ? data.primaryKeyValuesList[0]
        : { $or: data.primaryKeyValuesList }
      : (data.primaryKeys ?? {});
    return stringifyCommandPayload("delete", {
      collection: table,
      database,
      filter,
    });
  }

  async runTransaction(operations: TransactionOperation[]): Promise<void> {
    for (const operation of operations) {
      await this.query(operation.sql, operation.params);
    }
  }

  quoteIdentifier(name: string): string {
    return name;
  }

  qualifiedTableName(database: string, _schema: string, table: string): string {
    const db = database || this.defaultDatabaseName();
    return `${db}.${table}`;
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
    return "ORDER BY _id";
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
    return stringifyCommandPayload("insert", { table: qualifiedTableName });
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

  private buildUri(): string {
    const host = this.config.host?.trim() || "localhost";
    const port = this.config.port ?? 27017;
    const auth = this.config.username
      ? `${encodeURIComponent(this.config.username)}:${encodeURIComponent(this.config.password ?? "")}@`
      : "";
    const database = this.defaultDatabaseName();
    const params = new URLSearchParams();
    const authDatabase = this.config.authDatabase ?? this.config.authSource;
    if (authDatabase) {
      params.set("authSource", authDatabase);
    }
    if (this.config.replicaSet) {
      params.set("replicaSet", this.config.replicaSet);
    }
    if (this.config.directConnection !== undefined) {
      params.set("directConnection", String(this.config.directConnection));
    }
    const suffix = params.size > 0 ? `?${params.toString()}` : "";
    return `mongodb://${auth}${host}:${port}/${database}${suffix}`;
  }

  private requireClient(): MongoClient {
    if (!this.client || !this.connected) {
      throw new Error("MongoDB is not connected.");
    }
    return this.client;
  }

  private defaultDatabaseName(): string {
    return this.config.database || "admin";
  }

  private requireDb(database?: string) {
    return this.requireClient().db(database || this.defaultDatabaseName());
  }

  private toRow(document: Record<string, unknown>): Record<string, unknown> {
    const withStringId = {
      ...document,
      _id:
        document._id instanceof ObjectId
          ? document._id.toHexString()
          : document._id,
    };
    return flattenRootRecord(withStringId);
  }

  private async readRows(
    database: string,
    table: string,
    limit: number,
  ): Promise<Record<string, unknown>[]> {
    try {
      const docs = await this.requireDb(database)
        .collection(table)
        .find({})
        .limit(limit)
        .toArray();
      return docs.map((doc) => this.toRow(doc as Record<string, unknown>));
    } catch {
      return [];
    }
  }

  private normalizeCriteria(
    criteria: Record<string, unknown>,
  ): Record<string, unknown> {
    const normalized = { ...criteria };
    if (
      typeof normalized._id === "string" &&
      ObjectId.isValid(normalized._id)
    ) {
      normalized._id = new ObjectId(normalized._id);
    }
    return normalized;
  }

  private normalizeFilterCriteria(
    filter: Record<string, unknown>,
  ): Record<string, unknown> {
    const normalized = this.normalizeCriteria(filter);
    if (Array.isArray(normalized.$or)) {
      normalized.$or = normalized.$or.map((item) =>
        item !== null && typeof item === "object"
          ? this.normalizeCriteria(item as Record<string, unknown>)
          : item,
      );
    }
    if (Array.isArray(normalized.$and)) {
      normalized.$and = normalized.$and.map((item) =>
        item !== null && typeof item === "object"
          ? this.normalizeCriteria(item as Record<string, unknown>)
          : item,
      );
    }
    return normalized;
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
