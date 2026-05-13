import vm from "node:vm";
import { MongoClient, ObjectId } from "mongodb";
import type { ConnectionConfig } from "../connectionManager";
import {
  applyFilters,
  applySort,
  flattenRootRecord,
  inferColumnsFromRows,
  pageRows,
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

const MONGODB_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table", "view"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "supported",
    triggers: "not_applicable",
  },
};

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

  getEntityManifest(): DriverEntityManifest {
    return MONGODB_ENTITY_MANIFEST;
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
        .listCollections({}, { nameOnly: false })
        .toArray();
      return collections
        .filter((collection) => !this.isSystemNamespace(collection.name))
        .map((collection) => ({
          schema: database || this.defaultDatabaseName(),
          name: collection.name,
          type: collection.type === "view" ? "view" : "table",
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
    return inferColumnsFromRows(rows, "_id", {
      nullableMode: "schemaLess",
    }).map((column) => ({
      name: column.name,
      type: column.nativeType,
      nullable: column.nullable,
      defaultValue: column.isPrimaryKey ? "ObjectId()" : undefined,
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
    const rows = await this.readRows(database, table, 50);
    return inferColumnsFromRows(rows, "_id", {
      nullableMode: "schemaLess",
    }).map((column) => ({
      ...column,
      defaultValue: column.isPrimaryKey ? "ObjectId()" : undefined,
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
    const trimmed = sql
      .split("\n")
      .filter((line) => !line.trim().startsWith("//"))
      .join("\n")
      .trim()
      .replace(/;+\s*$/, "")
      .trim();

    if (trimmed.length === 0) {
      return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
    }

    const startedAt = Date.now();

    let dbName: string | undefined;
    let collName: string | undefined;
    let opName: string | undefined;
    let opArgs: unknown[] = [];
    const chainOps: Array<{ op: string; args: unknown[] }> = [];

    const createChainProxy = (): object => {
      return new Proxy({} as Record<string, unknown>, {
        get(_t, method: string | symbol) {
          if (typeof method !== "string") return undefined;
          return (...a: unknown[]) => {
            chainOps.push({ op: method, args: a });
            return createChainProxy();
          };
        },
      });
    };

    const createCollProxy = (coll: string): object => {
      return new Proxy({} as Record<string, unknown>, {
        get(_t, method: string | symbol) {
          if (typeof method !== "string") return undefined;
          if (method === "then") return undefined;
          return (...a: unknown[]) => {
            collName = coll;
            opName = method;
            opArgs = a;
            return createChainProxy();
          };
        },
      });
    };

    const createDbProxy = (): object => {
      return new Proxy({} as Record<string, unknown>, {
        get(_t, prop: string | symbol) {
          if (typeof prop !== "string") return undefined;
          if (prop === "getSiblingDB") {
            return (name: string) => {
              dbName = name;
              return createDbProxy();
            };
          }
          if (prop === "runCommand") {
            return (cmd: unknown) => {
              opName = "runCommand";
              opArgs = [cmd];
              return createChainProxy();
            };
          }
          if (prop === "createCollection") {
            return (name: string) => {
              opName = "createCollection";
              opArgs = [name];
            };
          }
          return createCollProxy(prop);
        },
      });
    };

    const sandbox = {
      db: createDbProxy(),
      ObjectId: (hex: string) => new ObjectId(hex),
      ISODate: (s: string) => new Date(s),
      NumberLong: (n: number | string) => Number(n),
      NumberInt: (n: number | string) => Number(n),
      NumberDecimal: (s: string) => parseFloat(s),
    };

    try {
      vm.runInNewContext(trimmed, sandbox, { timeout: 5000 });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      throw new Error(
        `mongosh error: ${msg}\n\nExamples:\n  db.users.find({})\n  db.users.find({ name: "Alice" }).limit(10)\n  db.users.insertOne({ name: "Alice" })\n  db.users.updateMany({ status: "active" }, { $set: { updated: true } })\n  db.users.deleteMany({ _id: ObjectId("507f1f77bcf86cd799439011") })\n  db.runCommand({ ping: 1 })\n  db.getSiblingDB("mydb").users.find({})`,
      );
    }

    const op = opName;
    if (!op) {
      throw new Error(
        'No operation found in mongosh expression.\n\nExamples:\n  db.users.find({})\n  db.users.insertOne({ name: "Alice" })\n  db.runCommand({ ping: 1 })',
      );
    }

    const limitOp = chainOps.find((c) => c.op === "limit");
    const limit = typeof limitOp?.args[0] === "number" ? limitOp.args[0] : 100;
    const skipOp = chainOps.find((c) => c.op === "skip");
    const skip = typeof skipOp?.args[0] === "number" ? skipOp.args[0] : 0;

    if (op === "runCommand") {
      const cmd = this.normalizeFilterCriteria(
        opArgs[0] as Record<string, unknown>,
      );
      const result = await this.requireDb(dbName).command(cmd);
      const row = flattenRootRecord(result as Record<string, unknown>);
      const columns = Object.keys(row);
      return {
        columns,
        rows: [this.mapRowToQueryRow(row, columns)],
        rowCount: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (op === "createCollection") {
      const name = String(opArgs[0]);
      await this.requireDb(dbName).createCollection(name);
      const row = { ok: 1, name };
      const columns = Object.keys(row);
      return {
        columns,
        rows: [this.mapRowToQueryRow(row, columns)],
        rowCount: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (!collName) {
      throw new Error(`Collection name is required for operation "${op}"`);
    }

    const mongoCollection = this.requireDb(dbName).collection(collName);

    if (op === "find" || op === "findOne") {
      const filter = this.normalizeFilterCriteria(
        (opArgs[0] as Record<string, unknown>) ?? {},
      );
      const actualLimit = op === "findOne" ? 1 : limit;
      const docs = await mongoCollection
        .find(filter)
        .limit(actualLimit)
        .skip(skip)
        .toArray();
      const rows = docs.map((doc) =>
        this.toRow(doc as Record<string, unknown>),
      );
      const columns = inferColumnsFromRows(rows, "_id").map((c) => c.name);
      return {
        columns,
        rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
        rowCount: rows.length,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (op === "countDocuments") {
      const filter = this.normalizeFilterCriteria(
        (opArgs[0] as Record<string, unknown>) ?? {},
      );
      const count = await mongoCollection.countDocuments(filter);
      return {
        columns: ["count"],
        rows: [this.mapRowToQueryRow({ count }, ["count"])],
        rowCount: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (op === "insertOne") {
      const doc = opArgs[0] as Record<string, unknown>;
      const result = await mongoCollection.insertOne(doc);
      const row = {
        acknowledged: result.acknowledged,
        insertedId: String(result.insertedId),
      };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: result.acknowledged ? 1 : 0,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (op === "insertMany") {
      const docs = opArgs[0] as Record<string, unknown>[];
      const result = await mongoCollection.insertMany(docs);
      const row = {
        acknowledged: result.acknowledged,
        insertedCount: result.insertedCount,
      };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: result.insertedCount,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (op === "updateOne" || op === "updateMany") {
      const filter = this.normalizeFilterCriteria(
        opArgs[0] as Record<string, unknown>,
      );
      const update = opArgs[1] as Record<string, unknown>;
      const result =
        op === "updateOne"
          ? await mongoCollection.updateOne(filter, update)
          : await mongoCollection.updateMany(filter, update);
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

    if (op === "deleteOne" || op === "deleteMany") {
      const filter = this.normalizeFilterCriteria(
        opArgs[0] as Record<string, unknown>,
      );
      const result =
        op === "deleteOne"
          ? await mongoCollection.deleteOne(filter)
          : await mongoCollection.deleteMany(filter);
      const row = { deletedCount: result.deletedCount };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: result.deletedCount,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (op === "aggregate") {
      const pipeline = opArgs[0] as Record<string, unknown>[];
      const docs = await mongoCollection.aggregate(pipeline).toArray();
      const rows = docs.map((doc) =>
        this.toRow(doc as Record<string, unknown>),
      );
      const columns = inferColumnsFromRows(rows, "_id").map((c) => c.name);
      return {
        columns,
        rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
        rowCount: rows.length,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    throw new Error(
      `Unsupported mongosh operation: "${op}".\n\nSupported: find, findOne, countDocuments, insertOne, insertMany, updateOne, updateMany, deleteOne, deleteMany, aggregate, runCommand, createCollection`,
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
      columns: inferColumnsFromRows(sorted, "_id", {
        nullableMode: "schemaLess",
      }),
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
    const dbRef = database
      ? `db.getSiblingDB(${JSON.stringify(database)})`
      : "db";

    if (operation === "insert") {
      const doc = this.serializeMongosh(data.values ?? {});
      return `${dbRef}.${table}.insertOne(${doc})`;
    }
    if (operation === "update") {
      const filter = this.serializeMongosh(data.primaryKeys ?? {});
      const update = this.serializeMongosh({ $set: data.changes ?? {} });
      return `${dbRef}.${table}.updateMany(\n  ${filter},\n  ${update}\n)`;
    }
    // delete
    const filterValue = data.primaryKeyValuesList?.length
      ? data.primaryKeyValuesList.length === 1
        ? data.primaryKeyValuesList[0]
        : { $or: data.primaryKeyValuesList }
      : (data.primaryKeys ?? {});
    return `${dbRef}.${table}.deleteMany(${this.serializeMongosh(filterValue)})`;
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
    const dotIdx = qualifiedTableName.indexOf(".");
    const db = dotIdx !== -1 ? qualifiedTableName.slice(0, dotIdx) : "";
    const coll =
      dotIdx !== -1 ? qualifiedTableName.slice(dotIdx + 1) : qualifiedTableName;
    const dbRef = db ? `db.getSiblingDB(${JSON.stringify(db)})` : "db";
    return `${dbRef}.${coll}.insertOne({ })`;
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

  private serializeMongosh(value: unknown): string {
    if (value === null || value === undefined) return "null";
    if (typeof value === "boolean") return String(value);
    if (typeof value === "number") return String(value);
    if (typeof value === "string") {
      if (ObjectId.isValid(value) && value.length === 24) {
        return `ObjectId(${JSON.stringify(value)})`;
      }
      return JSON.stringify(value);
    }
    if (value instanceof ObjectId) {
      return `ObjectId(${JSON.stringify(value.toHexString())})`;
    }
    if (Array.isArray(value)) {
      return `[${value.map((v) => this.serializeMongosh(v)).join(", ")}]`;
    }
    if (typeof value === "object") {
      const entries = Object.entries(value as Record<string, unknown>).map(
        ([k, v]) => `${k}: ${this.serializeMongosh(v)}`,
      );
      if (entries.length === 0) return "{}";
      return `{ ${entries.join(", ")} }`;
    }
    return JSON.stringify(value);
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

  private isSystemNamespace(name: string): boolean {
    return /^system\./i.test(name);
  }
}
