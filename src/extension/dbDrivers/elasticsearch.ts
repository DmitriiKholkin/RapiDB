import { Client } from "@elastic/elasticsearch";
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

const ELASTICSEARCH_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "supported",
    triggers: "not_applicable",
  },
};

export class ElasticsearchDriver implements IDBDriver {
  private client: Client | null = null;
  private connected = false;

  constructor(private readonly config: ConnectionConfig) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const protocol = this.config.ssl ? "https" : "http";
    const node =
      this.config.connectionUri ??
      this.config.endpoint ??
      `${protocol}://${this.config.host || "localhost"}:${this.config.port ?? 9200}`;
    this.client = new Client({
      node: this.config.cloudId ? undefined : node,
      cloud: this.config.cloudId
        ? {
            id: this.config.cloudId,
          }
        : undefined,
      auth: this.config.apiKey
        ? {
            apiKey: this.config.apiKey,
          }
        : this.config.username
          ? {
              username: this.config.username,
              password: this.config.password ?? "",
            }
          : undefined,
      tls: this.config.ssl
        ? {
            rejectUnauthorized: this.config.rejectUnauthorized !== false,
          }
        : undefined,
    });
    await this.client.ping();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.client = null;
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  getEntityManifest(): DriverEntityManifest {
    return ELASTICSEARCH_ENTITY_MANIFEST;
  }

  getCapabilities() {
    return {
      tabularRead: "nosql" as const,
      queryMode: "text" as const,
      supportsMutations: true,
    };
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    return [{ name: this.config.database || "default", schemas: [] }];
  }

  async listSchemas(): Promise<SchemaInfo[]> {
    return [{ name: "indices" }];
  }

  async listObjects(): Promise<TableInfo[]> {
    try {
      const indices = await this.requireClient().cat.indices({
        format: "json",
      });
      const list = indices as Array<{ index?: string }>;
      return list
        .map((entry) => entry.index)
        .filter((name): name is string => Boolean(name))
        .sort((left, right) => left.localeCompare(right))
        .map((name) => ({
          schema: "indices",
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
    const [rows, mapping] = await Promise.all([
      this.readRows(table, 1000),
      this.fetchMappingMeta(table),
    ]);
    return inferColumnsFromRows(rows, "_id", {
      nullableMode: "schemaLess",
    }).map((column) => {
      const meta = mapping.get(column.name);
      return {
        name: column.name,
        type: meta ? this.esTypeToNative(meta.type) : column.nativeType,
        nullable: column.nullable,
        defaultValue:
          meta?.nullValue !== undefined ? String(meta.nullValue) : undefined,
        isPrimaryKey: column.isPrimaryKey,
        primaryKeyOrdinal: column.primaryKeyOrdinal,
        isForeignKey: false,
      };
    });
  }

  async describeColumns(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const [rows, mapping] = await Promise.all([
      this.readRows(table, 1000),
      this.fetchMappingMeta(table),
    ]);
    return inferColumnsFromRows(rows, "_id", {
      nullableMode: "schemaLess",
    }).map((column) => {
      const meta = mapping.get(column.name);
      const nativeType = meta
        ? this.esTypeToNative(meta.type)
        : column.nativeType;
      return {
        ...column,
        type: nativeType,
        nativeType,
        category: column.name !== "_id" ? column.category : column.category,
        defaultValue:
          meta?.nullValue !== undefined ? String(meta.nullValue) : undefined,
      };
    });
  }

  async getIndexes(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<IndexMeta[]> {
    try {
      await this.requireClient().indices.get({ index: table });
      return [
        {
          name: `${table}_id_idx`,
          columns: ["_id"],
          unique: true,
          primary: true,
        },
      ];
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
    unsupported("Elasticsearch constraints DDL");
  }

  async getIndexDDL(): Promise<string> {
    unsupported("Elasticsearch index DDL");
  }

  async getTriggerDDL(): Promise<string> {
    unsupported("Elasticsearch trigger DDL");
  }

  async getCreateTableDDL(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<string> {
    const response = await this.requireClient().indices.get({ index: table });
    const definition = this.extractIndexDefinition(response, table);
    if (!definition) {
      throw new Error(`Index "${table}" not found`);
    }

    const payload: Record<string, unknown> = {};
    const settings = this.filterCreateIndexSettings(definition.settings?.index);
    if (settings && Object.keys(settings).length > 0) {
      payload.settings = settings;
    }
    if (definition.mappings && Object.keys(definition.mappings).length > 0) {
      payload.mappings = definition.mappings;
    }
    if (definition.aliases && Object.keys(definition.aliases).length > 0) {
      payload.aliases = definition.aliases;
    }

    const body =
      Object.keys(payload).length > 0
        ? `\n${JSON.stringify(payload, null, 2)}`
        : "";
    return `PUT /${table}${body}`;
  }

  async getObjectDefinition(): Promise<string | null> {
    return null;
  }

  async getRoutineDefinition(): Promise<string> {
    unsupported("Elasticsearch routine definition");
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
    if (trimmed.startsWith("search ")) {
      const payload = JSON.parse(trimmed.slice("search ".length)) as {
        index: string;
        query?: Record<string, unknown>;
        size?: number;
      };
      const response = await this.requireClient().search({
        index: payload.index,
        size: payload.size ?? 100,
        query: payload.query as never,
      });
      const rows = this.hitsToRows(
        response.hits.hits as unknown as Array<Record<string, unknown>>,
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
        index: string;
        id: string;
        doc: Record<string, unknown>;
      };
      await this.requireClient().update({
        index: payload.index,
        id: String(payload.id),
        doc: payload.doc,
        refresh: "wait_for",
      });
      const row = { result: "updated", index: payload.index, id: payload.id };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (trimmed.startsWith("delete ")) {
      const payload = JSON.parse(trimmed.slice("delete ".length)) as {
        index: string;
        id: string;
      };
      await this.requireClient().delete({
        index: payload.index,
        id: String(payload.id),
        refresh: "wait_for",
      });
      const row = { result: "deleted", index: payload.index, id: payload.id };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (trimmed.startsWith("index ")) {
      const payload = JSON.parse(trimmed.slice("index ".length)) as {
        index: string;
        id?: string;
        document: Record<string, unknown>;
      };
      const response = await this.requireClient().index({
        index: payload.index,
        id: payload.id,
        document: payload.document,
        refresh: "wait_for",
      });
      const row = { result: response.result, id: response._id };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    throw new Error(
      'Elasticsearch query mode expects:\n  search {"index":"my-index","query":{"match_all":{}}}\n  update {"index":"my-index","id":"...","doc":{"field":"value"}}\n  delete {"index":"my-index","id":"..."}\n  index {"index":"my-index","document":{...}}',
    );
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    const rows = await this.readRows(request.table, 5000);
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
    let affectedRows = 0;
    for (const update of request.updates) {
      if (
        Object.hasOwn(update.changes, "_id") &&
        update.changes._id !== update.primaryKeys._id
      ) {
        throw new Error(
          "Elasticsearch does not support updating the _id field.",
        );
      }
      const id = update.primaryKeys._id;
      if (typeof id !== "string" && typeof id !== "number") {
        continue;
      }
      await this.requireClient().update({
        index: request.table,
        id: String(id),
        doc: update.changes,
      });
      affectedRows += 1;
    }
    return { affectedRows };
  }

  async insertRow(
    request: DriverInsertRowRequest,
  ): Promise<DriverMutationResult> {
    const id = request.values._id;
    await this.requireClient().index({
      index: request.table,
      id:
        typeof id === "string" || typeof id === "number"
          ? String(id)
          : undefined,
      document: request.values,
      op_type:
        typeof id === "string" || typeof id === "number" ? "create" : undefined,
      refresh: "wait_for",
    });
    return { affectedRows: 1 };
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
  ): Promise<DriverMutationResult> {
    let affectedRows = 0;
    for (const entry of request.primaryKeyValuesList) {
      const id = entry._id;
      if (typeof id !== "string" && typeof id !== "number") {
        continue;
      }
      await this.requireClient().delete({
        index: request.table,
        id: String(id),
        refresh: "wait_for",
      });
      affectedRows += 1;
    }
    return { affectedRows };
  }

  buildMutationPreviewStatement(
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
  ): string {
    if (operation === "insert") {
      const id = data.values?._id;
      return `index ${JSON.stringify({
        index: table,
        ...(id !== undefined ? { id: String(id) } : {}),
        document: data.values ?? {},
      })}`;
    }
    const id = data.primaryKeys?._id ?? data.primaryKeyValuesList?.[0]?._id;
    if (operation === "update") {
      return `update ${JSON.stringify({
        index: table,
        id: id !== undefined ? String(id) : "<id>",
        doc: data.changes ?? {},
      })}`;
    }
    // delete
    return `delete ${JSON.stringify({
      index: table,
      id: id !== undefined ? String(id) : "<id>",
    })}`;
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
    return stringifyCommandPayload("es_insert", { table: qualifiedTableName });
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

  private requireClient(): Client {
    if (!this.client || !this.connected) {
      throw new Error("Elasticsearch is not connected.");
    }
    return this.client;
  }

  private extractIndexDefinition(
    response: unknown,
    index: string,
  ):
    | {
        settings?: { index?: Record<string, unknown> };
        mappings?: Record<string, unknown>;
        aliases?: Record<string, unknown>;
      }
    | undefined {
    if (!response || typeof response !== "object" || Array.isArray(response)) {
      return undefined;
    }

    const entry = (response as Record<string, unknown>)[index];
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      return undefined;
    }

    const typedEntry = entry as {
      settings?: { index?: Record<string, unknown> };
      mappings?: Record<string, unknown>;
      aliases?: Record<string, unknown>;
    };

    return {
      settings: typedEntry.settings,
      mappings: typedEntry.mappings,
      aliases: typedEntry.aliases,
    };
  }

  private filterCreateIndexSettings(
    settings: Record<string, unknown> | undefined,
    path: readonly string[] = [],
  ): Record<string, unknown> | undefined {
    if (!settings) {
      return undefined;
    }

    const filteredSettings = Object.entries(settings).reduce<
      Record<string, unknown>
    >((result, [key, value]) => {
      const nextPath = [...path, key];
      const pathKey = nextPath.join(".");
      if (
        key === "creation_date" ||
        key === "provided_name" ||
        key === "uuid" ||
        key === "version" ||
        pathKey === "routing.allocation.initial_recovery._id"
      ) {
        return result;
      }

      if (value && typeof value === "object" && !Array.isArray(value)) {
        const nested = this.filterCreateIndexSettings(
          value as Record<string, unknown>,
          nextPath,
        );
        if (nested && Object.keys(nested).length > 0) {
          result[key] = nested;
        }
        return result;
      }

      if (value !== undefined) {
        result[key] = value;
      }
      return result;
    }, {});

    return Object.keys(filteredSettings).length > 0
      ? filteredSettings
      : undefined;
  }

  private async fetchMappingMeta(
    index: string,
  ): Promise<Map<string, { type: string; nullValue?: unknown }>> {
    try {
      const response = await this.requireClient().indices.getMapping({ index });
      const mappings =
        (
          response as Record<
            string,
            { mappings?: { properties?: Record<string, unknown> } }
          >
        )[index]?.mappings?.properties ?? {};
      const result = new Map<string, { type: string; nullValue?: unknown }>();
      for (const [field, def] of Object.entries(mappings)) {
        const fieldDef = def as { type?: string; null_value?: unknown };
        if (fieldDef.type) {
          result.set(field, {
            type: fieldDef.type,
            nullValue: fieldDef.null_value,
          });
        }
      }
      return result;
    } catch {
      return new Map();
    }
  }

  private esTypeToNative(esType: string): string {
    switch (esType) {
      case "keyword":
      case "text":
      case "match_only_text":
      case "wildcard":
      case "ip":
      case "version":
        return "text";
      case "long":
      case "integer":
      case "short":
      case "byte":
      case "unsigned_long":
        return "integer";
      case "float":
      case "double":
      case "half_float":
      case "scaled_float":
        return "float";
      case "boolean":
        return "boolean";
      case "date":
      case "date_nanos":
        return "datetime";
      case "binary":
        return "binary";
      case "object":
      case "nested":
      case "flattened":
        return "json";
      case "geo_point":
      case "geo_shape":
      case "point":
      case "shape":
        return "spatial";
      default:
        return "text";
    }
  }

  private async readRows(
    index: string,
    size: number,
  ): Promise<Record<string, unknown>[]> {
    try {
      const response = await this.requireClient().search({
        index,
        size,
        query: { match_all: {} },
        sort: ["_doc"],
      });
      return this.hitsToRows(
        response.hits.hits as unknown as Array<Record<string, unknown>>,
      );
    } catch {
      return [];
    }
  }

  private hitsToRows(
    hits: Array<Record<string, unknown>>,
  ): Record<string, unknown>[] {
    return hits.map((hit) => {
      const source = (hit._source ?? {}) as Record<string, unknown>;
      return flattenRootRecord({
        _id: hit._id,
        ...source,
      });
    });
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
