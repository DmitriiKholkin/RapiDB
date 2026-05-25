import { Client } from "@elastic/elasticsearch";
import { HttpConnection } from "@elastic/transport";
import { ELASTICSEARCH_READ_BUDGET } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import { getSshHttpAgentTransport } from "../driverRuntimeConfig";
import { allowReadOnlyQuery, denyReadOnlyQuery } from "../utils/readOnlyGuards";
import {
  formatDatetimeForDisplay,
  normalizeSqlDatetimeOffsetSpacing,
} from "./BaseDBDriver";
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
  TypeCategory,
} from "./types";
import { resolveFilterOperators } from "./types";

const ELASTICSEARCH_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "not_applicable",
    triggers: "not_applicable",
  },
};

type ElasticsearchFieldMeta = {
  type: string;
  nullValue?: unknown;
  kind: "mapped" | "runtime" | "alias";
  aliasPath?: string;
  targetType?: string;
};

type ElasticsearchRestMethod = "GET" | "POST" | "PUT" | "DELETE";

type ElasticsearchFilterClauses = {
  must: unknown[];
  mustNot: unknown[];
};

const ELASTICSEARCH_READ_ONLY_QUERY_REASON =
  "[RapiDB] Read-only Elasticsearch connections allow only GET requests and POST _search requests.";

interface ElasticsearchRestCommand {
  method: ElasticsearchRestMethod;
  path: string;
  pathSegments: string[];
  queryParams: URLSearchParams;
  body?: unknown;
}

const ELASTICSEARCH_TEXT_TYPES = new Set([
  "annotated_text",
  "completion",
  "constant_keyword",
  "ip",
  "keyword",
  "match_only_text",
  "pattern_text",
  "search_as_you_type",
  "semantic_text",
  "text",
  "version",
  "wildcard",
]);

const ELASTICSEARCH_INTEGER_TYPES = new Set([
  "byte",
  "integer",
  "long",
  "short",
  "token_count",
  "unsigned_long",
]);

const ELASTICSEARCH_FLOAT_TYPES = new Set([
  "double",
  "float",
  "half_float",
  "rank_feature",
  "scaled_float",
]);

const ELASTICSEARCH_DATETIME_TYPES = new Set(["date", "date_nanos"]);

const ELASTICSEARCH_JSON_TYPES = new Set([
  "aggregate_metric_double",
  "exponential_histogram",
  "flattened",
  "histogram",
  "join",
  "nested",
  "object",
  "passthrough",
  "percolator",
  "rank_features",
  "sparse_vector",
  "tdigest",
]);

const ELASTICSEARCH_ARRAY_TYPES = new Set(["dense_vector"]);

const ELASTICSEARCH_SPATIAL_TYPES = new Set([
  "geo_point",
  "geo_shape",
  "point",
  "shape",
]);

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
    const sshAgentTransport = getSshHttpAgentTransport(this.config);
    const client = new Client({
      ...(sshAgentTransport ? { Connection: HttpConnection } : {}),
      node: this.config.cloudId ? undefined : node,
      cloud: this.config.cloudId
        ? {
            id: this.config.cloudId,
          }
        : undefined,
      agent: sshAgentTransport
        ? (options: { url: URL }) =>
            options.url.protocol === "https:"
              ? sshAgentTransport.httpsAgent
              : sshAgentTransport.httpAgent
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
    try {
      await client.ping();
    } catch (error) {
      await client.close().catch(() => undefined);
      throw error;
    }
    this.client = client;
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    const client = this.client;
    this.client = null;
    this.connected = false;
    await client?.close();
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
      readOnlyQueryGuard: (queryText: string) =>
        this.decideReadOnlyQuery(queryText),
      editorPresentation: {
        formatOnOpen: false,
        editorLanguage: "plaintext" as const,
      },
    };
  }

  private decideReadOnlyQuery(queryText: string) {
    const trimmed = queryText.trim().replace(/;+$/, "");
    if (!trimmed) {
      return denyReadOnlyQuery(ELASTICSEARCH_READ_ONLY_QUERY_REASON);
    }

    try {
      const statements = this.splitRestStatements(trimmed);
      if (statements.length === 0) {
        return denyReadOnlyQuery(ELASTICSEARCH_READ_ONLY_QUERY_REASON);
      }

      return statements.every((statement) => {
        const command = this.parseRestCommand(statement);
        return command ? this.isReadOnlyRestCommand(command) : false;
      })
        ? allowReadOnlyQuery()
        : denyReadOnlyQuery(ELASTICSEARCH_READ_ONLY_QUERY_REASON);
    } catch (error: unknown) {
      return denyReadOnlyQuery(
        error instanceof Error ? error.message : String(error),
      );
    }
  }

  private isReadOnlyRestCommand(command: ElasticsearchRestCommand): boolean {
    const [firstSegment, secondSegment, thirdSegment] = command.pathSegments;
    if (command.method === "GET") {
      return true;
    }

    return (
      command.method === "POST" &&
      ((command.pathSegments.length === 1 && firstSegment === "_search") ||
        (secondSegment === "_search" && thirdSegment === undefined))
    );
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    return [{ name: "default", schemas: [] }];
  }

  async listSchemas(): Promise<SchemaInfo[]> {
    return [{ name: "indices" }];
  }

  async listObjects(): Promise<TableInfo[]> {
    try {
      const response = await this.requireClient().indices.resolveIndex({
        name: "*",
        expand_wildcards: ["open", "closed", "hidden"],
        allow_no_indices: true,
        ignore_unavailable: true,
      });
      const backingIndexNames = new Set(
        response.data_streams.flatMap((stream) => stream.backing_indices),
      );
      return response.indices
        .filter((entry) => {
          const attributes = new Set(entry.attributes ?? []);
          return (
            !attributes.has("hidden") &&
            !attributes.has("system") &&
            !entry.data_stream &&
            !backingIndexNames.has(entry.name)
          );
        })
        .map((entry) => entry.name)
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
    const columns = await this.describeIndexColumns(table);
    return columns.map(
      ({
        category: _category,
        nativeType: _nativeType,
        filterable: _filterable,
        filterOperators: _filterOperators,
        valueSemantics: _valueSemantics,
        ...column
      }) => column,
    );
  }

  async describeColumns(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    return this.describeIndexColumns(table);
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
    const statements = this.splitRestStatements(trimmed);
    if (statements.length === 0) {
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      };
    }

    if (statements.length === 1) {
      const command = this.parseRestCommand(statements[0]);
      if (!command) {
        throw this.buildInvalidRestCommandError();
      }

      return this.executeRestCommand(command, startedAt);
    }

    const results: Array<Record<string, unknown>> = [];
    let affectedRows = 0;
    for (const statement of statements) {
      const command = this.parseRestCommand(statement);
      if (!command) {
        throw this.buildInvalidRestCommandError();
      }

      const result = await this.executeRestCommand(command, startedAt);
      affectedRows += result.affectedRows ?? 0;
      results.push({
        statement,
        rowCount: result.rowCount,
        affectedRows: result.affectedRows,
        rows: this.toPlainRows(result),
      });
    }

    const row = flattenRootRecord({ results });
    return {
      columns: ["results"],
      rows: [this.mapRowToQueryRow(row, ["results"])],
      rowCount: 1,
      affectedRows: affectedRows > 0 ? affectedRows : undefined,
      executionTimeMs: Date.now() - startedAt,
    };
  }

  private splitRestStatements(input: string): string[] {
    const statements: string[] = [];
    let cursor = 0;

    while (cursor < input.length) {
      cursor = this.skipRestStatementSeparators(input, cursor);
      if (cursor >= input.length) {
        break;
      }

      const headerMatch = /^(GET|POST|PUT|DELETE)\s+\S+/i.exec(
        input.slice(cursor),
      );
      if (!headerMatch) {
        throw new Error(
          `Invalid Elasticsearch REST command near: ${input.slice(cursor, cursor + 80)}`,
        );
      }

      const statementStart = cursor;
      cursor += headerMatch[0].length;
      const bodyStart = this.skipWhitespace(input, cursor);
      if (bodyStart >= input.length) {
        statements.push(input.slice(statementStart, cursor).trim());
        break;
      }

      const remainder = input.slice(bodyStart);
      const nextHeader = /^(GET|POST|PUT|DELETE)\s+\S+/i.exec(remainder);
      if (nextHeader) {
        statements.push(input.slice(statementStart, cursor).trim());
        cursor = bodyStart;
        continue;
      }

      const jsonEnd = this.findJsonValueEnd(input, bodyStart);
      statements.push(input.slice(statementStart, jsonEnd).trim());
      cursor = jsonEnd;
    }

    return statements;
  }

  private skipRestStatementSeparators(input: string, cursor: number): number {
    let next = cursor;
    while (next < input.length) {
      const char = input[next];
      if (char === ";" || /\s/.test(char)) {
        next += 1;
        continue;
      }
      break;
    }
    return next;
  }

  private skipWhitespace(input: string, cursor: number): number {
    let next = cursor;
    while (next < input.length && /\s/.test(input[next])) {
      next += 1;
    }
    return next;
  }

  private findJsonValueEnd(input: string, cursor: number): number {
    const opening = input[cursor];
    if (opening !== "{" && opening !== "[") {
      throw new Error(
        `Elasticsearch REST command body must be valid JSON near: ${input.slice(cursor, cursor + 80)}`,
      );
    }

    const stack: string[] = [opening];
    let inString = false;
    let escaping = false;

    for (let index = cursor + 1; index < input.length; index += 1) {
      const char = input[index];

      if (escaping) {
        escaping = false;
        continue;
      }

      if (char === "\\") {
        escaping = true;
        continue;
      }

      if (char === '"') {
        inString = !inString;
        continue;
      }

      if (inString) {
        continue;
      }

      if (char === "{" || char === "[") {
        stack.push(char);
        continue;
      }

      if (char === "}" || char === "]") {
        const expected = char === "}" ? "{" : "[";
        const actual = stack.pop();
        if (actual !== expected) {
          throw new Error(
            "Elasticsearch REST command contains malformed JSON.",
          );
        }
        if (stack.length === 0) {
          return index + 1;
        }
      }
    }

    throw new Error(
      "Elasticsearch REST command has an unterminated JSON body.",
    );
  }

  private buildInvalidRestCommandError(): Error {
    return new Error(
      'Elasticsearch query mode expects REST-like commands:\n  PUT /my-index\n  {"settings":{...},"mappings":{...}}\n  POST /my-index/_search\n  {"query":{"match_all":{}}}\n  PUT /my-index/_doc/my-id\n  {"field":"value"}\n  POST /my-index/_update/my-id\n  {"doc":{"field":"value"}}\n  DELETE /my-index/_doc/my-id',
    );
  }

  private parseRestCommand(
    commandText: string,
  ): ElasticsearchRestCommand | null {
    const match = /^(GET|POST|PUT|DELETE)\s+(\S+)/i.exec(commandText);
    if (!match) {
      return null;
    }

    const method = match[1].toUpperCase() as ElasticsearchRestMethod;
    const rawPath = match[2];
    const bodyText = commandText.slice(match[0].length).trim();
    const parsedUrl = new URL(rawPath, "http://rapidb.local");
    const pathSegments = parsedUrl.pathname
      .split("/")
      .filter((segment) => segment.length > 0)
      .map((segment) => decodeURIComponent(segment));

    return {
      method,
      path: parsedUrl.pathname,
      pathSegments,
      queryParams: parsedUrl.searchParams,
      body: bodyText.length > 0 ? JSON.parse(bodyText) : undefined,
    };
  }

  private async executeRestCommand(
    command: ElasticsearchRestCommand,
    startedAt: number,
  ): Promise<QueryResult> {
    const client = this.requireClient();
    const queryParams = this.queryParamsToObject(command.queryParams);
    const [firstSegment, secondSegment, thirdSegment] = command.pathSegments;

    if (command.method === "PUT" && command.pathSegments.length === 1) {
      const response = await client.indices.create({
        index: firstSegment,
        ...queryParams,
        ...this.readObjectBody(command, `PUT ${command.path}`),
      });
      return this.buildSingleRowQueryResult(
        {
          acknowledged: response.acknowledged,
          shards_acknowledged: response.shards_acknowledged,
          index: response.index,
        },
        startedAt,
        1,
      );
    }

    if (command.method === "DELETE" && command.pathSegments.length === 1) {
      const response = await client.indices.delete({
        index: firstSegment,
        ...queryParams,
      });
      return this.buildSingleRowQueryResult(
        {
          acknowledged: response.acknowledged,
          index: firstSegment,
        },
        startedAt,
        1,
      );
    }

    if (
      (command.method === "GET" || command.method === "POST") &&
      thirdSegment === undefined &&
      secondSegment === "_search"
    ) {
      const body = this.enforceSearchRequestHardCap(
        this.readObjectBody(command, `${command.method} ${command.path}`, {
          allowEmpty: true,
        }),
      );
      const response = await client.search({
        index: firstSegment,
        ...this.enforceSearchQueryParamsHardCap(queryParams),
        ...body,
      } as never);
      const rows = this.hitsToRows(
        response.hits.hits as unknown as Array<Record<string, unknown>>,
      );
      return this.buildRowsQueryResult(rows, startedAt);
    }

    if (
      (command.method === "GET" || command.method === "POST") &&
      command.pathSegments.length === 1 &&
      firstSegment === "_search"
    ) {
      const body = this.enforceSearchRequestHardCap(
        this.readObjectBody(command, `${command.method} ${command.path}`, {
          allowEmpty: true,
        }),
      );
      const response = await client.search({
        ...this.enforceSearchQueryParamsHardCap(queryParams),
        ...body,
      } as never);
      const rows = this.hitsToRows(
        response.hits.hits as unknown as Array<Record<string, unknown>>,
      );
      return this.buildRowsQueryResult(rows, startedAt);
    }

    if (
      command.method === "GET" &&
      command.pathSegments.length === 3 &&
      secondSegment === "_doc"
    ) {
      const response = await client.get({
        index: firstSegment,
        id: thirdSegment,
        ...queryParams,
      });
      const row = flattenRootRecord({
        _id: response._id,
        ...(((response as { _source?: Record<string, unknown> })._source ??
          {}) as Record<string, unknown>),
      });
      return this.buildRowsQueryResult([row], startedAt);
    }

    if (
      (command.method === "PUT" || command.method === "POST") &&
      secondSegment === "_doc" &&
      (command.pathSegments.length === 2 || command.pathSegments.length === 3)
    ) {
      const response = await client.index({
        index: firstSegment,
        ...(thirdSegment ? { id: thirdSegment } : {}),
        document: this.stripDocumentId(
          this.readObjectBody(command, `${command.method} ${command.path}`),
        ),
        ...queryParams,
      });
      return this.buildSingleRowQueryResult(
        {
          result: response.result,
          index: response._index,
          id: response._id,
        },
        startedAt,
        1,
      );
    }

    if (
      command.method === "POST" &&
      command.pathSegments.length === 3 &&
      secondSegment === "_update"
    ) {
      const body = this.readObjectBody(command, `POST ${command.path}`);
      const response = await client.update({
        index: firstSegment,
        id: thirdSegment,
        ...queryParams,
        ...body,
      } as never);
      return this.buildSingleRowQueryResult(
        {
          result: response.result,
          index: firstSegment,
          id: thirdSegment,
        },
        startedAt,
        1,
      );
    }

    if (
      command.method === "DELETE" &&
      command.pathSegments.length === 3 &&
      secondSegment === "_doc"
    ) {
      const response = await client.delete({
        index: firstSegment,
        id: thirdSegment,
        ...queryParams,
      });
      return this.buildSingleRowQueryResult(
        {
          result: response.result,
          index: firstSegment,
          id: thirdSegment,
        },
        startedAt,
        1,
      );
    }

    if (
      command.method === "PUT" &&
      command.pathSegments.length === 2 &&
      secondSegment === "_mapping"
    ) {
      const response = await client.indices.putMapping({
        index: firstSegment,
        ...queryParams,
        ...this.readObjectBody(command, `PUT ${command.path}`),
      });
      return this.buildSingleRowQueryResult(
        {
          acknowledged: response.acknowledged,
          index: firstSegment,
        },
        startedAt,
        1,
      );
    }

    if (
      command.method === "PUT" &&
      command.pathSegments.length === 2 &&
      secondSegment === "_settings"
    ) {
      const response = await client.indices.putSettings({
        index: firstSegment,
        ...queryParams,
        ...this.readObjectBody(command, `PUT ${command.path}`),
      });
      return this.buildSingleRowQueryResult(
        {
          acknowledged: response.acknowledged,
          index: firstSegment,
        },
        startedAt,
        1,
      );
    }

    if (
      command.method === "POST" &&
      command.pathSegments.length === 1 &&
      firstSegment === "_aliases"
    ) {
      const response = await client.indices.updateAliases({
        ...queryParams,
        ...this.readObjectBody(command, `POST ${command.path}`),
      });
      return this.buildSingleRowQueryResult(
        {
          acknowledged: response.acknowledged,
        },
        startedAt,
        1,
      );
    }

    throw new Error(
      `Unsupported Elasticsearch REST command: ${command.method} ${command.path}`,
    );
  }

  private readObjectBody(
    command: ElasticsearchRestCommand,
    label: string,
    options?: { allowEmpty?: boolean },
  ): Record<string, unknown> {
    if (command.body === undefined) {
      if (options?.allowEmpty) {
        return {};
      }
      throw new Error(`${label} expects a JSON object body.`);
    }
    if (
      !command.body ||
      typeof command.body !== "object" ||
      Array.isArray(command.body)
    ) {
      throw new Error(`${label} expects a JSON object body.`);
    }
    return command.body as Record<string, unknown>;
  }

  private queryParamsToObject(
    queryParams: URLSearchParams,
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const key of new Set(queryParams.keys())) {
      const values = queryParams
        .getAll(key)
        .map((value) => this.coerceRestQueryParamValue(value));
      if (values.length === 1) {
        result[key] = values[0];
      } else if (values.length > 1) {
        result[key] = values;
      }
    }
    return result;
  }

  private coerceRestQueryParamValue(value: string): boolean | number | string {
    if (value === "true") {
      return true;
    }
    if (value === "false") {
      return false;
    }
    if (/^-?\d+(?:\.\d+)?$/.test(value)) {
      const numeric = Number(value);
      if (!Number.isNaN(numeric)) {
        return numeric;
      }
    }
    return value;
  }

  private buildRowsQueryResult(
    rows: readonly Record<string, unknown>[],
    startedAt: number,
    affectedRows?: number,
  ): QueryResult {
    const columns = inferColumnsFromRows(rows, "_id").map(
      (column) => column.name,
    );
    return {
      columns,
      rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
      rowCount: rows.length,
      affectedRows,
      executionTimeMs: Date.now() - startedAt,
    };
  }

  private buildSingleRowQueryResult(
    row: Record<string, unknown>,
    startedAt: number,
    affectedRows?: number,
  ): QueryResult {
    const columns = Object.keys(row);
    return {
      columns,
      rows: [this.mapRowToQueryRow(row, columns)],
      rowCount: 1,
      affectedRows,
      executionTimeMs: Date.now() - startedAt,
    };
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    const offset = Math.max(0, (request.page - 1) * request.pageSize);
    const size = this.clampSearchSize(request.pageSize);
    const mapping = await this.fetchMappingMeta(request.table);
    const mappedColumns = this.buildColumnsFromMetadata([], mapping);
    const columnMetaByName = new Map(
      mappedColumns.map((column) => [column.name, column]),
    );
    const filterClauses = this.buildElasticsearchFilterClauses(
      request.filters,
      columnMetaByName,
    );
    const query =
      filterClauses.must.length > 0 || filterClauses.mustNot.length > 0
        ? {
            bool: {
              ...(filterClauses.must.length > 0
                ? { must: filterClauses.must }
                : {}),
              ...(filterClauses.mustNot.length > 0
                ? { must_not: filterClauses.mustNot }
                : {}),
            },
          }
        : { match_all: {} };
    const sort = request.sort
      ? [{ [request.sort.column]: { order: request.sort.direction } }]
      : ["_doc"];

    const response = await this.requireClient().search({
      index: request.table,
      query,
      sort,
      from: offset,
      size,
      ...(request.skipCount ? {} : { track_total_hits: true }),
    } as never);

    const rows = this.hitsToRows(
      response.hits.hits as unknown as Array<Record<string, unknown>>,
    );
    const columns = this.buildColumnsFromMetadata(rows, mapping);
    const formattedColumnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    const paged = rows.map((row) =>
      Object.fromEntries(
        Object.entries(row).map(([columnName, value]) => {
          const column = formattedColumnMetaByName.get(columnName);
          return [
            columnName,
            column ? this.formatOutputValue(value, column) : value,
          ];
        }),
      ),
    );
    return {
      columns,
      rows: paged,
      totalCount: request.skipCount
        ? 0
        : this.resolveElasticsearchTotalCount(response.hits.total),
    };
  }

  private resolveElasticsearchTotalCount(totalHits: unknown): number {
    if (typeof totalHits === "number") {
      return totalHits;
    }

    if (
      totalHits &&
      typeof totalHits === "object" &&
      typeof (totalHits as { value?: unknown }).value === "number"
    ) {
      return (totalHits as { value: number }).value;
    }

    return 0;
  }

  private buildElasticsearchFilterClauses(
    filters: DriverTablePageRequest["filters"],
    columnMetaByName: ReadonlyMap<string, ColumnTypeMeta>,
  ): ElasticsearchFilterClauses {
    const must: unknown[] = [];
    const mustNot: unknown[] = [];

    for (const filter of filters) {
      const column = columnMetaByName.get(filter.column);
      switch (filter.operator) {
        case "is_null":
          mustNot.push({ exists: { field: filter.column } });
          break;
        case "is_not_null":
          must.push({ exists: { field: filter.column } });
          break;
        case "between": {
          const start = this.coerceFilterValue(filter.value[0], column);
          const end = this.coerceFilterValue(filter.value[1], column);
          must.push({
            range: {
              [filter.column]: {
                gte: start,
                lte: end,
              },
            },
          });
          break;
        }
        case "eq": {
          const value = this.coerceFilterValue(filter.value, column);
          must.push(this.buildElasticsearchExactClause(filter.column, value));
          break;
        }
        case "neq": {
          const value = this.coerceFilterValue(filter.value, column);
          mustNot.push(
            this.buildElasticsearchExactClause(filter.column, value),
          );
          break;
        }
        case "gt":
        case "gte":
        case "lt":
        case "lte": {
          const value = this.coerceFilterValue(filter.value, column);
          must.push({
            range: {
              [filter.column]: {
                [filter.operator]: value,
              },
            },
          });
          break;
        }
        case "in": {
          const values = this.splitInFilterValues(filter.value).map((entry) =>
            this.coerceFilterValue(entry, column),
          );
          if (values.length > 0) {
            must.push({
              terms: {
                [filter.column]: values,
              },
            });
          }
          break;
        }
        case "like":
        case "ilike": {
          must.push({
            wildcard: {
              [filter.column]: {
                value: this.toElasticsearchWildcard(filter.value),
                ...(filter.operator === "ilike"
                  ? { case_insensitive: true }
                  : {}),
              },
            },
          });
          break;
        }
      }
    }

    return { must, mustNot };
  }

  private buildElasticsearchExactClause(
    column: string,
    value: unknown,
  ): Record<string, unknown> {
    return {
      term: {
        [column]: value,
      },
    };
  }

  private coerceFilterValue(
    value: string,
    column: ColumnTypeMeta | undefined,
  ): unknown {
    if (!column) {
      return value;
    }
    return this.coerceInputValue(value, column);
  }

  private splitInFilterValues(value: string): string[] {
    return value
      .split(",")
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
  }

  private toElasticsearchWildcard(value: string): string {
    const escaped = value
      .replace(/\\/g, "\\\\")
      .replace(/\*/g, "\\*")
      .replace(/\?/g, "\\?");
    const normalized = escaped.replace(/%/g, "*").replace(/_/g, "?").trim();
    return normalized.length > 0 ? normalized : "*";
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
        refresh: "wait_for",
      });
      affectedRows += 1;
    }
    return { affectedRows };
  }

  async insertRow(
    request: DriverInsertRowRequest,
  ): Promise<DriverMutationResult> {
    const id = request.values._id;
    const document = this.stripDocumentId(request.values);
    await this.requireClient().index({
      index: request.table,
      id:
        typeof id === "string" || typeof id === "number"
          ? String(id)
          : undefined,
      document,
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

  async buildMutationPreviewStatements(
    operation: "insert" | "update" | "delete",
    database: string,
    schema: string,
    table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
  ): Promise<string[]> {
    if (operation === "delete" && data.primaryKeyValuesList) {
      return data.primaryKeyValuesList.map((primaryKeys) =>
        this.buildMutationPreviewStatement(operation, database, schema, table, {
          primaryKeys,
        }),
      );
    }

    return [
      this.buildMutationPreviewStatement(
        operation,
        database,
        schema,
        table,
        data,
      ),
    ];
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
      const document = this.stripDocumentId(data.values ?? {});
      return this.formatRestCommand(
        id !== undefined ? "PUT" : "POST",
        `/${encodeURIComponent(table)}/_doc${
          id !== undefined ? `/${encodeURIComponent(String(id))}` : ""
        }?${new URLSearchParams({
          ...(id !== undefined ? { op_type: "create" } : {}),
          refresh: "wait_for",
        }).toString()}`,
        document,
      );
    }
    const id = data.primaryKeys?._id ?? data.primaryKeyValuesList?.[0]?._id;
    if (operation === "update") {
      return this.formatRestCommand(
        "POST",
        `/${encodeURIComponent(table)}/_update/${encodeURIComponent(
          id !== undefined ? String(id) : "<id>",
        )}?refresh=wait_for`,
        { doc: data.changes ?? {} },
      );
    }
    return this.formatRestCommand(
      "DELETE",
      `/${encodeURIComponent(table)}/_doc/${encodeURIComponent(
        id !== undefined ? String(id) : "<id>",
      )}?refresh=wait_for`,
    );
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

  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (typeof value === "string" && column.category === "datetime") {
      const trimmed = normalizeSqlDatetimeOffsetSpacing(value.trim());
      const match =
        /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}:\d{2}|Z)?$/i.exec(
          trimmed,
        );
      if (match) {
        const [, date, time, fraction, timezone] = match;
        return `${date}T${time}${fraction ?? ""}${timezone ?? ""}`;
      }
    }
    return value;
  }

  formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (column.category === "datetime") {
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) {
        return formatted;
      }
    }
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
  ): Promise<Map<string, ElasticsearchFieldMeta>> {
    try {
      const response = await this.requireClient().indices.getMapping({ index });
      const mappings = (
        response as Record<
          string,
          {
            mappings?: {
              properties?: Record<string, unknown>;
              runtime?: Record<string, unknown>;
            };
          }
        >
      )[index]?.mappings;
      const result = new Map<string, ElasticsearchFieldMeta>();

      const addField = (name: string, meta: ElasticsearchFieldMeta): void => {
        if (!name || result.has(name)) {
          return;
        }
        result.set(name, meta);
      };

      const walkProperties = (
        properties: Record<string, unknown> | undefined,
        parentPath = "",
      ): void => {
        if (!properties) {
          return;
        }

        for (const [fieldName, rawDefinition] of Object.entries(properties)) {
          if (
            !rawDefinition ||
            typeof rawDefinition !== "object" ||
            Array.isArray(rawDefinition)
          ) {
            continue;
          }

          const definition = rawDefinition as {
            type?: string;
            path?: string;
            null_value?: unknown;
            properties?: Record<string, unknown>;
            fields?: Record<string, unknown>;
          };
          const fullPath = parentPath
            ? `${parentPath}.${fieldName}`
            : fieldName;
          const hasChildren =
            definition.properties !== undefined &&
            Object.keys(definition.properties).length > 0;
          const effectiveType =
            definition.type ?? (hasChildren ? "object" : undefined);

          if (effectiveType) {
            addField(fullPath, {
              type: effectiveType,
              nullValue: definition.null_value,
              kind: effectiveType === "alias" ? "alias" : "mapped",
              aliasPath:
                effectiveType === "alias" ? definition.path : undefined,
            });
          }

          if (hasChildren) {
            walkProperties(definition.properties, fullPath);
          }

          if (definition.fields && Object.keys(definition.fields).length > 0) {
            walkProperties(definition.fields, fullPath);
          }
        }
      };

      walkProperties(mappings?.properties);

      for (const [fieldName, rawDefinition] of Object.entries(
        mappings?.runtime ?? {},
      )) {
        if (
          !rawDefinition ||
          typeof rawDefinition !== "object" ||
          Array.isArray(rawDefinition)
        ) {
          continue;
        }
        const definition = rawDefinition as { type?: string };
        addField(fieldName, {
          type: definition.type ?? "keyword",
          kind: "runtime",
        });
      }

      for (const [fieldName, meta] of [...result.entries()]) {
        if (meta.kind !== "alias" || !meta.aliasPath) {
          continue;
        }

        const target = result.get(meta.aliasPath);
        if (!target) {
          continue;
        }

        result.set(fieldName, {
          ...meta,
          nullValue: meta.nullValue ?? target.nullValue,
          targetType: target.targetType ?? target.type,
        });
      }

      return result;
    } catch {
      return new Map();
    }
  }

  private async describeIndexColumns(table: string): Promise<ColumnTypeMeta[]> {
    const [rows, mapping] = await Promise.all([
      this.readRows(table, 1000),
      this.fetchMappingMeta(table),
    ]);
    return this.buildColumnsFromMetadata(rows, mapping);
  }

  private buildColumnsFromMetadata(
    sampledRows: readonly Record<string, unknown>[],
    mapping: ReadonlyMap<string, ElasticsearchFieldMeta>,
  ): ColumnTypeMeta[] {
    const sampledColumns = inferColumnsFromRows(sampledRows, "_id", {
      nullableMode: "schemaLess",
    });
    const sampledByName = new Map(
      sampledColumns.map((column) => [column.name, column]),
    );
    const orderedNames = [
      ...mapping.keys(),
      ...sampledColumns
        .map((column) => column.name)
        .filter((name) => !mapping.has(name)),
    ].filter((name, index, list) => list.indexOf(name) === index);

    const columns: ColumnTypeMeta[] = [this.makeSyntheticIdColumn()];

    for (const fieldName of orderedNames) {
      if (fieldName === "_id") {
        continue;
      }

      const sampled = sampledByName.get(fieldName);
      const mapped = mapping.get(fieldName);
      if (!sampled && !mapped) {
        continue;
      }

      if (!mapped && sampled) {
        columns.push(sampled);
        continue;
      }

      const resolvedType = this.resolveElasticsearchFieldType(
        mapped?.type ?? sampled?.nativeType ?? "keyword",
        mapped,
        sampled,
      );

      columns.push({
        name: fieldName,
        type: resolvedType.type,
        nativeType: resolvedType.nativeType,
        category: resolvedType.category,
        nullable: sampled?.nullable ?? true,
        defaultValue:
          mapped?.nullValue !== undefined
            ? String(mapped.nullValue)
            : undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: false,
        filterable: resolvedType.filterable,
        filterOperators: resolveFilterOperators(resolvedType.category, {
          filterable: resolvedType.filterable,
          nullable: sampled?.nullable ?? true,
        }),
        valueSemantics: "plain",
      });
    }

    return columns;
  }

  private makeSyntheticIdColumn(): ColumnTypeMeta {
    const category: TypeCategory = "text";
    return {
      name: "_id",
      type: "text",
      nativeType: "text",
      category,
      nullable: false,
      defaultValue: undefined,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      isForeignKey: false,
      filterable: true,
      filterOperators: resolveFilterOperators(category, {
        filterable: true,
        nullable: false,
      }),
      valueSemantics: "plain",
    };
  }

  private resolveElasticsearchFieldType(
    esType: string,
    mapped: ElasticsearchFieldMeta | undefined,
    sampled: ColumnTypeMeta | undefined,
  ): {
    type: string;
    nativeType: string;
    category: TypeCategory;
    filterable: boolean;
  } {
    const normalizedType = esType.toLowerCase().replace(/-/g, "_");

    if (normalizedType === "alias") {
      const targetType = mapped?.targetType;
      const targetInfo = targetType
        ? this.resolveElasticsearchFieldType(targetType, undefined, sampled)
        : undefined;
      const category = targetInfo?.category ?? sampled?.category ?? "text";
      return {
        type: esType,
        nativeType: esType,
        category,
        filterable: this.isFilterableCategory(category),
      };
    }

    if (ELASTICSEARCH_TEXT_TYPES.has(normalizedType)) {
      return {
        type: esType,
        nativeType: esType,
        category: "text",
        filterable: true,
      };
    }

    if (ELASTICSEARCH_INTEGER_TYPES.has(normalizedType)) {
      return {
        type: esType,
        nativeType: esType,
        category: "integer",
        filterable: true,
      };
    }

    if (ELASTICSEARCH_FLOAT_TYPES.has(normalizedType)) {
      return {
        type: esType,
        nativeType: esType,
        category: "float",
        filterable: true,
      };
    }

    if (ELASTICSEARCH_DATETIME_TYPES.has(normalizedType)) {
      return {
        type: esType,
        nativeType: esType,
        category: "datetime",
        filterable: true,
      };
    }

    if (normalizedType === "boolean") {
      return {
        type: esType,
        nativeType: esType,
        category: "boolean",
        filterable: true,
      };
    }

    if (normalizedType === "binary") {
      return {
        type: esType,
        nativeType: esType,
        category: "binary",
        filterable: false,
      };
    }

    if (ELASTICSEARCH_SPATIAL_TYPES.has(normalizedType)) {
      return {
        type: esType,
        nativeType: esType,
        category: "spatial",
        filterable: false,
      };
    }

    if (ELASTICSEARCH_ARRAY_TYPES.has(normalizedType)) {
      return {
        type: esType,
        nativeType: esType,
        category: "array",
        filterable: true,
      };
    }

    if (
      ELASTICSEARCH_JSON_TYPES.has(normalizedType) ||
      normalizedType.endsWith("_range")
    ) {
      return {
        type: esType,
        nativeType: esType,
        category: "json",
        filterable: true,
      };
    }

    if (sampled) {
      return {
        type: esType,
        nativeType: esType,
        category: sampled.category,
        filterable: this.isFilterableCategory(sampled.category),
      };
    }

    return {
      type: esType,
      nativeType: esType,
      category: "other",
      filterable: true,
    };
  }

  private isFilterableCategory(category: TypeCategory): boolean {
    return category !== "binary" && category !== "spatial";
  }

  private async readRows(
    index: string,
    size: number,
  ): Promise<Record<string, unknown>[]> {
    try {
      const response = await this.requireClient().search({
        index,
        size: this.clampSearchSize(size),
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

  private stripDocumentId(
    document: Record<string, unknown>,
  ): Record<string, unknown> {
    const { _id: _ignored, ...rest } = document;
    return rest;
  }

  private formatRestCommand(
    method: ElasticsearchRestMethod,
    path: string,
    body?: Record<string, unknown>,
  ): string {
    if (!body) {
      return `${method} ${path}`;
    }
    return `${method} ${path}\n${JSON.stringify(body, null, 2)}`;
  }

  private toPlainRows(result: QueryResult): Array<Record<string, unknown>> {
    return result.rows.map((row) =>
      Object.fromEntries(
        result.columns.map((columnName, index) => [
          columnName,
          row[`__col_${index}`],
        ]),
      ),
    );
  }

  private enforceSearchQueryParamsHardCap(
    queryParams: Record<string, unknown>,
  ): Record<string, unknown> {
    if (!Object.hasOwn(queryParams, "size")) {
      return queryParams;
    }

    return {
      ...queryParams,
      size: this.clampSearchSize(queryParams.size),
    };
  }

  private enforceSearchRequestHardCap(
    body: Record<string, unknown>,
  ): Record<string, unknown> {
    if (!Object.hasOwn(body, "size")) {
      return body;
    }

    return {
      ...body,
      size: this.clampSearchSize(body.size),
    };
  }

  private clampSearchSize(value: unknown): number {
    const fallback = ELASTICSEARCH_READ_BUDGET.hardCap;
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return fallback;
    }

    const normalized = Math.trunc(value);
    if (normalized < 1) {
      return 1;
    }

    return Math.min(normalized, ELASTICSEARCH_READ_BUDGET.hardCap);
  }
}
