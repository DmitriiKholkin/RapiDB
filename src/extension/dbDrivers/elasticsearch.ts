import type { ConnectionOptions as TlsConnectionOptions } from "node:tls";
import { Client } from "@elastic/elasticsearch";
import { HttpConnection } from "@elastic/transport";
import {
  isConnectionTlsEnabled,
  resolveConnectionTlsMode,
} from "../../shared/connectionConfig";
import { ELASTICSEARCH_READ_BUDGET } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import { getSshHttpAgentTransport } from "../driverRuntimeConfig";
import { resolveConnectionTlsSettings } from "../services/connectionTls";
import { createOwnedSshConnectionAgentFactory } from "../services/sshConnectionAgent";
import { allowReadOnlyQuery, denyReadOnlyQuery } from "../utils/readOnlyGuards";
import {
  formatDatetimeForDisplay,
  normalizeSqlDatetimeOffsetSpacing,
} from "./BaseDBDriver";
import { deleteRowsSequentially } from "./deleteOutcomes";
import {
  drainElasticsearchCleanup,
  ElasticsearchPitSession,
} from "./elasticsearchPitSession";
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
  type DriverTimeoutSettingsProvider,
  getDefaultDriverTimeoutSettings,
} from "./timeout";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverDeleteRowsRequest,
  DriverEntityManifest,
  DriverInsertRowRequest,
  DriverMutationResult,
  DriverOperationContext,
  DriverTableExportChunk,
  DriverTableExportRequest,
  DriverTablePageRequest,
  DriverTablePageResult,
  DriverUpdateRowOutcome,
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
import {
  completeDriverUpdateRowsResult,
  failedDriverUpdateRows,
} from "./updateRowOutcomes";

const ELASTICSEARCH_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "not_applicable",
    triggers: "not_applicable",
  },
};

type ElasticsearchRestMethod = "GET" | "POST" | "PUT" | "DELETE";

class SshTlsHttpConnection extends HttpConnection {
  override buildRequestObject(
    ...args: Parameters<HttpConnection["buildRequestObject"]>
  ): ReturnType<HttpConnection["buildRequestObject"]> {
    const request = super.buildRequestObject(...args);
    // HttpConnection normally puts TLS settings on its own HTTPS agent. With
    // the SSH agent factory it does neither that nor per-request TLS options.
    // Keep settings request-local and use a connection-owned socket pool.
    return this.url.protocol === "https:"
      ? { ...request, ...this.tls }
      : request;
  }
}

const ELASTICSEARCH_READ_ONLY_QUERY_REASON =
  "[RapiDB] Read-only Elasticsearch connections allow only GET requests and POST _search requests.";
const ELASTICSEARCH_COMPATIBILITY_HEADERS = {
  accept: "application/vnd.elasticsearch+json; compatible-with=8",
  "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
} as const;

interface ElasticsearchRestCommand {
  method: ElasticsearchRestMethod;
  path: string;
  pathSegments: string[];
  queryParams: URLSearchParams;
  body?: unknown;
}

export class ElasticsearchDriver implements IDBDriver {
  private client: Client | null = null;
  private connected = false;
  private readonly pitReaders = new Set<ElasticsearchPitSession>();
  private connectionEpoch = 0;
  private connectionAbortController?: AbortController;

  constructor(
    private readonly config: ConnectionConfig,
    private readonly timeoutSettingsProvider: DriverTimeoutSettingsProvider = getDefaultDriverTimeoutSettings,
  ) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const epoch = ++this.connectionEpoch;
    this.connectionAbortController?.abort();
    const abortController = new AbortController();
    this.connectionAbortController = abortController;
    let client: Client | undefined;
    try {
      const protocol = isConnectionTlsEnabled(
        resolveConnectionTlsMode(this.config),
      )
        ? "https"
        : "http";
      const node =
        this.config.connectionUri ??
        this.config.endpoint ??
        `${protocol}://${this.config.host || "localhost"}:${this.config.port ?? 9200}`;
      const sshAgentTransport = getSshHttpAgentTransport(this.config);
      const tlsSettings = await resolveConnectionTlsSettings(
        this.config,
        abortController.signal,
      );
      this.assertConnectionEpoch(epoch);
      const createOwnedAgent = sshAgentTransport
        ? await createOwnedSshConnectionAgentFactory()
        : undefined;
      this.assertConnectionEpoch(epoch);
      client = new Client({
        ...(sshAgentTransport ? { Connection: SshTlsHttpConnection } : {}),
        node: this.config.cloudId ? undefined : node,
        headers: ELASTICSEARCH_COMPATIBILITY_HEADERS,
        cloud: this.config.cloudId
          ? {
              id: this.config.cloudId,
            }
          : undefined,
        agent:
          sshAgentTransport && createOwnedAgent
            ? (options: { url: URL; tls?: TlsConnectionOptions | null }) =>
                createOwnedAgent(
                  options.url.protocol === "https:"
                    ? sshAgentTransport.httpsAgent
                    : sshAgentTransport.httpAgent,
                  options.url.protocol === "https:",
                  options.tls,
                )
            : undefined,
        auth: this.config.username
          ? {
              username: this.config.username,
              password: this.config.password ?? "",
            }
          : this.config.apiKey
            ? {
                apiKey: this.config.apiKey,
              }
            : undefined,
        tls: tlsSettings
          ? {
              rejectUnauthorized: tlsSettings.rejectUnauthorized,
              ca: tlsSettings.ca,
              cert: tlsSettings.cert,
              key: tlsSettings.key,
              passphrase: tlsSettings.passphrase,
              servername: tlsSettings.servername,
              checkServerIdentity: tlsSettings.checkServerIdentity,
            }
          : undefined,
      });
      this.client = client;
      this.assertConnectionEpoch(epoch);
      await client.ping();
      this.assertConnectionEpoch(epoch);
      this.connected = true;
    } catch (error) {
      if (this.client === client && client) {
        this.client = null;
        this.connected = false;
        await client.close().catch(() => undefined);
      }
      throw error;
    } finally {
      if (this.connectionAbortController === abortController) {
        this.connectionAbortController = undefined;
      }
    }
  }

  private assertConnectionEpoch(epoch: number): void {
    if (epoch !== this.connectionEpoch) {
      throw new DOMException(
        "Elasticsearch connection attempt cancelled",
        "AbortError",
      );
    }
  }

  async disconnect(): Promise<void> {
    this.connectionEpoch += 1;
    this.connectionAbortController?.abort();
    this.connectionAbortController = undefined;
    const client = this.client;
    this.client = null;
    this.connected = false;
    // Logical cancellation does not depend on a stuck physical request. Drain
    // cleanup briefly, then close the transport independently of that request.
    await drainElasticsearchCleanup(
      Promise.allSettled([...this.pitReaders].map((reader) => reader.close())),
      100,
    );
    if (client)
      await drainElasticsearchCleanup(
        Promise.resolve().then(() => client.close()),
        1000,
      );
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

  async getRoutineDefinition(
    _database?: string,
    _schema?: string,
    _name?: string,
    _kind?: "function" | "procedure",
    _routineIdentity?: string,
  ): Promise<string> {
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
    const commands = this.parseRestCommands(trimmed);
    if (commands.length === 0) {
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      };
    }

    return this.executeRestCommands(commands, startedAt);
  }

  private parseRestCommands(input: string): ElasticsearchRestCommand[] {
    return this.splitRestStatements(input).map((statement) => {
      const command = this.parseRestCommand(statement);
      if (!command) {
        throw this.buildInvalidRestCommandError();
      }
      return command;
    });
  }

  private async executeRestCommands(
    commands: readonly ElasticsearchRestCommand[],
    startedAt: number,
  ): Promise<QueryResult> {
    if (commands.length === 1) {
      const [command] = commands;
      return this.executeRestCommand(command, startedAt);
    }

    const results: Array<Record<string, unknown>> = [];
    let affectedRows = 0;
    for (const command of commands) {
      const result = await this.executeRestCommand(command, startedAt);
      affectedRows += result.affectedRows ?? 0;
      results.push({
        statement: this.formatParsedRestCommand(command),
        rowCount: result.rowCount,
        affectedRows: result.affectedRows,
        rows: this.toPlainRows(result),
      });
    }
    return this.buildMultiStatementQueryResult(
      results,
      startedAt,
      affectedRows,
    );
  }

  private buildMultiStatementQueryResult(
    results: Array<Record<string, unknown>>,
    startedAt: number,
    affectedRows: number,
  ): QueryResult {
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

  private commandBodyAsObject(
    body: unknown,
  ): Record<string, unknown> | undefined {
    return body && typeof body === "object" && !Array.isArray(body)
      ? (body as Record<string, unknown>)
      : undefined;
  }

  private formatParsedRestCommand(command: ElasticsearchRestCommand): string {
    const queryString = command.queryParams.toString();
    const path =
      queryString.length > 0 ? `${command.path}?${queryString}` : command.path;
    return this.formatRestCommand(
      command.method,
      path,
      this.commandBodyAsObject(command.body),
    );
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
    request.signal?.throwIfAborted();
    const startTime = performance.now();
    const columns = this.describeIndexColumnsSync();
    const columnMetaByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    const canDelegateToServer =
      request.filters.length === 0 && request.sort === null;

    if (canDelegateToServer) {
      const offset = Math.max(0, (request.page - 1) * request.pageSize);
      const size = this.clampSearchSize(request.pageSize);
      const rows: Record<string, unknown>[] = [];
      let totalCount = 0;
      let seen = 0;
      // A fresh snapshot per page request. Walk to arbitrary offsets using
      // the same unique PIT order as export, without a result-window limit.
      for await (const batch of this.readPitBatches(
        request.table,
        Math.min(size, 500),
        !request.skipCount,
        request.signal,
        request.deadline ??
          Date.now() + this.timeoutSettingsProvider().dbOperationTimeoutMs,
      )) {
        totalCount = batch.totalCount;
        const start = Math.max(0, offset - seen);
        rows.push(...batch.rows.slice(start, start + size - rows.length));
        seen += batch.rows.length;
        if (rows.length === size) break;
      }
      return {
        columns,
        rows,
        totalCount,
        executionTimeMs: Math.round(performance.now() - startTime),
      };
    }

    const client = this.requireClient();
    // Reuse the bounded read lifetime without opening a PIT: this path must
    // still materialize the entire index in exactly one capped search.
    const reader = new ElasticsearchPitSession(
      client,
      this.timeoutSettingsProvider().dbOperationTimeoutMs,
      "readTablePage",
      request.signal,
      request.deadline ?? Infinity,
      () => this.pitReaders.delete(reader),
    );
    this.pitReaders.add(reader);
    try {
      return await reader.advance(async () => {
        const response = await reader.request((options) =>
          client.search(
            {
              index: request.table,
              query: { match_all: {} },
              sort: ["_doc"],
              size: ELASTICSEARCH_READ_BUDGET.hardCap,
              track_total_hits: true,
            },
            { ...options, maxRetries: 0 },
          ),
        );
        const rows = this.hitsToRows(response.hits.hits);
        if (
          this.resolveElasticsearchTotalCount(response.hits.total) > rows.length
        ) {
          throw new Error(
            `Elasticsearch filtering or sorting requires reading the entire index and exceeded the ${ELASTICSEARCH_READ_BUDGET.hardCap}-row safety limit. Use an unfiltered, unsorted read or export.`,
          );
        }
        const filtered = applyFilters(rows, request.filters, columns);
        const sorted = applySort(filtered, request.sort, columns);
        const paged = pageRows(sorted, request.page, request.pageSize).map(
          (row) =>
            Object.fromEntries(
              Object.entries(row).map(([columnName, value]) => {
                const column = columnMetaByName.get(columnName);
                return [
                  columnName,
                  column ? this.formatOutputValue(value, column) : value,
                ];
              }),
            ),
        );
        reader.check();
        return {
          columns,
          rows: paged,
          totalCount: request.skipCount ? 0 : sorted.length,
          executionTimeMs: Math.round(performance.now() - startTime),
        };
      });
    } finally {
      // Logical cancellation must not wait for an uncooperative transport.
      // The bounded drain removes the reader even if its search never settles.
      if (reader.signal.aborted) void reader.close().catch(() => undefined);
      else await reader.waitForClose();
    }
  }

  async *exportTableChunks(
    request: DriverTableExportRequest,
    signal?: AbortSignal,
  ): AsyncGenerator<DriverTableExportChunk> {
    const columns = this.describeIndexColumnsSync();
    for await (const batch of this.readPitBatches(
      request.table,
      request.chunkSize,
      false,
      signal,
    )) {
      yield { columns, rows: batch.rows };
    }
  }

  private async *readPitBatches(
    table: string,
    requestedSize: number,
    trackCount: boolean,
    signal?: AbortSignal,
    pageDeadline = Infinity,
  ): AsyncGenerator<{ rows: Record<string, unknown>[]; totalCount: number }> {
    signal?.throwIfAborted();
    const client = this.requireClient();
    const reader = new ElasticsearchPitSession(
      client,
      this.timeoutSettingsProvider().dbOperationTimeoutMs,
      pageDeadline === Infinity ? "exportTableChunks" : "readTablePage",
      signal,
      pageDeadline,
      () => this.pitReaders.delete(reader),
    );
    this.pitReaders.add(reader);
    let failed = false;
    try {
      let size = this.clampSearchSize(requestedSize);
      let initialized = false;
      let unknownWindow = false;
      let cursor: number | undefined;
      let totalCount = 0;
      let first = true;
      while (true) {
        const response = await reader.advance(async () => {
          if (!initialized) {
            try {
              const settings = await reader.request((options) =>
                client.indices.getSettings(
                  {
                    index: table,
                    name: "index.max_result_window",
                    include_defaults: true,
                  },
                  options,
                ),
              );
              for (const entry of Object.values(settings)) {
                const window = Number(
                  entry.settings?.index?.max_result_window ??
                    entry.defaults?.index?.max_result_window ??
                    ELASTICSEARCH_READ_BUDGET.hardCap,
                );
                if (!Number.isSafeInteger(window) || window < 1)
                  throw new Error(
                    "Invalid Elasticsearch max_result_window setting.",
                  );
                size = Math.min(size, window);
              }
            } catch (error) {
              // PIT/search permission does not imply view_index_metadata.
              // Only a metadata 403 permits adaptive window discovery.
              if (this.elasticsearchStatus(error) !== 403) throw error;
              unknownWindow = true;
            }
            await reader.request(
              (options) =>
                client.openPointInTime(
                  {
                    index: table,
                    keep_alive: "1m",
                    allow_partial_search_results: false,
                  },
                  options,
                ),
              (result) => reader.recordPitId(result.id),
            );
            initialized = true;
          }
          while (true) {
            reader.check();
            const pitId = reader.pitId;
            if (!pitId)
              throw new Error("Elasticsearch returned an invalid PIT ID.");
            try {
              return await reader.request(
                (options) =>
                  client.search(
                    {
                      pit: { id: pitId, keep_alive: "1m" },
                      query: { match_all: {} },
                      sort: [{ _shard_doc: "asc" }],
                      size,
                      search_after: cursor === undefined ? undefined : [cursor],
                      track_total_hits: trackCount && first,
                      allow_partial_search_results: false,
                    },
                    options,
                  ),
                (result) => {
                  if (result.pit_id) reader.recordPitId(result.pit_id);
                },
              );
            } catch (error) {
              if (
                !unknownWindow ||
                size === 1 ||
                !this.isResultWindowError(error)
              )
                throw error;
              // Retry the same cursor; rejected window requests emit no rows.
              size = Math.max(1, Math.floor(size / 2));
            }
          }
        });
        reader.check();
        if (response.timed_out || (response._shards?.failed ?? 0) > 0) {
          throw new Error(
            "Elasticsearch PIT search returned incomplete results.",
          );
        }
        if (first && trackCount) {
          totalCount = this.resolveElasticsearchTotalCount(response.hits.total);
        }
        const hits = response.hits.hits;
        // _shard_doc is unique and stable within this PIT. Validate every hit
        // so malformed/repeated/backwards cursors fail closed, never dedup by _id.
        for (const hit of hits) {
          const next = hit.sort?.[0];
          if (
            hit.sort?.length !== 1 ||
            typeof next !== "number" ||
            !Number.isSafeInteger(next) ||
            next < 0 ||
            (cursor !== undefined && next <= cursor)
          ) {
            throw new Error(
              "Elasticsearch PIT cursor is malformed or made no progress.",
            );
          }
          cursor = next;
        }
        if (hits.length > size) {
          throw new Error(
            "Elasticsearch PIT search exceeded the requested batch size.",
          );
        }
        if (hits.length > 0 || first) {
          yield {
            rows: this.hitsToRows(hits),
            totalCount,
          };
          reader.check();
        }
        first = false;
        if (hits.length < size) break;
      }
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      const wasAborted = reader.signal.aborted;
      // A cancellation/deadline must settle even if physical cleanup is stuck.
      if (failed && wasAborted) void reader.close().catch(() => undefined);
      else
        await reader.waitForClose().catch((error: unknown) => {
          // Preserve the primary failure; a successful export must surface a
          // cleanup failure before it can replace the destination file.
          if (!failed) throw error;
        });
    }
  }

  private elasticsearchStatus(error: unknown): number | undefined {
    return (error as { meta?: { statusCode?: number } } | null)?.meta
      ?.statusCode;
  }

  private isResultWindowError(error: unknown): boolean {
    const detail = (
      error as {
        meta?: {
          body?: {
            error?: {
              reason?: unknown;
              root_cause?: Array<{ reason?: unknown }>;
            };
          };
        };
      } | null
    )?.meta?.body?.error;
    const reasons = [
      detail?.reason,
      ...(detail?.root_cause ?? []).map((cause) => cause.reason),
    ];
    return (
      this.elasticsearchStatus(error) === 400 &&
      reasons.some(
        (reason) =>
          typeof reason === "string" &&
          /result window is too large/i.test(reason),
      )
    );
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

  async updateRows(
    request: DriverUpdateRowsRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    let affectedRows = 0;
    const updateRowOutcomes: DriverUpdateRowOutcome[] = [];
    for (const [rowIndex, update] of request.updates.entries()) {
      let writeMayHaveApplied = false;
      try {
        context?.signal.throwIfAborted();
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
          updateRowOutcomes.push({ rowIndex, status: "not_applied" });
          continue;
        }
        const document = this.resolveDocumentForMutation(
          {
            ...update.primaryKeys,
            ...update.changes,
          },
          update.changes,
        );
        const client = this.requireClient();
        writeMayHaveApplied = true;
        const response = Object.hasOwn(update.originalValues ?? {}, "_source")
          ? await client.update(
              {
                index: request.table,
                id: String(id),
                script: {
                  lang: "painless",
                  source:
                    "if (ctx._source != params.original) { ctx.op = 'none' } else { ctx._source = params.next }",
                  params: {
                    original: this.resolveDocumentForMutation({
                      _source: update.originalValues?._source,
                    }),
                    next: document,
                  },
                },
                refresh: "wait_for",
              },
              context ? { signal: context.signal } : undefined,
            )
          : Object.hasOwn(update.changes, "_source")
            ? await client.index(
                {
                  index: request.table,
                  id: String(id),
                  document,
                  refresh: "wait_for",
                },
                context ? { signal: context.signal } : undefined,
              )
            : await client.update(
                {
                  index: request.table,
                  id: String(id),
                  doc: document,
                  refresh: "wait_for",
                },
                context ? { signal: context.signal } : undefined,
              );
        writeMayHaveApplied = false;
        if (response.result === "noop") {
          updateRowOutcomes.push({ rowIndex, status: "not_applied" });
        } else if (
          response.result === "updated" ||
          response.result === "created"
        ) {
          affectedRows++;
          updateRowOutcomes.push({ rowIndex, status: "applied" });
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

  async insertRow(
    request: DriverInsertRowRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    context?.signal.throwIfAborted();
    const id = request.values._id;
    const document = this.resolveDocumentForMutation(request.values);
    await this.requireClient().index(
      {
        index: request.table,
        id:
          typeof id === "string" || typeof id === "number"
            ? String(id)
            : undefined,
        document,
        op_type:
          typeof id === "string" || typeof id === "number"
            ? "create"
            : undefined,
        refresh: "wait_for",
      },
      context ? { signal: context.signal } : undefined,
    );
    return { affectedRows: 1 };
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    return deleteRowsSequentially(
      request.primaryKeyValuesList,
      context,
      (entry) => {
        const id = entry._id;
        if (typeof id !== "string" && typeof id !== "number") {
          throw new Error("Elasticsearch delete requires an _id.");
        }
        const client = this.requireClient();
        return async () => {
          try {
            const result = await client.delete(
              {
                index: request.table,
                id: String(id),
                refresh: "wait_for",
              },
              context ? { signal: context.signal } : undefined,
            );
            return result.result === "deleted"
              ? "deleted"
              : result.result === "not_found"
                ? "notfound"
                : "unknown";
          } catch (error) {
            // A 404 is only row evidence when ES returned an explicit not_found
            // document result (an absent index or proxy 404 is not sufficient).
            const body = (error as { meta?: { body?: { result?: string } } })
              ?.meta?.body;
            if (body?.result === "not_found") return "notfound";
            throw error;
          }
        };
      },
    );
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
      return this.buildInsertMutationPreviewStatement(table, data.values ?? {});
    }
    if (operation === "update") {
      return this.buildUpdateMutationPreviewStatement(
        table,
        data.primaryKeys ?? {},
        data.changes ?? {},
      );
    }
    return this.buildDeleteMutationPreviewStatement(
      table,
      data.primaryKeys ?? data.primaryKeyValuesList?.[0] ?? {},
    );
  }

  private buildInsertMutationPreviewStatement(
    table: string,
    values: Record<string, unknown>,
  ): string {
    const id = values._id;
    return this.formatRestCommand(
      id !== undefined ? "PUT" : "POST",
      `/${encodeURIComponent(table)}/_doc${
        id !== undefined ? `/${encodeURIComponent(String(id))}` : ""
      }?${new URLSearchParams({
        ...(id !== undefined ? { op_type: "create" } : {}),
        refresh: "wait_for",
      }).toString()}`,
      this.resolveDocumentForMutation(values),
    );
  }

  private buildUpdateMutationPreviewStatement(
    table: string,
    primaryKeys: Record<string, unknown>,
    changes: Record<string, unknown>,
  ): string {
    const id = primaryKeys._id;
    return this.formatRestCommand(
      "PUT",
      `/${encodeURIComponent(table)}/_doc/${encodeURIComponent(
        id !== undefined ? String(id) : "<id>",
      )}?refresh=wait_for`,
      this.resolveDocumentForMutation(
        {
          ...primaryKeys,
          ...changes,
        },
        changes,
      ),
    );
  }

  private buildDeleteMutationPreviewStatement(
    table: string,
    primaryKeys: Record<string, unknown>,
  ): string {
    const id = primaryKeys._id;
    return this.formatRestCommand(
      "DELETE",
      `/${encodeURIComponent(table)}/_doc/${encodeURIComponent(
        id !== undefined ? String(id) : "<id>",
      )}?refresh=wait_for`,
    );
  }

  async runTransaction(operations: TransactionOperation[]): Promise<void> {
    // Elasticsearch has no transactions; sequential requests are not atomic.
    if (operations.length > 1) {
      throw new Error(
        "[RapiDB] Elasticsearch driver does not support atomic multi-operation transactions. Apply one row at a time.",
      );
    }
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
    if (column.name === "_source") {
      if (typeof value === "string") {
        const trimmed = value.trim();
        if (!trimmed) {
          return {};
        }
        try {
          const parsed = JSON.parse(trimmed) as unknown;
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
            throw new Error();
          }
          return parsed;
        } catch {
          throw new Error(
            'Elasticsearch field "_source" must be a valid JSON object.',
          );
        }
      }

      if (!value || typeof value !== "object" || Array.isArray(value)) {
        throw new Error(
          'Elasticsearch field "_source" must be a valid JSON object.',
        );
      }

      return value;
    }

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

  private async describeIndexColumns(
    _table: string,
  ): Promise<ColumnTypeMeta[]> {
    return this.describeIndexColumnsSync();
  }

  private describeIndexColumnsSync(): ColumnTypeMeta[] {
    const idCategory: TypeCategory = "text";
    const sourceCategory: TypeCategory = "json";
    return [
      {
        name: "_id",
        type: "text",
        nativeType: "text",
        category: idCategory,
        nullable: false,
        defaultValue: undefined,
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
        isForeignKey: false,
        filterable: true,
        filterOperators: resolveFilterOperators(idCategory, {
          filterable: true,
          nullable: false,
        }),
        valueSemantics: "plain",
      },
      {
        name: "_source",
        type: "json",
        nativeType: "json",
        category: sourceCategory,
        nullable: false,
        defaultValue: undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: false,
        filterable: true,
        filterOperators: resolveFilterOperators(sourceCategory, {
          filterable: true,
          nullable: false,
        }),
        valueSemantics: "plain",
      },
    ];
  }

  private hitsToRows(
    hits: ReadonlyArray<{ _id?: unknown; _source?: unknown }>,
  ): Record<string, unknown>[] {
    return hits.map((hit) => {
      const source = this.normalizeSourceDocument(hit._source);
      return {
        _id: hit._id,
        _source: JSON.stringify(source),
      };
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

  private resolveDocumentForMutation(
    record: Record<string, unknown>,
    changes?: Record<string, unknown>,
  ): Record<string, unknown> {
    if (
      changes &&
      Object.hasOwn(changes, "_source") &&
      Object.keys(changes).some((key) => key !== "_source")
    ) {
      throw new Error(
        'Elasticsearch updates cannot mix "_source" with other changed fields.',
      );
    }

    if (Object.hasOwn(record, "_source")) {
      return this.normalizeSourceDocument(record._source);
    }

    return this.stripDocumentId(record);
  }

  private normalizeSourceDocument(source: unknown): Record<string, unknown> {
    if (typeof source === "string") {
      const trimmed = source.trim();
      if (!trimmed) {
        return {};
      }
      const parsed = JSON.parse(trimmed) as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error(
          'Elasticsearch field "_source" must be a valid JSON object.',
        );
      }
      return parsed as Record<string, unknown>;
    }

    if (!source) {
      return {};
    }

    if (typeof source !== "object" || Array.isArray(source)) {
      throw new Error(
        'Elasticsearch field "_source" must be a valid JSON object.',
      );
    }

    return source as Record<string, unknown>;
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
