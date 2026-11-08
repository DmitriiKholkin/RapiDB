import { createHmac, randomBytes } from "node:crypto";
import type { ConnectionOptions as TlsConnectionOptions } from "node:tls";
import { Client, errors } from "@elastic/elasticsearch";
import { HttpConnection } from "@elastic/transport";
import {
  getElasticsearchTlsEndpointError,
  isConnectionTlsEnabled,
  resolveConnectionTlsMode,
} from "../../shared/connectionConfig";
import { ELASTICSEARCH_READ_BUDGET } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import {
  getSshHttpAgentTransport,
  getTlsServername,
} from "../driverRuntimeConfig";
import { resolveConnectionTlsSettings } from "../services/connectionTls";
import { createOwnedSshConnectionAgentFactory } from "../services/sshConnectionAgent";
import {
  isRoundTripJsonNumberToken,
  JsonNumericPrecisionError,
  parseJsonForWrite,
} from "../utils/jsonWriteSafety";
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
  ElasticsearchSerializer,
  stringifyElasticsearchJson,
} from "./elasticsearchSerializer";
import { getMetadataContext } from "./metadataCancellation";
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
  getQueryRemainingTimeoutMs,
  throwIfQueryCancelled,
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
  QueryExecutionOptions,
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

interface ElasticsearchMutationIdentity {
  index: string;
  id: string;
  routing?: string;
  if_seq_no: number;
  if_primary_term: number;
}

export class ElasticsearchDriver implements IDBDriver {
  private client: Client | null = null;
  private connected = false;
  private readonly pitReaders = new Set<ElasticsearchPitSession>();
  private connectionEpoch = 0;
  private connectionAbortController?: AbortController;
  private readonly identitySecret = randomBytes(32);

  constructor(
    private readonly config: ConnectionConfig,
    private readonly timeoutSettingsProvider: DriverTimeoutSettingsProvider = getDefaultDriverTimeoutSettings,
  ) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const endpointError = getElasticsearchTlsEndpointError(this.config);
    if (endpointError) throw new Error(endpointError);
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
        `${protocol}://${this.formatUrlHost(this.config.host || "localhost")}:${this.config.port ?? 9200}`;
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
        Serializer: ElasticsearchSerializer,
        maxRetries: 0,
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
          : getTlsServername(this.config)
            ? { servername: getTlsServername(this.config) }
            : undefined,
      });
      this.client = client;
      this.assertConnectionEpoch(epoch);
      if (!(await client.ping())) {
        throw new Error(
          "Elasticsearch ping failed: the endpoint did not respond as an Elasticsearch cluster.",
        );
      }
      this.assertConnectionEpoch(epoch);
      this.connected = true;
    } catch (error) {
      if (client) {
        if (this.client === client) {
          this.client = null;
          this.connected = false;
        }
        await client.close().catch(() => undefined);
      }
      throw error;
    } finally {
      if (this.connectionAbortController === abortController) {
        this.connectionAbortController = undefined;
      }
    }
  }

  private formatUrlHost(host: string): string {
    return host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
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
      schemaMutationGuard: (queryText: string) =>
        this.mayChangeDiscoverableIndices(queryText),
      editorPresentation: {
        formatOnOpen: false,
        editorLanguage: "plaintext" as const,
      },
    };
  }

  private mayChangeDiscoverableIndices(queryText: string): boolean {
    try {
      return this.parseRestCommands(queryText).some((command) => {
        const [index, endpoint] = command.pathSegments;
        if (
          command.pathSegments.length === 1 &&
          (command.method === "PUT" || command.method === "DELETE")
        ) {
          return true;
        }

        if (command.method === "POST" && index === "_aliases") {
          return true;
        }

        if (
          index &&
          (command.method === "PUT" || command.method === "POST") &&
          endpoint === "_doc" &&
          (command.pathSegments.length === 2 ||
            command.pathSegments.length === 3)
        ) {
          return true;
        }

        if (
          index &&
          command.method === "POST" &&
          endpoint === "_update" &&
          command.pathSegments.length === 3
        ) {
          return true;
        }

        return (
          command.method === "PUT" &&
          command.pathSegments.length === 2 &&
          (endpoint === "_mapping" || endpoint === "_settings")
        );
      });
    } catch {
      return false;
    }
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

  private metadataTransportOptions(): [] | [{ signal: AbortSignal }] {
    const metadata = getMetadataContext();
    return metadata ? [{ signal: metadata.signal }] : [];
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    return [{ name: "default", schemas: [] }];
  }

  async listSchemas(): Promise<SchemaInfo[]> {
    return [{ name: "indices" }];
  }

  async listObjects(): Promise<TableInfo[]> {
    const response = await this.requireClient().indices.resolveIndex(
      {
        name: "*",
        expand_wildcards: ["open", "closed", "hidden"],
        allow_no_indices: true,
        ignore_unavailable: true,
      },
      ...this.metadataTransportOptions(),
    );
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
      await this.requireClient().indices.get(
        { index: table },
        ...this.metadataTransportOptions(),
      );
      return [
        {
          name: `${table}_id_idx`,
          columns: ["_id", "_identity"],
          unique: true,
          primary: true,
        },
      ];
    } catch (error: unknown) {
      const detail = (
        error as {
          meta?: { body?: { error?: { type?: unknown } } };
        } | null
      )?.meta?.body?.error;
      if (
        this.elasticsearchStatus(error) === 404 &&
        detail?.type === "index_not_found_exception"
      ) {
        return [];
      }
      throw error;
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
    const response = await this.requireClient().indices.get(
      { index: table },
      ...this.metadataTransportOptions(),
    );
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

  async query(
    sql: string,
    _params?: unknown[],
    options?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    throwIfQueryCancelled(options);
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

    return this.executeRestCommands(commands, startedAt, options);
  }

  private parseRestCommands(input: string): ElasticsearchRestCommand[] {
    return this.splitRestStatements(input).map((statement) => {
      const command = this.parseRestCommand(statement);
      if (!command) {
        throw this.buildInvalidRestCommandError();
      }
      this.validateRestCommand(command);
      return command;
    });
  }

  private async executeRestCommands(
    commands: readonly ElasticsearchRestCommand[],
    startedAt: number,
    options?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    if (commands.length === 1) {
      const [command] = commands;
      const result = await this.executeRestCommand(command, startedAt, options);
      throwIfQueryCancelled(options);
      return result;
    }

    const results: Array<Record<string, unknown>> = [];
    let affectedRows = 0;
    let mutated = false;
    let partialError: string | undefined;
    const total = commands.length;
    const completedText = (count: number) =>
      `${count} of ${total} statement${total === 1 ? "" : "s"} ${count === 1 ? "was" : "were"} completed and earlier writes were applied`;
    for (const [index, command] of commands.entries()) {
      let result: QueryResult;
      try {
        result = await this.executeRestCommand(command, startedAt, options);
      } catch (error: unknown) {
        if (!mutated) throw error;
        const message = error instanceof Error ? error.message : String(error);
        const statement = `statement ${index + 1} of ${total}`;
        partialError = options?.signal?.aborted
          ? `Elasticsearch script was cancelled during ${statement} after ${completedText(index)}. The outcome of ${statement} is unknown and the remaining statements were not sent.`
          : error instanceof errors.ResponseError &&
              error.statusCode !== undefined &&
              error.statusCode >= 400 &&
              error.statusCode < 500
            ? `Elasticsearch ${statement} failed after ${completedText(index)}. It was not applied and the remaining statements were not sent. ${message}`
            : `Elasticsearch ${statement} failed after ${completedText(index)}. Its outcome is unknown and the remaining statements were not sent. ${message}`;
        break;
      }
      mutated ||= !this.isReadOnlyRestCommand(command);
      try {
        throwIfQueryCancelled(options);
      } catch (error: unknown) {
        if (!mutated) throw error;
        partialError = `Elasticsearch script was cancelled after ${completedText(index + 1)}.${index + 1 < total ? " The remaining statements were not sent." : ""}`;
      }
      affectedRows += result.affectedRows ?? 0;
      results.push({
        statement: this.formatParsedRestCommand(command),
        rowCount: result.rowCount,
        affectedRows: result.affectedRows,
        ...(result.resultSets
          ? {
              resultSets: result.resultSets.map((resultSet) => ({
                id: resultSet.id,
                rows: this.toPlainRows(resultSet),
              })),
            }
          : { rows: this.toPlainRows(result) }),
      });
      if (partialError !== undefined) break;
    }
    return {
      ...this.buildMultiStatementQueryResult(results, startedAt, affectedRows),
      ...(partialError !== undefined ? { error: partialError } : {}),
    };
  }

  private buildMultiStatementQueryResult(
    results: Array<Record<string, unknown>>,
    startedAt: number,
    affectedRows: number,
  ): QueryResult {
    const row = flattenRootRecord({ results }, stringifyElasticsearchJson);
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
        const statementEnd = input[cursor - 1] === ";" ? cursor - 1 : cursor;
        statements.push(input.slice(statementStart, statementEnd).trim());
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
    const rawSegments = rawPath.split("?", 1)[0].split("/");
    if (
      !rawPath.startsWith("/") ||
      rawPath.startsWith("//") ||
      /[\\#\p{Cc}]/u.test(rawPath) ||
      Buffer.from(rawPath, "utf8").toString("utf8") !== rawPath ||
      rawSegments.some((segment) => {
        const decoded = decodeURIComponent(segment);
        return decoded === "." || decoded === "..";
      })
    ) {
      throw new Error(
        "Elasticsearch REST path must be an absolute path on the current connection, without dot segments, backslashes, fragments, or invalid Unicode. Encode literal document ID delimiters in the path.",
      );
    }
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
      body:
        bodyText.length > 0
          ? parseJsonForWrite(bodyText, isRoundTripJsonNumberToken)
          : undefined,
    };
  }

  private resolveRestCommandKind(command: ElasticsearchRestCommand) {
    const [first, second] = command.pathSegments;
    const length = command.pathSegments.length;
    const method = command.method;
    if (
      method === "GET" &&
      length === 2 &&
      (second === "_mapping" || second === "_settings")
    )
      return "metadata";
    if (method === "PUT" && length === 1) return "createIndex";
    if (method === "DELETE" && length === 1) return "deleteIndex";
    if (
      (method === "GET" || method === "POST") &&
      ((length === 2 && second === "_search") ||
        (length === 1 && first === "_search"))
    )
      return "search";
    if (method === "GET" && length === 3 && second === "_doc")
      return "getDocument";
    if (
      ((method === "PUT" && length === 3) ||
        (method === "POST" && (length === 2 || length === 3))) &&
      second === "_doc"
    )
      return "indexDocument";
    if (method === "POST" && length === 3 && second === "_update")
      return "updateDocument";
    if (method === "DELETE" && length === 3 && second === "_doc")
      return "deleteDocument";
    if (method === "PUT" && length === 2 && second === "_mapping")
      return "putMapping";
    if (method === "PUT" && length === 2 && second === "_settings")
      return "putSettings";
    if (method === "POST" && length === 1 && first === "_aliases")
      return "updateAliases";
    throw new Error(
      `Unsupported Elasticsearch REST command: ${command.method} ${command.path}`,
    );
  }

  private validateRestCommand(command: ElasticsearchRestCommand): void {
    const kind = this.resolveRestCommandKind(command);
    const label = `${command.method} ${command.path}`;
    if (
      ["metadata", "deleteIndex", "getDocument", "deleteDocument"].includes(
        kind,
      )
    ) {
      if (command.body !== undefined) {
        throw new Error(
          `${label} does not accept a JSON body; refusing to silently ignore it.`,
        );
      }
      return;
    }
    const body = this.readObjectBody(command, label, {
      allowEmpty: kind === "search",
    });
    if (kind === "indexDocument" && Object.hasOwn(body, "_id")) {
      throw new Error(
        `${label} cannot set the "_id" metadata field in the document body. Put the document ID in the path instead.`,
      );
    }
    if (kind === "search") {
      this.enforceSearchRequestHardCap(body);
      this.enforceSearchQueryParamsHardCap(
        this.queryParamsToObject(command.queryParams),
      );
    }
  }

  private async executeRestCommand(
    command: ElasticsearchRestCommand,
    startedAt: number,
    options?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    throwIfQueryCancelled(options);
    const requestTimeout = getQueryRemainingTimeoutMs(options);
    const transportOptions = {
      ...(options?.signal ? { signal: options.signal } : {}),
      ...(requestTimeout !== undefined ? { requestTimeout } : {}),
    };
    const transportArgs =
      Object.keys(transportOptions).length > 0
        ? ([transportOptions] as const)
        : ([] as const);
    const client = this.requireClient();
    const queryParams = this.queryParamsToObject(command.queryParams);
    const [firstSegment, secondSegment, thirdSegment] = command.pathSegments;
    const kind = this.resolveRestCommandKind(command);

    if (kind === "metadata") {
      const params = { index: firstSegment, querystring: queryParams };
      const response =
        secondSegment === "_mapping"
          ? await client.indices.getMapping(params, ...transportArgs)
          : await client.indices.getSettings(params, ...transportArgs);
      return this.buildRowsQueryResult(
        Object.entries(response).map(([index, metadata]) => ({
          index,
          ...metadata,
        })),
        startedAt,
      );
    }

    if (kind === "createIndex") {
      const response = await client.indices.create(
        {
          index: firstSegment,
          querystring: queryParams,
          body: this.readObjectBody(command, `PUT ${command.path}`),
        },
        ...transportArgs,
      );
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

    if (kind === "deleteIndex") {
      const response = await client.indices.delete(
        {
          index: firstSegment,
          querystring: queryParams,
        },
        ...transportArgs,
      );
      return this.buildSingleRowQueryResult(
        {
          acknowledged: response.acknowledged,
          index: firstSegment,
        },
        startedAt,
        1,
      );
    }

    if (kind === "search" && command.pathSegments.length === 2) {
      const body = this.enforceSearchRequestHardCap(
        this.readObjectBody(command, `${command.method} ${command.path}`, {
          allowEmpty: true,
        }),
      );
      const response = await client.search(
        {
          index: firstSegment,
          querystring: this.enforceSearchQueryParamsHardCap(queryParams),
          body,
        } as never,
        ...transportArgs,
      );
      this.assertCompleteSearch(response);
      return this.buildSearchQueryResult(response, firstSegment, startedAt);
    }

    if (kind === "search") {
      const body = this.enforceSearchRequestHardCap(
        this.readObjectBody(command, `${command.method} ${command.path}`, {
          allowEmpty: true,
        }),
      );
      const response = await client.search(
        {
          querystring: this.enforceSearchQueryParamsHardCap(queryParams),
          body,
        } as never,
        ...transportArgs,
      );
      this.assertCompleteSearch(response);
      return this.buildSearchQueryResult(response, undefined, startedAt);
    }

    if (kind === "getDocument") {
      const response = await client.get(
        {
          index: firstSegment,
          id: thirdSegment,
          querystring: queryParams,
        },
        ...transportArgs,
      );
      const row = flattenRootRecord(
        {
          _id: response._id,
          ...(((response as { _source?: Record<string, unknown> })._source ??
            {}) as Record<string, unknown>),
        },
        stringifyElasticsearchJson,
      );
      return this.buildRowsQueryResult([row], startedAt);
    }

    if (kind === "indexDocument") {
      const response = await client.index(
        {
          index: firstSegment,
          ...(thirdSegment ? { id: thirdSegment } : {}),
          document: this.readObjectBody(
            command,
            `${command.method} ${command.path}`,
          ),
          querystring: queryParams,
        },
        ...transportArgs,
      );
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

    if (kind === "updateDocument") {
      const body = this.readObjectBody(command, `POST ${command.path}`);
      const response = await client.update(
        {
          index: firstSegment,
          id: thirdSegment,
          querystring: queryParams,
          body,
        } as never,
        ...transportArgs,
      );
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

    if (kind === "deleteDocument") {
      const response = await client.delete(
        {
          index: firstSegment,
          id: thirdSegment,
          querystring: queryParams,
        },
        ...transportArgs,
      );
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

    if (kind === "putMapping") {
      const response = await client.indices.putMapping(
        {
          index: firstSegment,
          querystring: queryParams,
          body: this.readObjectBody(command, `PUT ${command.path}`),
        },
        ...transportArgs,
      );
      return this.buildSingleRowQueryResult(
        {
          acknowledged: response.acknowledged,
          index: firstSegment,
        },
        startedAt,
        1,
      );
    }

    if (kind === "putSettings") {
      const response = await client.indices.putSettings(
        {
          index: firstSegment,
          querystring: queryParams,
          body: this.readObjectBody(command, `PUT ${command.path}`),
        },
        ...transportArgs,
      );
      return this.buildSingleRowQueryResult(
        {
          acknowledged: response.acknowledged,
          index: firstSegment,
        },
        startedAt,
        1,
      );
    }

    if (kind === "updateAliases") {
      const response = await client.indices.updateAliases(
        {
          querystring: queryParams,
          body: this.readObjectBody(command, `POST ${command.path}`),
        },
        ...transportArgs,
      );
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
    const result: Record<string, unknown> = Object.create(null);
    for (const key of new Set(queryParams.keys())) {
      const values = queryParams.getAll(key);
      if (values.length === 1) {
        result[key] = values[0];
      } else if (values.length > 1) {
        result[key] = values;
      }
    }
    return result;
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

  private buildSearchQueryResult(
    response: {
      hits: {
        total?: unknown;
        hits: ReadonlyArray<{
          _index?: unknown;
          fields?: unknown;
          highlight?: unknown;
        }>;
      };
      aggregations?: unknown;
      suggest?: unknown;
    },
    target: string | undefined,
    startedAt: number,
  ): QueryResult {
    const hits = response.hits.hits;
    const includeIndex =
      target === undefined ||
      /[*,]/.test(target) ||
      hits.some(
        (hit) => typeof hit._index === "string" && hit._index !== target,
      );
    const rows = this.hitsToRows(hits).map((row, index) => {
      const hit = hits[index];
      return {
        ...(includeIndex
          ? { _id: row._id, _index: hit._index, _source: row._source }
          : row),
        ...(hit.fields !== undefined
          ? { fields: stringifyElasticsearchJson(hit.fields) }
          : {}),
        ...(hit.highlight !== undefined
          ? { highlight: stringifyElasticsearchJson(hit.highlight) }
          : {}),
      };
    });
    const hitsResult = this.buildRowsQueryResult(rows, startedAt);
    const hasSummary =
      response.aggregations !== undefined ||
      response.suggest !== undefined ||
      (hits.length === 0 && response.hits.total !== undefined);
    if (!hasSummary) return hitsResult;
    const summaryResult = this.buildSingleRowQueryResult(
      flattenRootRecord(
        {
          ...(response.hits.total !== undefined
            ? { total: response.hits.total }
            : {}),
          ...(response.aggregations !== undefined
            ? { aggregations: response.aggregations }
            : {}),
          ...(response.suggest !== undefined
            ? { suggest: response.suggest }
            : {}),
        },
        stringifyElasticsearchJson,
      ),
      startedAt,
    );
    if (hits.length === 0) return summaryResult;
    return {
      ...summaryResult,
      rowCount: summaryResult.rowCount + hitsResult.rowCount,
      resultSets: [
        {
          id: "summary",
          columns: summaryResult.columns,
          rows: summaryResult.rows,
          rowCount: summaryResult.rowCount,
        },
        {
          id: "hits",
          columns: hitsResult.columns,
          rows: hitsResult.rows,
          rowCount: hitsResult.rowCount,
        },
      ],
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

    const timeoutMs = this.timeoutSettingsProvider().dbOperationTimeoutMs;
    const deadline = Math.min(
      request.deadline ?? Infinity,
      Date.now() + timeoutMs,
    );
    const rows: Record<string, unknown>[] = [];
    for await (const batch of this.readPitBatches(
      request.table,
      500,
      true,
      request.signal,
      deadline,
      ELASTICSEARCH_READ_BUDGET.hardCap,
    )) {
      rows.push(...batch.rows);
    }
    const filtered = applyFilters(rows, request.filters, columns);
    const sorted = applySort(filtered, request.sort, columns);
    const paged = pageRows(sorted, request.page, request.pageSize).map((row) =>
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
    request.signal?.throwIfAborted();
    if (Date.now() >= deadline)
      throw new DriverTimeoutError("dbOperation", "readTablePage", timeoutMs);
    return {
      columns,
      rows: paged,
      totalCount: request.skipCount ? 0 : sorted.length,
      executionTimeMs: Math.round(performance.now() - startTime),
    };
  }

  async *exportTableChunks(
    request: DriverTableExportRequest,
    signal?: AbortSignal,
  ): AsyncGenerator<DriverTableExportChunk> {
    const columns = this.describeIndexColumnsSync();
    const filters = request.filters ?? [];
    const sort = request.sort ?? null;
    if (filters.length === 0 && sort === null) {
      for await (const batch of this.readPitBatches(
        request.table,
        request.chunkSize,
        false,
        signal,
      )) {
        yield { columns, rows: batch.rows };
      }
      return;
    }
    const rows: Record<string, unknown>[] = [];
    for await (const batch of this.readPitBatches(
      request.table,
      500,
      true,
      signal,
      Infinity,
      ELASTICSEARCH_READ_BUDGET.hardCap,
    )) {
      rows.push(...batch.rows);
    }
    const sorted = applySort(
      applyFilters(rows, filters, columns),
      sort,
      columns,
    );
    const chunkSize = Math.max(1, request.chunkSize);
    if (sorted.length === 0) yield { columns, rows: [] };
    for (let offset = 0; offset < sorted.length; offset += chunkSize) {
      signal?.throwIfAborted();
      yield { columns, rows: sorted.slice(offset, offset + chunkSize) };
    }
  }

  private async *readPitBatches(
    table: string,
    requestedSize: number,
    trackCount: boolean,
    signal?: AbortSignal,
    pageDeadline = Infinity,
    fullIndexSafetyCap?: number,
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
      let readCount = 0;
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
              if (this.elasticsearchStatus(error) !== 403) throw error;
              unknownWindow = true;
            }
            await reader.request(
              (options) =>
                client.openPointInTime(
                  {
                    index: table,
                    keep_alive: "1m",
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
                      seq_no_primary_term: true,
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
              size = Math.max(1, Math.floor(size / 2));
            }
          }
        });
        reader.check();
        this.assertCompleteSearch(response);
        if (first && trackCount) {
          totalCount = this.resolveElasticsearchTotalCount(response.hits.total);
          if (fullIndexSafetyCap !== undefined) {
            const total = response.hits.total;
            if (
              total === undefined ||
              (typeof total === "object" && total.relation !== "eq") ||
              !Number.isSafeInteger(totalCount) ||
              totalCount < 0
            ) {
              throw new Error(
                "Elasticsearch full-index read requires an exact total hit count.",
              );
            }
          }
        }
        const hits = response.hits.hits;
        readCount += hits.length;
        if (fullIndexSafetyCap !== undefined) {
          if (
            totalCount > fullIndexSafetyCap ||
            readCount > fullIndexSafetyCap
          ) {
            throw new Error(
              `Elasticsearch filtering or sorting requires reading the entire index and exceeded the ${fullIndexSafetyCap}-row safety limit. Use an unfiltered, unsorted read or export.`,
            );
          }
          if (hits.length < size && readCount !== totalCount) {
            throw new Error(
              "Elasticsearch full-index read returned incomplete results (hit count mismatch).",
            );
          }
        }
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
            rows: this.hitsToRows(hits, table),
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
      if (failed && wasAborted) void reader.close().catch(() => undefined);
      else
        await reader.waitForClose().catch((error: unknown) => {
          if (!failed) throw error;
        });
    }
  }

  private assertCompleteSearch(response: {
    timed_out?: boolean;
    _shards?: { failed?: number };
  }): void {
    if (response.timed_out || (response._shards?.failed ?? 0) > 0) {
      throw new Error(
        "Elasticsearch search returned incomplete results (server timeout or shard failure). Retry after resolving the cluster error.",
      );
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
    for (const [rowIndex, update] of request.updates.entries()) {
      try {
        this.assertConditionalSourceReplacement(
          update.originalValues,
          update.changes,
        );
      } catch (error) {
        throw failedDriverUpdateRows(
          error,
          request.updates.length,
          [],
          rowIndex,
          0,
          false,
        );
      }
    }
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
        const identity = this.resolveMutationIdentity(
          request.table,
          update.primaryKeys,
        );
        if (Object.hasOwn(update.changes, "_identity")) {
          throw new Error(
            "Elasticsearch does not support updating the _identity field.",
          );
        }
        const document = this.resolveDocumentForMutation(
          {
            ...update.primaryKeys,
            ...update.changes,
          },
          update.changes,
        );
        const client = this.requireClient();
        const conditional = Object.hasOwn(
          update.originalValues ?? {},
          "_source",
        );
        const script = conditional
          ? this.originalSourceScript(update.originalValues, document)
          : undefined;
        const replace = Object.hasOwn(update.changes, "_source");
        writeMayHaveApplied = true;
        const response = conditional
          ? await client.update(
              {
                ...identity,
                script,
                refresh: "wait_for",
              },
              context ? { signal: context.signal } : undefined,
            )
          : replace
            ? await client.index(
                {
                  ...identity,
                  document,
                  refresh: "wait_for",
                },
                context ? { signal: context.signal } : undefined,
              )
            : await client.update(
                {
                  ...identity,
                  doc: document,
                  refresh: "wait_for",
                },
                context ? { signal: context.signal } : undefined,
              );
        writeMayHaveApplied = false;
        if (response.result === "noop" && conditional) {
          updateRowOutcomes.push({ rowIndex, status: "not_applied" });
        } else if (
          response.result === "updated" ||
          response.result === "created" ||
          (response.result === "noop" && !replace)
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
          writeMayHaveApplied && !this.isVersionConflict(error),
        );
      }
    }
    return completeDriverUpdateRowsResult(affectedRows, updateRowOutcomes);
  }

  private isVersionConflict(error: unknown): boolean {
    const body = (
      error as { meta?: { body?: { error?: { type?: unknown } } } } | null
    )?.meta?.body;
    return (
      this.elasticsearchStatus(error) === 409 &&
      body?.error?.type === "version_conflict_engine_exception"
    );
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
        const identity = this.resolveMutationIdentity(request.table, entry);
        const client = this.requireClient();
        return async () => {
          try {
            const result = await client.delete(
              {
                ...identity,
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
      originalValues?: Record<string, unknown>;
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
      originalValues?: Record<string, unknown>;
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
        data.originalValues,
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
    originalValues?: Record<string, unknown>,
  ): string {
    const identity = this.resolveMutationIdentity(table, primaryKeys);
    if (
      Object.hasOwn(changes, "_identity") ||
      (Object.hasOwn(changes, "_id") && changes._id !== primaryKeys._id)
    ) {
      throw new Error("Elasticsearch document identity is read-only.");
    }
    this.assertConditionalSourceReplacement(originalValues, changes);
    const document = this.resolveDocumentForMutation(
      { ...primaryKeys, ...changes },
      changes,
    );
    const conditional = Object.hasOwn(originalValues ?? {}, "_source");
    const replace = Object.hasOwn(changes, "_source") && !conditional;
    return this.formatRestCommand(
      replace ? "PUT" : "POST",
      this.mutationIdentityPath(identity, replace ? "_doc" : "_update"),
      conditional
        ? { script: this.originalSourceScript(originalValues, document) }
        : replace
          ? document
          : { doc: document },
    );
  }

  private assertConditionalSourceReplacement(
    originalValues: Record<string, unknown> | undefined,
    changes: Record<string, unknown>,
  ): void {
    if (
      Object.hasOwn(originalValues ?? {}, "_source") &&
      !Object.hasOwn(changes, "_source")
    ) {
      throw new Error(
        "Elasticsearch conditional _source updates require a complete replacement _source; partial changes are not supported.",
      );
    }
  }

  private originalSourceScript(
    originalValues: Record<string, unknown> | undefined,
    next: Record<string, unknown>,
  ) {
    return {
      lang: "painless",
      source: `boolean same(def a, def b, int[] budget) {
  if (--budget[0] < 0) throw new IllegalArgumentException('JSON comparison exceeds the 2000000-node safety limit');
  if (a == null || b == null) return a == null && b == null;
  if (a instanceof Number && b instanceof Number) {
    boolean af = a instanceof Double || a instanceof Float;
    boolean bf = b instanceof Double || b instanceof Float;
    if (af && bf) return ((Number)a).doubleValue() == ((Number)b).doubleValue();
    if (!af && !bf) return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString())) == 0;
    double d = ((Number)(af ? a : b)).doubleValue();
    def n = af ? b : a;
    if (d - d != 0.0) return false;
    int k = 0;
    while (d >= 9007199254740992.0 || d <= -9007199254740992.0) { d /= 2; k++; }
    long m = (long) d;
    if (m != d) return false;
    if (k == 0 && (n instanceof Integer || n instanceof Long)) return m == ((Number)n).longValue();
    BigDecimal exact = new BigDecimal('' + m).multiply(new BigDecimal('2').pow(k));
    return exact.compareTo(new BigDecimal(n.toString())) == 0;
  }
  if (a instanceof Map && b instanceof Map) {
    if (a.size() != b.size()) return false;
    return a.entrySet().stream().allMatch(e -> b.containsKey(e.getKey()) && same(e.getValue(), b.get(e.getKey()), budget));
  }
  if (a instanceof List && b instanceof List) {
    if (a.size() != b.size()) return false;
    def it = b.iterator();
    return a.stream().allMatch(x -> same(x, it.next(), budget));
  }
  return a.equals(b);
}
if (!same(ctx._source, params.original, new int[] {2000000})) { ctx.op = 'none' } else { ctx._source = params.next }`,
      params: {
        original: this.resolveDocumentForMutation({
          _source: originalValues?._source,
        }),
        next,
      },
    };
  }

  private buildDeleteMutationPreviewStatement(
    table: string,
    primaryKeys: Record<string, unknown>,
  ): string {
    const identity = this.resolveMutationIdentity(table, primaryKeys);
    return this.formatRestCommand(
      "DELETE",
      this.mutationIdentityPath(identity, "_doc"),
    );
  }

  async runTransaction(operations: TransactionOperation[]): Promise<void> {
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

  coerceOriginalValue(value: unknown, column: ColumnTypeMeta): unknown {
    try {
      return this.coerceInputValue(value, column);
    } catch (error) {
      if (
        error instanceof JsonNumericPrecisionError &&
        typeof value === "string"
      ) {
        return JSON.parse(value.trim()) as unknown;
      }
      throw error;
    }
  }

  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (column.name === "_source") {
      if (typeof value === "string") {
        const trimmed = value.trim();
        if (!trimmed) {
          return {};
        }
        try {
          const parsed = parseJsonForWrite(trimmed, isRoundTripJsonNumberToken);
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
            throw new Error();
          }
          return parsed;
        } catch (error) {
          if (error instanceof JsonNumericPrecisionError) throw error;
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

  coerceTableEditValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (column.name === "_source") this.assertSourceUpdateValue(value);
    return this.coerceInputValue(value, column);
  }

  private assertSourceUpdateValue(value: unknown): void {
    if (
      value === null ||
      value === undefined ||
      (typeof value === "string" && !value.trim())
    ) {
      throw new Error(
        'Elasticsearch field "_source" must be a JSON object. Enter {} explicitly to replace the document with an empty one.',
      );
    }
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
    column: ColumnTypeMeta,
    expectedValue: unknown,
    _options?: { persistedValue: unknown },
  ) {
    if (column.name === "_source") {
      try {
        this.coerceInputValue(expectedValue, column);
      } catch (error) {
        return {
          ok: false,
          shouldVerify: false,
          message: error instanceof Error ? error.message : String(error),
        };
      }
    }
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
        name: "_identity",
        type: "text",
        nativeType: "text",
        category: "text",
        nullable: false,
        isReadOnly: true,
        isPrimaryKey: true,
        primaryKeyOrdinal: 2,
        isForeignKey: false,
        filterable: false,
        filterOperators: [],
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
    hits: ReadonlyArray<{
      _id?: unknown;
      _source?: unknown;
      _index?: unknown;
      _routing?: unknown;
      _seq_no?: unknown;
      _primary_term?: unknown;
    }>,
    table?: string,
  ): Record<string, unknown>[] {
    return hits.map((hit) => {
      const source = this.normalizeSourceDocument(hit._source);
      return {
        _id: hit._id,
        ...(table === undefined
          ? {}
          : {
              _identity: this.encodeRowIdentity(table, hit),
            }),
        _source: stringifyElasticsearchJson(source),
      };
    });
  }

  private encodeRowIdentity(
    table: string,
    hit: {
      _index?: unknown;
      _id?: unknown;
      _routing?: unknown;
      _seq_no?: unknown;
      _primary_term?: unknown;
    },
  ): string | null {
    if (
      typeof hit._index !== "string" ||
      !hit._index ||
      typeof hit._id !== "string" ||
      !hit._id ||
      (hit._routing !== undefined && typeof hit._routing !== "string") ||
      !Number.isSafeInteger(hit._seq_no) ||
      Number(hit._seq_no) < 0 ||
      !Number.isSafeInteger(hit._primary_term) ||
      Number(hit._primary_term) < 1
    )
      return null;
    const payload = Buffer.from(
      JSON.stringify([
        1,
        this.connectionEpoch,
        table,
        hit._index,
        hit._id,
        hit._routing ?? null,
        hit._seq_no,
        hit._primary_term,
      ]),
    ).toString("base64url");
    return `${payload}.${this.signRowIdentity(payload)}`;
  }

  private signRowIdentity(payload: string): string {
    return createHmac("sha256", this.identitySecret)
      .update(payload)
      .digest("base64url");
  }

  private resolveMutationIdentity(
    table: string,
    primaryKeys: Record<string, unknown>,
  ): ElasticsearchMutationIdentity {
    const invalid = () =>
      new Error(
        "Elasticsearch mutation requires a complete, unchanged _id/_identity from the current table read. Refresh the table and retry.",
      );
    const token = primaryKeys._identity;
    if (typeof token !== "string") throw invalid();
    const [payload, signature, extra] = token.split(".");
    if (
      !payload ||
      !signature ||
      extra !== undefined ||
      signature !== this.signRowIdentity(payload)
    )
      throw invalid();
    let identity: unknown;
    try {
      identity = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    } catch {
      throw invalid();
    }
    if (!Array.isArray(identity) || identity.length !== 8) throw invalid();
    const [version, epoch, readTable, index, id, routing, seqNo, primaryTerm] =
      identity as unknown[];
    if (
      version !== 1 ||
      epoch !== this.connectionEpoch ||
      readTable !== table ||
      typeof index !== "string" ||
      !index ||
      typeof id !== "string" ||
      !id ||
      primaryKeys._id !== id ||
      (routing !== null && typeof routing !== "string") ||
      typeof seqNo !== "number" ||
      !Number.isSafeInteger(seqNo) ||
      seqNo < 0 ||
      typeof primaryTerm !== "number" ||
      !Number.isSafeInteger(primaryTerm) ||
      primaryTerm < 1
    )
      throw invalid();
    return {
      index,
      id,
      ...(routing === null ? {} : { routing }),
      if_seq_no: seqNo,
      if_primary_term: primaryTerm,
    };
  }

  private mutationIdentityPath(
    identity: ElasticsearchMutationIdentity,
    endpoint: "_doc" | "_update",
  ): string {
    const params = new URLSearchParams({
      ...(identity.routing === undefined ? {} : { routing: identity.routing }),
      if_seq_no: String(identity.if_seq_no),
      if_primary_term: String(identity.if_primary_term),
      refresh: "wait_for",
    });
    return `/${encodeURIComponent(identity.index)}/${endpoint}/${encodeURIComponent(identity.id)}?${params}`;
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

    if (changes && Object.hasOwn(changes, "_source")) {
      this.assertSourceUpdateValue(changes._source);
    }

    if (Object.hasOwn(record, "_source")) {
      return this.normalizeSourceDocument(record._source);
    }

    const { _identity: _ignoredIdentity, ...document } = record;
    return this.stripDocumentId(document);
  }

  private normalizeSourceDocument(source: unknown): Record<string, unknown> {
    if (typeof source === "string") {
      const trimmed = source.trim();
      if (!trimmed) {
        return {};
      }
      const parsed = parseJsonForWrite(trimmed, isRoundTripJsonNumberToken);
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
    return `${method} ${path}\n${stringifyElasticsearchJson(body, 2)}`;
  }

  private toPlainRows(
    result: Pick<QueryResult, "columns" | "rows">,
  ): Array<Record<string, unknown>> {
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
    if (Object.hasOwn(queryParams, "source")) {
      throw new Error(
        "Elasticsearch REST search does not support the source query parameter. Put the JSON search body after the request path so its size limit can be checked.",
      );
    }
    if (!Object.hasOwn(queryParams, "size")) {
      return queryParams;
    }

    return {
      ...queryParams,
      size: this.clampSearchSize(Number(queryParams.size)),
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

    if (value < 0) {
      return value;
    }

    const normalized = Math.trunc(value);
    return Math.min(normalized, ELASTICSEARCH_READ_BUDGET.hardCap);
  }
}
