import {
  type AttributeValue,
  BatchGetItemCommand,
  type BatchGetItemCommandInput,
  type BatchGetItemCommandOutput,
  BatchWriteItemCommand,
  type BatchWriteItemCommandInput,
  type BatchWriteItemCommandOutput,
  DeleteItemCommand,
  type DeleteItemCommandInput,
  type DeleteItemCommandOutput,
  DescribeTableCommand,
  DynamoDBClient,
  GetItemCommand,
  type GetItemCommandInput,
  type GetItemCommandOutput,
  type GlobalSecondaryIndexDescription,
  ListTablesCommand,
  type LocalSecondaryIndexDescription,
  type Projection,
  type ProvisionedThroughputDescription,
  PutItemCommand,
  type PutItemCommandInput,
  type PutItemCommandOutput,
  QueryCommand,
  type QueryCommandInput,
  type QueryCommandOutput,
  ScanCommand,
  type ScanCommandInput,
  type ScanCommandOutput,
  type TableDescription,
  TransactGetItemsCommand,
  type TransactGetItemsCommandInput,
  type TransactGetItemsCommandOutput,
  TransactWriteItemsCommand,
  type TransactWriteItemsCommandInput,
  UpdateItemCommand,
  type UpdateItemCommandInput,
  type UpdateItemCommandOutput,
} from "@aws-sdk/client-dynamodb";
import { fromIni } from "@aws-sdk/credential-providers";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import {
  type DynamoDbNativeOperationName,
  inferDynamoDbNativeOperationName,
  parseDynamoDbNativeQueryInputs,
} from "../../shared/dynamodbNative";
import type { PrimaryKeyRole } from "../../shared/tableTypes";
import type { ConnectionConfig } from "../connectionManager";
import {
  applyFilters,
  applySort,
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
  DriverSortConfig,
  DriverTablePageRequest,
  DriverTablePageResult,
  DriverUpdateRowsRequest,
  FilterConditionResult,
  FilterExpression,
  FilterOperator,
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
  ValueSemantics,
} from "./types";
import { NULL_SENTINEL, resolveFilterOperators } from "./types";

const DYNAMODB_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "supported",
    triggers: "not_applicable",
  },
};

const DYNAMODB_CURSOR_FETCH_LIMIT = 200;
const DYNAMODB_MAX_MATERIALIZED_ROWS = 5000;

type IndexedFilter = {
  index: number;
  filter: FilterExpression;
};

type ExpressionState = {
  names: Record<string, string>;
  values: Record<string, AttributeValue>;
  nameCounter: number;
  valueCounter: number;
  nameByColumn: Map<string, string>;
};

type DynamoSecondaryIndexSchema = {
  name: string;
  partitionKey: string;
  sortKey?: string;
  type: "global" | "local";
};

type DynamoTableSchema = {
  keys: string[];
  keyRoles: Map<string, PrimaryKeyRole>;
  attrTypes: Map<string, string>;
  partitionKey?: string;
  sortKey?: string;
  secondaryIndexes: DynamoSecondaryIndexSchema[];
};

type GetItemReadPlan = {
  kind: "getItem";
  table: string;
  schema: DynamoTableSchema;
  key: Record<string, unknown>;
  requestSignature: string;
};

type QueryReadPlan = {
  kind: "query";
  table: string;
  schema: DynamoTableSchema;
  baseInput: Omit<QueryCommandInput, "ExclusiveStartKey" | "Limit">;
  requestSignature: string;
  sortKeyName?: string;
};

type ScanReadPlan = {
  kind: "scan";
  table: string;
  schema: DynamoTableSchema;
  baseInput: Omit<ScanCommandInput, "ExclusiveStartKey" | "Limit">;
  requestSignature: string;
};

type DynamoReadPlan = GetItemReadPlan | QueryReadPlan | ScanReadPlan;

type ReadStepResult = {
  rows: Record<string, unknown>[];
  nextCursor?: Record<string, AttributeValue>;
};

type MaterializedReadResult = {
  rows: Record<string, unknown>[];
  truncated: boolean;
};

type DynamoCursorSession = {
  pageStarts: Map<number, Record<string, AttributeValue> | undefined>;
  terminalPage: number | null;
  totalCount?: number;
};

type QueryDispatchResult = {
  rows: Record<string, unknown>[];
  affectedRows?: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(",")}]`;
  }
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort((left, right) => left.localeCompare(right))
      .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

export class DynamoDBDriver implements IDBDriver {
  private client: DynamoDBClient | null = null;
  private connected = false;
  private readonly cursorCache = new Map<string, DynamoCursorSession>();

  constructor(private readonly config: ConnectionConfig) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    const client = new DynamoDBClient({
      region: this.config.awsRegion || "us-east-1",
      endpoint: this.config.endpoint ?? this.config.awsEndpoint,
      credentials:
        this.config.awsAccessKeyId && this.config.awsSecretAccessKey
          ? {
              accessKeyId: this.config.awsAccessKeyId,
              secretAccessKey: this.config.awsSecretAccessKey,
              sessionToken: this.config.awsSessionToken,
            }
          : this.config.awsProfile
            ? fromIni({ profile: this.config.awsProfile })
            : undefined,
    });

    try {
      await client.send(new ListTablesCommand({ Limit: 1 }));
    } catch (error) {
      client.destroy();
      throw error;
    }

    this.client = client;
    this.connected = true;
    this.cursorCache.clear();
  }

  async disconnect(): Promise<void> {
    const client = this.client;
    this.client = null;
    this.connected = false;
    this.cursorCache.clear();
    client?.destroy();
  }

  isConnected(): boolean {
    return this.connected;
  }

  getEntityManifest(): DriverEntityManifest {
    return DYNAMODB_ENTITY_MANIFEST;
  }

  getCapabilities() {
    return {
      tabularRead: "nosql" as const,
      queryMode: "text" as const,
      supportsMutations: true,
      editorPresentation: {
        queryMode: "text" as const,
        formatOnOpen: false,
        editorLanguage: "json" as const,
        allowFormatting: false,
      },
    };
  }

  private databaseName(): string {
    return this.config.database || this.config.awsRegion || "default";
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    return [
      {
        name: this.databaseName(),
        schemas: [],
      },
    ];
  }

  async listSchemas(database: string): Promise<SchemaInfo[]> {
    return [{ name: database || this.databaseName() }];
  }

  async listObjects(database: string): Promise<TableInfo[]> {
    try {
      const result = await this.requireClient().send(new ListTablesCommand({}));
      const schemaName = database || this.databaseName();
      return (result.TableNames ?? [])
        .slice()
        .sort((left, right) => left.localeCompare(right))
        .map((tableName) => ({
          schema: schemaName,
          name: tableName,
          type: "table" as const,
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
    const [rows, schema] = await Promise.all([
      this.readRowsForDescription(table, 1000),
      this.getTableSchema(table),
    ]);
    const columns = this.buildDynamoColumns(rows, schema);
    return columns.map((column) => ({
      name: column.name,
      type: column.type,
      nullable: column.nullable,
      defaultValue: undefined,
      isPrimaryKey: column.isPrimaryKey,
      primaryKeyOrdinal: column.primaryKeyOrdinal,
      primaryKeyRole: column.primaryKeyRole,
      isForeignKey: false,
    }));
  }

  async describeColumns(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const [rows, schema] = await Promise.all([
      this.readRowsForDescription(table, 1000),
      this.getTableSchema(table),
    ]);
    return this.buildDynamoColumns(rows, schema);
  }

  async getIndexes(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<IndexMeta[]> {
    try {
      const description = await this.requireClient().send(
        new DescribeTableCommand({ TableName: table }),
      );
      const globalIndexes = (
        description.Table?.GlobalSecondaryIndexes ?? []
      ).map((index) => ({
        name: index.IndexName ?? "index",
        columns: (index.KeySchema ?? [])
          .map((entry) => entry.AttributeName)
          .filter((name): name is string => Boolean(name)),
        unique: false,
        primary: false,
        ddlSupport: "supported" as const,
      }));
      const localIndexes = (description.Table?.LocalSecondaryIndexes ?? []).map(
        (index) => ({
          name: index.IndexName ?? "index",
          columns: (index.KeySchema ?? [])
            .map((entry) => entry.AttributeName)
            .filter((name): name is string => Boolean(name)),
          unique: false,
          primary: false,
          ddlSupport: "unsupported" as const,
        }),
      );
      return [...globalIndexes, ...localIndexes];
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
    unsupported("DynamoDB constraints DDL");
  }

  async getIndexDDL(
    _database: string,
    _schema: string,
    table: string,
    indexName: string,
  ): Promise<string> {
    const description = await this.describeTableDefinition(table);
    const globalIndex = (description.GlobalSecondaryIndexes ?? []).find(
      (index) => index.IndexName === indexName,
    );
    if (!globalIndex) {
      const localIndex = (description.LocalSecondaryIndexes ?? []).find(
        (index) => index.IndexName === indexName,
      );
      if (localIndex) {
        throw new Error(
          `DynamoDB Open DDL currently supports only global secondary indexes. "${indexName}" is a local secondary index.`,
        );
      }

      throw new Error(`Index "${indexName}" not found`);
    }

    const createPayload: Record<string, unknown> = {
      IndexName: globalIndex.IndexName,
      KeySchema: globalIndex.KeySchema ?? [],
      Projection: this.normalizeProjection(globalIndex.Projection),
    };
    const throughput = this.normalizeProvisionedThroughput(
      globalIndex.ProvisionedThroughput,
    );
    if (this.resolveBillingMode(description) === "PROVISIONED" && throughput) {
      createPayload.ProvisionedThroughput = throughput;
    }

    return this.buildAwsCliCommand("aws dynamodb update-table", [
      ["--table-name", JSON.stringify(table)],
      [
        "--attribute-definitions",
        this.formatCliJson(description.AttributeDefinitions ?? []),
      ],
      [
        "--global-secondary-index-updates",
        this.formatCliJson([{ Create: createPayload }]),
      ],
    ]);
  }

  async getTriggerDDL(): Promise<string> {
    unsupported("DynamoDB trigger DDL");
  }

  async getCreateTableDDL(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<string> {
    const description = await this.describeTableDefinition(table);
    const billingMode = this.resolveBillingMode(description);
    const globalSecondaryIndexes = (
      description.GlobalSecondaryIndexes ?? []
    ).map((index) => this.serializeCreateIndex(index, billingMode));
    const localSecondaryIndexes = (description.LocalSecondaryIndexes ?? []).map(
      (index) => this.serializeCreateIndex(index),
    );

    return this.buildAwsCliCommand("aws dynamodb create-table", [
      ["--table-name", JSON.stringify(description.TableName ?? table)],
      [
        "--attribute-definitions",
        this.formatCliJson(description.AttributeDefinitions ?? []),
      ],
      ["--key-schema", this.formatCliJson(description.KeySchema ?? [])],
      ["--billing-mode", billingMode],
      [
        "--provisioned-throughput",
        billingMode === "PROVISIONED"
          ? this.formatCliJson(
              this.normalizeProvisionedThroughput(
                description.ProvisionedThroughput,
              ) ?? {
                ReadCapacityUnits: 5,
                WriteCapacityUnits: 5,
              },
            )
          : undefined,
      ],
      [
        "--local-secondary-indexes",
        localSecondaryIndexes.length > 0
          ? this.formatCliJson(localSecondaryIndexes)
          : undefined,
      ],
      [
        "--global-secondary-indexes",
        globalSecondaryIndexes.length > 0
          ? this.formatCliJson(globalSecondaryIndexes)
          : undefined,
      ],
      [
        "--stream-specification",
        description.StreamSpecification?.StreamEnabled
          ? this.formatCliJson(description.StreamSpecification)
          : undefined,
      ],
      [
        "--sse-specification",
        description.SSEDescription?.Status
          ? this.formatCliJson({
              Enabled: description.SSEDescription.Status === "ENABLED",
              SSEType: description.SSEDescription.SSEType,
              KMSMasterKeyId: description.SSEDescription.KMSMasterKeyArn,
            })
          : undefined,
      ],
      ["--table-class", description.TableClassSummary?.TableClass ?? undefined],
      [
        "--deletion-protection-enabled",
        description.DeletionProtectionEnabled === undefined
          ? undefined
          : String(description.DeletionProtectionEnabled),
      ],
    ]);
  }

  async getObjectDefinition(): Promise<string | null> {
    return null;
  }

  async getRoutineDefinition(): Promise<string> {
    unsupported("DynamoDB routine definition");
  }

  async query(queryText: string, _params?: unknown[]): Promise<QueryResult> {
    const inputs = parseDynamoDbNativeQueryInputs(queryText);
    const operation = this.resolveNativeOperation(inputs);
    const startedAt = Date.now();
    const rawRows: Record<string, unknown>[] = [];
    let affectedRows = 0;
    let sawMutation = false;

    for (const input of inputs) {
      const result = await this.dispatchNativeCommand(operation, input);
      rawRows.push(...result.rows);
      if (result.affectedRows !== undefined) {
        sawMutation = true;
        affectedRows += result.affectedRows;
      }
    }

    const columns = inferColumnsFromRows(rawRows, "id").map(
      (column) => column.name,
    );

    return {
      columns,
      rows: rawRows.map((row) => this.mapRowToQueryRow(row, columns)),
      rowCount: rawRows.length,
      affectedRows: sawMutation ? affectedRows : undefined,
      executionTimeMs: Date.now() - startedAt,
    };
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    const schema = await this.getTableSchema(request.table);
    const describedColumns = await this.describeColumns(
      request.database,
      request.schema,
      request.table,
    );
    const describedByName = new Map(
      describedColumns.map((column) => [column.name, column]),
    );
    const plan = this.buildReadPlan(request, schema, describedColumns);
    const requiresMaterializedFiltering = this.requiresClientSideFiltering(
      request.filters,
      describedColumns,
    );
    const requiresMaterializedSort =
      request.sort !== null && !this.supportsServerSort(plan, request.sort);

    if (requiresMaterializedSort || requiresMaterializedFiltering) {
      const materialized = await this.materializeReadPlanRows(
        plan,
        DYNAMODB_MAX_MATERIALIZED_ROWS,
      );
      if (materialized.truncated) {
        const reason = requiresMaterializedSort ? "sorting" : "filtering";
        throw new Error(
          `${reason[0]?.toUpperCase() ?? "F"}${reason.slice(1)} DynamoDB data with the current request requires materializing up to ${DYNAMODB_MAX_MATERIALIZED_ROWS} rows. Narrow the filters or sort by the key-compatible order instead.`,
        );
      }

      const formattedRows = materialized.rows.map((row) =>
        this.formatDynamoRowForDisplay(row, describedByName),
      );
      const filteredRows = applyFilters(formattedRows, request.filters);
      const sortedRows = applySort(filteredRows, request.sort);
      const columns = this.mergeDescribedColumns(
        sortedRows.length > 0
          ? this.buildDynamoColumns(materialized.rows, schema)
          : describedColumns,
        describedColumns,
        schema,
      );
      return {
        columns,
        rows: pageRows(sortedRows, request.page, request.pageSize),
        totalCount: request.skipCount ? 0 : sortedRows.length,
      };
    }

    const pageResult = await this.readCursorBackedPage(
      plan,
      request.page,
      request.pageSize,
      request.sort,
    );
    const formattedRows = pageResult.rows.map((row) =>
      this.formatDynamoRowForDisplay(row, describedByName),
    );
    const columns = this.mergeDescribedColumns(
      pageResult.rows.length > 0
        ? this.buildDynamoColumns(pageResult.rows, schema)
        : describedColumns,
      describedColumns,
      schema,
    );

    return {
      columns,
      rows: formattedRows,
      totalCount: request.skipCount ? 0 : await this.countReadPlan(plan),
    };
  }

  async updateRows(
    request: DriverUpdateRowsRequest,
  ): Promise<DriverMutationResult> {
    const schema = await this.getTableSchema(request.table);
    const keys = schema.keys;
    const partitionKey = schema.partitionKey;
    if (!partitionKey) {
      throw new Error("DynamoDB update requires a table partition key.");
    }

    let affectedRows = 0;
    for (const update of request.updates) {
      const input = this.buildUpdateItemInput(request.table, keys, update);
      if (!input) {
        continue;
      }
      try {
        const response = await this.requireClient().send(
          new UpdateItemCommand(input),
        );
        if (response.Attributes) {
          affectedRows += 1;
        }
      } catch (error: unknown) {
        if (this.isConditionalCheckFailure(error)) {
          continue;
        }
        throw error;
      }
    }

    this.invalidateCursorCacheForTable(request.table);
    return { affectedRows };
  }

  async insertRow(
    request: DriverInsertRowRequest,
  ): Promise<DriverMutationResult> {
    const schema = await this.getTableSchema(request.table);
    const input = this.buildPutItemInput(
      request.table,
      schema.keys,
      request.values,
    );

    try {
      await this.requireClient().send(new PutItemCommand(input));
      this.invalidateCursorCacheForTable(request.table);
      return { affectedRows: 1 };
    } catch (error: unknown) {
      if (this.isConditionalCheckFailure(error)) {
        return { affectedRows: 0 };
      }
      throw error;
    }
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
  ): Promise<DriverMutationResult> {
    const schema = await this.getTableSchema(request.table);
    const keys = schema.keys;
    let affectedRows = 0;

    for (const criteria of request.primaryKeyValuesList) {
      const input = this.buildDeleteItemInput(request.table, keys, criteria);
      try {
        const response = await this.requireClient().send(
          new DeleteItemCommand(input),
        );
        if (response.Attributes) {
          affectedRows += 1;
        }
      } catch (error: unknown) {
        if (this.isConditionalCheckFailure(error)) {
          continue;
        }
        throw error;
      }
    }

    this.invalidateCursorCacheForTable(request.table);
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
    return this.buildMutationPreviewDocuments(operation, table, data)
      .map((document) => this.formatNativePreviewEnvelope(document))
      .join("\n\n");
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
    const keyNames =
      operation === "insert"
        ? (await this.getTableSchema(table)).keys
        : undefined;
    return this.buildMutationPreviewDocuments(
      operation,
      table,
      data,
      keyNames,
    ).map((document) => this.formatNativePreviewEnvelope(document));
  }

  async runTransaction(operations: TransactionOperation[]): Promise<void> {
    for (const operation of operations) {
      await this.query(operation.sql, operation.params);
    }
  }

  quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`;
  }

  qualifiedTableName(
    _database: string,
    _schema: string,
    table: string,
  ): string {
    return this.quoteIdentifier(table);
  }

  buildPagination(
    _offset: number,
    limit: number,
    _paramIndex: number,
  ): PaginationResult {
    return {
      sql: "LIMIT ?",
      params: [limit],
    };
  }

  buildOrderByDefault(_cols: ColumnTypeMeta[]): string {
    return "";
  }

  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === NULL_SENTINEL) {
      return null;
    }
    if (value === null || value === undefined) {
      return value;
    }
    if (typeof value !== "string") {
      return value;
    }

    const trimmed = value.trim();
    const nativeType = column.nativeType.toLowerCase();

    switch (nativeType) {
      case "null":
        return /^null$/i.test(trimmed) ? null : value;
      case "boolean":
        if (/^(true|1)$/i.test(trimmed)) {
          return true;
        }
        if (/^(false|0)$/i.test(trimmed)) {
          return false;
        }
        return value;
      case "number": {
        const numeric = Number(trimmed);
        return Number.isFinite(numeric) ? numeric : value;
      }
      case "binary":
        return this.parseBinaryInput(trimmed) ?? value;
      case "string set":
        return this.parseSetInput(trimmed, "string") ?? value;
      case "number set":
        return this.parseSetInput(trimmed, "number") ?? value;
      case "binary set":
        return this.parseSetInput(trimmed, "binary") ?? value;
      case "list": {
        const parsed = this.parseJsonValue(trimmed);
        return Array.isArray(parsed) ? parsed : value;
      }
      case "map": {
        const parsed = this.parseJsonValue(trimmed);
        return this.isPlainObject(parsed) ? parsed : value;
      }
      default:
        if (column.category === "array") {
          const parsed = this.parseJsonValue(trimmed);
          return Array.isArray(parsed) ? parsed : value;
        }
        if (column.category === "json") {
          return this.parseJsonValue(trimmed) ?? value;
        }
        return value;
    }
  }

  formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) {
      return null;
    }

    const nativeType = column.nativeType.toLowerCase();
    if (nativeType === "binary") {
      return this.formatBinaryForDisplay(value);
    }
    if (nativeType.endsWith(" set")) {
      return this.formatSetForDisplay(value);
    }
    if (nativeType === "list" || nativeType === "map") {
      const normalized = this.normalizeValueForDisplay(value);
      return typeof normalized === "string"
        ? normalized
        : JSON.stringify(normalized);
    }

    const normalized = this.normalizeValueForDisplay(value);
    return typeof normalized === "string" ||
      typeof normalized === "number" ||
      typeof normalized === "boolean" ||
      normalized === null
      ? normalized
      : JSON.stringify(normalized);
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
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ) {
    if (operator === "is_null" || operator === "is_not_null") {
      return undefined;
    }
    if (value === undefined) {
      return undefined;
    }
    if (Array.isArray(value)) {
      return value.map((entry) => entry.trim()) as [string, string];
    }
    return value.trim();
  }

  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    _paramIndex: number,
  ): FilterConditionResult | null {
    const identifier = this.quoteIdentifier(column.name);
    if (operator === "is_null") {
      return {
        sql: `(${identifier} IS NULL OR ${identifier} IS MISSING)`,
        params: [],
      };
    }
    if (operator === "is_not_null") {
      return {
        sql: `${identifier} IS NOT NULL`,
        params: [],
      };
    }
    if (value === undefined) {
      return null;
    }
    if (operator === "between") {
      if (!Array.isArray(value)) {
        return null;
      }
      return {
        sql: `${identifier} BETWEEN ? AND ?`,
        params: value.map((entry) => this.coerceFilterParameter(column, entry)),
      };
    }
    if (operator === "in") {
      if (typeof value !== "string") {
        return null;
      }
      const parts = this.splitFilterList(value);
      if (parts.length === 0) {
        return null;
      }
      return {
        sql: `${identifier} IN (${parts.map(() => "?").join(", ")})`,
        params: parts.map((entry) => this.coerceFilterParameter(column, entry)),
      };
    }
    if (operator === "like" || operator === "ilike") {
      if (typeof value !== "string") {
        return null;
      }
      return {
        sql: `contains(${identifier}, ?)`,
        params: [value.replace(/%/g, "")],
      };
    }
    if (typeof value !== "string") {
      return null;
    }
    const param = this.coerceFilterParameter(column, value);
    switch (operator) {
      case "eq":
        return { sql: `${identifier} = ?`, params: [param] };
      case "neq":
        return { sql: `${identifier} <> ?`, params: [param] };
      case "gt":
        return { sql: `${identifier} > ?`, params: [param] };
      case "gte":
        return { sql: `${identifier} >= ?`, params: [param] };
      case "lt":
        return { sql: `${identifier} < ?`, params: [param] };
      case "lte":
        return { sql: `${identifier} <= ?`, params: [param] };
      default:
        return null;
    }
  }

  buildInsertDefaultValuesSql(qualifiedTableName: string): string {
    return this.formatNativePreviewEnvelope({
      operation: "PutItem",
      input: {
        TableName: qualifiedTableName,
        Item: {},
      },
    });
  }

  buildInsertValueExpr(_column: ColumnTypeMeta, _paramIndex: number): string {
    return "?";
  }

  buildSetExpr(column: ColumnTypeMeta): string {
    return `${this.quoteIdentifier(column.name)} = ?`;
  }

  materializePreviewSql(sql: string, _params?: readonly unknown[]): string {
    return sql;
  }

  private requireClient(): DynamoDBClient {
    if (!this.client || !this.connected) {
      throw new Error("DynamoDB is not connected.");
    }
    return this.client;
  }

  private invalidateCursorCacheForTable(table: string): void {
    if (!table) {
      return;
    }
    for (const key of [...this.cursorCache.keys()]) {
      if (key.includes(`"table":"${table}"`)) {
        this.cursorCache.delete(key);
      }
    }
  }

  private async describeTableDefinition(
    table: string,
  ): Promise<TableDescription> {
    const response = await this.requireClient().send(
      new DescribeTableCommand({ TableName: table }),
    );
    if (!response.Table) {
      throw new Error(`Table "${table}" not found`);
    }

    return response.Table;
  }

  private buildAwsCliCommand(
    baseCommand: string,
    args: Array<readonly [string, string | undefined]>,
  ): string {
    const definedArgs = args.filter(([, value]) => value !== undefined);
    return [
      `${baseCommand} \\`,
      ...definedArgs.map(
        ([flag, value], index) =>
          `  ${flag} ${value}${index === definedArgs.length - 1 ? "" : " \\"}`,
      ),
    ].join("\n");
  }

  private formatCliJson(value: unknown): string {
    return `'${JSON.stringify(value).replaceAll("'", "'\\''")}'`;
  }

  private resolveBillingMode(
    table: TableDescription,
  ): "PAY_PER_REQUEST" | "PROVISIONED" {
    const summaryMode = table.BillingModeSummary?.BillingMode;
    if (summaryMode === "PAY_PER_REQUEST" || summaryMode === "PROVISIONED") {
      return summaryMode;
    }

    const throughput = table.ProvisionedThroughput;
    return throughput?.ReadCapacityUnits !== undefined ||
      throughput?.WriteCapacityUnits !== undefined
      ? "PROVISIONED"
      : "PAY_PER_REQUEST";
  }

  private normalizeProvisionedThroughput(
    throughput: ProvisionedThroughputDescription | undefined,
  ): { ReadCapacityUnits: number; WriteCapacityUnits: number } | undefined {
    const readCapacityUnits = throughput?.ReadCapacityUnits;
    const writeCapacityUnits = throughput?.WriteCapacityUnits;
    if (readCapacityUnits === undefined || writeCapacityUnits === undefined) {
      return undefined;
    }

    return {
      ReadCapacityUnits: Number(readCapacityUnits),
      WriteCapacityUnits: Number(writeCapacityUnits),
    };
  }

  private normalizeProjection(projection: Projection | undefined): Projection {
    return {
      ProjectionType: projection?.ProjectionType ?? "ALL",
      NonKeyAttributes: projection?.NonKeyAttributes,
    };
  }

  private serializeCreateIndex(
    index: GlobalSecondaryIndexDescription | LocalSecondaryIndexDescription,
    billingMode?: "PAY_PER_REQUEST" | "PROVISIONED",
  ): Record<string, unknown> {
    const payload: Record<string, unknown> = {
      IndexName: index.IndexName,
      KeySchema: index.KeySchema ?? [],
      Projection: this.normalizeProjection(index.Projection),
    };

    if (
      billingMode === "PROVISIONED" &&
      "ProvisionedThroughput" in index &&
      index.ProvisionedThroughput
    ) {
      const throughput = this.normalizeProvisionedThroughput(
        index.ProvisionedThroughput,
      );
      if (throughput) {
        payload.ProvisionedThroughput = throughput;
      }
    }

    return payload;
  }

  private async getTableSchema(table: string): Promise<DynamoTableSchema> {
    try {
      const description = await this.requireClient().send(
        new DescribeTableCommand({ TableName: table }),
      );
      const keyRoles = new Map<string, PrimaryKeyRole>();
      const keys = (description.Table?.KeySchema ?? []).flatMap((entry) => {
        const name = entry.AttributeName;
        if (!name) {
          return [];
        }

        if (entry.KeyType === "HASH") {
          keyRoles.set(name, "partition");
        } else if (entry.KeyType === "RANGE") {
          keyRoles.set(name, "sort");
        }

        return [name];
      });
      const attrTypes = new Map(
        (description.Table?.AttributeDefinitions ?? []).map((attr) => [
          attr.AttributeName ?? "",
          attr.AttributeType ?? "S",
        ]),
      );
      return {
        keys,
        keyRoles,
        attrTypes,
        partitionKey: keys[0],
        sortKey: keys[1],
        secondaryIndexes: [
          ...this.extractSecondaryIndexes(
            description.Table?.GlobalSecondaryIndexes ?? [],
            "global",
          ),
          ...this.extractSecondaryIndexes(
            description.Table?.LocalSecondaryIndexes ?? [],
            "local",
          ),
        ],
      };
    } catch {
      return {
        keys: [],
        keyRoles: new Map(),
        attrTypes: new Map(),
        secondaryIndexes: [],
      };
    }
  }

  private extractSecondaryIndexes(
    indexes: ReadonlyArray<
      GlobalSecondaryIndexDescription | LocalSecondaryIndexDescription
    >,
    type: "global" | "local",
  ): DynamoSecondaryIndexSchema[] {
    return indexes
      .map((index): DynamoSecondaryIndexSchema | null => {
        const keyNames = (index.KeySchema ?? [])
          .map((entry) => entry.AttributeName)
          .filter((name): name is string => Boolean(name));
        const partitionKey = keyNames[0];
        if (!partitionKey || !index.IndexName) {
          return null;
        }
        const sortKey = keyNames[1];
        return {
          name: index.IndexName,
          partitionKey,
          ...(sortKey ? { sortKey } : {}),
          type,
        } satisfies DynamoSecondaryIndexSchema;
      })
      .filter((index): index is DynamoSecondaryIndexSchema => index !== null);
  }

  private buildReadPlan(
    request: DriverTablePageRequest,
    schema: DynamoTableSchema,
    columns: readonly ColumnTypeMeta[],
  ): DynamoReadPlan {
    const indexedFilters = request.filters.map((filter, index) => ({
      filter,
      index,
    }));
    const columnsByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    const fullPrimaryKey = this.resolveFullPrimaryKeyEquality(
      indexedFilters,
      schema.keys,
      columnsByName,
    );
    if (fullPrimaryKey) {
      return {
        kind: "getItem",
        table: request.table,
        schema,
        key: fullPrimaryKey,
        requestSignature: stableStringify({
          kind: "getItem",
          table: request.table,
          key: fullPrimaryKey,
        }),
      };
    }

    const queryCandidates = [
      {
        indexName: undefined,
        partitionKey: schema.partitionKey,
        sortKey: schema.sortKey,
      },
      ...schema.secondaryIndexes.map((index) => ({
        indexName: index.name,
        partitionKey: index.partitionKey,
        sortKey: index.sortKey,
      })),
    ];

    for (const candidate of queryCandidates) {
      const plan = this.buildQueryReadPlan(
        request.table,
        schema,
        candidate.indexName,
        candidate.partitionKey,
        candidate.sortKey,
        indexedFilters,
        columnsByName,
      );
      if (plan) {
        return plan;
      }
    }

    return this.buildScanReadPlan(
      request.table,
      schema,
      indexedFilters,
      columnsByName,
    );
  }

  private resolveFullPrimaryKeyEquality(
    indexedFilters: readonly IndexedFilter[],
    keyNames: readonly string[],
    columnsByName: ReadonlyMap<string, ColumnTypeMeta>,
  ): Record<string, unknown> | null {
    if (keyNames.length === 0) {
      return null;
    }

    const key: Record<string, unknown> = {};
    for (const keyName of keyNames) {
      const eqFilter = indexedFilters.find(
        ({ filter }) => filter.column === keyName && filter.operator === "eq",
      );
      if (!eqFilter || !("value" in eqFilter.filter)) {
        return null;
      }
      key[keyName] = this.coerceFilterValueForColumn(
        columnsByName.get(keyName),
        eqFilter.filter.value,
      );
    }
    return key;
  }

  private buildQueryReadPlan(
    table: string,
    schema: DynamoTableSchema,
    indexName: string | undefined,
    partitionKey: string | undefined,
    sortKey: string | undefined,
    indexedFilters: readonly IndexedFilter[],
    columnsByName: ReadonlyMap<string, ColumnTypeMeta>,
  ): QueryReadPlan | null {
    if (!partitionKey) {
      return null;
    }

    const state = this.createExpressionState();
    const consumed = new Set<number>();
    const partitionFilter = indexedFilters.find(
      ({ filter }) =>
        filter.column === partitionKey && filter.operator === "eq",
    );
    if (!partitionFilter || !("value" in partitionFilter.filter)) {
      return null;
    }
    consumed.add(partitionFilter.index);

    const partitionName = this.addNamePlaceholder(state, partitionKey);
    const partitionValue = this.addValuePlaceholder(
      state,
      this.coerceFilterValueForColumn(
        columnsByName.get(partitionKey),
        partitionFilter.filter.value,
      ),
    );
    let keyConditionExpression = `${partitionName} = ${partitionValue}`;

    if (sortKey) {
      const sortCondition = this.buildSortKeyCondition(
        sortKey,
        indexedFilters,
        columnsByName.get(sortKey),
        state,
      );
      if (sortCondition) {
        keyConditionExpression += ` AND ${sortCondition.expression}`;
        sortCondition.consumedIndexes.forEach((index) => {
          consumed.add(index);
        });
      }
    }

    const filterExpression = this.buildNativeFilterExpression(
      indexedFilters.filter(({ index }) => !consumed.has(index)),
      columnsByName,
      state,
    );

    const baseInput: Omit<QueryCommandInput, "ExclusiveStartKey" | "Limit"> = {
      TableName: table,
      KeyConditionExpression: keyConditionExpression,
      ...(indexName ? { IndexName: indexName } : {}),
      ...(filterExpression ? { FilterExpression: filterExpression } : {}),
      ...(Object.keys(state.names).length > 0
        ? { ExpressionAttributeNames: state.names }
        : {}),
      ...(Object.keys(state.values).length > 0
        ? { ExpressionAttributeValues: state.values }
        : {}),
    };

    return {
      kind: "query",
      table,
      schema,
      baseInput,
      sortKeyName: sortKey,
      requestSignature: stableStringify({
        kind: "query",
        table,
        indexName,
        keyConditionExpression,
        filterExpression,
        names: state.names,
        values: state.values,
      }),
    };
  }

  private buildScanReadPlan(
    table: string,
    schema: DynamoTableSchema,
    indexedFilters: readonly IndexedFilter[],
    columnsByName: ReadonlyMap<string, ColumnTypeMeta>,
  ): ScanReadPlan {
    const state = this.createExpressionState();
    const filterExpression = this.buildNativeFilterExpression(
      indexedFilters,
      columnsByName,
      state,
    );

    return {
      kind: "scan",
      table,
      schema,
      baseInput: {
        TableName: table,
        ...(filterExpression ? { FilterExpression: filterExpression } : {}),
        ...(Object.keys(state.names).length > 0
          ? { ExpressionAttributeNames: state.names }
          : {}),
        ...(Object.keys(state.values).length > 0
          ? { ExpressionAttributeValues: state.values }
          : {}),
      },
      requestSignature: stableStringify({
        kind: "scan",
        table,
        filterExpression,
        names: state.names,
        values: state.values,
      }),
    };
  }

  private buildSortKeyCondition(
    sortKey: string,
    indexedFilters: readonly IndexedFilter[],
    column: ColumnTypeMeta | undefined,
    state: ExpressionState,
  ): { expression: string; consumedIndexes: number[] } | null {
    const keyFilters = indexedFilters.filter(
      ({ filter }) => filter.column === sortKey,
    );
    if (keyFilters.length === 0) {
      return null;
    }

    const eqFilter = keyFilters.find(({ filter }) => filter.operator === "eq");
    if (eqFilter && "value" in eqFilter.filter) {
      const name = this.addNamePlaceholder(state, sortKey);
      const value = this.addValuePlaceholder(
        state,
        this.coerceFilterValueForColumn(column, eqFilter.filter.value),
      );
      return {
        expression: `${name} = ${value}`,
        consumedIndexes: [eqFilter.index],
      };
    }

    const betweenFilter = keyFilters.find(
      ({ filter }) => filter.operator === "between",
    );
    if (
      betweenFilter &&
      "value" in betweenFilter.filter &&
      Array.isArray(betweenFilter.filter.value)
    ) {
      const name = this.addNamePlaceholder(state, sortKey);
      const [leftRaw, rightRaw] = betweenFilter.filter.value;
      const left = this.addValuePlaceholder(
        state,
        this.coerceFilterValueForColumn(column, leftRaw),
      );
      const right = this.addValuePlaceholder(
        state,
        this.coerceFilterValueForColumn(column, rightRaw),
      );
      return {
        expression: `${name} BETWEEN ${left} AND ${right}`,
        consumedIndexes: [betweenFilter.index],
      };
    }

    const comparator = keyFilters.find(({ filter }) =>
      ["gte", "gt", "lte", "lt"].includes(filter.operator),
    );
    if (comparator && "value" in comparator.filter) {
      const name = this.addNamePlaceholder(state, sortKey);
      const value = this.addValuePlaceholder(
        state,
        this.coerceFilterValueForColumn(column, comparator.filter.value),
      );
      const operator =
        comparator.filter.operator === "gte"
          ? ">="
          : comparator.filter.operator === "gt"
            ? ">"
            : comparator.filter.operator === "lte"
              ? "<="
              : "<";
      return {
        expression: `${name} ${operator} ${value}`,
        consumedIndexes: [comparator.index],
      };
    }

    return null;
  }

  private createExpressionState(): ExpressionState {
    return {
      names: {},
      values: {},
      nameCounter: 0,
      valueCounter: 0,
      nameByColumn: new Map(),
    };
  }

  private addNamePlaceholder(
    state: ExpressionState,
    columnName: string,
  ): string {
    const existing = state.nameByColumn.get(columnName);
    if (existing) {
      return existing;
    }
    const placeholder = `#n${state.nameCounter}`;
    state.nameCounter += 1;
    state.nameByColumn.set(columnName, placeholder);
    state.names[placeholder] = columnName;
    return placeholder;
  }

  private addValuePlaceholder(state: ExpressionState, value: unknown): string {
    const placeholder = `:v${state.valueCounter}`;
    state.valueCounter += 1;
    state.values[placeholder] = this.toAttributeValue(value);
    return placeholder;
  }

  private buildNativeFilterExpression(
    indexedFilters: readonly IndexedFilter[],
    columnsByName: ReadonlyMap<string, ColumnTypeMeta>,
    state: ExpressionState,
  ): string | undefined {
    const parts = indexedFilters.flatMap(({ filter }) => {
      const column = columnsByName.get(filter.column);
      if (this.requiresClientSideFilter(filter, column)) {
        return [];
      }
      const expression = this.buildNativeFilterExpressionPart(
        filter,
        column,
        state,
      );
      return expression ? [expression] : [];
    });
    return parts.length > 0 ? parts.join(" AND ") : undefined;
  }

  private buildNativeFilterExpressionPart(
    filter: FilterExpression,
    column: ColumnTypeMeta | undefined,
    state: ExpressionState,
  ): string | null {
    const name = this.addNamePlaceholder(state, filter.column);
    switch (filter.operator) {
      case "is_null": {
        const nullType = this.addValuePlaceholder(state, "NULL");
        return `(attribute_not_exists(${name}) OR attribute_type(${name}, ${nullType}))`;
      }
      case "is_not_null": {
        const nullType = this.addValuePlaceholder(state, "NULL");
        return `(attribute_exists(${name}) AND NOT attribute_type(${name}, ${nullType}))`;
      }
      case "between": {
        if (!("value" in filter) || !Array.isArray(filter.value)) {
          return null;
        }
        const left = this.addValuePlaceholder(
          state,
          this.coerceFilterValueForColumn(column, filter.value[0]),
        );
        const right = this.addValuePlaceholder(
          state,
          this.coerceFilterValueForColumn(column, filter.value[1]),
        );
        return `${name} BETWEEN ${left} AND ${right}`;
      }
      case "in": {
        if (!("value" in filter) || typeof filter.value !== "string") {
          return null;
        }
        const values = this.splitFilterList(filter.value);
        if (values.length === 0) {
          return null;
        }
        const placeholders = values.map((entry) =>
          this.addValuePlaceholder(
            state,
            this.coerceFilterValueForColumn(column, entry),
          ),
        );
        return `${name} IN (${placeholders.join(", ")})`;
      }
      case "like":
      case "ilike": {
        if (!("value" in filter) || typeof filter.value !== "string") {
          return null;
        }
        const value = this.addValuePlaceholder(
          state,
          filter.value.replace(/%/g, ""),
        );
        return `contains(${name}, ${value})`;
      }
      case "eq":
      case "neq":
      case "gt":
      case "gte":
      case "lt":
      case "lte": {
        if (!("value" in filter)) {
          return null;
        }
        const value = this.addValuePlaceholder(
          state,
          this.coerceFilterValueForColumn(column, filter.value),
        );
        const operator =
          filter.operator === "eq"
            ? "="
            : filter.operator === "neq"
              ? "<>"
              : filter.operator === "gt"
                ? ">"
                : filter.operator === "gte"
                  ? ">="
                  : filter.operator === "lt"
                    ? "<"
                    : "<=";
        return `${name} ${operator} ${value}`;
      }
      default:
        return null;
    }
  }

  private supportsServerSort(
    plan: DynamoReadPlan,
    sort: DriverSortConfig,
  ): boolean {
    return plan.kind === "query" && plan.sortKeyName === sort.column;
  }

  private requiresClientSideFiltering(
    filters: readonly FilterExpression[],
    columns: readonly ColumnTypeMeta[],
  ): boolean {
    if (filters.length === 0) {
      return false;
    }
    const columnsByName = new Map(
      columns.map((column) => [column.name, column]),
    );
    return filters.some((filter) =>
      this.requiresClientSideFilter(filter, columnsByName.get(filter.column)),
    );
  }

  private requiresClientSideFilter(
    filter: FilterExpression,
    column: ColumnTypeMeta | undefined,
  ): boolean {
    if (!column) {
      return false;
    }
    if (filter.operator === "ilike") {
      return true;
    }
    const nativeType = column.nativeType.toLowerCase();
    return (
      column.category === "json" ||
      column.category === "array" ||
      column.category === "binary" ||
      nativeType === "map" ||
      nativeType === "list" ||
      nativeType.endsWith(" set")
    );
  }

  private async readCursorBackedPage(
    plan: DynamoReadPlan,
    page: number,
    pageSize: number,
    sort: DriverSortConfig | null,
  ): Promise<{ rows: Record<string, unknown>[] }> {
    if (plan.kind === "getItem") {
      const response = await this.requireClient().send(
        new GetItemCommand({
          TableName: plan.table,
          Key: this.marshallKey(plan.key),
        }),
      );
      return {
        rows: response.Item ? [unmarshall(response.Item)] : [],
      };
    }

    const cacheKey = `${plan.requestSignature}::${pageSize}`;
    const session = this.getCursorSession(cacheKey);
    if (session.terminalPage !== null && page >= session.terminalPage) {
      return { rows: [] };
    }

    let currentPage = 1;
    let cursor = session.pageStarts.get(1);
    for (const candidatePage of [...session.pageStarts.keys()].sort(
      (a, b) => a - b,
    )) {
      if (candidatePage <= page) {
        currentPage = candidatePage;
        cursor = session.pageStarts.get(candidatePage);
      }
    }

    while (currentPage <= page) {
      const step = await this.executeReadPlanStep(plan, cursor, pageSize, sort);
      const nextPage = currentPage + 1;
      if (step.nextCursor === undefined) {
        session.terminalPage = step.rows.length === 0 ? currentPage : nextPage;
      } else {
        session.pageStarts.set(nextPage, step.nextCursor);
      }

      if (currentPage === page) {
        return { rows: step.rows };
      }
      if (step.nextCursor === undefined) {
        return { rows: [] };
      }

      cursor = step.nextCursor;
      currentPage = nextPage;
    }

    return { rows: [] };
  }

  private getCursorSession(cacheKey: string): DynamoCursorSession {
    const existing = this.cursorCache.get(cacheKey);
    if (existing) {
      return existing;
    }
    const session: DynamoCursorSession = {
      pageStarts: new Map([[1, undefined]]),
      terminalPage: null,
    };
    this.cursorCache.set(cacheKey, session);
    return session;
  }

  private async executeReadPlanStep(
    plan: QueryReadPlan | ScanReadPlan,
    cursor: Record<string, AttributeValue> | undefined,
    pageSize: number,
    sort: DriverSortConfig | null,
  ): Promise<ReadStepResult> {
    if (plan.kind === "query") {
      const input: QueryCommandInput = {
        ...plan.baseInput,
        Limit: pageSize,
        ...(cursor ? { ExclusiveStartKey: cursor } : {}),
      };
      if (sort && plan.sortKeyName === sort.column) {
        input.ScanIndexForward = sort.direction !== "desc";
      }
      const response = await this.requireClient().send(new QueryCommand(input));
      return {
        rows: (response.Items ?? []).map((item) => unmarshall(item)),
        nextCursor: response.LastEvaluatedKey,
      };
    }

    const input: ScanCommandInput = {
      ...plan.baseInput,
      Limit: pageSize,
      ...(cursor ? { ExclusiveStartKey: cursor } : {}),
    };
    const response = await this.requireClient().send(new ScanCommand(input));
    return {
      rows: (response.Items ?? []).map((item) => unmarshall(item)),
      nextCursor: response.LastEvaluatedKey,
    };
  }

  private async countReadPlan(plan: DynamoReadPlan): Promise<number> {
    if (plan.kind === "getItem") {
      const response = await this.requireClient().send(
        new GetItemCommand({
          TableName: plan.table,
          Key: this.marshallKey(plan.key),
        }),
      );
      return response.Item ? 1 : 0;
    }

    const cacheKey = `${plan.requestSignature}::count`;
    const session = this.getCursorSession(cacheKey);
    if (session.totalCount !== undefined) {
      return session.totalCount;
    }

    let totalCount = 0;
    let cursor: Record<string, AttributeValue> | undefined;
    do {
      if (plan.kind === "query") {
        const response = await this.requireClient().send(
          new QueryCommand({
            ...plan.baseInput,
            Select: "COUNT",
            ...(cursor ? { ExclusiveStartKey: cursor } : {}),
          }),
        );
        totalCount += response.Count ?? 0;
        cursor = response.LastEvaluatedKey;
      } else {
        const response = await this.requireClient().send(
          new ScanCommand({
            ...plan.baseInput,
            Select: "COUNT",
            ...(cursor ? { ExclusiveStartKey: cursor } : {}),
          }),
        );
        totalCount += response.Count ?? 0;
        cursor = response.LastEvaluatedKey;
      }
    } while (cursor !== undefined);

    session.totalCount = totalCount;
    return totalCount;
  }

  private async materializeReadPlanRows(
    plan: DynamoReadPlan,
    maxRows: number,
  ): Promise<MaterializedReadResult> {
    if (plan.kind === "getItem") {
      const response = await this.requireClient().send(
        new GetItemCommand({
          TableName: plan.table,
          Key: this.marshallKey(plan.key),
        }),
      );
      return {
        rows: response.Item ? [unmarshall(response.Item)] : [],
        truncated: false,
      };
    }

    const rows: Record<string, unknown>[] = [];
    let cursor: Record<string, AttributeValue> | undefined;
    let truncated = false;
    do {
      const step = await this.executeReadPlanStep(
        plan,
        cursor,
        Math.min(maxRows - rows.length, DYNAMODB_CURSOR_FETCH_LIMIT),
        null,
      );
      rows.push(...step.rows);
      cursor = step.nextCursor;
      if (rows.length >= maxRows && cursor !== undefined) {
        truncated = true;
        break;
      }
    } while (cursor !== undefined && rows.length < maxRows);

    return { rows, truncated };
  }

  private buildPutItemInput(
    table: string,
    keyNames: readonly string[],
    values: Record<string, unknown>,
  ): PutItemCommandInput {
    return {
      TableName: table,
      Item: this.marshallItem(values),
      ConditionExpression: this.buildMissingItemConditionExpression(keyNames),
    };
  }

  private buildUpdateItemInput(
    table: string,
    keyNames: readonly string[],
    update: DriverUpdateRowsRequest["updates"][number],
  ): UpdateItemCommandInput | null {
    const key = this.extractCompleteKey(keyNames, update.primaryKeys);
    const entries = Object.entries(update.changes).filter(
      ([name, value]) => !keyNames.includes(name) && value !== undefined,
    );
    for (const keyName of keyNames) {
      if (
        Object.hasOwn(update.changes, keyName) &&
        update.changes[keyName] !== update.primaryKeys[keyName]
      ) {
        throw new Error(
          `DynamoDB does not support updating key attribute '${keyName}'.`,
        );
      }
    }
    if (!key || entries.length === 0) {
      return null;
    }

    const names: Record<string, string> = {};
    const values: Record<string, AttributeValue> = {};
    const assignments = entries.map(([name, value], index) => {
      const namePlaceholder = `#u${index}`;
      const valuePlaceholder = `:u${index}`;
      names[namePlaceholder] = name;
      values[valuePlaceholder] = this.toAttributeValue(value);
      return `${namePlaceholder} = ${valuePlaceholder}`;
    });

    return {
      TableName: table,
      Key: this.marshallKey(key),
      UpdateExpression: `SET ${assignments.join(", ")}`,
      ConditionExpression: this.buildExistingItemConditionExpression(keyNames),
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
      ReturnValues: "ALL_NEW",
    };
  }

  private buildDeleteItemInput(
    table: string,
    keyNames: readonly string[],
    criteria: Record<string, unknown>,
  ): DeleteItemCommandInput {
    const key = this.extractCompleteKey(keyNames, criteria);
    if (!key) {
      throw new Error("DynamoDB delete requires the full primary key.");
    }
    return {
      TableName: table,
      Key: this.marshallKey(key),
      ConditionExpression: this.buildExistingItemConditionExpression(keyNames),
      ReturnValues: "ALL_OLD",
    };
  }

  private extractCompleteKey(
    keyNames: readonly string[],
    source: Record<string, unknown>,
  ): Record<string, unknown> | null {
    if (keyNames.length === 0) {
      return null;
    }
    const key: Record<string, unknown> = {};
    for (const keyName of keyNames) {
      if (source[keyName] === undefined) {
        return null;
      }
      key[keyName] = source[keyName];
    }
    return key;
  }

  private buildExistingItemConditionExpression(
    keyNames: readonly string[],
  ): string {
    return keyNames
      .map((keyName) => `attribute_exists(${keyName})`)
      .join(" AND ");
  }

  private buildMissingItemConditionExpression(
    keyNames: readonly string[],
  ): string {
    return keyNames
      .map((keyName) => `attribute_not_exists(${keyName})`)
      .join(" AND ");
  }

  private marshallItem(
    item: Record<string, unknown>,
  ): Record<string, AttributeValue> {
    return marshall(item, { removeUndefinedValues: true });
  }

  private marshallKey(
    key: Record<string, unknown>,
  ): Record<string, AttributeValue> {
    return marshall(key, { removeUndefinedValues: true });
  }

  private toAttributeValue(value: unknown): AttributeValue {
    return marshall({ value }, { removeUndefinedValues: true })
      .value as AttributeValue;
  }

  private async dispatchNativeCommand(
    operation: DynamoDbNativeOperationName,
    rawInput: Record<string, unknown>,
  ): Promise<QueryDispatchResult> {
    const input = this.requireInputRecord(rawInput);
    switch (operation) {
      case "GetItem": {
        const output = await this.requireClient().send(
          new GetItemCommand(input as unknown as GetItemCommandInput),
        );
        return this.toQueryDispatchResult(operation, output);
      }
      case "Query": {
        const output = await this.requireClient().send(
          new QueryCommand(input as unknown as QueryCommandInput),
        );
        return this.toQueryDispatchResult(operation, output);
      }
      case "Scan": {
        const output = await this.requireClient().send(
          new ScanCommand(input as unknown as ScanCommandInput),
        );
        return this.toQueryDispatchResult(operation, output);
      }
      case "PutItem": {
        const output = await this.requireClient().send(
          new PutItemCommand(input as unknown as PutItemCommandInput),
        );
        this.invalidateCursorCacheForTable(String(input.TableName ?? ""));
        return this.toQueryDispatchResult(operation, output, input);
      }
      case "UpdateItem": {
        const output = await this.requireClient().send(
          new UpdateItemCommand(input as unknown as UpdateItemCommandInput),
        );
        this.invalidateCursorCacheForTable(String(input.TableName ?? ""));
        return this.toQueryDispatchResult(operation, output, input);
      }
      case "DeleteItem": {
        const output = await this.requireClient().send(
          new DeleteItemCommand(input as unknown as DeleteItemCommandInput),
        );
        this.invalidateCursorCacheForTable(String(input.TableName ?? ""));
        return this.toQueryDispatchResult(operation, output, input);
      }
      case "BatchGetItem": {
        const output = await this.requireClient().send(
          new BatchGetItemCommand(input as unknown as BatchGetItemCommandInput),
        );
        return this.toQueryDispatchResult(operation, output);
      }
      case "BatchWriteItem": {
        const output = await this.requireClient().send(
          new BatchWriteItemCommand(
            input as unknown as BatchWriteItemCommandInput,
          ),
        );
        this.invalidateAllTablesInRequest(input);
        return this.toQueryDispatchResult(operation, output, input);
      }
      case "TransactGetItems": {
        const output = await this.requireClient().send(
          new TransactGetItemsCommand(
            input as unknown as TransactGetItemsCommandInput,
          ),
        );
        return this.toQueryDispatchResult(operation, output);
      }
      case "TransactWriteItems": {
        const output = await this.requireClient().send(
          new TransactWriteItemsCommand(
            input as unknown as TransactWriteItemsCommandInput,
          ),
        );
        this.invalidateTablesInTransaction(input);
        return this.toQueryDispatchResult(operation, output, input);
      }
      default:
        throw new Error(`Unsupported DynamoDB native operation: ${operation}`);
    }
  }

  private invalidateAllTablesInRequest(input: Record<string, unknown>): void {
    if (!isRecord(input.RequestItems)) {
      return;
    }
    for (const tableName of Object.keys(input.RequestItems)) {
      this.invalidateCursorCacheForTable(tableName);
    }
  }

  private invalidateTablesInTransaction(input: Record<string, unknown>): void {
    const transactItems = Array.isArray(input.TransactItems)
      ? input.TransactItems
      : [];
    for (const transactItem of transactItems) {
      if (!isRecord(transactItem)) {
        continue;
      }
      for (const value of Object.values(transactItem)) {
        if (isRecord(value) && typeof value.TableName === "string") {
          this.invalidateCursorCacheForTable(value.TableName);
        }
      }
    }
  }

  private requireInputRecord(input: unknown): Record<string, unknown> {
    if (input === undefined) {
      return {};
    }
    if (!isRecord(input)) {
      throw new Error("DynamoDB native command input must be a JSON object.");
    }
    return this.normalizeNativeInputValue(input) as Record<string, unknown>;
  }

  private resolveNativeOperation(
    inputs: readonly Record<string, unknown>[],
  ): DynamoDbNativeOperationName {
    const inferredOperations = new Set<DynamoDbNativeOperationName>();
    for (const input of inputs) {
      const inferred = inferDynamoDbNativeOperationName(input);
      if (inferred) {
        inferredOperations.add(inferred);
      }
    }

    if (inferredOperations.size > 1) {
      throw new Error(
        "DynamoDB native query text contains request bodies for multiple different actions. Split them by action or run them separately.",
      );
    }

    const inferredOperation = inferredOperations.values().next().value as
      | DynamoDbNativeOperationName
      | undefined;

    if (inferredOperation) {
      return inferredOperation;
    }

    throw new Error(
      "DynamoDB request type could not be determined from the JSON body. Use an unambiguous AWS request shape.",
    );
  }

  private normalizeNativeInputValue(value: unknown): unknown {
    if (Array.isArray(value)) {
      return value.map((entry) => this.normalizeNativeInputValue(entry));
    }
    if (!isRecord(value)) {
      return value;
    }
    const attributeValue = this.normalizeAttributeValueEnvelope(value);
    if (attributeValue !== null) {
      return attributeValue;
    }
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        this.normalizeNativeInputValue(entry),
      ]),
    );
  }

  private normalizeAttributeValueEnvelope(
    value: Record<string, unknown>,
  ): AttributeValue | null {
    const keys = Object.keys(value);
    if (keys.length !== 1) {
      return null;
    }

    const [key] = keys;
    switch (key) {
      case "B": {
        if (typeof value.B !== "string") {
          return null;
        }
        return { B: this.parseBinaryInput(value.B) ?? Buffer.from(value.B) };
      }
      case "BS": {
        if (!Array.isArray(value.BS)) {
          return null;
        }
        return {
          BS: value.BS.map((entry) => {
            if (typeof entry !== "string") {
              return Buffer.from(String(entry));
            }
            return this.parseBinaryInput(entry) ?? Buffer.from(entry);
          }),
        };
      }
      case "L": {
        if (!Array.isArray(value.L)) {
          return null;
        }
        return {
          L: value.L.map(
            (entry) => this.normalizeNativeInputValue(entry) as AttributeValue,
          ),
        };
      }
      case "M": {
        if (!isRecord(value.M)) {
          return null;
        }
        return {
          M: Object.fromEntries(
            Object.entries(value.M).map(([entryKey, entryValue]) => [
              entryKey,
              this.normalizeNativeInputValue(entryValue) as AttributeValue,
            ]),
          ),
        };
      }
      case "S":
      case "N":
      case "BOOL":
      case "NULL":
      case "SS":
      case "NS":
        return value as unknown as AttributeValue;
      default:
        return null;
    }
  }

  private toQueryDispatchResult(
    operation: DynamoDbNativeOperationName,
    output:
      | GetItemCommandOutput
      | QueryCommandOutput
      | ScanCommandOutput
      | PutItemCommandOutput
      | UpdateItemCommandOutput
      | DeleteItemCommandOutput
      | BatchGetItemCommandOutput
      | TransactGetItemsCommandOutput
      | Record<string, unknown>,
    input?: Record<string, unknown>,
  ): QueryDispatchResult {
    switch (operation) {
      case "GetItem": {
        const typedOutput = output as GetItemCommandOutput;
        return {
          rows: typedOutput.Item ? [unmarshall(typedOutput.Item)] : [],
        };
      }
      case "Query": {
        const typedOutput = output as QueryCommandOutput;
        return {
          rows: (typedOutput.Items ?? []).map(
            (item: Record<string, AttributeValue>) => unmarshall(item),
          ),
        };
      }
      case "Scan": {
        const typedOutput = output as ScanCommandOutput;
        return {
          rows: (typedOutput.Items ?? []).map(
            (item: Record<string, AttributeValue>) => unmarshall(item),
          ),
        };
      }
      case "BatchGetItem": {
        const typedOutput = output as BatchGetItemCommandOutput;
        const responses = isRecord(typedOutput.Responses)
          ? typedOutput.Responses
          : {};
        const tableNames = Object.keys(responses);
        return {
          rows: tableNames.flatMap((tableName) => {
            const items = Array.isArray(responses[tableName])
              ? (responses[tableName] as Array<Record<string, AttributeValue>>)
              : [];
            return items.map((item) => {
              const row = unmarshall(item);
              return tableNames.length > 1
                ? { __tableName: tableName, ...row }
                : row;
            });
          }),
        };
      }
      case "TransactGetItems": {
        const typedOutput = output as TransactGetItemsCommandOutput;
        const responses = Array.isArray(typedOutput.Responses)
          ? typedOutput.Responses
          : [];
        return {
          rows: responses.flatMap(
            (response: { Item?: Record<string, AttributeValue> }) =>
              response.Item ? [unmarshall(response.Item)] : [],
          ),
        };
      }
      case "PutItem":
      case "UpdateItem":
      case "DeleteItem": {
        const typedOutput = output as
          | PutItemCommandOutput
          | UpdateItemCommandOutput
          | DeleteItemCommandOutput;
        return {
          rows: typedOutput.Attributes
            ? [unmarshall(typedOutput.Attributes)]
            : [],
          affectedRows: 1,
        };
      }
      case "BatchWriteItem": {
        const typedOutput = output as BatchWriteItemCommandOutput;
        const requestedCount = this.countBatchWriteRequests(
          input?.RequestItems,
        );
        const unprocessedCount = this.countBatchWriteRequests(
          typedOutput.UnprocessedItems,
        );
        return {
          rows: [],
          affectedRows: Math.max(0, requestedCount - unprocessedCount),
        };
      }
      case "TransactWriteItems":
        return {
          rows: [],
          affectedRows: Array.isArray(input?.TransactItems)
            ? input.TransactItems.length
            : 0,
        };
      default:
        return { rows: [] };
    }
  }

  private buildMutationPreviewDocuments(
    operation: "insert" | "update" | "delete",
    table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
    keyNames?: readonly string[],
  ): Record<string, unknown>[] {
    if (operation === "insert") {
      return [
        this.buildPutItemInput(
          table,
          keyNames ?? Object.keys(data.values ?? {}),
          data.values ?? {},
        ) as unknown as Record<string, unknown>,
      ];
    }

    if (operation === "update") {
      const input = this.buildUpdateItemInput(
        table,
        Object.keys(data.primaryKeys ?? {}),
        {
          primaryKeys: data.primaryKeys ?? {},
          changes: data.changes ?? {},
        },
      );
      return input ? [input as unknown as Record<string, unknown>] : [];
    }

    const criteriaList =
      data.primaryKeyValuesList && data.primaryKeyValuesList.length > 0
        ? data.primaryKeyValuesList
        : [data.primaryKeys ?? {}];
    return criteriaList.map(
      (criteria) =>
        this.buildDeleteItemInput(
          table,
          Object.keys(criteria),
          criteria,
        ) as unknown as Record<string, unknown>,
    );
  }

  private formatNativePreviewEnvelope(
    document: Record<string, unknown>,
  ): string {
    return JSON.stringify(this.serializeNativePreviewValue(document), null, 2);
  }

  private countBatchWriteRequests(requestItems: unknown): number {
    if (!isRecord(requestItems)) {
      return 0;
    }

    return Object.values(requestItems).reduce<number>(
      (total, value) => total + (Array.isArray(value) ? value.length : 0),
      0,
    );
  }

  private serializeNativePreviewValue(value: unknown): unknown {
    if (value === null || value === undefined) {
      return value;
    }
    if (this.isBinaryValue(value)) {
      const buffer = this.toBinaryBuffer(value);
      return buffer ? buffer.toString("base64") : value;
    }
    if (Array.isArray(value)) {
      return value.map((entry) => this.serializeNativePreviewValue(entry));
    }
    if (value instanceof Set) {
      return [...value].map((entry) => this.serializeNativePreviewValue(entry));
    }
    if (this.isPlainObject(value)) {
      return Object.fromEntries(
        Object.entries(value).map(([key, entry]) => [
          key,
          this.serializeNativePreviewValue(entry),
        ]),
      );
    }
    return value;
  }

  private async readRowsForDescription(
    table: string,
    limit: number,
  ): Promise<Record<string, unknown>[]> {
    const result = await this.materializeReadPlanRows(
      {
        kind: "scan",
        table,
        schema: {
          keys: [],
          keyRoles: new Map(),
          attrTypes: new Map(),
          secondaryIndexes: [],
        },
        baseInput: { TableName: table },
        requestSignature: stableStringify({ kind: "scan", table }),
      },
      limit,
    );
    return result.rows;
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

  private isConditionalCheckFailure(error: unknown): boolean {
    return (
      typeof error === "object" &&
      error !== null &&
      "name" in error &&
      (error as { name?: unknown }).name === "ConditionalCheckFailedException"
    );
  }

  private mergeDescribedColumns(
    sourceColumns: readonly ColumnTypeMeta[],
    describedColumns: readonly ColumnTypeMeta[],
    schema: DynamoTableSchema,
  ): ColumnTypeMeta[] {
    const describedByName = new Map(
      describedColumns.map((column) => [column.name, column]),
    );
    return sourceColumns.map((column) => ({
      ...column,
      type: describedByName.get(column.name)?.type ?? column.type,
      nativeType:
        describedByName.get(column.name)?.nativeType ?? column.nativeType,
      category: describedByName.get(column.name)?.category ?? column.category,
      nullable: describedByName.get(column.name)?.nullable ?? column.nullable,
      filterable:
        describedByName.get(column.name)?.filterable ?? column.filterable,
      filterOperators:
        describedByName.get(column.name)?.filterOperators ??
        column.filterOperators,
      valueSemantics:
        describedByName.get(column.name)?.valueSemantics ??
        column.valueSemantics,
      isPrimaryKey: schema.keys.includes(column.name),
      primaryKeyOrdinal: schema.keys.includes(column.name)
        ? schema.keys.indexOf(column.name) + 1
        : undefined,
      primaryKeyRole: schema.keyRoles.get(column.name),
    }));
  }

  private splitFilterList(value: string): string[] {
    return value
      .split(",")
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
  }

  private coerceFilterValueForColumn(
    column: ColumnTypeMeta | undefined,
    rawValue: string | [string, string] | undefined,
  ): unknown {
    if (rawValue === undefined) {
      return undefined;
    }
    if (Array.isArray(rawValue)) {
      return rawValue.map((entry) => this.coerceFilterParameter(column, entry));
    }
    return this.coerceFilterParameter(column, rawValue);
  }

  private coerceFilterParameter(
    column: ColumnTypeMeta | undefined,
    rawValue: string,
  ): unknown {
    const value = rawValue.trim();
    if (!column) {
      return value;
    }
    if (
      column.category === "integer" ||
      column.category === "float" ||
      column.category === "decimal"
    ) {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : value;
    }
    if (column.category === "boolean" || column.valueSemantics === "boolean") {
      if (/^(true|1)$/i.test(value)) {
        return true;
      }
      if (/^(false|0)$/i.test(value)) {
        return false;
      }
    }
    if (column.category === "json" || column.category === "array") {
      try {
        return JSON.parse(value) as unknown;
      } catch {
        return value;
      }
    }
    return value;
  }

  private buildDynamoColumns(
    rows: readonly Record<string, unknown>[],
    schema: DynamoTableSchema,
  ): ColumnTypeMeta[] {
    const columnNames = new Set<string>(schema.attrTypes.keys());
    for (const row of rows) {
      for (const key of Object.keys(row)) {
        columnNames.add(key);
      }
    }

    const keyOrder = new Map(
      schema.keys.map((key, index) => [key, index] as const),
    );

    return [...columnNames]
      .sort((left, right) => {
        const leftKeyOrder = keyOrder.get(left);
        const rightKeyOrder = keyOrder.get(right);
        if (leftKeyOrder !== undefined || rightKeyOrder !== undefined) {
          if (leftKeyOrder === undefined) {
            return 1;
          }
          if (rightKeyOrder === undefined) {
            return -1;
          }
          return leftKeyOrder - rightKeyOrder;
        }
        return left.localeCompare(right);
      })
      .map((name) => {
        const descriptor = this.resolveDynamoColumnDescriptor(
          rows.map((row) => row[name]),
          schema.attrTypes.get(name),
        );
        const isPrimaryKey = schema.keys.includes(name);
        const nullable = isPrimaryKey ? false : true;
        const filterable = descriptor.category !== "binary";
        return {
          name,
          type: descriptor.nativeType,
          nativeType: descriptor.nativeType,
          category: descriptor.category,
          nullable,
          defaultValue: undefined,
          isPrimaryKey,
          primaryKeyOrdinal: isPrimaryKey
            ? schema.keys.indexOf(name) + 1
            : undefined,
          primaryKeyRole: schema.keyRoles.get(name),
          isForeignKey: false,
          filterable,
          filterOperators: resolveFilterOperators(descriptor.category, {
            filterable,
            nullable,
          }),
          valueSemantics: descriptor.valueSemantics,
        } satisfies ColumnTypeMeta;
      });
  }

  private resolveDynamoColumnDescriptor(
    samples: readonly unknown[],
    attrType?: string,
  ): {
    nativeType: string;
    category: TypeCategory;
    valueSemantics: ValueSemantics;
  } {
    const schemaDescriptor = attrType
      ? this.dynamoAttrTypeToDescriptor(attrType)
      : null;
    const sampleDescriptor = this.describeSampleValues(samples);

    if (schemaDescriptor) {
      return {
        nativeType: schemaDescriptor.nativeType,
        category: sampleDescriptor?.category ?? schemaDescriptor.category,
        valueSemantics:
          sampleDescriptor?.valueSemantics ?? schemaDescriptor.valueSemantics,
      };
    }

    return (
      sampleDescriptor ?? {
        nativeType: "other",
        category: "other",
        valueSemantics: "plain",
      }
    );
  }

  private dynamoAttrTypeToDescriptor(attributeType: string): {
    nativeType: string;
    category: TypeCategory;
    valueSemantics: ValueSemantics;
  } {
    switch (attributeType) {
      case "S":
        return {
          nativeType: "string",
          category: "text",
          valueSemantics: "plain",
        };
      case "N":
        return {
          nativeType: "number",
          category: "decimal",
          valueSemantics: "plain",
        };
      case "B":
        return {
          nativeType: "binary",
          category: "binary",
          valueSemantics: "plain",
        };
      default:
        return {
          nativeType: "string",
          category: "text",
          valueSemantics: "plain",
        };
    }
  }

  private describeSampleValues(samples: readonly unknown[]): {
    nativeType: string;
    category: TypeCategory;
    valueSemantics: ValueSemantics;
  } | null {
    let nullDescriptor: {
      nativeType: string;
      category: TypeCategory;
      valueSemantics: ValueSemantics;
    } | null = null;

    for (const sample of samples) {
      if (sample === undefined) {
        continue;
      }
      const descriptor = this.describeDynamoValue(sample);
      if (descriptor.nativeType !== "null") {
        return descriptor;
      }
      nullDescriptor = descriptor;
    }

    return nullDescriptor;
  }

  private describeDynamoValue(value: unknown): {
    nativeType: string;
    category: TypeCategory;
    valueSemantics: ValueSemantics;
  } {
    if (value === null || value === undefined) {
      return {
        nativeType: "null",
        category: "other",
        valueSemantics: "plain",
      };
    }
    if (typeof value === "string") {
      return {
        nativeType: "string",
        category: "text",
        valueSemantics: "plain",
      };
    }
    if (typeof value === "number") {
      return {
        nativeType: "number",
        category: Number.isInteger(value) ? "integer" : "float",
        valueSemantics: "plain",
      };
    }
    if (typeof value === "bigint") {
      return {
        nativeType: "number",
        category: "integer",
        valueSemantics: "plain",
      };
    }
    if (typeof value === "boolean") {
      return {
        nativeType: "boolean",
        category: "boolean",
        valueSemantics: "boolean",
      };
    }
    if (this.isBinaryValue(value)) {
      return {
        nativeType: "binary",
        category: "binary",
        valueSemantics: "plain",
      };
    }
    if (value instanceof Set) {
      return {
        nativeType: this.describeSetType(value),
        category: "array",
        valueSemantics: "plain",
      };
    }
    if (Array.isArray(value)) {
      return {
        nativeType: "list",
        category: "array",
        valueSemantics: "plain",
      };
    }
    return {
      nativeType: "map",
      category: "json",
      valueSemantics: "plain",
    };
  }

  private describeSetType(value: Set<unknown>): string {
    const entries = [...value];
    if (entries.length === 0) {
      return "string set";
    }
    if (entries.every((entry) => typeof entry === "string")) {
      return "string set";
    }
    if (
      entries.every(
        (entry) => typeof entry === "number" || typeof entry === "bigint",
      )
    ) {
      return "number set";
    }
    if (entries.every((entry) => this.isBinaryValue(entry))) {
      return "binary set";
    }
    return "string set";
  }

  private formatDynamoRowForDisplay(
    row: Record<string, unknown>,
    columnsByName?: ReadonlyMap<string, ColumnTypeMeta>,
  ): Record<string, unknown> {
    return Object.fromEntries(
      Object.entries(row).map(([name, value]) => {
        const column = columnsByName?.get(name);
        return [
          name,
          column
            ? this.formatOutputValue(value, column)
            : this.formatGenericDisplayValue(value),
        ];
      }),
    );
  }

  private formatGenericDisplayValue(value: unknown): unknown {
    if (value === null || value === undefined) {
      return null;
    }
    if (this.isBinaryValue(value)) {
      return this.formatBinaryForDisplay(value);
    }
    if (value instanceof Set) {
      return this.formatSetForDisplay(value);
    }
    const normalized = this.normalizeValueForDisplay(value);
    return typeof normalized === "string" ||
      typeof normalized === "number" ||
      typeof normalized === "boolean" ||
      normalized === null
      ? normalized
      : JSON.stringify(normalized);
  }

  private normalizeValueForDisplay(value: unknown): unknown {
    if (value === null || value === undefined) {
      return null;
    }
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean"
    ) {
      return value;
    }
    if (typeof value === "bigint") {
      return value.toString();
    }
    if (value instanceof Date) {
      return value.toISOString();
    }
    if (this.isBinaryValue(value)) {
      return this.formatBinaryForDisplay(value);
    }
    if (value instanceof Set) {
      return this.formatSetForDisplay(value);
    }
    if (Array.isArray(value)) {
      return value.map((entry) => this.normalizeValueForDisplay(entry));
    }
    if (this.isPlainObject(value)) {
      return Object.fromEntries(
        Object.entries(value).map(([key, entry]) => [
          key,
          this.normalizeValueForDisplay(entry),
        ]),
      );
    }
    return String(value);
  }

  private formatSetForDisplay(value: unknown): string {
    if (typeof value === "string") {
      return value;
    }
    if (value instanceof Set) {
      return `<<${[...value]
        .map((entry) => this.formatSetEntryForDisplay(entry))
        .join(", ")}>>`;
    }
    if (Array.isArray(value)) {
      return `<<${value.map((entry) => this.formatSetEntryForDisplay(entry)).join(", ")}>>`;
    }
    return String(value);
  }

  private formatSetEntryForDisplay(value: unknown): string {
    if (typeof value === "string") {
      return `'${value.replace(/'/g, "''")}'`;
    }
    if (typeof value === "number" || typeof value === "bigint") {
      return String(value);
    }
    if (typeof value === "boolean") {
      return value ? "true" : "false";
    }
    if (value === null) {
      return "NULL";
    }
    if (this.isBinaryValue(value)) {
      return this.formatBinaryForDisplay(value);
    }
    return JSON.stringify(this.normalizeValueForDisplay(value));
  }

  private formatBinaryForDisplay(value: unknown): string {
    const bytes = this.toBinaryBuffer(value);
    return bytes ? `0x${bytes.toString("hex")}` : String(value);
  }

  private parseBinaryInput(value: string): Buffer | null {
    const normalized = value.trim();
    const hex = this.extractBinaryHex(normalized);
    if (hex !== null) {
      return Buffer.from(hex, "hex");
    }
    if (
      normalized.length > 0 &&
      normalized.length % 4 === 0 &&
      /^[A-Za-z0-9+/]+={0,2}$/.test(normalized)
    ) {
      try {
        return Buffer.from(normalized, "base64");
      } catch {
        return null;
      }
    }
    return null;
  }

  private parseSetInput(
    value: string,
    subtype: "string" | "number" | "binary",
  ): Set<unknown> | null {
    const parsedJson = this.parseJsonValue(value);
    if (Array.isArray(parsedJson)) {
      return new Set(
        parsedJson.map((entry) => this.coerceSetEntryValue(entry, subtype)),
      );
    }

    const trimmed = value.trim();
    if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
      return null;
    }
    const parsed = this.parseJsonValue(trimmed);
    if (!Array.isArray(parsed)) {
      return null;
    }
    return new Set(
      parsed.map((entry) => this.coerceSetEntryValue(entry, subtype)),
    );
  }

  private coerceSetEntryValue(
    value: unknown,
    subtype: "string" | "number" | "binary",
  ): unknown {
    if (subtype === "string") {
      return typeof value === "string" ? value : String(value);
    }
    if (subtype === "number") {
      if (typeof value === "number") {
        return value;
      }
      if (typeof value === "bigint") {
        return Number(value);
      }
      const numeric = Number(String(value));
      return Number.isFinite(numeric) ? numeric : value;
    }
    if (this.isBinaryValue(value)) {
      return value;
    }
    return this.parseBinaryInput(String(value).trim()) ?? value;
  }

  private parseJsonValue(value: string): unknown {
    try {
      return JSON.parse(value) as unknown;
    } catch {
      return null;
    }
  }

  private extractBinaryHex(value: string): string | null {
    let hex = value;
    if (
      value.startsWith("0x") ||
      value.startsWith("0X") ||
      value.startsWith("\\x") ||
      value.startsWith("\\X")
    ) {
      hex = value.slice(2);
    }
    if (hex.length === 0 || hex.length % 2 !== 0 || !/^[0-9a-f]+$/i.test(hex)) {
      return null;
    }
    return hex;
  }

  private isBinaryValue(value: unknown): boolean {
    return this.toBinaryBuffer(value) !== null;
  }

  private toBinaryBuffer(value: unknown): Buffer | null {
    if (Buffer.isBuffer(value)) {
      return value;
    }
    if (value instanceof Uint8Array) {
      return Buffer.from(value);
    }
    if (ArrayBuffer.isView(value)) {
      return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    }
    if (value instanceof ArrayBuffer) {
      return Buffer.from(value);
    }
    return null;
  }

  private isPlainObject(value: unknown): value is Record<string, unknown> {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }
}
