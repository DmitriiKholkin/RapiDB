import {
  DescribeTableCommand,
  DynamoDBClient,
  type GlobalSecondaryIndexDescription,
  ListTablesCommand,
  type LocalSecondaryIndexDescription,
  type Projection,
  type ProvisionedThroughputDescription,
  type TableDescription,
} from "@aws-sdk/client-dynamodb";
import { fromIni } from "@aws-sdk/credential-providers";
import {
  DynamoDBDocumentClient,
  ExecuteStatementCommand,
} from "@aws-sdk/lib-dynamodb";
import type { PrimaryKeyRole } from "../../shared/tableTypes";
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
} from "./types";

const DYNAMODB_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "supported",
    triggers: "not_applicable",
  },
};

const DYNAMODB_MAX_TABLE_ROWS = 5000;
const DYNAMODB_READ_BATCH_SIZE = 200;

export class DynamoDBDriver implements IDBDriver {
  private client: DynamoDBClient | null = null;
  private documentClient: DynamoDBDocumentClient | null = null;
  private connected = false;

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

    await client.send(new ListTablesCommand({ Limit: 1 }));
    this.client = client;
    this.documentClient = DynamoDBDocumentClient.from(client, {
      marshallOptions: { removeUndefinedValues: true },
    });
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.client = null;
    this.documentClient = null;
    this.connected = false;
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
    const [rows, { keys, keyRoles, attrTypes }] = await Promise.all([
      this.readRows(table, 1000),
      this.getTableSchema(table),
    ]);
    const columns = inferColumnsFromRows(rows, keys[0] ?? "id", {
      primaryKeyNames: keys,
      nullableMode: "schemaLess",
    });
    return columns.map((column) => {
      const attrType = attrTypes.get(column.name);
      return {
        name: column.name,
        type: attrType
          ? this.dynamoAttrTypeToNative(attrType)
          : column.nativeType,
        nullable: column.nullable,
        defaultValue: undefined,
        isPrimaryKey: column.isPrimaryKey,
        primaryKeyOrdinal: column.primaryKeyOrdinal,
        primaryKeyRole: keyRoles.get(column.name),
        isForeignKey: false,
      };
    });
  }

  async describeColumns(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const [rows, { keys, keyRoles, attrTypes }] = await Promise.all([
      this.readRows(table, 1000),
      this.getTableSchema(table),
    ]);
    const columns = inferColumnsFromRows(rows, keys[0] ?? "id", {
      primaryKeyNames: keys,
      nullableMode: "schemaLess",
    });
    return columns.map((column) => {
      const attrType = attrTypes.get(column.name);
      const nativeType = attrType
        ? this.dynamoAttrTypeToNative(attrType)
        : column.nativeType;
      return {
        ...column,
        type: nativeType,
        nativeType,
        primaryKeyRole: keyRoles.get(column.name),
      };
    });
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
    const statementKind = this.detectPartiqlStatementKind(trimmed);
    if (!statementKind) {
      throw new Error(
        'DynamoDB query mode expects a PartiQL statement, for example:\n  SELECT * FROM "Users"\n  INSERT INTO "Users" VALUE {\'id\': \'user-1\'}\n  UPDATE "Users" SET "email" = \'person@example.com\' WHERE "id" = \'user-1\'\n  DELETE FROM "Users" WHERE "id" = \'user-1\'',
      );
    }

    const response = await this.executeStatement(trimmed, _params);
    const rows = (response.Items ?? []).map((item) =>
      flattenRootRecord(item as Record<string, unknown>),
    );
    const columns = inferColumnsFromRows(rows, "id").map(
      (column) => column.name,
    );
    const rowCount = rows.length;
    const affectedRows =
      statementKind === "select"
        ? undefined
        : rowCount > 0
          ? rowCount
          : statementKind === "delete"
            ? 0
            : 1;

    return {
      columns,
      rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
      rowCount,
      affectedRows,
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
    const rows = await this.readRows(request.table, DYNAMODB_MAX_TABLE_ROWS, {
      filters: request.filters,
      columns: describedColumns,
      sort: request.sort,
    });
    const filtered = applyFilters(rows, request.filters);
    const sorted = applySort(filtered, request.sort);
    const paged = pageRows(sorted, request.page, request.pageSize);
    const describedByName = new Map(
      describedColumns.map((column) => [column.name, column]),
    );
    const sourceColumns =
      sorted.length > 0
        ? inferColumnsFromRows(sorted, schema.keys[0] ?? "id", {
            primaryKeyNames: schema.keys,
            nullableMode: "schemaLess",
          })
        : describedColumns;
    const columns = sourceColumns.map((column) => {
      const described = describedByName.get(column.name);
      return {
        ...column,
        type: described?.type ?? column.type,
        nativeType: described?.nativeType ?? column.nativeType,
        category: described?.category ?? column.category,
        nullable: described?.nullable ?? column.nullable,
        filterable: described?.filterable ?? column.filterable,
        filterOperators: described?.filterOperators ?? column.filterOperators,
        valueSemantics: described?.valueSemantics ?? column.valueSemantics,
        isPrimaryKey: schema.keys.includes(column.name),
        primaryKeyOrdinal: schema.keys.includes(column.name)
          ? schema.keys.indexOf(column.name) + 1
          : undefined,
        primaryKeyRole: schema.keyRoles.get(column.name),
      };
    });
    return {
      columns,
      rows: paged,
      totalCount: request.skipCount ? 0 : sorted.length,
    };
  }

  async updateRows(
    request: DriverUpdateRowsRequest,
  ): Promise<DriverMutationResult> {
    const keys = await this.getTableKeyNames(request.table);
    const partitionKey = keys[0];
    if (!partitionKey) {
      throw new Error("DynamoDB update requires a table partition key.");
    }

    let affectedRows = 0;
    for (const update of request.updates) {
      const keyWhere = this.buildKeyWhereClause(keys, update.primaryKeys);
      if (!keyWhere) {
        throw new Error("DynamoDB update requires the full primary key.");
      }

      const entries = Object.entries(update.changes).filter(
        ([name, value]) => !keys.includes(name) && value !== undefined,
      );
      for (const keyName of keys) {
        if (
          Object.hasOwn(update.changes, keyName) &&
          update.changes[keyName] !== update.primaryKeys[keyName]
        ) {
          throw new Error(
            `DynamoDB does not support updating key attribute '${keyName}'.`,
          );
        }
      }
      if (entries.length === 0) {
        continue;
      }

      const setClauses = entries.map(
        ([name]) => `SET ${this.quoteIdentifier(name)} = ?`,
      );
      const parameters = [
        ...entries.map(([, value]) => value),
        ...keyWhere.params,
      ];

      try {
        await this.executeStatement(
          `UPDATE ${this.quoteIdentifier(request.table)} ${setClauses.join(" ")} WHERE ${keyWhere.sql} RETURNING ALL NEW *`,
          parameters,
        );
        affectedRows += 1;
      } catch (error: unknown) {
        if (this.isConditionalCheckFailure(error)) {
          continue;
        }
        throw error;
      }
    }

    return { affectedRows };
  }

  async insertRow(
    request: DriverInsertRowRequest,
  ): Promise<DriverMutationResult> {
    const statement = this.buildInsertStatement(request.table, request.values);

    try {
      await this.executeStatement(statement.sql, statement.params);
      return { affectedRows: 1 };
    } catch (error: unknown) {
      if (this.isDuplicateItemError(error)) {
        return { affectedRows: 0 };
      }
      throw error;
    }
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
  ): Promise<DriverMutationResult> {
    const keys = await this.getTableKeyNames(request.table);
    let affectedRows = 0;
    for (const criteria of request.primaryKeyValuesList) {
      const keyWhere = this.buildKeyWhereClause(keys, criteria);
      if (!keyWhere) {
        throw new Error("DynamoDB delete requires the full primary key.");
      }
      try {
        const response = await this.executeStatement(
          `DELETE FROM ${this.quoteIdentifier(request.table)} WHERE ${keyWhere.sql} RETURNING ALL OLD *`,
          keyWhere.params,
        );
        affectedRows += response.Items && response.Items.length > 0 ? 1 : 0;
      } catch (error: unknown) {
        if (this.isConditionalCheckFailure(error)) {
          continue;
        }
        throw error;
      }
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
      const statement = this.buildInsertStatement(table, data.values ?? {});
      return this.materializePreviewSql(statement.sql, statement.params);
    }
    if (operation === "update") {
      const keyNames = Object.keys(data.primaryKeys ?? {});
      const keyWhere = this.buildKeyWhereClause(
        keyNames,
        data.primaryKeys ?? {},
      );
      const entries = Object.entries(data.changes ?? {}).filter(
        ([, value]) => value !== undefined,
      );
      const statement = `UPDATE ${this.quoteIdentifier(table)} ${entries
        .map(([name]) => `SET ${this.quoteIdentifier(name)} = ?`)
        .join(" ")} WHERE ${keyWhere?.sql ?? "1 = 1"} RETURNING ALL NEW *`;
      const params = [
        ...entries.map(([, value]) => value),
        ...(keyWhere?.params ?? []),
      ];
      return this.materializePreviewSql(statement, params);
    }
    return (
      data.primaryKeyValuesList?.length
        ? data.primaryKeyValuesList
        : [data.primaryKeys ?? {}]
    )
      .map((criteria) => {
        const keyNames = Object.keys(criteria);
        const keyWhere = this.buildKeyWhereClause(keyNames, criteria);
        return this.materializePreviewSql(
          `DELETE FROM ${this.quoteIdentifier(table)} WHERE ${keyWhere?.sql ?? "1 = 1"} RETURNING ALL OLD *`,
          keyWhere?.params,
        );
      })
      .join(";\n");
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
    offset: number,
    limit: number,
    _paramIndex: number,
  ): PaginationResult {
    return {
      sql: offset > 0 ? "LIMIT ?" : "LIMIT ?",
      params: [limit],
    };
  }

  buildOrderByDefault(_cols: ColumnTypeMeta[]): string {
    return "";
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
        sql: `(${identifier} IS MISSING OR ${identifier} IS NULL)`,
        params: [],
      };
    }
    if (operator === "is_not_null") {
      return {
        sql: `NOT (${identifier} IS MISSING OR ${identifier} IS NULL)`,
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
        sql: `${identifier} IN [${parts.map(() => "?").join(", ")}]`,
        params: parts.map((entry) => this.coerceFilterParameter(column, entry)),
      };
    }
    if (operator === "like" || operator === "ilike") {
      if (typeof value !== "string") {
        return null;
      }
      const needle = value.replace(/%/g, "");
      return {
        sql: `contains(${identifier}, ?)`,
        params: [needle],
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
    return `INSERT INTO ${qualifiedTableName} VALUE {}`;
  }

  buildInsertValueExpr(_column: ColumnTypeMeta, _paramIndex: number): string {
    return "?";
  }

  buildSetExpr(column: ColumnTypeMeta): string {
    return `${this.quoteIdentifier(column.name)} = ?`;
  }

  materializePreviewSql(sql: string, params?: readonly unknown[]): string {
    if (!params || params.length === 0) {
      return sql;
    }

    let paramIndex = 0;
    return sql.replace(/\?/g, () => {
      const value = params[paramIndex];
      paramIndex += 1;
      return this.formatPartiqlLiteral(value);
    });
  }

  private requireClient(): DynamoDBClient {
    if (!this.client || !this.connected) {
      throw new Error("DynamoDB is not connected.");
    }
    return this.client;
  }

  private requireDocumentClient(): DynamoDBDocumentClient {
    if (!this.documentClient || !this.connected) {
      throw new Error("DynamoDB document client is not connected.");
    }
    return this.documentClient;
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

  private async getTableSchema(table: string): Promise<{
    keys: string[];
    keyRoles: Map<string, PrimaryKeyRole>;
    attrTypes: Map<string, string>;
  }> {
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
      return { keys, keyRoles, attrTypes };
    } catch {
      return { keys: [], keyRoles: new Map(), attrTypes: new Map() };
    }
  }

  private async getTableKeyNames(table: string): Promise<string[]> {
    return (await this.getTableSchema(table)).keys;
  }

  private async executeStatement(
    statement: string,
    parameters?: readonly unknown[],
    options?: { limit?: number; nextToken?: string },
  ) {
    return this.requireDocumentClient().send(
      new ExecuteStatementCommand({
        Statement: statement,
        Parameters:
          parameters && parameters.length > 0 ? [...parameters] : undefined,
        Limit: options?.limit,
        NextToken: options?.nextToken,
      }),
    );
  }

  private detectPartiqlStatementKind(
    statement: string,
  ): "select" | "insert" | "update" | "delete" | null {
    const match = /^\s*(select|insert|update|delete)\b/i.exec(statement);
    if (!match) {
      return null;
    }

    return match[1].toLowerCase() as "select" | "insert" | "update" | "delete";
  }

  private dynamoAttrTypeToNative(attributeType: string): string {
    switch (attributeType) {
      case "S":
        return "text";
      case "N":
        return "float";
      case "B":
        return "binary";
      default:
        return "text";
    }
  }

  private async readRows(
    table: string,
    limit: number,
    options?: {
      filters?: readonly FilterExpression[];
      columns?: readonly ColumnTypeMeta[];
      sort?: DriverSortConfig | null;
    },
  ): Promise<Record<string, unknown>[]> {
    try {
      const serverFilter = this.buildServerFilterClause(
        options?.filters ?? [],
        options?.columns ?? [],
      );
      const serverSort = await this.buildServerSortClause(
        table,
        options?.sort ?? null,
      );
      const statement = [
        `SELECT * FROM ${this.quoteIdentifier(table)}`,
        serverFilter.clause,
        serverSort,
      ]
        .filter(Boolean)
        .join(" ");
      const rows: Record<string, unknown>[] = [];
      let nextToken: string | undefined;

      do {
        const remaining = limit - rows.length;
        if (remaining <= 0) {
          break;
        }

        const result = await this.executeStatement(
          statement,
          serverFilter.params,
          {
            limit: Math.min(remaining, DYNAMODB_READ_BATCH_SIZE),
            nextToken,
          },
        );
        rows.push(
          ...(result.Items ?? []).map((item) =>
            flattenRootRecord(item as Record<string, unknown>),
          ),
        );
        nextToken = result.NextToken;
      } while (nextToken);

      return rows;
    } catch {
      return [];
    }
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

  private isDuplicateItemError(error: unknown): boolean {
    return (
      typeof error === "object" &&
      error !== null &&
      "name" in error &&
      (error as { name?: unknown }).name === "DuplicateItemException"
    );
  }

  private buildServerFilterClause(
    filters: readonly FilterExpression[],
    columns: readonly ColumnTypeMeta[],
  ): { clause: string; params: unknown[] } {
    if (filters.length === 0 || columns.length === 0) {
      return { clause: "", params: [] };
    }

    const columnMap = new Map(columns.map((column) => [column.name, column]));
    const conditions: string[] = [];
    const params: unknown[] = [];

    for (const filter of filters) {
      const column = columnMap.get(filter.column);
      if (!column) {
        continue;
      }

      const normalizedValue = this.normalizeFilterValue(
        column,
        filter.operator,
        "value" in filter ? filter.value : undefined,
      );
      const condition = this.buildFilterCondition(
        column,
        filter.operator,
        normalizedValue,
        params.length + 1,
      );
      if (!condition) {
        continue;
      }

      conditions.push(condition.sql);
      params.push(...condition.params);
    }

    if (conditions.length === 0) {
      return { clause: "", params: [] };
    }

    return {
      clause: `WHERE ${conditions.join(" AND ")}`,
      params,
    };
  }

  private async buildServerSortClause(
    table: string,
    sort: DriverSortConfig | null,
  ): Promise<string> {
    if (!sort) {
      return "";
    }

    const keyNames = await this.getTableKeyNames(table);
    if (!keyNames.includes(sort.column)) {
      return "";
    }

    return `ORDER BY ${this.quoteIdentifier(sort.column)} ${sort.direction === "desc" ? "DESC" : "ASC"}`;
  }

  private splitFilterList(value: string): string[] {
    return value
      .split(",")
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
  }

  private coerceFilterParameter(
    column: ColumnTypeMeta,
    rawValue: string,
  ): unknown {
    const value = rawValue.trim();
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

  private buildInsertStatement(
    table: string,
    values: Record<string, unknown>,
  ): TransactionOperation {
    const entries = Object.entries(values).filter(
      ([, value]) => value !== undefined,
    );
    const sql = `INSERT INTO ${this.quoteIdentifier(table)} VALUE {${entries
      .map(([name]) => `${this.quoteItemKey(name)}: ?`)
      .join(", ")}}`;
    return {
      sql,
      params: entries.map(([, value]) => value),
    };
  }

  private buildKeyWhereClause(
    keyNames: readonly string[],
    criteria: Record<string, unknown>,
  ): { sql: string; params: unknown[] } | null {
    if (
      keyNames.length === 0 ||
      keyNames.some((name) => criteria[name] === undefined)
    ) {
      return null;
    }

    return {
      sql: keyNames
        .map((name) => `${this.quoteIdentifier(name)} = ?`)
        .join(" AND "),
      params: keyNames.map((name) => criteria[name]),
    };
  }

  private quoteItemKey(name: string): string {
    return `'${name.replace(/'/g, "''")}'`;
  }

  private formatPartiqlLiteral(value: unknown): string {
    if (value === null) {
      return "NULL";
    }
    if (value === undefined) {
      return "MISSING";
    }
    if (typeof value === "string") {
      return `'${value.replace(/'/g, "''")}'`;
    }
    if (typeof value === "number" || typeof value === "bigint") {
      return String(value);
    }
    if (typeof value === "boolean") {
      return value ? "true" : "false";
    }
    if (value instanceof Date) {
      return `'${value.toISOString().replace(/'/g, "''")}'`;
    }
    if (value instanceof Set) {
      return `<<${[...value].map((entry) => this.formatPartiqlLiteral(entry)).join(", ")}>>`;
    }
    if (Array.isArray(value)) {
      return `[${value.map((entry) => this.formatPartiqlLiteral(entry)).join(", ")}]`;
    }
    if (Buffer.isBuffer(value)) {
      return `'${value.toString("base64")}'`;
    }
    if (typeof value === "object") {
      return `{${Object.entries(value as Record<string, unknown>)
        .map(
          ([key, entryValue]) =>
            `${this.quoteItemKey(key)}: ${this.formatPartiqlLiteral(entryValue)}`,
        )
        .join(", ")}}`;
    }

    return `'${String(value).replace(/'/g, "''")}'`;
  }
}
