import {
  DescribeTableCommand,
  DynamoDBClient,
  ListTablesCommand,
} from "@aws-sdk/client-dynamodb";
import { fromIni } from "@aws-sdk/credential-providers";
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  PutCommand,
  ScanCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
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

  getCapabilities() {
    return {
      tabularRead: "nosql" as const,
      queryMode: "text" as const,
      supportsMutations: true,
    };
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    return [
      {
        name: this.config.database || this.config.awsRegion || "default",
        schemas: [],
      },
    ];
  }

  async listSchemas(): Promise<SchemaInfo[]> {
    return [{ name: "public" }];
  }

  async listObjects(): Promise<TableInfo[]> {
    try {
      const result = await this.requireClient().send(new ListTablesCommand({}));
      return (result.TableNames ?? [])
        .slice()
        .sort((left, right) => left.localeCompare(right))
        .map((tableName) => ({
          schema: "public",
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
    const [rows, { keys, attrTypes }] = await Promise.all([
      this.readRows(table, 1000),
      this.getTableSchema(table),
    ]);
    const columns = inferColumnsFromRows(rows, keys[0] ?? "id");
    return columns.map((column) => ({
      name: column.name,
      type: attrTypes.has(column.name)
        ? this.dynamoAttrTypeToNative(attrTypes.get(column.name)!)
        : column.nativeType,
      nullable: !keys.includes(column.name),
      defaultValue: undefined,
      isPrimaryKey: keys.includes(column.name),
      primaryKeyOrdinal: keys.includes(column.name)
        ? keys.indexOf(column.name) + 1
        : undefined,
      isForeignKey: false,
    }));
  }

  async describeColumns(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const [rows, { keys, attrTypes }] = await Promise.all([
      this.readRows(table, 1000),
      this.getTableSchema(table),
    ]);
    const columns = inferColumnsFromRows(rows, keys[0] ?? "id");
    return columns.map((column) => {
      const nativeType = attrTypes.has(column.name)
        ? this.dynamoAttrTypeToNative(attrTypes.get(column.name)!)
        : column.nativeType;
      return {
        ...column,
        type: nativeType,
        nativeType,
        nullable: !keys.includes(column.name),
        isPrimaryKey: keys.includes(column.name),
        primaryKeyOrdinal: keys.includes(column.name)
          ? keys.indexOf(column.name) + 1
          : undefined,
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
      const indexes = description.Table?.GlobalSecondaryIndexes ?? [];
      return indexes.map((index) => ({
        name: index.IndexName ?? "index",
        columns: (index.KeySchema ?? [])
          .map((entry) => entry.AttributeName)
          .filter((name): name is string => Boolean(name)),
        unique: false,
        primary: false,
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
    unsupported("DynamoDB constraints DDL");
  }

  async getIndexDDL(): Promise<string> {
    unsupported("DynamoDB index DDL");
  }

  async getTriggerDDL(): Promise<string> {
    unsupported("DynamoDB trigger DDL");
  }

  async getCreateTableDDL(): Promise<string> {
    unsupported("DynamoDB create table DDL");
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
    if (trimmed.startsWith("scan ")) {
      const payload = JSON.parse(trimmed.slice("scan ".length)) as {
        table: string;
        limit?: number;
      };
      const rows = await this.readRows(payload.table, payload.limit ?? 200);
      const columns = inferColumnsFromRows(rows, "id").map(
        (column) => column.name,
      );
      return {
        columns,
        rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
        rowCount: rows.length,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (trimmed.startsWith("put ")) {
      const payload = JSON.parse(trimmed.slice("put ".length)) as {
        table: string;
        item: Record<string, unknown>;
      };
      await this.requireDocumentClient().send(
        new PutCommand({ TableName: payload.table, Item: payload.item }),
      );
      const row = { result: "ok", table: payload.table };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    if (trimmed.startsWith("update ")) {
      const payload = JSON.parse(trimmed.slice("update ".length)) as {
        table: string;
        key: Record<string, unknown>;
        set: Record<string, unknown>;
      };
      const entries = Object.entries(payload.set);
      const expressionParts = entries.map(
        ([,], index) => `#n${index} = :v${index}`,
      );
      const names = Object.fromEntries(
        entries.map(([name], index) => [`#n${index}`, name]),
      );
      const values = Object.fromEntries(
        entries.map(([, value], index) => [`:v${index}`, value]),
      );
      await this.requireDocumentClient().send(
        new UpdateCommand({
          TableName: payload.table,
          Key: payload.key,
          UpdateExpression: `SET ${expressionParts.join(", ")}`,
          ExpressionAttributeNames: names,
          ExpressionAttributeValues: values,
        }),
      );
      const row = { result: "ok", table: payload.table };
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
        table: string;
        key: Record<string, unknown>;
      };
      await this.requireDocumentClient().send(
        new DeleteCommand({ TableName: payload.table, Key: payload.key }),
      );
      const row = { result: "ok", table: payload.table };
      return {
        columns: Object.keys(row),
        rows: [this.mapRowToQueryRow(row, Object.keys(row))],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: Date.now() - startedAt,
      };
    }

    throw new Error(
      'DynamoDB query mode expects:\n  scan {"table":"Users","limit":100}\n  put {"table":"Users","item":{...}}\n  update {"table":"Users","key":{"id":"..."},"set":{"field":"value"}}\n  delete {"table":"Users","key":{"id":"..."}}',
    );
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    const rows = await this.readRows(request.table, 5000);
    const filtered = applyFilters(rows, request.filters);
    const sorted = applySort(filtered, request.sort);
    const paged = pageRows(sorted, request.page, request.pageSize);
    const keySchema = await this.getTableKeyNames(request.table);
    const columns = inferColumnsFromRows(sorted, keySchema[0] ?? "id").map(
      (column) => ({
        ...column,
        isPrimaryKey: keySchema.includes(column.name),
        primaryKeyOrdinal: keySchema.includes(column.name)
          ? keySchema.indexOf(column.name) + 1
          : undefined,
      }),
    );
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
      const key = Object.fromEntries(
        keys
          .filter((name) => update.primaryKeys[name] !== undefined)
          .map((name) => [name, update.primaryKeys[name]]),
      );
      if (Object.keys(key).length === 0) {
        continue;
      }

      const entries = Object.entries(update.changes).filter(
        ([name]) => !keys.includes(name),
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

      const expressionParts = entries.map(
        ([, _value], index) => `#n${index} = :v${index}`,
      );
      const names = Object.fromEntries(
        entries.map(([name], index) => [`#n${index}`, name]),
      );
      const values = Object.fromEntries(
        entries.map(([, value], index) => [`:v${index}`, value]),
      );
      const conditionNames = Object.fromEntries(
        keys.map((name, index) => [`#k${index}`, name]),
      );
      const conditionExpression = keys
        .map((_, index) => `attribute_exists(#k${index})`)
        .join(" AND ");

      try {
        await this.requireDocumentClient().send(
          new UpdateCommand({
            TableName: request.table,
            Key: key,
            UpdateExpression: `SET ${expressionParts.join(", ")}`,
            ConditionExpression: conditionExpression,
            ExpressionAttributeNames: {
              ...names,
              ...conditionNames,
            },
            ExpressionAttributeValues: values,
          }),
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
    const keys = await this.getTableKeyNames(request.table);
    const conditionNames = Object.fromEntries(
      keys.map((name, index) => [`#k${index}`, name]),
    );
    const conditionExpression =
      keys.length === 0
        ? undefined
        : keys
            .map((_, index) => `attribute_not_exists(#k${index})`)
            .join(" AND ");

    try {
      await this.requireDocumentClient().send(
        new PutCommand({
          TableName: request.table,
          Item: request.values,
          ConditionExpression: conditionExpression,
          ExpressionAttributeNames:
            conditionExpression === undefined ? undefined : conditionNames,
        }),
      );
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
    const keys = await this.getTableKeyNames(request.table);
    let affectedRows = 0;
    for (const criteria of request.primaryKeyValuesList) {
      const key = Object.fromEntries(
        keys
          .filter((name) => criteria[name] !== undefined)
          .map((name) => [name, criteria[name]]),
      );
      if (Object.keys(key).length === 0) {
        continue;
      }
      const conditionNames = Object.fromEntries(
        keys.map((name, index) => [`#k${index}`, name]),
      );
      const conditionExpression = keys
        .map((_, index) => `attribute_exists(#k${index})`)
        .join(" AND ");
      try {
        await this.requireDocumentClient().send(
          new DeleteCommand({
            TableName: request.table,
            Key: key,
            ConditionExpression: conditionExpression,
            ExpressionAttributeNames: conditionNames,
          }),
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
      return `put ${JSON.stringify({ table, item: data.values ?? {} })}`;
    }
    if (operation === "update") {
      return `update ${JSON.stringify({ table, key: data.primaryKeys ?? {}, set: data.changes ?? {} })}`;
    }
    // delete — show the first (or only) key
    const key = data.primaryKeyValuesList?.[0] ?? data.primaryKeys ?? {};
    return `delete ${JSON.stringify({ table, key })}`;
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
    return stringifyCommandPayload("ddb_insert", { table: qualifiedTableName });
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

  private async getTableSchema(
    table: string,
  ): Promise<{ keys: string[]; attrTypes: Map<string, string> }> {
    try {
      const description = await this.requireClient().send(
        new DescribeTableCommand({ TableName: table }),
      );
      const keys = (description.Table?.KeySchema ?? [])
        .map((entry) => entry.AttributeName)
        .filter((name): name is string => Boolean(name));
      const attrTypes = new Map(
        (description.Table?.AttributeDefinitions ?? []).map((attr) => [
          attr.AttributeName ?? "",
          attr.AttributeType ?? "S",
        ]),
      );
      return { keys, attrTypes };
    } catch {
      return { keys: [], attrTypes: new Map() };
    }
  }

  private async getTableKeyNames(table: string): Promise<string[]> {
    return (await this.getTableSchema(table)).keys;
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
  ): Promise<Record<string, unknown>[]> {
    try {
      const result = await this.requireDocumentClient().send(
        new ScanCommand({
          TableName: table,
          Limit: limit,
        }),
      );
      return (result.Items ?? []).map((item) =>
        flattenRootRecord(item as Record<string, unknown>),
      );
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
}
