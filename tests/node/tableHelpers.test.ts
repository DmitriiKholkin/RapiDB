import { describe, expect, it } from "vitest";
import type {
  ColumnTypeMeta,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import { buildWhere } from "../../src/extension/table/filterSql";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    isForeignKey: false,
    isAutoIncrement: true,
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "display_name",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "amount",
    type: "DECIMAL(10,2)",
    nativeType: "DECIMAL(10,2)",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "decimal",
    filterable: true,
    filterOperators: ["eq", "between", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "amount_x2",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    isComputed: true,
    computedExpression: "amount * 2",
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
];

const compositePrimaryKeyColumns: ColumnTypeMeta[] = [
  {
    name: "tenant_id",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "external_id",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 2,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
  {
    name: "description",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  },
];

const fakeDriver: IDBDriver = {
  connect: async () => undefined,
  disconnect: async () => undefined,
  isConnected: () => true,
  listDatabases: async () => [],
  listSchemas: async () => [],
  listObjects: async () => [],
  describeTable: async () => [],
  describeColumns: async () => columns,
  getIndexes: async () => [],
  getForeignKeys: async () => [],
  getCreateTableDDL: async () => "",
  getRoutineDefinition: async () => "",
  query: async () => ({
    columns: [],
    rows: [],
    rowCount: 0,
    executionTimeMs: 0,
  }),
  runTransaction: async () => undefined,
  quoteIdentifier: (name: string) => `"${name}"`,
  qualifiedTableName: (_database: string, schema: string, table: string) =>
    `${schema}.${table}`,
  buildPagination: (offset: number, limit: number) => ({
    sql: `LIMIT ${limit} OFFSET ${offset}`,
    params: [],
  }),
  buildOrderByDefault: () => "ORDER BY 1",
  coerceInputValue: (value: unknown) => value,
  formatOutputValue: (value: unknown) => value,
  checkPersistedEdit: (column, expectedValue, options) => {
    if (column.name === "amount" && expectedValue === "invalid") {
      return { ok: false, shouldVerify: false, message: "Invalid amount" };
    }

    if (column.name === "amount" && options?.persistedValue === "mismatch") {
      return { ok: false, shouldVerify: true, message: "Persisted mismatch" };
    }

    if (column.name === "amount") {
      return { ok: true, shouldVerify: true };
    }

    return null;
  },
  normalizeFilterValue: (_column, _operator, value) => value,
  buildFilterCondition: (column, operator, value, paramIndex) => ({
    sql:
      operator === "between"
        ? `${column.name} BETWEEN $${paramIndex} AND $${paramIndex + 1}`
        : `${column.name} ${operator} $${paramIndex}`,
    params: value === undefined ? [] : Array.isArray(value) ? value : [value],
  }),
  buildInsertDefaultValuesSql: (qualifiedTableName: string) =>
    `INSERT INTO ${qualifiedTableName} DEFAULT VALUES`,
  buildInsertValueExpr: (_column, paramIndex) => `$${paramIndex}`,
  buildSetExpr: (column, paramIndex) => `"${column.name}" = $${paramIndex}`,
  materializePreviewSql: (sql, params = []) => `${sql} -- ${params.join(",")}`,
};

describe("table helpers", () => {
  it("builds a combined WHERE clause from valid filters", () => {
    const result = buildWhere(
      fakeDriver,
      [
        { column: "display_name", operator: "like", value: "Alpha" },
        { column: "amount", operator: "between", value: ["10", "20"] },
        { column: "missing", operator: "eq", value: "x" },
      ],
      columns,
    );

    expect(result.clause).toBe(
      "WHERE display_name like $1 AND amount BETWEEN $2 AND $3",
    );
    expect(result.params).toEqual(["Alpha", "10", "20"]);
  });

  it("builds an insert operation including explicit key and identity values", () => {
    const operation = buildInsertRowOperation(
      fakeDriver,
      "main",
      "public",
      "fixture_rows",
      {
        id: 99,
        display_name: "Gamma",
        amount: "10.25",
        ignored: "noop",
      },
      columns,
    );

    expect(operation.sql).toBe(
      'INSERT INTO public.fixture_rows ("id", "display_name", "amount") VALUES ($1, $2, $3)',
    );
    expect(operation.params).toEqual([99, "Gamma", "10.25"]);
  });

  it("builds a default-values insert operation when no fields are provided", () => {
    const operation = buildInsertRowOperation(
      fakeDriver,
      "main",
      "public",
      "fixture_rows",
      {},
      columns,
    );

    expect(operation.sql).toBe(
      "INSERT INTO public.fixture_rows DEFAULT VALUES",
    );
    expect(operation.params).toEqual([]);
  });

  it("includes computed columns in insert operations", () => {
    const operation = buildInsertRowOperation(
      fakeDriver,
      "main",
      "public",
      "fixture_rows",
      {
        display_name: "Gamma",
        amount: "10.25",
        amount_x2: "20.50",
      },
      columns,
    );

    expect(operation.sql).toBe(
      'INSERT INTO public.fixture_rows ("display_name", "amount", "amount_x2") VALUES ($1, $2, $3)',
    );
    expect(operation.params).toEqual(["Gamma", "10.25", "20.50"]);
  });

  it("builds Oracle-safe default-only insert SQL using explicit DEFAULT expressions", () => {
    const oracleLikeDriver: IDBDriver = {
      ...fakeDriver,
      quoteIdentifier: (name: string) => `"${name}"`,
      qualifiedTableName: (_database: string, schema: string, table: string) =>
        `"${schema}"."${table}"`,
      buildInsertDefaultValuesSql: (
        qualifiedTableName: string,
        tableColumns?: readonly ColumnTypeMeta[],
      ) => {
        if (!tableColumns || tableColumns.length === 0) {
          return `INSERT INTO ${qualifiedTableName} DEFAULT VALUES`;
        }

        const columnNames = tableColumns.map((column) => `"${column.name}"`);
        const defaults = tableColumns.map(() => "DEFAULT");

        return `INSERT INTO ${qualifiedTableName} (${columnNames.join(", ")}) VALUES (${defaults.join(", ")})`;
      },
      buildInsertValueExpr: (_column, paramIndex) => `:${paramIndex}`,
      buildSetExpr: (column, paramIndex) => `"${column.name}" = :${paramIndex}`,
    };

    const operation = buildInsertRowOperation(
      oracleLikeDriver,
      "main",
      "PUBLIC",
      "fixture_rows",
      {},
      columns,
    );

    expect(operation.sql).toBe(
      'INSERT INTO "PUBLIC"."fixture_rows" ("id", "display_name", "amount", "amount_x2") VALUES (DEFAULT, DEFAULT, DEFAULT, DEFAULT)',
    );
    expect(operation.params).toEqual([]);
  });

  it("prepares preview SQL without prevalidation blocking and keeps key edits", () => {
    const result = prepareApplyChangesPlan(
      {
        getDriver: () => fakeDriver,
      } as never,
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "invalid" },
        },
        {
          primaryKeys: { id: 2 },
          changes: { id: 3 },
        },
      ],
      columns,
    );

    expect(result.executable).toBe(true);
    if (!result.executable) {
      throw new Error("Expected executable plan");
    }

    expect(result.plan.previewStatements).toHaveLength(2);
    expect(result.plan.previewStatements[0]).toContain(
      "UPDATE public.fixture_rows",
    );
    expect(result.plan.previewStatements[1]).toContain('SET "id" = $1');
    expect(result.plan.skippedRows).toEqual([]);
  });

  it("returns executable preview statements for valid row updates and skips empty changes", () => {
    const result = prepareApplyChangesPlan(
      {
        getDriver: () => fakeDriver,
      } as never,
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      [
        {
          primaryKeys: { id: 1 },
          changes: { display_name: "Alpha Updated", amount: "10.25" },
        },
        {
          primaryKeys: { id: 2 },
          changes: {},
        },
      ],
      columns,
    );

    expect(result.executable).toBe(true);
    if (!result.executable) {
      throw new Error("Expected executable plan");
    }

    expect(result.plan.previewStatements).toHaveLength(1);
    expect(result.plan.previewStatements[0]).toContain(
      "UPDATE public.fixture_rows",
    );
    expect(result.plan.skippedRows).toEqual([1]);
  });

  it("requires the full primary key for delete previews", async () => {
    const mutationService = new TableMutationService(
      {
        getConnection: () => ({ id: "conn-1" }),
        getDriver: () => fakeDriver,
      } as never,
      {
        getColumns: async () => compositePrimaryKeyColumns,
      },
    );

    await expect(
      mutationService.prepareDeleteRowsPlan(
        "conn-1",
        "main",
        "public",
        "composite_fixture_rows",
        [{ tenant_id: 1 }],
      ),
    ).rejects.toThrow(/full primary key/i);

    await expect(
      mutationService.prepareDeleteRowsPlan(
        "conn-1",
        "main",
        "public",
        "composite_fixture_rows",
        [{ tenant_id: 1, external_id: 2, description: "extra" }],
      ),
    ).rejects.toThrow(/full primary key/i);
  });
});
