import { describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type {
  ColumnTypeMeta,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import { buildWhere } from "../../src/extension/table/filterSql";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    isForeignKey: false,
    identityGeneration: "auto_increment",
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
  {
    name: "display_name",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
    valueSemantics: "plain",
  },
  {
    name: "amount",
    type: "DECIMAL(10,2)",
    nativeType: "DECIMAL(10,2)",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "decimal",
    filterable: true,
    filterOperators: ["eq", "between"],
    valueSemantics: "plain",
  },
  {
    name: "amount_x2",
    type: "INTEGER",
    nativeType: "INTEGER",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
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
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
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
    category: "integer",
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
  {
    name: "description",
    type: "TEXT",
    nativeType: "TEXT",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: ["eq", "like"],
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
  getConstraints: async () => [],
  getTriggers: async () => [],
  getConstraintDDL: async () => "",
  getIndexDDL: async () => "",
  getTriggerDDL: async () => "",
  getCreateTableDDL: async () => "",
  getObjectDefinition: async () => null,
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

  it("rejects forged null-only filters when a column policy excludes them", () => {
    const nonNullableColumns: ColumnTypeMeta[] = [
      {
        name: "display_name",
        type: "TEXT",
        nativeType: "TEXT",
        nullable: false,
        isPrimaryKey: false,
        isForeignKey: false,
        category: "text",
        filterable: true,
        filterOperators: ["eq", "like"],
        valueSemantics: "plain",
      },
    ];

    expect(() =>
      buildWhere(
        fakeDriver,
        [{ column: "display_name", operator: "is_null" }],
        nonNullableColumns,
      ),
    ).toThrow(
      "[RapiDB Filter] Column display_name does not support is_null filters.",
    );

    expect(() =>
      buildWhere(
        fakeDriver,
        [{ column: "display_name", operator: "is_not_null" }],
        nonNullableColumns,
      ),
    ).toThrow(
      "[RapiDB Filter] Column display_name does not support is_not_null filters.",
    );
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

  it("skips strict verification for temporal on-update columns across drivers", async () => {
    const temporalColumns: ColumnTypeMeta[] = [
      {
        name: "id",
        type: "INTEGER",
        nativeType: "INTEGER",
        nullable: false,
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
        isForeignKey: false,
        category: "integer",
        filterable: true,
        filterOperators: ["eq"],
        valueSemantics: "plain",
      },
      {
        name: "updated_at",
        type: "TIMESTAMP",
        nativeType: "TIMESTAMP",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
        onUpdateExpression: "CURRENT_TIMESTAMP",
        category: "datetime",
        filterable: true,
        filterOperators: ["eq", "neq", "is_null", "is_not_null"],
        valueSemantics: "plain",
      },
    ];

    const strictDriver: IDBDriver = {
      ...fakeDriver,
      describeColumns: async () => temporalColumns,
      checkPersistedEdit: (column) => {
        if (column.name === "updated_at") {
          throw new Error("updated_at should not be verified strictly");
        }
        return null;
      },
      runTransaction: async () => undefined,
      query: async () => ({
        columns: ["updated_at"],
        rows: [{ __col_0: "2026-05-29 14:57:54.510808" }],
        rowCount: 1,
        executionTimeMs: 0,
      }),
    };

    const prepared = prepareApplyChangesPlan(
      {
        getDriver: () => strictDriver,
      } as never,
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      [
        {
          primaryKeys: { id: 1 },
          changes: { updated_at: "2026-05-29 13:45:36.917354" },
        },
      ],
      temporalColumns,
    );

    expect(prepared.executable).toBe(true);
    if (!prepared.executable) {
      throw new Error("Expected executable plan");
    }

    expect(prepared.plan.verificationTargets[0]?.values).toEqual([]);

    const result = await executePreparedApplyPlan(
      {
        getDriver: () => strictDriver,
      } as never,
      prepared.plan,
    );

    expect(result.success).toBe(true);
    expect(result.warning).toBeUndefined();
  });

  it("uses column-aware PostgreSQL preview materialization for typed array updates", () => {
    const postgresDriver = new PostgresDriver({
      id: "pg-preview-test",
      name: "pg-preview-test",
      type: "pg",
      host: "localhost",
      port: 5432,
      database: "postgres",
      username: "postgres",
      password: "postgres",
    } as ConnectionConfig);

    const postgresColumns: ColumnTypeMeta[] = [
      {
        name: "id",
        type: "integer",
        nativeType: "integer",
        nullable: false,
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
        isForeignKey: false,
        category: "integer",
        filterable: true,
        filterOperators: ["eq"],
        valueSemantics: "plain",
      },
      {
        name: "col_jsonb_array",
        type: "jsonb[]",
        nativeType: "jsonb[]",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
        category: "array",
        filterable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
        valueSemantics: "plain",
      },
    ];

    const result = prepareApplyChangesPlan(
      {
        getDriver: () => postgresDriver,
      } as never,
      "conn-pg",
      "main",
      "public",
      "fixture_rows",
      [
        {
          primaryKeys: { id: 1 },
          changes: { col_jsonb_array: ['{"a":1}', '{"b":3}'] },
        },
      ],
      postgresColumns,
    );

    expect(result.executable).toBe(true);
    if (!result.executable) {
      throw new Error("Expected executable plan");
    }

    expect(result.plan.previewStatements).toHaveLength(1);
    expect(result.plan.previewStatements[0]).toContain(
      `"col_jsonb_array" = CAST(ARRAY['{"a":1}', '{"b":3}'] AS jsonb[])`,
    );
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

  it("builds and executes driver-backed update plans with warning on partial matches", async () => {
    const driver: IDBDriver = {
      ...fakeDriver,
      coerceInputValue: (value, column) => {
        if (typeof value !== "string") {
          return value;
        }
        if (column.category === "integer") {
          return Number.parseInt(value, 10);
        }
        if (column.name === "display_name") {
          return value.trim().toUpperCase();
        }
        return value;
      },
      updateRows: async () => ({ affectedRows: 1 }),
    };

    const prepared = prepareApplyChangesPlan(
      {
        getDriver: () => driver,
      } as never,
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      [
        {
          primaryKeys: { id: "1" },
          changes: { display_name: " Alpha Updated " },
        },
        {
          primaryKeys: { id: "2" },
          changes: { display_name: " Beta Updated " },
        },
      ],
      columns,
    );

    expect(prepared.executable).toBe(true);
    if (!prepared.executable) {
      throw new Error("Expected executable plan");
    }

    expect(prepared.plan.mode).toBe("driver");
    expect(prepared.plan.previewStatements).toEqual([
      'UPDATE public.fixture_rows {"primaryKeys":{"id":1},"changes":{"display_name":"ALPHA UPDATED"}}',
      'UPDATE public.fixture_rows {"primaryKeys":{"id":2},"changes":{"display_name":"BETA UPDATED"}}',
    ]);

    const result = await executePreparedApplyPlan(
      {
        getDriver: () => driver,
      } as never,
      prepared.plan,
    );

    expect(result).toEqual({
      success: true,
      warning: "Some updates may not have matched a row in the source backend.",
      rowOutcomes: [
        { rowIndex: 0, success: true, status: "applied" },
        { rowIndex: 1, success: true, status: "applied" },
      ],
    });
  });

  it("skips driver-backed updates whose writable change set becomes empty", async () => {
    const updateRowsSpy = vi.fn(async () => ({ affectedRows: 1 }));
    const driver: IDBDriver = {
      ...fakeDriver,
      updateRows: updateRowsSpy,
    };

    const prepared = prepareApplyChangesPlan(
      {
        getDriver: () => driver,
      } as never,
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      [
        {
          primaryKeys: { id: 1 },
          changes: { display_name: undefined, missing: "ignored" },
        },
        {
          primaryKeys: { id: 2 },
          changes: { display_name: "Updated" },
        },
      ],
      columns,
    );

    expect(prepared.executable).toBe(true);
    if (!prepared.executable) {
      throw new Error("Expected executable plan");
    }

    expect(prepared.plan.previewStatements).toEqual([
      'UPDATE public.fixture_rows {"primaryKeys":{"id":2},"changes":{"display_name":"Updated"}}',
    ]);
    expect(prepared.plan.skippedRows).toEqual([0]);

    const result = await executePreparedApplyPlan(
      {
        getDriver: () => driver,
      } as never,
      prepared.plan,
    );

    expect(updateRowsSpy).toHaveBeenCalledWith({
      database: "main",
      schema: "public",
      table: "fixture_rows",
      updates: [
        {
          primaryKeys: { id: 2 },
          changes: { display_name: "Updated" },
        },
      ],
    });
    expect(result).toEqual({
      success: true,
      rowOutcomes: [
        {
          rowIndex: 0,
          success: true,
          status: "skipped",
          message: "No changes to apply.",
        },
        { rowIndex: 1, success: true, status: "applied" },
      ],
    });
  });

  it("uses driver mutation hooks for update, insert, and delete flows", async () => {
    const updateRows = async () => ({ affectedRows: 1 });
    const insertRow = async () => ({ affectedRows: 1 });
    const deleteRows = async () => ({ affectedRows: 2 });
    const driver: IDBDriver = {
      ...fakeDriver,
      coerceInputValue: (value, column) => {
        if (typeof value !== "string") {
          return value;
        }
        if (column.category === "integer") {
          return Number.parseInt(value, 10);
        }
        if (column.name === "display_name") {
          return value.trim().toUpperCase();
        }
        return value;
      },
      updateRows,
      insertRow,
      deleteRows,
    };
    const updateRowsSpy = vi.spyOn(driver, "updateRows");
    const insertRowSpy = vi.spyOn(driver, "insertRow");
    const deleteRowsSpy = vi.spyOn(driver, "deleteRows");

    const mutationService = new TableMutationService(
      {
        getConnection: () => ({ id: "conn-1" }),
        getDriver: () => driver,
      } as never,
      {
        getColumns: async () => columns,
      },
    );

    await mutationService.updateRow(
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      { id: "1" },
      { display_name: "  Updated  " },
    );

    expect(updateRowsSpy).toHaveBeenCalledWith({
      database: "main",
      schema: "public",
      table: "fixture_rows",
      updates: [
        {
          primaryKeys: { id: 1 },
          changes: { display_name: "UPDATED" },
        },
      ],
    });

    const insertPlan = await mutationService.prepareInsertRow(
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      { id: "3", display_name: " inserted " },
    );

    expect(insertPlan.mode).toBe("driver");
    expect(insertPlan.previewStatements).toEqual([
      'INSERT public.fixture_rows {"id":3,"display_name":"INSERTED"}',
    ]);

    await mutationService.executePreparedInsertPlan(insertPlan);

    expect(insertRowSpy).toHaveBeenCalledWith({
      database: "main",
      schema: "public",
      table: "fixture_rows",
      values: { id: 3, display_name: "INSERTED" },
    });

    const deletePlan = await mutationService.prepareDeleteRowsPlan(
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      [{ id: "1" }, { id: "2" }],
    );

    expect(deletePlan).not.toBeNull();
    expect(deletePlan?.mode).toBe("driver");
    expect(deletePlan?.previewStatements).toEqual([
      'DELETE public.fixture_rows {"id":1}',
      'DELETE public.fixture_rows {"id":2}',
    ]);

    if (!deletePlan) {
      throw new Error("Expected delete plan");
    }

    await mutationService.executePreparedDeletePlan(deletePlan);

    expect(deleteRowsSpy).toHaveBeenCalledWith({
      database: "main",
      schema: "public",
      table: "fixture_rows",
      primaryKeyValuesList: [{ id: 1 }, { id: 2 }],
    });
  });

  it("throws when a driver-backed update reports no affected rows", async () => {
    const driver: IDBDriver = {
      ...fakeDriver,
      updateRows: async () => ({ affectedRows: 0 }),
    };

    const mutationService = new TableMutationService(
      {
        getConnection: () => ({ id: "conn-1" }),
        getDriver: () => driver,
      } as never,
      {
        getColumns: async () => columns,
      },
    );

    await expect(
      mutationService.updateRow(
        "conn-1",
        "main",
        "public",
        "fixture_rows",
        { id: 1 },
        { display_name: "Missing" },
      ),
    ).rejects.toThrow(/row not found/i);
  });

  it("does not call driver-backed update hooks when no writable changes remain", async () => {
    const updateRowsSpy = vi.fn(async () => ({ affectedRows: 1 }));
    const driver: IDBDriver = {
      ...fakeDriver,
      updateRows: updateRowsSpy,
    };

    const mutationService = new TableMutationService(
      {
        getConnection: () => ({ id: "conn-1" }),
        getDriver: () => driver,
      } as never,
      {
        getColumns: async () => columns,
      },
    );

    await mutationService.updateRow(
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      { id: 1 },
      { display_name: undefined, missing: "ignored" },
    );

    expect(updateRowsSpy).not.toHaveBeenCalled();
  });

  it("blocks readonly connections in prepare-time mutation paths", async () => {
    const readonlyManager = {
      getConnection: () => ({
        id: "conn-1",
        name: "Readonly",
        readOnly: true,
      }),
      getDriver: () => fakeDriver,
    };
    const readonlyMutationService = new TableMutationService(
      readonlyManager as never,
      {
        getColumns: async () => columns,
      },
    );

    await expect(
      readonlyMutationService.updateRow(
        "conn-1",
        "main",
        "public",
        "fixture_rows",
        { id: 1 },
        { display_name: "Updated" },
      ),
    ).rejects.toThrow(/read-only/i);

    await expect(
      readonlyMutationService.prepareInsertRow(
        "conn-1",
        "main",
        "public",
        "fixture_rows",
        { id: 3, display_name: "Inserted" },
      ),
    ).rejects.toThrow(/read-only/i);

    await expect(
      readonlyMutationService.prepareDeleteRowsPlan(
        "conn-1",
        "main",
        "public",
        "fixture_rows",
        [{ id: 1 }],
      ),
    ).rejects.toThrow(/read-only/i);

    expect(() =>
      prepareApplyChangesPlan(
        readonlyManager as never,
        "conn-1",
        "main",
        "public",
        "fixture_rows",
        [
          {
            primaryKeys: { id: 1 },
            changes: { display_name: "Updated" },
          },
        ],
        columns,
      ),
    ).toThrow(/read-only/i);
  });

  it("blocks readonly connections in prepared execution paths", async () => {
    const writableManager = {
      getConnection: () => ({
        id: "conn-1",
        name: "Writable",
        readOnly: false,
      }),
      getDriver: () => fakeDriver,
    };
    const readonlyManager = {
      getConnection: () => ({
        id: "conn-1",
        name: "Readonly",
        readOnly: true,
      }),
      getDriver: () => fakeDriver,
    };
    const writableMutationService = new TableMutationService(
      writableManager as never,
      {
        getColumns: async () => columns,
      },
    );
    const readonlyMutationService = new TableMutationService(
      readonlyManager as never,
      {
        getColumns: async () => columns,
      },
    );

    const insertPlan = await writableMutationService.prepareInsertRow(
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      { id: 3, display_name: "Inserted" },
    );
    await expect(
      readonlyMutationService.executePreparedInsertPlan(insertPlan),
    ).rejects.toThrow(/read-only/i);

    const deletePlan = await writableMutationService.prepareDeleteRowsPlan(
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      [{ id: 1 }],
    );
    if (!deletePlan) {
      throw new Error("Expected delete plan");
    }
    await expect(
      readonlyMutationService.executePreparedDeletePlan(deletePlan),
    ).rejects.toThrow(/read-only/i);

    const preparedApply = prepareApplyChangesPlan(
      writableManager as never,
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      [
        {
          primaryKeys: { id: 1 },
          changes: { display_name: "Updated" },
        },
      ],
      columns,
    );
    if (!preparedApply.executable) {
      throw new Error("Expected executable apply plan");
    }

    await expect(
      executePreparedApplyPlan(readonlyManager as never, preparedApply.plan),
    ).resolves.toEqual({
      success: false,
      error: expect.stringMatching(/read-only/i),
    });
  });

  it("verifies deleted rows with bounded concurrent existence checks", async () => {
    const pendingExistenceResolvers: Array<() => void> = [];
    let inFlightExistenceChecks = 0;
    let maxInFlightExistenceChecks = 0;

    const driver: IDBDriver = {
      ...fakeDriver,
      query: async (sql: string) => {
        if (sql.startsWith("SELECT 1 FROM")) {
          inFlightExistenceChecks += 1;
          maxInFlightExistenceChecks = Math.max(
            maxInFlightExistenceChecks,
            inFlightExistenceChecks,
          );
          await new Promise<void>((resolve) => {
            pendingExistenceResolvers.push(() => {
              inFlightExistenceChecks -= 1;
              resolve();
            });
          });
        }

        return {
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        };
      },
    };

    const mutationService = new TableMutationService(
      {
        getConnection: () => ({
          id: "conn-1",
          name: "Writable",
          readOnly: false,
        }),
        getDriver: () => driver,
      } as never,
      {
        getColumns: async () => columns,
      },
    );

    const deletePlan = await mutationService.prepareDeleteRowsPlan(
      "conn-1",
      "main",
      "public",
      "fixture_rows",
      Array.from({ length: 16 }, (_, index) => ({ id: index + 1 })),
    );

    if (!deletePlan) {
      throw new Error("Expected delete plan");
    }

    const pendingExecution =
      mutationService.executePreparedDeletePlan(deletePlan);
    for (let index = 0; index < 20; index += 1) {
      if (pendingExistenceResolvers.length > 0) {
        break;
      }
      await Promise.resolve();
    }

    expect(pendingExistenceResolvers.length).toBeGreaterThan(1);

    let releasedChecks = 0;
    while (releasedChecks < 16) {
      const release = pendingExistenceResolvers.shift();
      if (!release) {
        await Promise.resolve();
        continue;
      }

      release();
      releasedChecks += 1;
    }

    await pendingExecution;
    expect(maxInFlightExistenceChecks).toBeGreaterThan(1);
    expect(maxInFlightExistenceChecks).toBeLessThanOrEqual(8);
  });
});
