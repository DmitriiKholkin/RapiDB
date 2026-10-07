import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import type { PersistedEditValidationError } from "../../src/extension/table/persistedEditValidation";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { buildUpdateRowSql } from "../../src/extension/table/updateSql";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

function column(
  name: string,
  overrides: Partial<ColumnTypeMeta> = {},
): ColumnTypeMeta {
  return {
    name,
    type: "integer",
    nativeType: "integer",
    category: "integer",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
    ...overrides,
  };
}

const mssqlConfig = {
  id: "generated-mssql",
  name: "Generated MSSQL",
  type: "mssql",
} as ConnectionConfig;

const postgresConfig = {
  id: "generated-pg",
  name: "Generated Postgres",
  type: "pg",
} as ConnectionConfig;

const mysqlConfig = {
  id: "generated-mysql",
  name: "Generated MySQL",
  type: "mysql",
} as ConnectionConfig;

describe("generated and computed column mutation plans", () => {
  it("omits SQL Server computed columns while retaining an explicit identity value", () => {
    const driver = new MSSQLDriver(mssqlConfig);
    const columns = [
      column("id", {
        type: "int",
        nativeType: "int",
        isPrimaryKey: true,
        identityGeneration: "always",
      }),
      column("source", { type: "int", nativeType: "int" }),
      column("derived", {
        isComputed: true,
        computedExpression: "([source] * 2)",
        generatedKind: "stored",
        isPersisted: true,
      }),
    ];

    const insert = buildInsertRowOperation(
      driver,
      "db",
      "dbo",
      "items",
      { id: 9, source: 4, derived: 8 },
      columns,
    );
    const update = buildUpdateRowSql(
      driver,
      "db",
      "dbo",
      "items",
      { id: 9 },
      { source: 5, derived: 10 },
      columns,
    );

    expect(insert.sql).toContain("[id]");
    expect(insert.sql).toContain("[source]");
    expect(insert.sql).not.toContain("[derived]");
    expect(insert.params).toEqual([9, 4]);
    expect(update?.sql).toContain("[source] =");
    expect(update?.sql).not.toContain("[derived] =");
    expect(update?.params).toEqual([5, 9]);
  });

  it("omits PostgreSQL stored-generated columns from INSERT and UPDATE plans", () => {
    const driver = new PostgresDriver(postgresConfig);
    const columns = [
      column("id", {
        isPrimaryKey: true,
        identityGeneration: "by_default",
      }),
      column("label", { type: "text", nativeType: "text" }),
      column("label_lower", {
        type: "text",
        nativeType: "text",
        isComputed: true,
        computedExpression: "lower(label)",
        generatedKind: "stored",
        isPersisted: true,
      }),
    ];

    const insert = buildInsertRowOperation(
      driver,
      "db",
      "public",
      "items",
      { id: 11, label: "Hello", label_lower: "hello" },
      columns,
    );
    const update = buildUpdateRowSql(
      driver,
      "db",
      "public",
      "items",
      { id: 11 },
      { label: "World", label_lower: "world" },
      columns,
    );

    expect(insert.sql).toContain('"id"');
    expect(insert.sql).toContain('"label"');
    expect(insert.sql).not.toContain('"label_lower"');
    expect(insert.params).toEqual([11, "Hello"]);
    expect(update?.sql).toContain('"label" =');
    expect(update?.sql).not.toContain('"label_lower" =');
    expect(update?.params).toEqual(["World", 11]);
  });

  it("keeps MySQL auto-increment and default columns writable when supplied", () => {
    const driver = new MySQLDriver(mysqlConfig);
    const columns = [
      column("id", {
        type: "int",
        nativeType: "int",
        isPrimaryKey: true,
        identityGeneration: "auto_increment",
      }),
      column("created_at", {
        type: "timestamp",
        nativeType: "timestamp",
        defaultValue: "CURRENT_TIMESTAMP",
      }),
      column("label", { type: "varchar(30)", nativeType: "varchar(30)" }),
    ];

    const explicit = buildInsertRowOperation(
      driver,
      "db",
      "db",
      "items",
      { id: 23, label: "manual key" },
      columns,
    );
    const withDefaultOverride = buildInsertRowOperation(
      driver,
      "db",
      "db",
      "items",
      { created_at: "2026-01-01 00:00:00", label: "override" },
      columns,
    );

    expect(explicit.sql).toContain("`id`");
    expect(explicit.params).toEqual([23, "manual key"]);
    expect(withDefaultOverride.sql).toContain("`created_at`");
    expect(withDefaultOverride.params).toEqual([
      "2026-01-01 00:00:00",
      "override",
    ]);
  });

  it("prevalidates generated UPDATE and INSERT attempts before preview or writes", async () => {
    const driver = new PostgresDriver(postgresConfig);
    const columns = [
      column("id", { isPrimaryKey: true }),
      column("label", { type: "text", nativeType: "text" }),
      column("label_lower", {
        type: "text",
        nativeType: "text",
        isComputed: true,
        generatedKind: "stored",
      }),
    ];
    const manager = {
      getDriver: () => driver,
      getConnection: () => ({ id: "generated-pg", type: "pg" }),
    };
    const service = new TableMutationService(manager as never, {
      getColumns: async () => columns,
    });
    const preview = vi.spyOn(driver, "materializePreviewSql");
    const query = vi.spyOn(driver, "query");

    const update = prepareApplyChangesPlan(
      manager as never,
      "generated-pg",
      "db",
      "public",
      "items",
      [{ primaryKeys: { id: 1 }, changes: { label_lower: "wrong" } }],
      columns,
    );
    expect(update).toMatchObject({
      executable: false,
      result: {
        success: false,
        rowOutcomes: [
          {
            status: "prevalidation_failed",
            columns: ["label_lower"],
            message: expect.stringContaining("read-only"),
          },
        ],
      },
    });

    await expect(
      service.prepareInsertRow("generated-pg", "db", "public", "items", {
        label: "valid value",
        label_lower: "wrong",
      }),
    ).rejects.toMatchObject({
      status: "prevalidation_failed",
      columns: ["label_lower"],
      message: expect.stringContaining("read-only"),
    } satisfies Partial<PersistedEditValidationError>);
    expect(preview).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalled();
  });

  it("rejects unknown SQL insert typos before they can become default-only inserts", async () => {
    const driver = new PostgresDriver(postgresConfig);
    const columns = [column("label", { type: "text", nativeType: "text" })];
    const manager = {
      getDriver: () => driver,
      getConnection: () => ({ id: "generated-pg", type: "pg" }),
    };
    const service = new TableMutationService(manager as never, {
      getColumns: async () => columns,
    });
    const preview = vi.spyOn(driver, "materializePreviewSql");

    await expect(
      service.prepareInsertRow("generated-pg", "db", "public", "items", {
        lable: "misspelled",
      }),
    ).rejects.toMatchObject({
      status: "prevalidation_failed",
      columns: ["lable"],
      message: expect.stringContaining('unknown column "lable"'),
    });
    expect(preview).not.toHaveBeenCalled();
  });
});
