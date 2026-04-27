import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

function setDriverPoolQueryResult(
  driver: MSSQLDriver,
  recordset: unknown[],
): void {
  const query = vi.fn().mockResolvedValue({ recordset });
  const request = vi.fn().mockReturnValue({ query });
  (
    driver as unknown as { pool: { request: () => { query: () => unknown } } }
  ).pool = {
    request,
  };
}

function makeDriver(): MSSQLDriver {
  return new MSSQLDriver({
    id: "mssql-computed-test",
    name: "MSSQL Computed Test",
    type: "mssql",
    host: "localhost",
    port: 1433,
    database: "rapidb",
    username: "sa",
    password: "secret",
  } as ConnectionConfig);
}

describe("MSSQL computed columns", () => {
  it("surfaces computed metadata in describeTable", async () => {
    const driver = makeDriver();
    setDriverPoolQueryResult(driver, [
      {
        COLUMN_NAME: "col_int",
        DATA_TYPE: "int",
        max_length: 4,
        precision: 10,
        scale: 0,
        IS_NULLABLE: 0,
        is_identity: 0,
        is_computed: 0,
        COMPUTED_DEFINITION: null,
        is_persisted: 0,
        COLUMN_DEFAULT: null,
        IS_PK: 0,
        PK_ORDINAL: null,
        IS_FK: 0,
      },
      {
        COLUMN_NAME: "created_at",
        DATA_TYPE: "datetime2",
        max_length: 8,
        precision: 27,
        scale: 7,
        IS_NULLABLE: 0,
        is_identity: 0,
        is_computed: 0,
        COMPUTED_DEFINITION: null,
        is_persisted: 0,
        COLUMN_DEFAULT: "((sysdatetime()))",
        IS_PK: 0,
        PK_ORDINAL: null,
        IS_FK: 0,
      },
      {
        COLUMN_NAME: "col_computed",
        DATA_TYPE: "int",
        max_length: 4,
        precision: 10,
        scale: 0,
        IS_NULLABLE: 1,
        is_identity: 0,
        is_computed: 1,
        COMPUTED_DEFINITION: "([col_int]*(2))",
        is_persisted: 1,
        COLUMN_DEFAULT: null,
        IS_PK: 0,
        PK_ORDINAL: null,
        IS_FK: 0,
      },
    ]);

    const columns = await driver.describeTable("rapidb", "dbo", "t_calc");
    const createdAt = columns.find((column) => column.name === "created_at");
    const computed = columns.find((column) => column.name === "col_computed");

    expect(createdAt?.defaultValue).toBe("sysdatetime()");
    expect(createdAt?.defaultKind).toBe("expression");
    expect(computed).toBeTruthy();
    expect(computed?.type).toBe("int");
    expect(computed?.isComputed).toBe(true);
    expect(computed?.computedExpression).toBe("([col_int]*(2))");
    expect(computed?.defaultValue).toBeUndefined();
    expect(computed?.generatedKind).toBe("stored");
    expect(computed?.isPersisted).toBe(true);
  });

  it("renders computed expression in MSSQL create table DDL", async () => {
    const driver = makeDriver();
    setDriverPoolQueryResult(driver, [
      {
        COLUMN_NAME: "col_int",
        DATA_TYPE: "int",
        max_length: 4,
        precision: 10,
        scale: 0,
        IS_NULLABLE: 0,
        is_identity: 0,
        is_computed: 0,
        COMPUTED_DEFINITION: null,
        is_persisted: 0,
        COLUMN_DEFAULT: null,
        IS_PK: 0,
        PK_ORDINAL: null,
      },
      {
        COLUMN_NAME: "col_computed",
        DATA_TYPE: "int",
        max_length: 4,
        precision: 10,
        scale: 0,
        IS_NULLABLE: 1,
        is_identity: 0,
        is_computed: 1,
        COMPUTED_DEFINITION: "([col_int]*(2))",
        is_persisted: 1,
        COLUMN_DEFAULT: null,
        IS_PK: 0,
        PK_ORDINAL: null,
      },
    ]);

    const ddl = await driver.getCreateTableDDL("rapidb", "dbo", "t_calc");

    expect(ddl).toContain("[col_int] int NOT NULL");
    expect(ddl).toContain("[col_computed] AS (([col_int]*(2))) PERSISTED");
  });
});
