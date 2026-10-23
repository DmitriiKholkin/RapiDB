import { describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";

const schema = 'Mixed"Owner';
const table = 'Mixed"Table';

function setup(fallback = false) {
  const driver = new OracleDriver({
    id: "quoted",
    name: "quoted",
    type: "oracle",
  });
  const execute = vi.fn(async (sql: string, params?: unknown[]) => {
    if (sql.includes("DBMS_METADATA.GET_DDL")) {
      expect(params).toEqual([schema, table]);
      if (fallback) throw new Error("Metadata unavailable");
      return { rows: [{ DDL: "exact DDL" }] };
    }
    if (params?.length) {
      expect(params[0]).toBe(schema);
      if (params.length > 1) expect(params[1]).toBe(table);
    }
    if (sql.includes("SELECT object_type FROM all_objects")) {
      return { rows: [{ OBJECT_TYPE: "TABLE" }] };
    }
    if (sql.includes("SELECT trigger_type, triggering_event")) {
      expect(params?.[2]).toBe('Mixed"Trigger');
      return {
        rows: [
          {
            TRIGGER_TYPE: "BEFORE EACH ROW",
            TRIGGERING_EVENT: "UPDATE",
            TRIGGER_BODY: "BEGIN NULL; END;",
          },
        ],
      };
    }
    if (sql.includes("FROM all_sequences")) {
      return {
        rows: [
          {
            MIN_VALUE: "1",
            MAX_VALUE: "100",
            INCREMENT_BY: "1",
            CACHE_SIZE: "0",
            LAST_NUMBER: "1",
            CYCLE_FLAG: "N",
            ORDER_FLAG: "N",
          },
        ],
      };
    }
    if (sql.includes("FROM all_views"))
      return { rows: [{ TEXT: "SELECT 1 FROM dual" }] };
    if (sql.includes("FROM all_tab_cols")) {
      return {
        rows: [
          {
            COLUMN_NAME: 'Id"Column',
            DATA_TYPE: "NUMBER",
            DATA_PRECISION: 10,
            DATA_SCALE: 0,
            DATA_LENGTH: 22,
            CHAR_LENGTH: null,
            CHAR_USED: null,
            NULLABLE: "N",
            DATA_DEFAULT: null,
            VIRTUAL_COLUMN: "NO",
            COLUMN_ID: 1,
          },
        ],
      };
    }
    return { rows: [] };
  });
  const close = vi.fn(async () => undefined);
  vi.spyOn(
    driver as unknown as { getConnection(): Promise<unknown> },
    "getConnection",
  ).mockResolvedValue({ execute, close });
  return { driver, execute, close };
}

describe("Oracle catalog identifier case", () => {
  it.each([
    "listObjects",
    "describeTable",
    "getIndexes",
    "getForeignKeys",
    "getConstraints",
    "getTriggers",
  ] as const)("%s preserves catalog spelling in every lookup", async (method) => {
    const { driver, execute, close } = setup();
    await driver[method]("", schema, table);
    expect(execute).toHaveBeenCalled();
    expect(close).toHaveBeenCalled();
  });

  it.each([
    "function",
    "procedure",
  ] as const)("preserves %s source lookup names", async (kind) => {
    const { driver } = setup();
    await driver.getRoutineDefinition("", schema, table, kind);
  });

  it("preserves and quotes trigger names in reconstructed DDL", async () => {
    const { driver } = setup();
    const ddl = await driver.getTriggerDDL("", schema, table, 'Mixed"Trigger');
    expect(ddl).toContain('TRIGGER "Mixed""Owner"."Mixed""Trigger"');
    expect(ddl).toContain('ON "Mixed""Owner"."Mixed""Table"');
  });

  it("preserves table names in DBMS_METADATA and its fallback", async () => {
    expect(await setup().driver.getCreateTableDDL("", schema, table)).toBe(
      "exact DDL",
    );
    const ddl = await setup(true).driver.getCreateTableDDL("", schema, table);
    expect(ddl).toContain('CREATE TABLE "Mixed""Owner"."Mixed""Table"');
    expect(ddl).toContain('"Id""Column" NUMBER(10,0)');
  });

  it("preserves sequence names in DBMS_METADATA and its fallback", async () => {
    expect(
      await setup().driver.getObjectDefinition("", schema, table, "sequence"),
    ).toBe("exact DDL");
    const ddl = await setup(true).driver.getObjectDefinition(
      "",
      schema,
      table,
      "sequence",
    );
    expect(ddl).toContain('CREATE SEQUENCE "Mixed""Owner"."Mixed""Table"');
  });
});
