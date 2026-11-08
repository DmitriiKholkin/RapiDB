import { describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";

describe("Oracle metadata DDL", () => {
  it("binds the object before its owner so Show DDL retains native foreign keys", async () => {
    const schema = "ORACLE_TEST_USER";
    const table = "CHILD_RECORDS";
    const nativeDdl = `CREATE TABLE "${schema}"."${table}" (
      "ID" NUMBER PRIMARY KEY,
      "PARENT_ID" NUMBER,
      CONSTRAINT "FK_CHILD_PARENT" FOREIGN KEY ("PARENT_ID")
        REFERENCES "${schema}"."PARENT_RECORDS" ("ID")
    );`;
    const driver = new OracleDriver({ id: "ddl", name: "ddl", type: "oracle" });
    const execute = vi.fn(async (sql: string, binds?: unknown[]) => {
      if (sql.includes("SELECT object_type FROM all_objects")) {
        return { rows: [{ OBJECT_TYPE: "TABLE" }] };
      }
      if (sql.includes("DBMS_METADATA.GET_DDL('TABLE'")) {
        if (binds?.[0] !== table || binds?.[1] !== schema) {
          throw new Error("ORA-31603: object not found in schema");
        }
        return { rows: [{ DDL: nativeDdl }] };
      }
      return { rows: [] };
    });
    const close = vi.fn(async () => undefined);
    vi.spyOn(
      driver as unknown as { getConnection(): Promise<unknown> },
      "getConnection",
    ).mockResolvedValue({ execute, close });

    await expect(driver.getCreateTableDDL("", schema, table)).resolves.toBe(
      nativeDdl,
    );
    expect(execute).toHaveBeenCalledWith(
      "SELECT DBMS_METADATA.GET_DDL('TABLE', :1, :2) AS ddl FROM dual",
      [table, schema],
      expect.anything(),
    );
    expect(close).toHaveBeenCalledOnce();
  });
});
