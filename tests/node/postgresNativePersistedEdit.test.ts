import mssql from "mssql";
import { types as pgTypes } from "pg";
import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";

const pg = new PostgresDriver({ id: "native", name: "Native", type: "pg" });
const sqlServer = new MSSQLDriver({
  id: "native",
  name: "Native",
  type: "mssql",
});
const BOOLEAN_ARRAY_OID: number = 1000;

describe("PostgreSQL expected-input compatibility keeps output/MSSQL grammars strict", () => {
  it.each([
    "NaN",
    "+NaN",
    "-NaN",
    "nan()",
    "NaN(123)",
    "-nan(foo)",
  ])("verifies native special-float input %s without conflating Infinity", (input) => {
    for (const nativeType of ["real", "double precision"]) {
      const column = { name: "f", nativeType, category: "float" };
      expect(pg.checkPersistedEdit(column as never, input)).toEqual({
        ok: true,
        shouldVerify: true,
      });
      expect(
        pg.checkPersistedEdit(column as never, input, { persistedValue: NaN })
          ?.ok,
      ).toBe(true);
      expect(
        pg.checkPersistedEdit(column as never, input, {
          persistedValue: Infinity,
        })?.ok,
      ).toBe(false);
    }
  });

  it.each([
    "{1,0}",
    "{TRUE,FALSE}",
    "{yes,no}",
  ])("does not accept %s as boolean array server output", (value) => {
    expect(() => pgTypes.getTypeParser(BOOLEAN_ARRAY_OID)(value)).toThrow();
    const column = { name: "a", nativeType: "boolean[]", category: "array" };
    expect(() => pg.formatOutputValue(value, column as never)).toThrow();
    expect(pgTypes.getTypeParser(BOOLEAN_ARRAY_OID)("{t,f,NULL}")).toEqual([
      true,
      false,
      null,
    ]);
  });

  it("does not loosen MSSQL UUID validation or parameter inference for PG compact UUIDs", () => {
    const uuid = "01890f476a427cc398c4dc0c0c07398f";
    const column = {
      name: "u",
      nativeType: "uniqueidentifier",
      category: "uuid",
    };
    expect(sqlServer.checkPersistedEdit(column as never, uuid)?.ok).toBe(false);
    expect(
      (
        sqlServer as unknown as { typeForValue(value: unknown): unknown }
      ).typeForValue(uuid),
    ).not.toBe(mssql.UniqueIdentifier);
  });
});
