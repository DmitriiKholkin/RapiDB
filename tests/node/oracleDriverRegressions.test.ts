import oracledb from "oracledb";
import { describe, expect, it, vi } from "vitest";
import {
  OracleDriver,
  splitOracleStatements,
} from "../../src/extension/dbDrivers/oracle";
import type { ColumnMeta } from "../../src/extension/dbDrivers/types";
import { buildUpdateRowSql } from "../../src/extension/table/updateSql";

class InspectableOracleDriver extends OracleDriver {
  column(name: string, type: string) {
    const meta: ColumnMeta = {
      name,
      type,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    };
    return this.enrichColumn(meta);
  }
}

const config = { id: "oracle-regr", name: "Oracle", type: "oracle" as const };
const driver = () => new InspectableOracleDriver(config);

function mockPool(
  target: OracleDriver,
  execute: (sql: string, binds?: unknown) => unknown,
) {
  const connection = {
    callTimeout: 0,
    execute: vi.fn(async (sql: string, binds?: unknown) => execute(sql, binds)),
    commit: vi.fn(async () => undefined),
    rollback: vi.fn(async () => undefined),
    close: vi.fn(async (_options?: { drop?: boolean }) => undefined),
    break: vi.fn(async () => undefined),
  };
  Object.assign(target, {
    pool: {
      connectionsInUse: 0,
      connectionsOpen: 1,
      getConnection: vi.fn(async () => connection),
    },
  });
  return connection;
}

function mockLease(
  target: OracleDriver,
  execute: (sql: string, binds?: unknown) => unknown,
) {
  const connection = {
    execute: vi.fn(async (sql: string, binds?: unknown) => execute(sql, binds)),
    close: vi.fn(async () => undefined),
  };
  vi.spyOn(
    target as unknown as { getConnection(): Promise<unknown> },
    "getConnection",
  ).mockResolvedValue(connection);
  return connection;
}

describe("Oracle LOB binds", () => {
  const large = "x".repeat(5000);

  it("binds large CLOB, BLOB, JSON, XML and SDO values as LOBs", () => {
    const oracle = driver();
    expect(
      oracle.buildOriginalValueComparisonWithParams(
        oracle.column("DOC", "CLOB"),
        large,
        3,
      ),
    ).toEqual({
      sql: 'DBMS_LOB.COMPARE("DOC", TO_CLOB(:3)) = 0',
      params: [{ type: oracledb.DB_TYPE_CLOB, val: large }],
    });
    const bytes = Buffer.alloc(2500, 1);
    expect(
      oracle.buildOriginalValueComparisonWithParams(
        oracle.column("BIN", "BLOB"),
        bytes,
        2,
      ),
    ).toEqual({
      sql: 'DBMS_LOB.COMPARE("BIN", :2) = 0',
      params: [{ type: oracledb.DB_TYPE_BLOB, val: bytes }],
    });
    expect(
      oracle.buildOriginalValueComparisonWithParams(
        oracle.column("GEOM", "SDO_GEOMETRY"),
        large,
        1,
      )?.params,
    ).toEqual([{ type: oracledb.DB_TYPE_CLOB, val: large }]);
    expect(
      oracle.buildOriginalValueComparisonWithParams(
        oracle.column("J", "JSON"),
        `{"v":"${large}"}`,
        1,
      )?.params,
    ).toEqual([{ type: oracledb.DB_TYPE_CLOB, val: `{"v":"${large}"}` }]);
    const xml = oracle.column("X", "XMLTYPE");
    expect(
      oracle.buildMutationValueExpr(xml, `<a>${large}</a>`, "update", null, 1),
    ).toMatchObject({
      sql: "XMLTYPE(:1)",
      params: [{ type: oracledb.DB_TYPE_CLOB, val: `<a>${large}</a>` }],
    });
    expect(
      oracle.buildOriginalValueComparisonWithParams(xml, `<a>${large}</a>`, 1)
        ?.params,
    ).toEqual([{ type: oracledb.DB_TYPE_CLOB, val: `<a>${large}</a>` }]);
  });

  it("binds multibyte-safe CLOB thresholds as LOBs", () => {
    const oracle = driver();
    const text = "x".repeat(1500);
    expect(
      oracle.buildOriginalValueComparisonWithParams(
        oracle.column("DOC", "CLOB"),
        text,
        1,
      )?.params,
    ).toEqual([{ type: oracledb.DB_TYPE_CLOB, val: text }]);
  });

  it("keeps small LOB values as plain binds", () => {
    const oracle = driver();
    expect(
      oracle.buildOriginalValueComparisonWithParams(
        oracle.column("DOC", "CLOB"),
        "small",
        1,
      )?.params,
    ).toEqual(["small"]);
  });

  it("binds large CLOB and BLOB filter values as LOBs", () => {
    const oracle = driver();
    const clob = oracle.column("DOC", "CLOB");
    expect(oracle.buildFilterCondition(clob, "eq", large, 1)).toEqual({
      sql: 'NVL(DBMS_LOB.COMPARE("DOC", TO_CLOB(:1)), 1) = 0',
      params: [{ type: oracledb.DB_TYPE_CLOB, val: large }],
    });
    const blob = oracle.column("BIN", "BLOB");
    const hex = `0x${"ab".repeat(2500)}`;
    expect(oracle.buildFilterCondition(blob, "eq", hex, 1)?.params).toEqual([
      {
        type: oracledb.DB_TYPE_BLOB,
        val: Buffer.from("ab".repeat(2500), "hex"),
      },
    ]);
  });

  it("renders typed binds as literals in previews", () => {
    const oracle = driver();
    const doc = oracle.column("DOC", "CLOB");
    const update = buildUpdateRowSql(
      oracle,
      "",
      "APP",
      "T",
      { DOC: "pk" },
      { DOC: "next" },
      [{ ...doc, isPrimaryKey: true }],
      { DOC: large },
    );
    if (!update) throw new Error("Expected update SQL");
    const preview = oracle.materializePreviewSql(update.sql, update.params);
    expect(preview).toContain("TO_CLOB('xxx");
    expect(preview).not.toContain("[object Object]");
    expect(preview).not.toContain('"val"');
  });
});

describe("Oracle INTERVAL filters", () => {
  it("compares intervals natively instead of against CAST text", () => {
    const oracle = driver();
    expect(
      oracle.buildFilterCondition(
        oracle.column("DS", "INTERVAL DAY(2) TO SECOND(6)"),
        "eq",
        "1 02:03:04.5",
        1,
      ),
    ).toEqual({ sql: '"DS" = TO_DSINTERVAL(:1)', params: ["1 02:03:04.5"] });
    expect(
      oracle.buildFilterCondition(
        oracle.column("YM", "INTERVAL YEAR(2) TO MONTH"),
        "neq",
        "-1-02",
        4,
      ),
    ).toEqual({ sql: '"YM" <> TO_YMINTERVAL(:4)', params: ["-1-02"] });
  });

  it("rejects malformed interval filter text before it reaches Oracle", () => {
    const oracle = driver();
    const ds = oracle.column("DS", "INTERVAL DAY(2) TO SECOND(6)");
    const ym = oracle.column("YM", "INTERVAL YEAR(2) TO MONTH");
    for (const value of [
      "1 02:03:04.5",
      "-3 4:05:06",
      "2 1:2:3",
      "P1DT2H3M4.5S",
    ]) {
      expect(oracle.normalizeFilterValue(ds, "eq", value)).toBe(value);
    }
    for (const value of ["1-02", "-10-3", "P1Y2M"]) {
      expect(oracle.normalizeFilterValue(ym, "neq", value)).toBe(value);
    }
    expect(() => oracle.normalizeFilterValue(ds, "eq", "1 day")).toThrow(
      "[RapiDB Filter] Column DS expects a valid Oracle interval value.",
    );
    expect(() => oracle.normalizeFilterValue(ym, "eq", "1 02:03:04")).toThrow(
      TypeError,
    );
    expect(oracle.normalizeFilterValue(ds, "like", "1 day")).toBe("1 day");
  });
});

describe("Oracle NUMBER and FLOAT values", () => {
  it("adds a leading zero to unconstrained fractional NUMBER text", () => {
    const handler = (
      driver() as unknown as {
        _fetchTypeHandler(meta: unknown): {
          converter(value: unknown): unknown;
        };
      }
    )._fetchTypeHandler({
      dbType: oracledb.DB_TYPE_NUMBER,
      precision: 126,
      scale: -127,
    });
    expect(handler.converter(".5")).toBe("0.5");
    expect(handler.converter("-.25")).toBe("-0.25");
    expect(handler.converter("12.5")).toBe("12.5");
  });

  it("binds FLOAT input as exact decimal text and verifies it canonically", () => {
    const oracle = driver();
    const float = oracle.column("F", "FLOAT(126)");
    expect(oracle.coerceInputValue(" 1.2345678901234567890123 ", float)).toBe(
      "1.2345678901234567890123",
    );
    expect(
      oracle.coerceInputValue("1.5", oracle.column("B", "BINARY_DOUBLE")),
    ).toBe(1.5);
    for (const [expected, persisted] of [
      ["0.5", ".5"],
      ["1.50", "1.5"],
      ["1e3", "1000"],
      ["-0.25", "-.25"],
    ]) {
      expect(
        oracle.checkPersistedEdit(float, expected, {
          persistedValue: persisted,
        })?.ok,
      ).toBe(true);
    }
    expect(
      oracle.checkPersistedEdit(float, "1.25", { persistedValue: "1.2" })?.ok,
    ).toBe(false);
  });

  it("rejects non-decimal FLOAT input with a friendly error", () => {
    const oracle = driver();
    const float = oracle.column("F", "FLOAT(126)");
    for (const value of ["0x10", "0b1", "0o7", "Infinity", "1e"]) {
      expect(() => oracle.coerceInputValue(value, float)).toThrow(
        'Column "F" expects a numeric value.',
      );
    }
    for (const value of ["+1.", ".5", "-2.5E-3", "7"]) {
      expect(oracle.coerceInputValue(value, float)).toBe(value);
    }
  });

  it("verifies FLOAT(54..125) at the digits Oracle retains", () => {
    const oracle = driver();
    const float = oracle.column("F", "FLOAT(60)");
    expect(
      oracle.checkPersistedEdit(float, "0.1234567890123456789", {
        persistedValue: "0.123456789012345679",
      })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(float, "0.25", { persistedValue: "0.24" })?.ok,
    ).toBe(false);
  });
});

describe("Oracle session setup", () => {
  it("forces '.' as the decimal separator for string number binds", async () => {
    const oracle = driver();
    const connection = mockPool(oracle, () => ({ rows: [] }));
    await oracle.listDatabases();
    expect(
      connection.execute.mock.calls.some(([sql]) =>
        /ALTER SESSION SET .*NLS_NUMERIC_CHARACTERS = '\.,'/.test(sql),
      ),
    ).toBe(true);
  });

  it("drops pooled sessions after user SQL that alters session state", async () => {
    const oracle = driver();
    const connection = mockPool(oracle, () => ({}));
    await oracle.query("ALTER SESSION SET CURRENT_SCHEMA = OTHER");
    expect(connection.close).toHaveBeenCalledWith({ drop: true });

    const plain = driver();
    const plainConnection = mockPool(plain, () => ({ rowsAffected: 1 }));
    await plain.query("UPDATE t SET c = 1");
    expect(plainConnection.close).toHaveBeenCalledWith();

    const role = driver();
    const roleConnection = mockPool(role, () => ({}));
    await role.query("SET ROLE dba");
    expect(roleConnection.close).toHaveBeenCalledWith({ drop: true });

    const column = driver();
    const columnConnection = mockPool(column, () => ({ rowsAffected: 1 }));
    await column.query("UPDATE t SET role = 'x'");
    expect(columnConnection.close).toHaveBeenCalledWith();
  });
});

describe("Oracle trigger DDL", () => {
  it("uses DBMS_METADATA with the trigger owner", async () => {
    const oracle = driver();
    const connection = mockLease(oracle, (sql) => {
      if (sql.includes("FROM all_triggers")) {
        return {
          rows: [
            {
              OWNER: "TRG_OWNER",
              DESCRIPTION: "trg BEFORE INSERT ON t FOR EACH ROW",
              WHEN_CLAUSE: null,
              TRIGGER_BODY: "BEGIN NULL; END;",
            },
          ],
        };
      }
      if (sql.includes("DBMS_METADATA.GET_DDL('TRIGGER'")) {
        return { rows: [{ DDL: "CREATE OR REPLACE TRIGGER native" }] };
      }
      return { rows: [] };
    });
    await expect(oracle.getTriggerDDL("", "APP", "T", "TRG")).resolves.toBe(
      "CREATE OR REPLACE TRIGGER native",
    );
    expect(connection.execute).toHaveBeenCalledWith(
      "SELECT DBMS_METADATA.GET_DDL('TRIGGER', :1, :2) AS ddl FROM dual",
      ["TRG", "TRG_OWNER"],
      expect.anything(),
    );
  });

  it("falls back to DESCRIPTION, WHEN_CLAUSE and TRIGGER_BODY", async () => {
    const oracle = driver();
    mockLease(oracle, (sql) => {
      if (sql.includes("FROM all_triggers")) {
        return {
          rows: [
            {
              OWNER: "TRG_OWNER",
              DESCRIPTION:
                "trg\nBEFORE UPDATE OF amount ON app.t\nREFERENCING NEW AS n\nFOR EACH ROW\n",
              WHEN_CLAUSE: "n.amount > 0",
              TRIGGER_BODY: "BEGIN\n  :n.flag := 1;\nEND;",
            },
          ],
        };
      }
      if (sql.includes("DBMS_METADATA.GET_DDL")) {
        throw new Error("ORA-31603");
      }
      return { rows: [] };
    });
    await expect(oracle.getTriggerDDL("", "APP", "T", "TRG")).resolves.toBe(
      [
        'CREATE OR REPLACE TRIGGER "TRG_OWNER"."TRG"',
        "BEFORE UPDATE OF amount ON app.t",
        "REFERENCING NEW AS n",
        "FOR EACH ROW",
        "WHEN (n.amount > 0)",
        "BEGIN",
        "  :n.flag := 1;",
        "END;",
      ].join("\n"),
    );
  });
});

describe("Oracle national character binds", () => {
  it("binds NCHAR, NVARCHAR2 and NCLOB values in the national character set", () => {
    const oracle = driver();
    expect(
      oracle.coerceInputValue("Привет", oracle.column("N", "NVARCHAR2(20)")),
    ).toEqual({ type: oracledb.DB_TYPE_NVARCHAR, val: "Привет" });
    expect(
      oracle.coerceInputValue("你好", oracle.column("N", "NCHAR(4)")),
    ).toEqual({ type: oracledb.DB_TYPE_NCHAR, val: "你好" });
    const nclob = oracle.column("N", "NCLOB");
    expect(oracle.coerceInputValue("😀", nclob)).toEqual({
      type: oracledb.DB_TYPE_NVARCHAR,
      val: "😀",
    });
    expect(oracle.coerceInputValue("ж".repeat(3000), nclob)).toEqual({
      type: oracledb.DB_TYPE_NCLOB,
      val: "ж".repeat(3000),
    });
    expect(
      oracle.coerceInputValue("plain", oracle.column("V", "VARCHAR2(9)")),
    ).toBe("plain");
  });

  it("binds national character filters and renders N'' previews", () => {
    const oracle = driver();
    const column = oracle.column("N", "NVARCHAR2(20)");
    expect(oracle.buildFilterCondition(column, "like", "ж", 1)).toEqual({
      sql: `UPPER("N") LIKE UPPER(:1) ESCAPE '!'`,
      params: [{ type: oracledb.DB_TYPE_NVARCHAR, val: "%ж%" }],
    });
    expect(
      oracle.materializePreviewSql("SELECT :1 FROM dual", [
        oracle.coerceInputValue("ж", column),
      ]),
    ).toBe("SELECT N'ж' FROM dual");
    expect(
      oracle.formatOutputValue(oracle.coerceInputValue("ж", column), column),
    ).toBe("ж");
  });
});

describe("Oracle empty strings", () => {
  it.each([
    "VARCHAR2(10)",
    "CHAR(3)",
    "NVARCHAR2(10)",
    "CLOB",
  ])("treats '' as NULL for %s", (type) => {
    const oracle = driver();
    const column = oracle.column("C", type);
    expect(oracle.coerceInputValue("", column)).toBeNull();
    expect(
      oracle.checkPersistedEdit(column, "", { persistedValue: null })?.ok,
    ).toBe(true);
    expect(
      oracle.checkPersistedEdit(column, "", { persistedValue: "x" })?.ok,
    ).toBe(false);
  });

  it("compares an empty original value with IS NULL", () => {
    const oracle = driver();
    const id = { ...oracle.column("ID", "NUMBER(10,0)"), isPrimaryKey: true };
    const text = oracle.column("C", "VARCHAR2(10)");
    expect(
      buildUpdateRowSql(
        oracle,
        "",
        "APP",
        "T",
        { ID: "1" },
        { C: "x" },
        [id, text],
        { C: "" },
      )?.sql,
    ).toContain('"C" IS NULL');
  });

  it.each([
    "CLOB",
    "NCLOB",
    "BLOB",
  ])("matches an empty %s original as NULL or zero-length", (type) => {
    const oracle = driver();
    const id = { ...oracle.column("ID", "NUMBER(10,0)"), isPrimaryKey: true };
    const lob = oracle.column("DOC", type);
    const update = buildUpdateRowSql(
      oracle,
      "",
      "APP",
      "T",
      { ID: "1" },
      { DOC: type === "BLOB" ? "0x01" : "x" },
      [id, lob],
      { DOC: "" },
    );
    expect(update?.sql).toContain(
      '("DOC" IS NULL OR DBMS_LOB.GETLENGTH("DOC") = 0)',
    );
    expect(update?.params).toHaveLength(2);
  });

  it.each([
    "0x",
    "\\x",
    Buffer.alloc(0),
  ])("matches an empty-hex BLOB original %s as NULL or zero-length", (original) => {
    const oracle = driver();
    const id = { ...oracle.column("ID", "NUMBER(10,0)"), isPrimaryKey: true };
    const lob = oracle.column("BIN", "BLOB");
    const update = buildUpdateRowSql(
      oracle,
      "",
      "APP",
      "T",
      { ID: "1" },
      { BIN: "0x01" },
      [id, lob],
      { BIN: original },
    );
    expect(update?.sql).toContain(
      '("BIN" IS NULL OR DBMS_LOB.GETLENGTH("BIN") = 0)',
    );
    expect(update?.params).toEqual([Buffer.from([1]), "1"]);
  });
});

describe("Oracle catalog metadata", () => {
  it("lists invalid objects", async () => {
    const oracle = driver();
    const connection = mockLease(oracle, (sql) =>
      sql.includes("FROM all_objects")
        ? { rows: [{ OBJECT_NAME: "BROKEN_VIEW", OBJECT_TYPE: "VIEW" }] }
        : { rows: [] },
    );
    await expect(oracle.listObjects("", "APP")).resolves.toEqual([
      { schema: "APP", name: "BROKEN_VIEW", type: "view" },
    ]);
    const objectSql = connection.execute.mock.calls.find(([sql]) =>
      sql.includes("FROM all_objects"),
    )?.[0];
    expect(objectSql).not.toMatch(/status/i);
  });

  it("falls back to SEARCH_CONDITION before 12.2 and hides generated NOT NULL checks", async () => {
    const oracle = driver();
    const connection = mockLease(oracle, (sql) => {
      if (sql.includes("search_condition_vc")) {
        throw new Error('ORA-00904: "SEARCH_CONDITION_VC": invalid identifier');
      }
      if (sql.includes("AS search_condition")) {
        return {
          rows: [
            {
              CONSTRAINT_NAME: "CK_AMOUNT",
              GENERATED: "USER NAME",
              SEARCH_CONDITION: "amount > 0",
            },
            {
              CONSTRAINT_NAME: "SYS_C001",
              GENERATED: "GENERATED NAME",
              SEARCH_CONDITION: '"ID" IS NOT NULL',
            },
          ],
        };
      }
      return { rows: [] };
    });
    await expect(oracle.getConstraints("", "APP", "T")).resolves.toEqual([
      {
        name: "CK_AMOUNT",
        kind: "check",
        columns: [],
        checkExpression: "amount > 0",
        source: "catalog",
      },
    ]);
    expect(connection.close).toHaveBeenCalledTimes(2);
  });
});

describe("Oracle WITH FUNCTION statements", () => {
  it("keeps 12c PL/SQL declarations with their query", () => {
    expect(
      splitOracleStatements(
        "WITH FUNCTION f RETURN NUMBER IS v NUMBER; BEGIN SELECT 1 INTO v FROM dual; RETURN v; END;\nSELECT f FROM dual;\nSELECT 2 FROM dual;",
      ),
    ).toEqual([
      "WITH FUNCTION f RETURN NUMBER IS v NUMBER; BEGIN SELECT 1 INTO v FROM dual; RETURN v; END;\nSELECT f FROM dual",
      "SELECT 2 FROM dual",
    ]);
    expect(
      splitOracleStatements(
        "WITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\nSELECT f FROM dual\n/\nSELECT 2 FROM dual",
      ),
    ).toEqual([
      "WITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\nSELECT f FROM dual",
      "SELECT 2 FROM dual",
    ]);
  });

  it("ends at a factored query and ignores INTO inside quoted identifiers", () => {
    expect(
      splitOracleStatements(
        "WITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\nc AS (SELECT f() x FROM dual)\nSELECT * FROM c;\nSELECT 2 FROM dual;",
      ),
    ).toEqual([
      "WITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\nc AS (SELECT f() x FROM dual)\nSELECT * FROM c",
      "SELECT 2 FROM dual",
    ]);
    expect(
      splitOracleStatements(
        'WITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\nSELECT f AS "INTO" FROM dual;\nSELECT 2 FROM dual;',
      ),
    ).toEqual([
      'WITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END;\nSELECT f AS "INTO" FROM dual',
      "SELECT 2 FROM dual",
    ]);
    expect(
      splitOracleStatements(
        "WITH function AS (SELECT 1 x FROM dual) SELECT x FROM function;\nSELECT 2 FROM dual;",
      ),
    ).toEqual([
      "WITH function AS (SELECT 1 x FROM dual) SELECT x FROM function",
      "SELECT 2 FROM dual",
    ]);
  });
});
