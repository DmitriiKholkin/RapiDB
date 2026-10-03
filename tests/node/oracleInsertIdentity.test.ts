import oracledb from "oracledb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { executeAtomicSqlApplyPlan } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const base: ColumnTypeMeta = {
  name: "id",
  type: "NUMBER(38,0)",
  nativeType: "NUMBER(38,0)",
  category: "integer",
  nullable: false,
  isPrimaryKey: true,
  identityGeneration: "always",
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};
function fixture(
  options: {
    composite?: boolean;
    textKey?: boolean;
    mismatch?: boolean;
    out?: "missing" | "null" | "multiple";
    readError?: boolean;
  } = {},
) {
  const columns: ColumnTypeMeta[] = [
    options.textKey
      ? {
          ...base,
          type: "VARCHAR2(100)",
          nativeType: "VARCHAR2(100)",
          category: "text",
          identityGeneration: undefined,
          defaultValue: "SYS_GUID()",
        }
      : base,
    ...(options.composite
      ? [
          {
            ...base,
            name: "code",
            type: "RAW(16)",
            nativeType: "RAW(16)",
            category: "binary" as const,
            defaultValue: "SYS_GUID()",
            identityGeneration: undefined,
          },
        ]
      : []),
    {
      ...base,
      name: "name",
      type: "VARCHAR2(200)",
      nativeType: "VARCHAR2(200)",
      category: "text",
      isPrimaryKey: false,
      identityGeneration: undefined,
    },
    {
      ...base,
      name: "amount",
      type: "NUMBER(30,18)",
      nativeType: "NUMBER(30,18)",
      category: "decimal",
      isPrimaryKey: false,
      identityGeneration: undefined,
    },
  ];
  const config = { id: "oracle-b02", name: "Oracle", type: "oracle" as const };
  const driver = new OracleDriver(config);
  const events: string[] = [];
  const verificationBinds: unknown[][] = [];
  const cursors: Array<{ close: ReturnType<typeof vi.fn> }> = [];
  let nextId = 9007199254740992n;
  const stored = new Map<string, { name: unknown; amount: unknown }>();
  const code = Buffer.from("00112233445566778899aabbccddeeff", "hex");
  const execute = vi.fn(
    async (
      sql: string,
      binds: unknown[] = [],
      settings?: oracledb.ExecuteOptions,
    ) => {
      if (sql.startsWith("ALTER SESSION")) return {};
      expect(settings?.autoCommit).toBe(false);
      if (sql.startsWith("INSERT")) {
        events.push("INSERT");
        const id = options.textKey ? `generated-${++nextId}` : String(++nextId);
        const outputCount = options.composite ? 2 : 1;
        expect(sql).toContain(
          options.textKey
            ? 'RETURNING "id" INTO'
            : "RETURNING TO_CHAR(\"id\", 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')",
        );
        expect(sql.split(" RETURNING ")[0]).not.toContain('"id"'); // GENERATED ALWAYS: no PK input.
        expect(binds.slice(-outputCount)).toEqual([
          { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 32767 },
          ...(options.composite
            ? [
                {
                  dir: oracledb.BIND_OUT,
                  type: oracledb.BUFFER,
                  maxSize: 32767,
                },
              ]
            : []),
        ]);
        stored.set(id, {
          name: binds[0],
          amount:
            options.mismatch && stored.size === 1
              ? "99"
              : binds.length > outputCount + 1
                ? binds[1]
                : undefined,
        });
        return {
          rowsAffected: 1,
          outBinds:
            options.out === "missing"
              ? undefined
              : options.out === "multiple"
                ? [[id, "another"]]
                : options.out === "null"
                  ? [[null]]
                  : [[id], ...(options.composite ? [[code]] : [])],
        };
      }
      events.push("VERIFY");
      verificationBinds.push(binds);
      expect(settings).toMatchObject({
        resultSet: true,
        outFormat: oracledb.OUT_FORMAT_ARRAY,
        autoCommit: false,
      });
      const row = stored.get(String(binds[0]));
      if (!row) throw new Error("Wrong captured identity");
      if (options.composite) expect(binds[1]).toEqual(code);
      const selected = columns.filter((column) =>
        sql.includes(`"${column.name}" AS`),
      );
      const metadata = selected.map((column, index) => ({
        name: `__col_${index}`,
        dbType:
          column.category === "decimal"
            ? oracledb.DB_TYPE_NUMBER
            : oracledb.DB_TYPE_VARCHAR,
        precision: 30,
        scale: 18,
      }));
      const values = selected.map((column, index) => {
        const raw = row[column.name as "name" | "amount"];
        const handler = settings?.fetchTypeHandler?.(metadata[index]);
        if (column.category === "decimal")
          expect(handler?.type).toBe(oracledb.STRING);
        return handler?.converter?.(String(raw)) ?? raw;
      });
      let fetched = false;
      const cursor = {
        metaData: metadata,
        getRows: vi.fn(async () => {
          if (options.readError) throw new Error("verification fetch failed");
          const result = fetched ? [] : [values];
          fetched = true;
          return result;
        }),
        close: vi.fn(async () => {
          events.push("CURSOR_CLOSE");
        }),
      };
      cursors.push(cursor);
      return { resultSet: cursor };
    },
  );
  const connection = {
    execute,
    close: vi.fn(async () => {}),
    break: vi.fn(async () => {}),
    commit: vi.fn(async () => {
      events.push("COMMIT");
    }),
    rollback: vi.fn(async () => {
      events.push("ROLLBACK");
      stored.clear();
    }),
  };
  const lease = vi.fn(async () => connection);
  (driver as unknown as { pool: unknown }).pool = { getConnection: lease };
  const outside = vi
    .spyOn(driver, "query")
    .mockRejectedValue(new Error("Outside transaction lease forbidden"));
  const manager = {
    getConnection: () => config,
    getDriver: () => driver,
  } as never;
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  const prepare = (values: Record<string, unknown>) =>
    service.prepareInsertRow(config.id, "", "APP", "people", values);
  return {
    manager,
    service,
    prepare,
    events,
    verificationBinds,
    cursors,
    connection,
    lease,
    outside,
    stored,
  };
}
afterEach(() => vi.restoreAllMocks());

describe("Oracle generated INSERT identity using real transaction/read decoder", () => {
  it("inserts {name: Alice} with GENERATED ALWAYS NUMBER PK via RETURNING OUT STRING", async () => {
    const {
      service,
      prepare,
      events,
      lease,
      outside,
      verificationBinds,
      cursors,
    } = fixture();
    await service.executePreparedInsertPlan(await prepare({ name: "Alice" }));
    expect(verificationBinds).toEqual([["9007199254740993"]]);
    expect(events).toEqual(["INSERT", "VERIFY", "CURSOR_CLOSE", "COMMIT"]);
    expect(lease).toHaveBeenCalledOnce();
    expect(outside).not.toHaveBeenCalled();
    expect(cursors[0].close).toHaveBeenCalledOnce();
  });

  it("keeps exact NUMBER values through native fetch handler and composite RAW OUT binds", async () => {
    const { service, prepare, verificationBinds } = fixture({
      composite: true,
    });
    const requested = "1.234567890123456789";
    const plan = await prepare({ name: "Alice", amount: requested });
    expect(
      plan.verification?.values.find(({ column }) => column.name === "amount")
        ?.expectedValue,
    ).toBe(requested);
    await service.executePreparedInsertPlan(plan);
    expect(verificationBinds[0][0]).toBe("9007199254740993");
    expect(verificationBinds[0][1]).toBeInstanceOf(Buffer);
  });

  it("captures a default-generated VARCHAR2 PK for ordinary {name: Alice} inserts", async () => {
    const { service, prepare, verificationBinds, events } = fixture({
      textKey: true,
    });
    await service.executePreparedInsertPlan(await prepare({ name: "Alice" }));
    expect(verificationBinds).toEqual([["generated-9007199254740993"]]);
    expect(events.at(-1)).toBe("COMMIT");
  });

  it("captures each generated key and rolls back every insert on later native decoded mismatch", async () => {
    const { manager, prepare, events, verificationBinds, stored } = fixture({
      mismatch: true,
    });
    const inserts = await Promise.all([
      prepare({ name: "Alice", amount: "1.25" }),
      prepare({ name: "Bob", amount: "2.5" }),
    ]);
    const result = await executeAtomicSqlApplyPlan(manager, null, inserts);
    expect(result.success).toBe(false);
    expect(result.insertApplied).not.toBe(true);
    expect(verificationBinds).toEqual([
      ["9007199254740993"],
      ["9007199254740994"],
    ]);
    expect(events.at(-1)).toBe("ROLLBACK");
    expect(events).not.toContain("COMMIT");
    expect(stored.size).toBe(0);
  });

  it.each([
    "missing",
    "null",
    "multiple",
  ] as const)("rolls back unreliable %s RETURNING output without a second lease", async (out) => {
    const { service, prepare, connection, lease, verificationBinds } = fixture({
      out,
    });
    await expect(
      service.executePreparedInsertPlan(await prepare({ name: "Alice" })),
    ).rejects.toThrow(/identity could not be captured reliably/);
    expect(verificationBinds).toEqual([]);
    expect(connection.rollback).toHaveBeenCalledOnce();
    expect(connection.commit).not.toHaveBeenCalled();
    expect(lease).toHaveBeenCalledOnce();
  });

  it("closes the real read cursor and rolls back after native fetch failure", async () => {
    const { service, prepare, connection, cursors } = fixture({
      readError: true,
    });
    await expect(
      service.executePreparedInsertPlan(await prepare({ name: "Alice" })),
    ).rejects.toThrow(/verification fetch failed/);
    expect(cursors[0].close).toHaveBeenCalledOnce();
    expect(connection.rollback).toHaveBeenCalledOnce();
    expect(connection.commit).not.toHaveBeenCalled();
  });
});
