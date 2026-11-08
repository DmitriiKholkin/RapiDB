import { afterEach, describe, expect, it, vi } from "vitest";
import { isMutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

function column(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
  extra: Partial<ColumnTypeMeta> = {},
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
    ...extra,
  };
}

const withAutoKey = [
  column("id", "int", "integer", {
    isPrimaryKey: true,
    nullable: false,
    identityGeneration: "auto_increment",
  }),
  column("created", "timestamp", "datetime"),
];
const withoutKey = [
  column("created", "timestamp", "datetime"),
  column("note", "varchar(10)", "text"),
];

function harness(metadata: ColumnTypeMeta[], serverEpoch: string) {
  const config = { id: "dst", name: "DST", type: "mysql" as const };
  const driver = new MySQLDriver(config);
  const statements: Array<{ sql: string; values: unknown[] }> = [];
  const conn = {
    query: vi.fn(
      async (input: string | { sql: string; values?: unknown[] }) => {
        const sql = typeof input === "string" ? input : input.sql;
        const values = typeof input === "string" ? [] : (input.values ?? []);
        statements.push({ sql, values });
        if (sql === "DO 0") return [{ serverStatus: 2 }];
        if (sql.startsWith("SELECT @@session.time_zone")) {
          return [
            [
              {
                __rapidb_time_zone: "Europe/Berlin",
                __rapidb_epoch_0: serverEpoch,
              },
            ],
          ];
        }
        if (sql.startsWith("INSERT")) {
          return [{ affectedRows: 1, insertId: 7 }, undefined];
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      },
    ),
    beginTransaction: vi.fn(),
    commit: vi.fn(),
    release: vi.fn(),
    destroy: vi.fn(),
  };
  (driver as unknown as { pool: unknown }).pool = {
    getConnection: vi.fn(async () => conn),
    config: { connectionConfig: {} },
  };
  const runTransaction = vi.spyOn(driver, "runTransaction");
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
  } as never;
  const service = new TableMutationService(manager, {
    getColumns: async () => metadata,
  });
  return { service, statements, runTransaction, conn };
}

afterEach(() => vi.restoreAllMocks());

describe("MySQL INSERT of an offset TIMESTAMP without readback verification", () => {
  it.each([
    ["an AUTO_INCREMENT key", withAutoKey],
    ["no primary key", withoutKey],
  ])("refuses the repeated DST hour before writing (%s)", async (_label, metadata) => {
    const h = harness(metadata, "1729992600");
    const plan = await h.service.prepareInsertRow("dst", "db", "", "ev", {
      created: "2024-10-27T00:30:00Z",
    });
    expect(plan.verification).toBeUndefined();
    expect(plan.operation.sql).toContain(
      "CONVERT_TZ(?, '+00:00', @@session.time_zone)",
    );

    const error = await h.service
      .executePreparedInsertPlan(plan)
      .catch((caught: unknown) => caught);

    expect(isMutationNotExecutedError(error)).toBe(true);
    expect((error as Error).message).toBe(
      "[RapiDB] TIMESTAMP value 2024-10-27 00:30:00 UTC falls into a repeated local hour of the session time zone Europe/Berlin (for example when daylight saving time ends), so MySQL cannot store that exact instant. Nothing was written.",
    );
    expect(h.statements.some((s) => s.sql.startsWith("INSERT"))).toBe(false);
    expect(h.runTransaction).not.toHaveBeenCalled();
  });

  it("checks on the write connection and then inserts an unambiguous instant", async () => {
    const h = harness(withAutoKey, "1729980000");
    const plan = await h.service.prepareInsertRow("dst", "db", "", "ev", {
      created: "2024-10-26T22:00:00Z",
    });
    expect(plan.verification).toBeUndefined();

    await h.service.executePreparedInsertPlan(plan);

    const sqls = h.statements.map((s) => s.sql);
    const check = sqls.findIndex((sql) =>
      sql.startsWith("SELECT @@session.time_zone"),
    );
    expect(check).toBeGreaterThanOrEqual(0);
    expect(h.statements[check].values).toEqual(["2024-10-26 22:00:00"]);
    expect(sqls[check + 1]).toBe(plan.operation.sql);
    expect(h.statements[check + 1].values).toEqual(["2024-10-26 22:00:00"]);
    expect(h.conn.query).toHaveBeenCalledTimes(3);
  });

  it("adds no round trip to inserts without an offset TIMESTAMP", async () => {
    const h = harness(withAutoKey, "unused");
    const plan = await h.service.prepareInsertRow("dst", "db", "", "ev", {
      created: "2024-10-27 02:30:00",
    });

    await h.service.executePreparedInsertPlan(plan);

    expect(h.statements.map((s) => s.sql)).toEqual([
      plan.operation.sql,
      "DO 0",
    ]);
  });
});
