/* biome-ignore-all lint/suspicious/noExplicitAny: legacy mock-heavy test file uses explicit any casts throughout */
import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ConnectionConfig,
  ConnectionManager,
} from "../../src/extension/connectionManager";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  ColumnTypeMeta,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import {
  applyChangesTransactional,
  type ColumnDef,
  executePreparedApplyPlan,
  type Filter,
  prepareApplyChangesPlan,
  type RowUpdate,
  TableDataService,
} from "../../src/extension/tableDataService";
import type { ConnectionType } from "../../src/shared/connectionTypes";
import { StubDriver } from "./helpers";

// ─── Mock driver factory ───

function makeMockDriver(overrides: Partial<IDBDriver> = {}): IDBDriver {
  const baseDriver = new StubDriver();

  return {
    connect: vi.fn(),
    disconnect: vi.fn(),
    isConnected: vi.fn().mockReturnValue(true),
    listDatabases: vi.fn(),
    listSchemas: vi.fn(),
    listObjects: vi.fn(),
    describeTable: vi.fn(),
    describeColumns: vi.fn().mockResolvedValue([]),
    getIndexes: vi.fn(),
    getForeignKeys: vi.fn(),
    getCreateTableDDL: vi.fn(),
    getRoutineDefinition: vi.fn(),
    query: vi.fn().mockResolvedValue({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    }),
    runTransaction: vi.fn(),
    quoteIdentifier: (name: string) => `"${name}"`,
    qualifiedTableName: (_db: string, schema: string, table: string) =>
      schema ? `"${schema}"."${table}"` : `"${table}"`,
    buildPagination: (_offset: number, limit: number, _pi: number) => ({
      sql: "LIMIT ? OFFSET ?",
      params: [limit, _offset],
    }),
    buildOrderByDefault: (cols: ColumnTypeMeta[]) => {
      const pk = cols.filter((c) => c.isPrimaryKey);
      return pk.length > 0 ? `ORDER BY "${pk[0].name}"` : "";
    },
    coerceInputValue: (v: unknown) => v,
    formatOutputValue: (v: unknown) => v,
    checkPersistedEdit: () => null,
    normalizeFilterValue: baseDriver.normalizeFilterValue.bind(baseDriver),
    buildFilterCondition: vi.fn().mockReturnValue(null),
    buildInsertValueExpr: () => "?",
    buildSetExpr: (c: ColumnTypeMeta) => `"${c.name}" = ?`,
    materializePreviewSql: (sql: string) => sql,
    ...overrides,
  } as unknown as IDBDriver;
}

function makeMockCM(
  driver: IDBDriver,
  connectionType: ConnectionType = "pg",
): ConnectionManager {
  const cfg: ConnectionConfig = {
    id: "conn1",
    name: "test",
    type: connectionType,
    host: "localhost",
    port: 5432,
    database: "testdb",
    username: "u",
    password: "p",
  };
  return {
    getConnection: vi.fn().mockReturnValue(cfg),
    getDriver: vi.fn().mockReturnValue(driver),
  } as unknown as ConnectionManager;
}

function makeExactNumericColumns(nativeType = "numeric(10,2)"): ColumnDef[] {
  return [
    {
      name: "id",
      type: "integer",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      category: "integer",
      nativeType: "integer",
      filterable: true,
      editable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    },
    {
      name: "amount",
      type: nativeType,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "decimal",
      nativeType,
      filterable: true,
      editable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    },
  ];
}

function makeJsonColumns(nativeType = "jsonb"): ColumnDef[] {
  return [
    {
      name: "id",
      type: "integer",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      category: "integer",
      nativeType: "integer",
      filterable: true,
      editable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    },
    {
      name: "payload",
      type: nativeType,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "json",
      nativeType,
      filterable: true,
      editable: true,
      filterOperators: ["like"],
      valueSemantics: "plain",
    },
  ];
}

function makeFloatColumns(nativeType = "double precision"): ColumnDef[] {
  return [
    {
      name: "id",
      type: "integer",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      category: "integer",
      nativeType: "integer",
      filterable: true,
      editable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    },
    {
      name: "ratio",
      type: nativeType,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "float",
      nativeType,
      filterable: true,
      editable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    },
  ];
}

function makeDriverConfig(type: ConnectionType): ConnectionConfig {
  return {
    id: `${type}-conn`,
    name: type,
    type,
    host: "localhost",
    port:
      type === "pg"
        ? 5432
        : type === "mysql"
          ? 3306
          : type === "mssql"
            ? 1433
            : type === "oracle"
              ? 1521
              : 0,
    database: "testdb",
    username: "u",
    password: "p",
  };
}

function makeTestColumns(): ColumnDef[] {
  return [
    {
      name: "id",
      type: "integer",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      category: "integer",
      nativeType: "integer",
      filterable: true,
      editable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    },
    {
      name: "name",
      type: "text",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "text",
      nativeType: "text",
      filterable: true,
      editable: true,
      filterOperators: ["like"],
      valueSemantics: "plain",
    },
    {
      name: "active",
      type: "boolean",
      nullable: false,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "boolean",
      nativeType: "boolean",
      filterable: true,
      editable: true,
      filterOperators: ["eq"],
      valueSemantics: "boolean",
    },
  ];
}

// ─── TableDataService ───

describe("TableDataService", () => {
  let driver: IDBDriver;
  let cm: ConnectionManager;
  let svc: TableDataService;

  beforeEach(() => {
    // Reset static cache
    (TableDataService as any)._colCache.clear();
    driver = makeMockDriver();
    cm = makeMockCM(driver);
    svc = new TableDataService(cm);
  });

  describe("getColumns", () => {
    it("calls describeColumns and returns result", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      const result = await svc.getColumns("conn1", "testdb", "public", "users");
      expect(result).toEqual(cols);
      expect(driver.describeColumns).toHaveBeenCalledWith(
        "testdb",
        "public",
        "users",
      );
    });

    it("caches results on second call", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      await svc.getColumns("conn1", "testdb", "public", "users");
      await svc.getColumns("conn1", "testdb", "public", "users");
      expect(driver.describeColumns).toHaveBeenCalledTimes(1);
    });

    it("clearForConnection removes cached columns", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      await svc.getColumns("conn1", "testdb", "public", "users");
      svc.clearForConnection("conn1");
      await svc.getColumns("conn1", "testdb", "public", "users");
      expect(driver.describeColumns).toHaveBeenCalledTimes(2);
    });
  });

  describe("getPage", () => {
    it("returns formatted rows with correct columns", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);

      const queryResults = [
        {
          columns: ["cnt"],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
          executionTimeMs: 0,
        },
        {
          columns: ["id", "name", "active"],
          rows: [{ __col_0: 1, __col_1: "Alice", __col_2: true }],
          rowCount: 1,
          executionTimeMs: 0,
        },
      ];
      let callIdx = 0;
      (driver.query as any).mockImplementation(() =>
        Promise.resolve(queryResults[callIdx++]),
      );

      const page = await svc.getPage(
        "conn1",
        "testdb",
        "public",
        "users",
        1,
        25,
        [],
      );
      expect(page.totalCount).toBe(1);
      expect(page.rows).toHaveLength(1);
      expect(page.rows[0]).toEqual({ id: 1, name: "Alice", active: true });
    });

    it("keeps Oracle interval table values as plain strings", async () => {
      const oracle = new OracleDriver({
        id: "ora",
        name: "ora",
        type: "oracle",
        host: "localhost",
        port: 1521,
        database: "testdb",
        username: "u",
        password: "p",
      });

      driver = makeMockDriver({
        formatOutputValue: oracle.formatOutputValue.bind(oracle),
      });
      cm = makeMockCM(driver);
      svc = new TableDataService(cm);

      const cols: ColumnDef[] = [
        {
          name: "IVAL",
          type: "INTERVAL DAY TO SECOND",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "text",
          nativeType: "INTERVAL DAY TO SECOND",
          filterable: true,
          editable: true,
          filterOperators: ["like"],
          valueSemantics: "plain",
        },
      ];

      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: ["IVAL"],
          rows: [
            {
              __col_0: {
                days: 3,
                hours: 4,
                minutes: 5,
                seconds: 6,
                fseconds: 120000000,
              },
            },
          ],
          rowCount: 1,
          executionTimeMs: 0,
        });

      const page = await svc.getPage(
        "conn1",
        "testdb",
        "public",
        "users",
        1,
        25,
        [],
      );

      expect(page.rows).toEqual([{ IVAL: "3 04:05:06.12" }]);
      expect(typeof page.rows[0]?.IVAL).toBe("string");
      expect(page.rows[0]?.IVAL).not.toBe("[object Object]");
    });

    it("stringifies unexpected Oracle interval objects instead of leaking raw objects", async () => {
      const oracle = new OracleDriver({
        id: "ora",
        name: "ora",
        type: "oracle",
        host: "localhost",
        port: 1521,
        database: "testdb",
        username: "u",
        password: "p",
      });

      driver = makeMockDriver({
        formatOutputValue: oracle.formatOutputValue.bind(oracle),
      });
      cm = makeMockCM(driver);
      svc = new TableDataService(cm);

      const cols: ColumnDef[] = [
        {
          name: "IVAL",
          type: "INTERVAL DAY TO SECOND",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "text",
          nativeType: "INTERVAL DAY TO SECOND",
          filterable: true,
          editable: true,
          filterOperators: ["like"],
          valueSemantics: "plain",
        },
      ];

      vi.mocked(driver.describeColumns).mockResolvedValue(cols);
      vi.mocked(driver.query)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: ["IVAL"],
          rows: [
            {
              __col_0: {
                days: 3,
                hours: 4,
                minutes: 5,
              },
            },
          ],
          rowCount: 1,
          executionTimeMs: 0,
        });

      const page = await svc.getPage(
        "conn1",
        "testdb",
        "public",
        "users",
        1,
        25,
        [],
      );

      expect(page.rows).toEqual([{ IVAL: '{"days":3,"hours":4,"minutes":5}' }]);
      expect(page.rows[0]?.IVAL).not.toBe("[object Object]");
    });

    it("passes filters through buildFilterCondition with inferred operators", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.buildFilterCondition as any).mockReturnValue({
        sql: '"name" LIKE ?',
        params: ["%alice%"],
      });

      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: ["id", "name", "active"],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      const filters: Filter[] = [
        { column: "name", operator: "like", value: "alice" },
      ];
      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, filters);

      expect(driver.buildFilterCondition).toHaveBeenCalledWith(
        expect.objectContaining({ name: "name" }),
        "like",
        "alice",
        1,
      );
      // The COUNT query should include WHERE
      const countCall = (driver.query as any).mock.calls[0];
      expect(countCall[0]).toContain("WHERE");
    });

    it("skips non-filterable columns", async () => {
      const cols = makeTestColumns();
      cols[1] = { ...cols[1], filterable: false };
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
        { column: "name", operator: "like", value: "alice" },
      ]);

      expect(driver.buildFilterCondition).not.toHaveBeenCalled();
      const countCall = (driver.query as any).mock.calls[0];
      expect(countCall[0]).not.toContain("WHERE");
    });

    it("allows null-only filters for nullable non-filterable columns", async () => {
      const cols = makeTestColumns();
      cols[1] = {
        ...cols[1],
        filterable: false,
        nullable: true,
        filterOperators: ["is_null", "is_not_null"],
      };
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.buildFilterCondition as any).mockReturnValue({
        sql: '"name" IS NULL',
        params: [],
      });
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
        { column: "name", operator: "is_null" },
      ]);

      expect(driver.buildFilterCondition).toHaveBeenCalledWith(
        expect.objectContaining({ name: "name", filterable: false }),
        "is_null",
        undefined,
        1,
      );
      const countCall = (driver.query as any).mock.calls[0];
      expect(countCall[0]).toContain("WHERE");
    });

    it("allows IS NOT NULL filters for nullable non-filterable columns", async () => {
      const cols = makeTestColumns();
      cols[1] = {
        ...cols[1],
        filterable: false,
        nullable: true,
        filterOperators: ["is_null", "is_not_null"],
      };
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.buildFilterCondition as any).mockReturnValue({
        sql: '"name" IS NOT NULL',
        params: [],
      });
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
        { column: "name", operator: "is_not_null" },
      ]);

      expect(driver.buildFilterCondition).toHaveBeenCalledWith(
        expect.objectContaining({ name: "name", filterable: false }),
        "is_not_null",
        undefined,
        1,
      );
      const countCall = (driver.query as any).mock.calls[0];
      expect(countCall[0]).toContain("WHERE");
    });

    it("rejects null filters when the column does not expose is_null", async () => {
      const cols = makeTestColumns();
      cols[1] = {
        ...cols[1],
        filterable: false,
        nullable: true,
        filterOperators: ["is_not_null"],
      };
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
          { column: "name", operator: "is_null" },
        ]),
      ).rejects.toThrow("Column name does not support is_null filters");

      expect(driver.buildFilterCondition).not.toHaveBeenCalled();
      expect(driver.query).not.toHaveBeenCalled();
    });

    it("skips COUNT when skipCount=true", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      });

      const page = await svc.getPage(
        "conn1",
        "testdb",
        "public",
        "users",
        1,
        25,
        [],
        null,
        true,
      );
      expect(page.totalCount).toBe(0);
      expect(driver.query).toHaveBeenCalledTimes(1); // Only data query, no COUNT
    });

    it("handles empty filter values (skips them)", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      const filters: Filter[] = [
        { column: "name", operator: "like", value: "" },
        { column: "name", operator: "like", value: "  " },
      ];
      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, filters),
      ).rejects.toThrow("Column name expects a filter value");
    });

    it("throws a filter error for invalid numeric input", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
          { column: "id", operator: "eq", value: "not-a-number" },
        ]),
      ).rejects.toThrow("Column id expects a number");

      expect(driver.query).not.toHaveBeenCalled();
    });

    it("throws a filter error for non-finite numeric input", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
          { column: "id", operator: "eq", value: "Infinity" },
        ]),
      ).rejects.toThrow("Column id expects a number");

      expect(driver.query).not.toHaveBeenCalled();
    });

    it("throws a filter error for invalid boolean input", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
          { column: "active", operator: "eq", value: "maybe" },
        ]),
      ).rejects.toThrow("Column active expects true or false");

      expect(driver.query).not.toHaveBeenCalled();
    });

    it("rejects unsupported operators before delegating to the driver", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
          { column: "name", operator: "eq", value: "alice" },
        ]),
      ).rejects.toThrow("Column name does not support eq filters");

      expect(driver.buildFilterCondition).not.toHaveBeenCalled();
      expect(driver.query).not.toHaveBeenCalled();
    });

    it("normalizes numeric IN filters before delegating to the driver", async () => {
      const cols = [
        {
          ...makeTestColumns()[0],
          filterOperators: ["eq", "in"],
        },
      ];
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.buildFilterCondition as any).mockReturnValue({
        sql: '"id" IN (?, ?, ?)',
        params: [1, 2, 3],
      });
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
        { column: "id", operator: "in", value: "1,  2 ,3" },
      ]);

      expect(driver.buildFilterCondition).toHaveBeenCalledWith(
        expect.objectContaining({ name: "id" }),
        "in",
        "1, 2, 3",
        1,
      );
    });

    it("rejects unsupported date IN filters before querying", async () => {
      const cols = [
        {
          ...makeTestColumns()[1],
          name: "created_on",
          type: "date",
          category: "date",
          nativeType: "date",
          filterOperators: ["eq", "like", "is_null", "is_not_null"],
        },
      ];
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
          { column: "created_on", operator: "in", value: "2026-04-15" },
        ]),
      ).rejects.toThrow("Column created_on does not support in filters");

      expect(driver.buildFilterCondition).not.toHaveBeenCalled();
      expect(driver.query).not.toHaveBeenCalled();
    });

    it("normalizes SQL datetime text to a date-only equality filter for date columns", async () => {
      const cols = [
        {
          ...makeTestColumns()[1],
          name: "created_on",
          type: "date",
          category: "date",
          nativeType: "date",
          filterOperators: ["eq", "like"],
        },
      ];
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.buildFilterCondition as any).mockReturnValue({
        sql: '"created_on" = ?',
        params: ["2026-04-15"],
      });
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
        { column: "created_on", operator: "eq", value: "2026-04-15 00:00:00" },
      ]);

      expect(driver.buildFilterCondition).toHaveBeenCalledWith(
        expect.objectContaining({ name: "created_on" }),
        "eq",
        "2026-04-15",
        1,
      );
    });

    it("accepts displayed SQL datetimeoffset text with a spaced offset for date columns", async () => {
      const cols = [
        {
          ...makeTestColumns()[1],
          name: "created_on",
          type: "date",
          category: "date",
          nativeType: "date",
          filterOperators: ["eq", "like"],
        },
      ];
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.buildFilterCondition as any).mockReturnValue({
        sql: '"created_on" = ?',
        params: ["2026-04-15"],
      });
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
        {
          column: "created_on",
          operator: "eq",
          value: "2026-04-15 00:00:00 +00:00",
        },
      ]);

      expect(driver.buildFilterCondition).toHaveBeenCalledWith(
        expect.objectContaining({ name: "created_on" }),
        "eq",
        "2026-04-15",
        1,
      );
    });

    it("normalizes offset-bearing ISO datetimes to the resulting UTC date for date columns", async () => {
      const cols = [
        {
          ...makeTestColumns()[1],
          name: "created_on",
          type: "date",
          category: "date",
          nativeType: "date",
          filterOperators: ["eq", "like"],
        },
      ];
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.buildFilterCondition as any).mockReturnValue({
        sql: '"created_on" = ?',
        params: ["2026-04-16"],
      });
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
        {
          column: "created_on",
          operator: "eq",
          value: "2026-04-15T23:00:00-02:00",
        },
      ]);

      expect(driver.buildFilterCondition).toHaveBeenCalledWith(
        expect.objectContaining({ name: "created_on" }),
        "eq",
        "2026-04-16",
        1,
      );
    });

    it("throws a filter error for impossible calendar dates", async () => {
      const cols = [
        {
          ...makeTestColumns()[1],
          name: "created_on",
          type: "date",
          category: "date",
          nativeType: "date",
          filterOperators: ["eq", "like"],
        },
      ];
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
          { column: "created_on", operator: "eq", value: "2026-02-31" },
        ]),
      ).rejects.toThrow("Column created_on expects a valid date");

      expect(driver.query).not.toHaveBeenCalled();
    });

    it("throws a filter error for impossible offset-bearing datetime input on date columns", async () => {
      const cols = [
        {
          ...makeTestColumns()[1],
          name: "created_on",
          type: "date",
          category: "date",
          nativeType: "date",
          filterOperators: ["eq", "like"],
        },
      ];
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.getPage("conn1", "testdb", "public", "users", 1, 25, [
          {
            column: "created_on",
            operator: "eq",
            value: "2026-02-31 12:00:00 +00:00",
          },
        ]),
      ).rejects.toThrow("Column created_on expects a valid date");

      expect(driver.query).not.toHaveBeenCalled();
    });

    it("applies sort when provided", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any)
        .mockResolvedValueOnce({
          columns: ["cnt"],
          rows: [{ __col_0: 0 }],
          rowCount: 1,
          executionTimeMs: 0,
        })
        .mockResolvedValueOnce({
          columns: [],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });

      await svc.getPage("conn1", "testdb", "public", "users", 1, 25, [], {
        column: "name",
        direction: "desc",
      });

      const dataCall = (driver.query as any).mock.calls[1];
      expect(dataCall[0]).toContain("ORDER BY");
      expect(dataCall[0]).toContain("DESC");
    });
  });

  describe("insertRow", () => {
    it("builds INSERT with driver helpers", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: 0,
      });

      await svc.insertRow("conn1", "testdb", "public", "users", {
        name: "Bob",
        active: "true",
      });

      expect(driver.query).toHaveBeenCalled();
      const [sql, params] = (driver.query as any).mock.calls[0];
      expect(sql).toContain("INSERT INTO");
      expect(sql).toContain("VALUES");
      expect(params.length).toBe(2);
    });

    it("throws when no values provided", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);

      await expect(
        svc.insertRow("conn1", "testdb", "public", "users", {}),
      ).rejects.toThrow("no values provided");
    });

    it("omits only undefined values", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: 0,
      });

      await svc.insertRow("conn1", "testdb", "public", "users", {
        name: "Bob",
        active: undefined as any,
      });

      const [, params] = (driver.query as any).mock.calls[0];
      expect(params.length).toBe(1);
    });

    it("preserves explicit empty string values", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: 0,
      });

      await svc.insertRow("conn1", "testdb", "public", "users", {
        name: "",
        active: "true",
      });

      const [, params] = (driver.query as any).mock.calls[0];
      expect(params).toEqual(["", "true"]);
    });

    it("ignores non-editable and auto increment columns on insert", async () => {
      const cols = makeTestColumns();
      cols[0] = { ...cols[0], isAutoIncrement: true };
      cols[1] = { ...cols[1], editable: false };
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: 0,
      });

      await svc.insertRow("conn1", "testdb", "public", "users", {
        id: 99,
        name: "Bob",
        active: "true",
      });

      const [sql, params] = (driver.query as any).mock.calls[0];
      expect(sql).not.toContain('"id"');
      expect(sql).not.toContain('"name"');
      expect(params).toEqual(["true"]);
    });

    it("throws when 0 rows affected", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 0,
        affectedRows: 0,
        executionTimeMs: 0,
      });

      await expect(
        svc.insertRow("conn1", "testdb", "public", "users", { name: "Bob" }),
      ).rejects.toThrow("Insert failed");
    });
  });

  describe("updateRow", () => {
    it("builds UPDATE with driver helpers", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: 0,
      });

      await svc.updateRow(
        "conn1",
        "testdb",
        "public",
        "users",
        { id: 1 },
        { name: "Updated" },
      );

      const [sql] = (driver.query as any).mock.calls[0];
      expect(sql).toContain("UPDATE");
      expect(sql).toContain("SET");
      expect(sql).toContain("WHERE");
    });

    it("throws when row not found (0 affected)", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 0,
        affectedRows: 0,
        executionTimeMs: 0,
      });

      await expect(
        svc.updateRow(
          "conn1",
          "testdb",
          "public",
          "users",
          { id: 999 },
          { name: "X" },
        ),
      ).rejects.toThrow("Row not found");
    });

    it("ignores non-editable and auto increment columns on update", async () => {
      const cols = makeTestColumns();
      cols[0] = { ...cols[0], isAutoIncrement: true };
      cols[1] = { ...cols[1], editable: false };
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: 0,
      });

      await svc.updateRow(
        "conn1",
        "testdb",
        "public",
        "users",
        { id: 1 },
        { id: 2, name: "Updated", active: false },
      );

      const [sql, params] = (driver.query as any).mock.calls[0];
      const setClause = sql.split(" WHERE ")[0];
      expect(setClause).toContain('SET "active" = ?');
      expect(setClause).not.toContain('"id" = ?');
      expect(setClause).not.toContain('"name" = ?');
      expect(params).toEqual([false, 1]);
    });
  });

  describe("deleteRows", () => {
    it("does nothing for empty list", async () => {
      await svc.deleteRows("conn1", "testdb", "public", "users", []);
      expect(driver.query).not.toHaveBeenCalled();
    });

    it("uses IN clause for single PK", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 2,
        executionTimeMs: 0,
      });

      await svc.deleteRows("conn1", "testdb", "public", "users", [
        { id: 1 },
        { id: 2 },
      ]);

      const [sql] = (driver.query as any).mock.calls[0];
      expect(sql).toContain("DELETE FROM");
      expect(sql).toContain("IN");
    });

    it("uses transaction for composite PK", async () => {
      const cols = makeTestColumns();
      // Make both id and name primary keys
      cols[1] = { ...cols[1], isPrimaryKey: true };
      (driver.describeColumns as any).mockResolvedValue(cols);

      await svc.deleteRows("conn1", "testdb", "public", "users", [
        { id: 1, name: "a" },
        { id: 2, name: "b" },
      ]);

      expect(driver.runTransaction).toHaveBeenCalled();
      const ops = (driver.runTransaction as any).mock.calls[0][0];
      expect(ops).toHaveLength(2);
      expect(ops[0].sql).toContain("DELETE FROM");
    });
  });

  describe("exportAll", () => {
    it("yields pages until empty", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);

      let callCount = 0;
      (driver.query as any).mockImplementation(() => {
        callCount++;
        if (callCount <= 2) {
          // First two data queries return rows
          return Promise.resolve({
            columns: ["id"],
            rows: [{ __col_0: callCount }],
            rowCount: 1,
            executionTimeMs: 0,
          });
        }
        // Third data query returns empty → stop
        return Promise.resolve({
          columns: ["id"],
          rows: [],
          rowCount: 0,
          executionTimeMs: 0,
        });
      });

      const chunks: any[] = [];
      for await (const chunk of svc.exportAll(
        "conn1",
        "testdb",
        "public",
        "users",
        1,
      )) {
        chunks.push(chunk);
      }

      expect(chunks).toHaveLength(2);
    });

    it("respects abort signal", async () => {
      const cols = makeTestColumns();
      (driver.describeColumns as any).mockResolvedValue(cols);
      (driver.query as any).mockResolvedValue({
        columns: ["id"],
        rows: [{ __col_0: 1 }],
        rowCount: 1,
        executionTimeMs: 0,
      });

      const controller = new AbortController();
      controller.abort();

      const gen = svc.exportAll(
        "conn1",
        "testdb",
        "public",
        "users",
        10,
        null,
        [],
        controller.signal,
      );
      await expect(gen.next()).rejects.toThrow("Export cancelled by user");
    });
  });

  describe("conn throws for missing connection", () => {
    it("throws when connection not found", async () => {
      const badCm = {
        getConnection: vi.fn().mockReturnValue(undefined),
        getDriver: vi.fn().mockReturnValue(undefined),
      } as unknown as ConnectionManager;
      const badSvc = new TableDataService(badCm);

      await expect(
        badSvc.getColumns("missing", "db", "schema", "table"),
      ).rejects.toThrow("Not connected");
    });
  });
});

// ─── applyChangesTransactional ───

describe("applyChangesTransactional", () => {
  it("returns success for empty updates", async () => {
    const cm = {
      getConnection: vi.fn(),
      getDriver: vi.fn(),
    } as unknown as ConnectionManager;

    const result = await applyChangesTransactional(
      cm,
      "c1",
      "db",
      "s",
      "t",
      [],
      [],
    );
    expect(result.success).toBe(true);
  });

  it("returns error when not connected", async () => {
    const cm = {
      getConnection: vi.fn().mockReturnValue(undefined),
      getDriver: vi.fn().mockReturnValue(undefined),
    } as unknown as ConnectionManager;

    const updates: RowUpdate[] = [
      { primaryKeys: { id: 1 }, changes: { name: "x" } },
    ];
    const result = await applyChangesTransactional(
      cm,
      "c1",
      "db",
      "s",
      "t",
      updates,
      [],
    );
    expect(result.success).toBe(false);
    expect(result.error).toContain("Not connected");
  });

  it("calls runTransaction with operations", async () => {
    const mockRunTransaction = vi.fn();
    const driver = makeMockDriver({ runTransaction: mockRunTransaction });
    const cm = makeMockCM(driver);

    const cols = makeTestColumns();
    const updates: RowUpdate[] = [
      { primaryKeys: { id: 1 }, changes: { name: "Updated" } },
    ];

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      updates,
      cols,
    );
    expect(result.success).toBe(true);
    expect(mockRunTransaction).toHaveBeenCalled();
    const ops = mockRunTransaction.mock.calls[0][0];
    expect(ops.length).toBe(1);
    expect(ops[0].sql).toContain("UPDATE");
    expect(ops[0].checkAffectedRows).toBe(true);
  });

  it("catches transaction errors and returns failure", async () => {
    const mockRunTransaction = vi.fn().mockRejectedValue(new Error("deadlock"));
    const driver = makeMockDriver({ runTransaction: mockRunTransaction });
    const cm = makeMockCM(driver);

    const cols = makeTestColumns();
    const updates: RowUpdate[] = [
      { primaryKeys: { id: 1 }, changes: { name: "X" } },
    ];

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      updates,
      cols,
    );
    expect(result.success).toBe(false);
    expect(result.error).toContain("deadlock");
  });

  it("prevalidates exact numerics before writing for Oracle NUMBER(p,s)", async () => {
    const mockRunTransaction = vi.fn();
    const oracle = new OracleDriver(makeDriverConfig("oracle"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      checkPersistedEdit: oracle.checkPersistedEdit.bind(oracle),
    });
    const cm = makeMockCM(driver, "oracle");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.523" },
        },
      ],
      makeExactNumericColumns("NUMBER(10,2)"),
    );

    expect(result.success).toBe(false);
    expect(result.error).toContain("rejected before writing");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "prevalidation_failed",
        success: false,
        columns: ["amount"],
      }),
    ]);
    expect(mockRunTransaction).not.toHaveBeenCalled();
  });

  it("prevalidates exact numerics before writing for PostgreSQL NUMERIC(p,s)", async () => {
    const mockRunTransaction = vi.fn();
    const postgres = new PostgresDriver(makeDriverConfig("pg"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      checkPersistedEdit: postgres.checkPersistedEdit.bind(postgres),
    });
    const cm = makeMockCM(driver, "pg");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.523" },
        },
      ],
      makeExactNumericColumns("numeric(10,2)"),
    );

    expect(result.success).toBe(false);
    expect(result.error).toContain("rejected before writing");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "prevalidation_failed",
        success: false,
      }),
    ]);
    expect(mockRunTransaction).not.toHaveBeenCalled();
  });

  it("prevalidates MSSQL money scale overflow before writing", async () => {
    const mockRunTransaction = vi.fn();
    const mssql = new MSSQLDriver(makeDriverConfig("mssql"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      checkPersistedEdit: mssql.checkPersistedEdit.bind(mssql),
    });
    const cm = makeMockCM(driver, "mssql");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "dbo",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "12.12345" },
        },
      ],
      makeExactNumericColumns("money"),
    );

    expect(result.success).toBe(false);
    expect(result.error).toContain("rejected before writing");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "prevalidation_failed",
        success: false,
      }),
    ]);
    expect(mockRunTransaction).not.toHaveBeenCalled();
  });

  it("prevalidates MySQL DECIMAL(p,s) before writing", async () => {
    const mockRunTransaction = vi.fn();
    const mysql = new MySQLDriver(makeDriverConfig("mysql"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      checkPersistedEdit: mysql.checkPersistedEdit.bind(mysql),
    });
    const cm = makeMockCM(driver, "mysql");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.523" },
        },
      ],
      makeExactNumericColumns("decimal(10,2)"),
    );

    expect(result.success).toBe(false);
    expect(result.error).toContain("rejected before writing");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "prevalidation_failed",
        success: false,
      }),
    ]);
    expect(mockRunTransaction).not.toHaveBeenCalled();
  });

  it("prevalidates malformed JSON edits before writing", async () => {
    const mockRunTransaction = vi.fn();
    const postgres = new PostgresDriver(makeDriverConfig("pg"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      checkPersistedEdit: postgres.checkPersistedEdit.bind(postgres),
    });
    const cm = makeMockCM(driver, "pg");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { payload: "{bad json" },
        },
      ],
      makeJsonColumns("jsonb"),
    );

    expect(result.success).toBe(false);
    expect(result.error).toContain("rejected before writing");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "prevalidation_failed",
        success: false,
        columns: ["payload"],
      }),
    ]);
    expect(mockRunTransaction).not.toHaveBeenCalled();
  });

  it("prevalidates PostgreSQL double precision edits that exceed reliable float precision", async () => {
    const mockRunTransaction = vi.fn();
    const postgres = new PostgresDriver(makeDriverConfig("pg"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      checkPersistedEdit: postgres.checkPersistedEdit.bind(postgres),
    });
    const cm = makeMockCM(driver, "pg");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { ratio: "1.12345678901234567" },
        },
      ],
      makeFloatColumns("double precision"),
    );

    expect(result.success).toBe(false);
    expect(result.error).toContain("rejected before writing");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "prevalidation_failed",
        success: false,
        columns: ["ratio"],
      }),
    ]);
    expect(result.rowOutcomes?.[0]?.message).toContain("15 significant digits");
    expect(result.rowOutcomes?.[0]?.message).toContain("would round to");
    expect(mockRunTransaction).not.toHaveBeenCalled();
  });

  it("marks sibling rows as skipped when a driver hook prevalidation failure blocks the batch", async () => {
    const mockRunTransaction = vi.fn();
    const mockQuery = vi.fn();
    const checkPersistedEdit = vi.fn(
      (
        column: ColumnTypeMeta,
        expectedValue: unknown,
        options?: { persistedValue: unknown },
      ) => {
        if (column.name !== "amount") {
          return null;
        }

        if (options) {
          return {
            ok: true,
            shouldVerify: true,
          };
        }

        return expectedValue === "1234.523"
          ? {
              ok: false,
              shouldVerify: false,
              message: "Scale exceeds the column definition.",
            }
          : {
              ok: true,
              shouldVerify: true,
            };
      },
    );
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      query: mockQuery,
      checkPersistedEdit,
    });
    const cm = makeMockCM(driver);

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.523" },
        },
        {
          primaryKeys: { id: 2 },
          changes: { amount: "1234.52" },
        },
      ],
      makeExactNumericColumns("numeric(10,2)"),
    );

    expect(result.success).toBe(false);
    expect(result.error).toContain("rejected before writing");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "prevalidation_failed",
        success: false,
        columns: ["amount"],
        message: "Scale exceeds the column definition.",
      }),
      expect.objectContaining({
        rowIndex: 1,
        status: "skipped",
        success: false,
        message: "Not applied because another row failed validation.",
      }),
    ]);
    expect(mockRunTransaction).not.toHaveBeenCalled();
    expect(mockQuery).not.toHaveBeenCalled();
  });

  it("keeps no-op rows distinct when another row fails driver prevalidation", async () => {
    const mockRunTransaction = vi.fn();
    const checkPersistedEdit = vi.fn(
      (
        column: ColumnTypeMeta,
        expectedValue: unknown,
        options?: { persistedValue: unknown },
      ) => {
        if (column.name !== "amount") {
          return null;
        }

        if (options) {
          return {
            ok: true,
            shouldVerify: true,
          };
        }

        return expectedValue === "1234.523"
          ? {
              ok: false,
              shouldVerify: false,
              message: "Scale exceeds the column definition.",
            }
          : {
              ok: true,
              shouldVerify: true,
            };
      },
    );
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      checkPersistedEdit,
    });
    const cm = makeMockCM(driver);

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.523" },
        },
        {
          primaryKeys: { id: 2 },
          changes: { ignored: "stale client field" },
        },
      ],
      makeExactNumericColumns("numeric(10,2)"),
    );

    expect(result.success).toBe(false);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "prevalidation_failed",
        success: false,
      }),
      expect.objectContaining({
        rowIndex: 1,
        status: "skipped",
        success: true,
        message: "No editable changes to apply.",
      }),
    ]);
    expect(mockRunTransaction).not.toHaveBeenCalled();
  });

  it("returns a warning when exact numeric read-back does not match the requested value", async () => {
    const mockRunTransaction = vi.fn().mockResolvedValue(undefined);
    const mockQuery = vi.fn().mockResolvedValue({
      columns: ["amount"],
      rows: [{ __col_0: "100.00" }],
      rowCount: 1,
      executionTimeMs: 0,
    });
    const oracle = new OracleDriver(makeDriverConfig("oracle"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      query: mockQuery,
      checkPersistedEdit: oracle.checkPersistedEdit.bind(oracle),
    });
    const cm = makeMockCM(driver, "oracle");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.52" },
        },
      ],
      makeExactNumericColumns("NUMBER(10,2)"),
    );

    expect(result.success).toBe(true);
    expect(result.warning).toContain("could not be confirmed exactly");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "verification_failed",
        success: false,
      }),
    ]);
    expect(mockRunTransaction).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenCalledTimes(1);
  });

  it("returns a warning when a nullable exact numeric reads back as a non-null value", async () => {
    const mockRunTransaction = vi.fn().mockResolvedValue(undefined);
    const mockQuery = vi.fn().mockResolvedValue({
      columns: ["amount"],
      rows: [{ __col_0: "100.00" }],
      rowCount: 1,
      executionTimeMs: 0,
    });
    const postgres = new PostgresDriver(makeDriverConfig("pg"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      query: mockQuery,
      checkPersistedEdit: postgres.checkPersistedEdit.bind(postgres),
    });
    const cm = makeMockCM(driver, "pg");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: null },
        },
      ],
      makeExactNumericColumns("numeric(10,2)"),
    );

    expect(result.success).toBe(true);
    expect(result.warning).toContain("could not be confirmed exactly");
    expect(result.failedRows).toEqual([0]);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "verification_failed",
        success: false,
      }),
    ]);
    expect(mockRunTransaction).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenCalledTimes(1);
  });

  it("verifies edited primary keys using the post-update key values", async () => {
    const mockRunTransaction = vi.fn().mockResolvedValue(undefined);
    const mockQuery = vi.fn().mockResolvedValue({
      columns: ["id"],
      rows: [{ __col_0: "2" }],
      rowCount: 1,
      executionTimeMs: 0,
    });
    const postgres = new PostgresDriver(makeDriverConfig("pg"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      query: mockQuery,
      checkPersistedEdit: postgres.checkPersistedEdit.bind(postgres),
    });
    const cm = makeMockCM(driver, "pg");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { id: "2" },
        },
      ],
      makeExactNumericColumns("numeric(10,2)"),
    );

    expect(result.success).toBe(true);
    expect(result.warning).toBeUndefined();
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "applied",
        success: true,
      }),
    ]);
    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockQuery.mock.calls[0]?.[0]).toContain('WHERE "id" = ?');
    expect(mockQuery.mock.calls[0]?.[1]).toEqual(["2"]);
  });

  it("marks exact numeric edits as applied when read-back matches the requested value", async () => {
    const mockRunTransaction = vi.fn().mockResolvedValue(undefined);
    const mockQuery = vi.fn().mockResolvedValue({
      columns: ["amount"],
      rows: [{ __col_0: "1234.52" }],
      rowCount: 1,
      executionTimeMs: 0,
    });
    const postgres = new PostgresDriver(makeDriverConfig("pg"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      query: mockQuery,
      checkPersistedEdit: postgres.checkPersistedEdit.bind(postgres),
    });
    const cm = makeMockCM(driver, "pg");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.52" },
        },
      ],
      makeExactNumericColumns("numeric(10,2)"),
    );

    expect(result.success).toBe(true);
    expect(result.warning).toBeUndefined();
    expect(result.failedRows).toBeUndefined();
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "applied",
        success: true,
      }),
    ]);
    expect(mockRunTransaction).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockQuery.mock.calls[0]?.[0]).toContain('AS "__col_0"');
  });

  it("skips decimal scale enforcement for SQLite declared DECIMAL types", async () => {
    const mockRunTransaction = vi.fn().mockResolvedValue(undefined);
    const mockQuery = vi.fn().mockResolvedValue({
      columns: ["amount"],
      rows: [{ __col_0: "1234.523" }],
      rowCount: 1,
      executionTimeMs: 0,
    });
    const sqlite = new SQLiteDriver(makeDriverConfig("sqlite"));
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      query: mockQuery,
      checkPersistedEdit: sqlite.checkPersistedEdit.bind(sqlite),
    });
    const cm = makeMockCM(driver, "sqlite");

    const result = await applyChangesTransactional(
      cm,
      "conn1",
      "testdb",
      "main",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.523" },
        },
      ],
      makeExactNumericColumns("DECIMAL(10,2)"),
    );

    expect(result.success).toBe(true);
    expect(result.warning).toBeUndefined();
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "applied",
        success: true,
      }),
    ]);
    expect(mockRunTransaction).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenCalledTimes(1);
  });

  it("ignores read-only changes when building a prepared apply plan", () => {
    const driver = makeMockDriver();
    const cm = makeMockCM(driver);

    const prepared = prepareApplyChangesPlan(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { created_at: "2026-04-21 10:00:00" },
        },
      ],
      [
        {
          name: "id",
          type: "integer",
          nullable: false,
          isPrimaryKey: true,
          isForeignKey: false,
          category: "integer",
          nativeType: "integer",
          filterable: true,
          editable: true,
          filterOperators: ["eq"],
          valueSemantics: "plain",
        },
        {
          name: "created_at",
          type: "timestamp",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "datetime",
          nativeType: "timestamp",
          filterable: true,
          editable: false,
          filterOperators: ["like"],
          valueSemantics: "plain",
        },
      ],
    );

    expect(prepared).toEqual({
      executable: false,
      result: {
        success: true,
        rowOutcomes: [
          {
            rowIndex: 0,
            success: true,
            status: "skipped",
            message: "No editable changes to apply.",
          },
        ],
      },
    });
  });

  it("short-circuits prevalidation failures before returning an executable plan", () => {
    const postgres = new PostgresDriver(makeDriverConfig("pg"));
    const driver = makeMockDriver({
      checkPersistedEdit: postgres.checkPersistedEdit.bind(postgres),
    });
    const cm = makeMockCM(driver, "pg");

    const prepared = prepareApplyChangesPlan(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.567" },
        },
      ],
      makeExactNumericColumns("numeric(10,2)"),
    );

    expect(prepared.executable).toBe(false);
    if (prepared.executable) {
      throw new Error("expected a non-executable preparation result");
    }
    expect(prepared.result.success).toBe(false);
    expect(prepared.result.failedRows).toEqual([0]);
  });

  it("reuses prepared operations when executing an apply plan", async () => {
    const mockRunTransaction = vi.fn().mockResolvedValue(undefined);
    const mockQuery = vi.fn().mockResolvedValue({
      columns: ["amount"],
      rows: [{ __col_0: "1234.52" }],
      rowCount: 1,
      executionTimeMs: 0,
    });
    const driver = makeMockDriver({
      runTransaction: mockRunTransaction,
      query: mockQuery,
    });
    const cm = makeMockCM(driver);

    const prepared = prepareApplyChangesPlan(
      cm,
      "conn1",
      "testdb",
      "public",
      "users",
      [
        {
          primaryKeys: { id: 1 },
          changes: { amount: "1234.52" },
        },
      ],
      makeExactNumericColumns("numeric(10,2)"),
    );

    expect(prepared.executable).toBe(true);
    if (!prepared.executable) {
      throw new Error("expected an executable preparation result");
    }
    expect(mockRunTransaction).not.toHaveBeenCalled();

    const result = await executePreparedApplyPlan(cm, prepared.plan);

    expect(mockRunTransaction).toHaveBeenCalledWith(prepared.plan.operations);
    expect(result.success).toBe(true);
    expect(result.rowOutcomes).toEqual([
      expect.objectContaining({
        rowIndex: 0,
        status: "applied",
        success: true,
      }),
    ]);
  });

  it("prepares an INSERT once and executes the same operation later", async () => {
    const materializePreviewSql = vi.fn((sql: string) => sql);
    const driver = makeMockDriver({
      describeColumns: vi.fn().mockResolvedValue(makeTestColumns()),
      materializePreviewSql,
      query: vi.fn().mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: 0,
      }),
    });
    const cm = makeMockCM(driver);
    const svc = new TableDataService(cm);

    const prepared = await svc.prepareInsertRow(
      "conn1",
      "testdb",
      "public",
      "users",
      { id: 10, name: "Alice" },
    );

    expect(driver.query).not.toHaveBeenCalled();
    expect(materializePreviewSql).toHaveBeenCalledWith(
      'INSERT INTO "public"."users" ("id", "name") VALUES (?, ?)',
      [10, "Alice"],
    );
    expect(prepared.previewStatements).toEqual([
      'INSERT INTO "public"."users" ("id", "name") VALUES (?, ?)',
    ]);

    await svc.executePreparedInsertPlan(prepared);

    expect(driver.query).toHaveBeenCalledTimes(1);
    expect(driver.query).toHaveBeenCalledWith(
      'INSERT INTO "public"."users" ("id", "name") VALUES (?, ?)',
      [10, "Alice"],
    );
  });

  it("throws when a prepared INSERT reports zero affected rows", async () => {
    const driver = makeMockDriver({
      describeColumns: vi.fn().mockResolvedValue(makeTestColumns()),
      query: vi.fn().mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 0,
        affectedRows: 0,
        executionTimeMs: 0,
      }),
    });
    const cm = makeMockCM(driver);
    const svc = new TableDataService(cm);

    const prepared = await svc.prepareInsertRow(
      "conn1",
      "testdb",
      "public",
      "users",
      { id: 10, name: "Alice" },
    );

    await expect(svc.executePreparedInsertPlan(prepared)).rejects.toThrow(
      "Insert failed: the database reported 0 rows affected.",
    );
    expect(driver.query).toHaveBeenCalledTimes(1);
  });
});
