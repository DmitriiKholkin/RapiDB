import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ConnectionConfig,
  ConnectionManager,
} from "../../src/extension/connectionManager";
import type {
  ColumnTypeMeta,
  IDBDriver,
  QueryResult,
} from "../../src/extension/dbDrivers/types";
import { NULL_SENTINEL } from "../../src/extension/dbDrivers/types";
import {
  applyChangesTransactional,
  type ColumnDef,
  type Filter,
  type RowUpdate,
  TableDataService,
} from "../../src/extension/tableDataService";

// ─── Mock driver factory ───

function makeMockDriver(overrides: Partial<IDBDriver> = {}): IDBDriver {
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
    buildFilterCondition: vi.fn().mockReturnValue(null),
    buildInsertValueExpr: () => "?",
    buildSetExpr: (c: ColumnTypeMeta) => `"${c.name}" = ?`,
    ...overrides,
  } as unknown as IDBDriver;
}

function makeMockCM(driver: IDBDriver): ConnectionManager {
  const cfg: ConnectionConfig = {
    id: "conn1",
    name: "test",
    type: "pg",
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
      isBoolean: false,
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
      isBoolean: false,
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
      isBoolean: true,
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
});
