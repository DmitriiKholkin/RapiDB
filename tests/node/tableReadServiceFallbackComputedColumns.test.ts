import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "int",
    nativeType: "int",
    category: "integer",
    nullable: false,
    isPrimaryKey: true,
    primaryKeyOrdinal: 1,
    isForeignKey: false,
    filterable: true,
    filterOperators: [
      "eq",
      "neq",
      "gt",
      "gte",
      "lt",
      "lte",
      "between",
      "in",
      "is_null",
      "is_not_null",
    ],
    valueSemantics: "plain",
  },
  {
    name: "calc",
    type: "int",
    nativeType: "int",
    category: "integer",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [
      "eq",
      "neq",
      "gt",
      "gte",
      "lt",
      "lte",
      "between",
      "in",
      "is_null",
      "is_not_null",
    ],
    valueSemantics: "plain",
    isComputed: true,
  },
];

describe("TableReadService arithmetic overflow handling", () => {
  it("throws fatal error with computed column names", async () => {
    const firstQueryError = new Error(
      "Arithmetic overflow error converting expression",
    );
    const query = vi.fn().mockRejectedValueOnce(firstQueryError);

    const driver = {
      qualifiedTableName: vi.fn(() => "[db].[dbo].[t]"),
      describeColumns: vi.fn().mockResolvedValue(columns),
      buildOrderByDefault: vi.fn(() => "ORDER BY [id] ASC"),
      buildPagination: vi.fn(() => ({
        sql: "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
        params: [0, 50],
      })),
      query,
      quoteIdentifier: vi.fn((name: string) => `[${name}]`),
      formatOutputValue: vi.fn((value: unknown) => value),
      buildFilterCondition: vi.fn(),
      normalizeFilterValue: vi.fn(),
    };

    const connectionManager = {
      getConnection: vi.fn(() => ({ id: "c1" })),
      getDriver: vi.fn(() => driver),
    };

    const service = new TableReadService(connectionManager as never);

    await expect(
      service.getPage("c1", "db", "dbo", "t", 1, 50, [], null, true),
    ).rejects.toThrow(
      "Arithmetic overflow error converting expression (computed columns: calc)",
    );

    expect(query).toHaveBeenCalledTimes(1);
  });

  it("uses driver-native table page reads when available", async () => {
    const readTablePage = vi.fn().mockResolvedValue({
      columns,
      rows: [{ id: 1, calc: 2 }],
      totalCount: 1,
    });
    const query = vi.fn();

    const driver = {
      readTablePage,
      qualifiedTableName: vi.fn(() => "[db].[dbo].[t]"),
      describeColumns: vi.fn().mockResolvedValue(columns),
      buildOrderByDefault: vi.fn(() => "ORDER BY [id] ASC"),
      buildPagination: vi.fn(() => ({
        sql: "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
        params: [0, 50],
      })),
      query,
      quoteIdentifier: vi.fn((name: string) => `[${name}]`),
      formatOutputValue: vi.fn((value: unknown) => value),
      buildFilterCondition: vi.fn(),
      normalizeFilterValue: vi.fn(),
    };

    const connectionManager = {
      getConnection: vi.fn(() => ({ id: "c1" })),
      getDriver: vi.fn(() => driver),
    };

    const service = new TableReadService(connectionManager as never);
    const result = await service.getPage(
      "c1",
      "db",
      "dbo",
      "t",
      2,
      25,
      [],
      { column: "id", direction: "asc" },
      true,
    );

    expect(result).toEqual({
      columns,
      rows: [{ id: 1, calc: 2 }],
      totalCount: 1,
    });
    expect(readTablePage).toHaveBeenCalledWith({
      database: "db",
      schema: "dbo",
      table: "t",
      page: 2,
      pageSize: 25,
      filters: [],
      sort: { column: "id", direction: "asc" },
      skipCount: true,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("falls back to a lower-bound totalCount when COUNT fails", async () => {
    const query = vi
      .fn()
      .mockRejectedValueOnce(new Error("count failed"))
      .mockResolvedValueOnce({
        columns: ["id", "calc"],
        rows: [
          { __col_0: 51, __col_1: 99 },
          { __col_0: 52, __col_1: 100 },
        ],
      });

    const driver = {
      qualifiedTableName: vi.fn(() => "[db].[dbo].[t]"),
      describeColumns: vi.fn().mockResolvedValue(columns),
      buildOrderByDefault: vi.fn(() => "ORDER BY [id] ASC"),
      buildPagination: vi.fn(() => ({
        sql: "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
        params: [50, 25],
      })),
      query,
      quoteIdentifier: vi.fn((name: string) => `[${name}]`),
      formatOutputValue: vi.fn((value: unknown) => value),
      buildFilterCondition: vi.fn(),
      normalizeFilterValue: vi.fn(),
    };

    const connectionManager = {
      getConnection: vi.fn(() => ({ id: "c1" })),
      getDriver: vi.fn(() => driver),
    };

    const service = new TableReadService(connectionManager as never);
    const result = await service.getPage(
      "c1",
      "db",
      "dbo",
      "t",
      3,
      25,
      [],
      null,
      false,
    );

    expect(result.totalCount).toBe(52);
    expect(result.rows).toEqual([
      { id: 51, calc: 99 },
      { id: 52, calc: 100 },
    ]);
  });

  it("uses keyset-first export for fallback SQL reads to avoid duplicates under concurrent inserts", async () => {
    const keysetColumns: ColumnTypeMeta[] = [
      {
        name: "id",
        type: "int",
        nativeType: "int",
        category: "integer",
        nullable: false,
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
        isForeignKey: false,
        filterable: true,
        filterOperators: ["eq", "gt", "gte", "lt", "lte", "in"],
        valueSemantics: "plain",
      },
    ];
    const dataset = [1, 2, 3, 4, 5].map((id) => ({ id }));
    let dataQueryCount = 0;

    const buildPagination = vi.fn((offset: number, limit: number) => ({
      sql: "LIMIT ? OFFSET ?",
      params: [limit, offset],
    }));

    const query = vi.fn(async (_sql: string, params: unknown[] = []) => {
      dataQueryCount += 1;
      if (dataQueryCount === 2) {
        dataset.unshift({ id: 0 });
      }

      const limit = Number(params[params.length - 2] ?? 2);
      const cursorId = params.length > 2 ? Number(params[0]) : undefined;
      const rows = dataset
        .filter((row) => (cursorId === undefined ? true : row.id > cursorId))
        .slice(0, limit)
        .map((row) => ({ __col_0: row.id }));
      return {
        columns: ["id"],
        rows,
        rowCount: rows.length,
        executionTimeMs: 1,
      };
    });

    const driver = {
      qualifiedTableName: vi.fn(() => "tbl"),
      describeColumns: vi.fn().mockResolvedValue(keysetColumns),
      buildOrderByDefault: vi.fn(() => "ORDER BY id ASC"),
      buildPagination,
      query,
      quoteIdentifier: vi.fn((name: string) => name),
      formatOutputValue: vi.fn((value: unknown) => value),
      buildFilterCondition: vi.fn(
        (column: ColumnTypeMeta, operator: string, value: unknown) => ({
          sql: `${column.name} ${operator === "eq" ? "=" : ">"} ?`,
          params: [value],
        }),
      ),
      normalizeFilterValue: vi.fn(),
    };

    const connectionManager = {
      getConnection: vi.fn(() => ({ id: "c1" })),
      getDriver: vi.fn(() => driver),
    };

    const service = new TableReadService(connectionManager as never);
    const exportedIds: number[] = [];
    for await (const chunk of service.exportAll("c1", "db", "dbo", "t", 2)) {
      for (const row of chunk.rows) {
        exportedIds.push(Number(row.id));
      }
    }

    expect(exportedIds).toEqual([1, 2, 3, 4, 5]);
    expect(new Set(exportedIds).size).toBe(exportedIds.length);
    expect(buildPagination).toHaveBeenCalled();
    for (const [offset] of buildPagination.mock.calls) {
      expect(offset).toBe(0);
    }
  });

  it("falls back to offset export when keyset ordering is unavailable", async () => {
    const nonKeysetColumns: ColumnTypeMeta[] = [
      {
        name: "name",
        type: "text",
        nativeType: "text",
        category: "text",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
        filterable: true,
        filterOperators: ["eq", "like"],
        valueSemantics: "plain",
      },
    ];
    const dataset = ["a", "b", "c", "d", "e"];

    const buildPagination = vi.fn((offset: number, limit: number) => ({
      sql: "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
      params: [offset, limit],
    }));
    const query = vi.fn(async (_sql: string, params: unknown[] = []) => {
      const offset = Number(params[0] ?? 0);
      const limit = Number(params[1] ?? 2);
      const rows = dataset
        .slice(offset, offset + limit)
        .map((value) => ({ __col_0: value }));
      return {
        columns: ["name"],
        rows,
        rowCount: rows.length,
        executionTimeMs: 1,
      };
    });

    const driver = {
      qualifiedTableName: vi.fn(() => "tbl"),
      describeColumns: vi.fn().mockResolvedValue(nonKeysetColumns),
      buildOrderByDefault: vi.fn(() => "ORDER BY name ASC"),
      buildPagination,
      query,
      quoteIdentifier: vi.fn((name: string) => name),
      formatOutputValue: vi.fn((value: unknown) => value),
      buildFilterCondition: vi.fn(),
      normalizeFilterValue: vi.fn(),
    };

    const connectionManager = {
      getConnection: vi.fn(() => ({ id: "c1" })),
      getDriver: vi.fn(() => driver),
    };

    const service = new TableReadService(connectionManager as never);
    const exported: string[] = [];
    for await (const chunk of service.exportAll("c1", "db", "dbo", "t", 2)) {
      for (const row of chunk.rows) {
        exported.push(String(row.name));
      }
    }

    expect(exported).toEqual(dataset);
    expect(
      buildPagination.mock.calls.some(([offset]) => Number(offset) > 0),
    ).toBe(true);
  });
});
