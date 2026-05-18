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
});
