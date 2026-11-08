import { describe, expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";

describe("TableReadService query errors", () => {
  it("throws driver errors returned in QueryResult instead of showing an empty page", async () => {
    const dateColumn: ColumnTypeMeta = {
      name: "event_date",
      type: "date",
      nativeType: "date",
      category: "date",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const query = vi.fn().mockResolvedValue({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 1,
      error:
        "Conversion failed when converting date and/or time from character string.",
    });
    const driver = {
      describeColumns: vi.fn().mockResolvedValue([dateColumn]),
      qualifiedTableName: vi.fn(() => "[db].[dbo].[events]"),
      buildOrderByDefault: vi.fn(() => "ORDER BY [event_date] ASC"),
      buildPagination: vi.fn(() => ({
        sql: "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
        params: [0, 25],
      })),
      normalizeFilterValue: vi.fn((_column, _operator, value) => value),
      buildFilterCondition: vi.fn(() => ({
        sql: "[event_date] = CAST(? AS date)",
        params: ["not-a-date"],
      })),
      query,
    };
    const service = new TableReadService({
      getConnection: vi.fn(() => ({ id: "mssql-qa" })),
      getDriver: vi.fn(() => driver),
    } as never);

    await expect(
      service.getPage("mssql-qa", "db", "dbo", "events", 1, 25, [
        { column: "event_date", operator: "eq", value: "not-a-date" },
      ]),
    ).rejects.toThrow(
      "Conversion failed when converting date and/or time from character string.",
    );
    expect(query).toHaveBeenCalledTimes(2);
  });

  it("rejects invalid Oracle DATE filters before dispatching count or page SQL", async () => {
    const dateColumn: ColumnTypeMeta = {
      name: "DATE_COL",
      type: "DATE",
      nativeType: "DATE",
      category: "date",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const driver = new OracleDriver({
      id: "oracle-filter-validation",
      name: "Oracle Filter Validation",
      type: "oracle",
      host: "127.0.0.1",
      port: 1521,
      serviceName: "FREEPDB1",
      username: "qa",
      password: "qa",
    } as never);
    vi.spyOn(driver, "describeColumns").mockResolvedValue([dateColumn]);
    const query = vi.spyOn(driver, "query").mockResolvedValue({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 1,
    });
    const service = new TableReadService({
      getConnection: vi.fn(() => ({ id: "oracle-filter-validation" })),
      getDriver: vi.fn(() => driver),
    } as never);

    await expect(
      service.getPage(
        "oracle-filter-validation",
        "FREEPDB1",
        "ORACLE_TEST_USER",
        "ORACLE_MASTER",
        1,
        25,
        [{ column: "DATE_COL", operator: "eq", value: "not-a-date" }],
      ),
    ).rejects.toThrow(
      "[RapiDB Filter] Column DATE_COL expects a valid Oracle temporal value.",
    );
    expect(driver.describeColumns).toHaveBeenCalledOnce();
    expect(query).not.toHaveBeenCalled();
  });

  it.each([
    {
      label: "BETWEEN with an invalid upper bound",
      filter: {
        column: "DATE_COL",
        operator: "between" as const,
        value: ["2024-02-29", "not-a-date"] as [string, string],
      },
    },
    {
      label: "IN with an invalid member",
      filter: {
        column: "DATE_COL",
        operator: "in" as const,
        value: "2024-02-29,not-a-date",
      },
    },
    {
      label: "IN with an invalid timezone offset",
      filter: {
        column: "DATE_COL",
        operator: "in" as const,
        value: "2024-02-29 00:00:00 +99:99",
      },
    },
  ])("rejects invalid Oracle DATE $label before dispatching SQL", async ({
    filter,
  }) => {
    const dateColumn: ColumnTypeMeta = {
      name: "DATE_COL",
      type: "DATE",
      nativeType: "DATE",
      category: "date",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "between", "in"],
      valueSemantics: "plain",
    };
    const driver = new OracleDriver({
      id: "oracle-filter-validation",
      name: "Oracle Filter Validation",
      type: "oracle",
      host: "127.0.0.1",
      port: 1521,
      serviceName: "FREEPDB1",
      username: "qa",
      password: "qa",
    } as never);
    vi.spyOn(driver, "describeColumns").mockResolvedValue([dateColumn]);
    const query = vi.spyOn(driver, "query").mockResolvedValue({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 1,
    });
    const service = new TableReadService({
      getConnection: vi.fn(() => ({ id: "oracle-filter-validation" })),
      getDriver: vi.fn(() => driver),
    } as never);

    await expect(
      service.getPage(
        "oracle-filter-validation",
        "FREEPDB1",
        "ORACLE_TEST_USER",
        "ORACLE_MASTER",
        1,
        25,
        [filter],
      ),
    ).rejects.toThrow(
      "[RapiDB Filter] Column DATE_COL expects a valid Oracle temporal value.",
    );
    expect(driver.describeColumns).toHaveBeenCalledOnce();
    expect(query).not.toHaveBeenCalled();
  });
});
