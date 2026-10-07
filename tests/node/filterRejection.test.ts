import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import {
  type ColumnTypeMeta,
  type FilterExpression,
  resolveFilterOperators,
} from "../../src/extension/dbDrivers/types";
import {
  buildWhere,
  validateFilterExpressions,
} from "../../src/extension/table/filterSql";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { serializeFilterDrafts } from "../../src/shared/tableTypes";

function column(
  category: ColumnTypeMeta["category"] = "text",
  nativeType = "text",
): ColumnTypeMeta {
  return {
    name: "value",
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    valueSemantics:
      category === "boolean"
        ? "boolean"
        : nativeType.startsWith("bit(")
          ? "bit"
          : "plain",
    filterOperators: resolveFilterOperators(category, {
      filterable: true,
      nullable: true,
    }),
  };
}

describe.each([
  [
    "PostgreSQL",
    () => new PostgresDriver({ id: "filters", name: "filters", type: "pg" }),
  ],
  [
    "MySQL",
    () => new MySQLDriver({ id: "filters", name: "filters", type: "mysql" }),
  ],
] as const)("%s filter rejection", (_name, createDriver) => {
  it.each([
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
  ] as const)("rejects direct text %s instead of substituting LIKE", (operator) => {
    expect(() =>
      createDriver().buildFilterCondition(column(), operator, "needle", 3),
    ).toThrow(
      `[RapiDB Filter] Column value does not support ${operator} filters for text values.`,
    );
  });

  it("retains text search, null predicates, and the existing UI operator policy", () => {
    const driver = createDriver();
    const meta = column();
    expect(meta.filterOperators).toEqual(["like", "is_null", "is_not_null"]);
    for (const operator of ["like", "ilike"] as const) {
      const result = driver.buildFilterCondition(meta, operator, "needle", 3);
      expect(result?.sql).toMatch(/I?LIKE/);
      expect(result?.params).toEqual([
        _name === "MySQL"
          ? Buffer.from("%needle%").toString("hex")
          : "%needle%",
      ]);
    }
    for (const operator of ["is_null", "is_not_null"] as const) {
      // Null checks are allowed even when scalar filtering is unavailable.
      const result = buildWhere(
        driver,
        [{ column: "value", operator }],
        [{ ...meta, filterable: false }],
      );
      expect(result.clause).toContain(
        operator === "is_null" ? "IS NULL" : "IS NOT NULL",
      );
      expect(result.params).toEqual([]);
    }
    expect(buildWhere(driver, [], [meta])).toEqual({ clause: "", params: [] });
    expect(
      serializeFilterDrafts([meta], {
        value: { operator: "like", value: "  " },
      }),
    ).toEqual([]);
  });

  it("preserves legitimate typed comparison, range, and membership branches", () => {
    const driver = createDriver();
    const cases: [
      ColumnTypeMeta,
      FilterExpression["operator"],
      string | [string, string],
    ][] = [
      [column("integer", "int"), "gt", "12"],
      [column("decimal", "decimal"), "neq", "12.5"],
      [column("float", "float"), "eq", "12.5"],
      [column("binary", _name === "MySQL" ? "blob" : "bytea"), "neq", "0xAB"],
      [column("boolean", "boolean"), "eq", _name === "MySQL" ? "1" : "true"],
      [
        column("spatial", "point"),
        "eq",
        _name === "MySQL" ? "POINT(1 2)" : "(1,2)",
      ],
      [column("date", "date"), "gt", "2026-01-01"],
      [column("time", "time"), "lt", "12:00:00"],
      [
        column("datetime", "timestamp"),
        "between",
        ["2026-01-01 00:00:00", "2026-02-01 00:00:00"],
      ],
      [column("integer", "int"), "between", ["1", "2"]],
      [column("enum", "enum"), "in", "a,b"],
      [column("uuid", "uuid"), "in", "00000000-0000-0000-0000-000000000001"],
    ];
    if (_name === "MySQL") cases.push([column("integer", "bit(4)"), "eq", "3"]);
    else {
      cases.push([column("interval", "interval"), "neq", "1 day"]);
      cases.push([column("json", "jsonb"), "eq", '{"a":1}']);
    }
    for (const [meta, operator, value] of cases) {
      const result = driver.buildFilterCondition(meta, operator, value, 3);
      expect(result, `${meta.category} ${operator}`).not.toBeNull();
      expect(result?.sql).not.toMatch(/LIKE/);
      expect(result?.params.length).toBeGreaterThan(0);
    }
    for (const category of ["array", "json"] as const) {
      expect(
        driver.buildFilterCondition(column(category), "like", "needle", 3)?.sql,
      ).toMatch(/LIKE/);
    }
  });

  it.each([
    [
      "unknown",
      { column: "missing", operator: "like", value: "x" },
      column(),
      "Unknown filter column missing.",
    ],
    [
      "nonfilterable",
      { column: "value", operator: "like", value: "x" },
      { ...column(), filterable: false },
      "Column value is not filterable.",
    ],
    [
      "unsupported",
      { column: "value", operator: "neq", value: "x" },
      column(),
      "Column value does not support neq filters.",
    ],
    [
      "empty value",
      { column: "value", operator: "like", value: "" },
      column(),
      "Column value expects a filter value.",
    ],
    [
      "null builder result",
      { column: "value", operator: "like", value: "needle" },
      column(),
      "Column value could not build like filter.",
    ],
    [
      "unsupported null",
      { column: "value", operator: "is_null" },
      { ...column(), filterOperators: ["like"] },
      "Column value does not support is_null filters.",
    ],
  ] as const)("surfaces %s through the active WHERE/read path before querying", async (_case, filter, meta, message) => {
    const driver = createDriver();
    const query = vi.spyOn(driver, "query");
    if (_case === "null builder result") {
      const build = driver.buildFilterCondition.bind(driver);
      vi.spyOn(driver, "buildFilterCondition").mockImplementation(
        (meta, ...args) =>
          meta.name === "value" ? null : build(meta, ...args),
      );
    }
    const cols: ColumnTypeMeta[] = [
      { ...column(), name: "valid" },
      { ...meta, filterOperators: [...meta.filterOperators] },
    ];
    vi.spyOn(driver, "describeColumns").mockResolvedValue(cols);
    const filters: FilterExpression[] = [
      { column: "valid", operator: "like", value: "keep" },
      filter,
    ];
    const expected = `[RapiDB Filter] ${message}`;
    expect(() => buildWhere(driver, filters, cols)).toThrow(expected);
    const service = new TableReadService({
      getConnection: () => ({ id: "filters" }),
      getDriver: () => driver,
    } as never);
    await expect(
      service.getPage(
        "filters",
        "db",
        "public",
        "items",
        1,
        25,
        filters,
        null,
        true,
      ),
    ).rejects.toThrow(expected);
    expect(query).not.toHaveBeenCalled();
  });

  it("rejects a driver's null result even for an otherwise valid filter", () => {
    const driver = createDriver();
    vi.spyOn(driver, "buildFilterCondition").mockReturnValue(null);
    expect(() =>
      buildWhere(
        driver,
        [{ column: "value", operator: "like", value: "needle" }],
        [column()],
      ),
    ).toThrow("[RapiDB Filter] Column value could not build like filter.");
  });
});

describe("native page filter validation", () => {
  function createMongoDriver() {
    return new MongoDBDriver({
      id: "mongo-filters",
      name: "mongo-filters",
      type: "mongodb",
      host: "localhost",
      port: 27017,
      database: "db",
    });
  }

  function createReadService(driver: MongoDBDriver) {
    return new TableReadService({
      getConnection: () => ({ id: "mongo-filters" }),
      getDriver: () => driver,
    } as never);
  }

  it.each([
    { operator: "in" as const, value: "alpha,beta" },
    { operator: "eq" as const, value: "alpha" },
  ])("rejects forged text $operator before MongoDB's native page reader is called", async ({
    operator,
    value,
  }) => {
    const driver = createMongoDriver();
    const textColumn = { ...column(), name: "text" };
    const describeColumns = vi
      .spyOn(driver, "describeColumns")
      .mockResolvedValue([textColumn]);
    const readTablePage = vi.spyOn(driver, "readTablePage").mockResolvedValue({
      columns: [textColumn],
      rows: [],
      totalCount: 0,
    });
    const query = vi.spyOn(driver, "query");
    const service = createReadService(driver);

    await expect(
      service.getPage("mongo-filters", "db", "public", "items", 1, 25, [
        { column: "text", operator, value },
      ]),
    ).rejects.toThrow(
      `[RapiDB Filter] Column text does not support ${operator} filters.`,
    );

    expect(describeColumns).toHaveBeenCalledTimes(1);
    expect(readTablePage).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalled();
  });

  it("preserves allowed UUID and enum IN filters for native page readers", async () => {
    const driver = createMongoDriver();
    const columns = [
      { ...column("uuid", "uuid"), name: "id" },
      { ...column("enum", "enum"), name: "status" },
    ];
    vi.spyOn(driver, "describeColumns").mockResolvedValue(columns);
    const readTablePage = vi.spyOn(driver, "readTablePage").mockResolvedValue({
      columns,
      rows: [],
      totalCount: 0,
    });
    const service = createReadService(driver);
    const filters: FilterExpression[] = [
      {
        column: "id",
        operator: "in",
        value: "00000000-0000-0000-0000-000000000001",
      },
      { column: "status", operator: "in", value: "active,pending" },
    ];

    await service.getPage(
      "mongo-filters",
      "db",
      "public",
      "items",
      1,
      25,
      filters,
    );

    expect(readTablePage).toHaveBeenCalledWith(
      expect.objectContaining({ filters }),
    );
  });

  it.each([
    "is_null",
    "is_not_null",
  ] as const)("accepts an own undefined value for %s filters", (operator) => {
    const filter = {
      column: "value",
      operator,
      value: undefined,
    } as unknown as FilterExpression;

    expect(() =>
      validateFilterExpressions([filter], [column("integer", "int")]),
    ).not.toThrow();
  });

  it.each([
    [
      "missing scalar value",
      { column: "value", operator: "eq" },
      "Column value requires a value for eq filters.",
    ],
    [
      "non-scalar value",
      { column: "value", operator: "eq", value: ["a", "b"] },
      "Column value expects a string value for eq filters.",
    ],
    [
      "malformed range",
      { column: "value", operator: "between", value: ["a"] },
      "Column value expects two string values for between filters.",
    ],
    [
      "null check with a value",
      { column: "value", operator: "is_null", value: "null" },
      "Column value does not accept a value for is_null filters.",
    ],
  ] as const)("rejects malformed %s expressions", (_case, filter, message) => {
    const meta = column("integer", "int");
    expect(() =>
      validateFilterExpressions(
        [filter as unknown as FilterExpression],
        [meta],
      ),
    ).toThrow(`[RapiDB Filter] ${message}`);
  });
});
