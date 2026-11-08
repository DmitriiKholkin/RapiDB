import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { createProjectTempDir } from "../runtime/tempDirectories";

function column(
  name: string,
  overrides: Partial<ColumnTypeMeta> = {},
): ColumnTypeMeta {
  return {
    name,
    type: "text",
    nativeType: "text",
    category: "text",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
    ...overrides,
  };
}

function fakeDriver(
  columns: ColumnTypeMeta[],
  options: {
    rowIdentity?: string[];
    defaultOrderBy?: string;
  } = {},
) {
  const query = vi.fn(
    async (
      _sql: string,
      _params?: unknown[],
    ): Promise<{ columns: string[]; rows: Record<string, unknown>[] }> => ({
      columns: [],
      rows: [],
    }),
  );
  const getTableRowIdentityOrderExpressions = vi.fn(
    async () => options.rowIdentity ?? [],
  );
  return {
    query,
    getTableRowIdentityOrderExpressions,
    driver: {
      qualifiedTableName: vi.fn(() => '"s"."t"'),
      describeColumns: vi.fn(async () => columns),
      buildOrderByDefault: vi.fn(() => options.defaultOrderBy ?? ""),
      buildPagination: vi.fn((offset: number, limit: number) => ({
        sql: "LIMIT ? OFFSET ?",
        params: [limit, offset],
      })),
      query,
      quoteIdentifier: vi.fn((name: string) => `"${name}"`),
      formatOutputValue: vi.fn((value: unknown) => value),
      getTableRowIdentityOrderExpressions,
    },
  };
}

function service(driver: object) {
  return new TableReadService({
    getConnection: vi.fn(() => ({ id: "c1" })),
    getDriver: vi.fn(() => driver),
  } as never);
}

const dataSql = (query: ReturnType<typeof fakeDriver>["query"]) =>
  query.mock.calls.map(([sql]) => sql);

describe("TableReadService stable row order without a primary key", () => {
  it("orders unsorted pages by the physical row identity and caches it", async () => {
    const fake = fakeDriver([column("name")], {
      rowIdentity: ["tableoid", "ctid"],
    });
    const reader = service(fake.driver);

    await reader.getPage("c1", "db", "s", "t", 1, 10, [], null, true);
    await reader.getPage("c1", "db", "s", "t", 2, 10, [], null, true);

    expect(dataSql(fake.query)).toEqual([
      'SELECT * FROM "s"."t"  ORDER BY tableoid ASC, ctid ASC LIMIT ? OFFSET ?',
      'SELECT * FROM "s"."t"  ORDER BY tableoid ASC, ctid ASC LIMIT ? OFFSET ?',
    ]);
    expect(fake.getTableRowIdentityOrderExpressions).toHaveBeenCalledOnce();
  });

  it("adds the row identity as a tie-breaker for a non-unique sort column", async () => {
    const fake = fakeDriver([column("name")], { rowIdentity: ["ROWID"] });

    await service(fake.driver).getPage(
      "c1",
      "db",
      "s",
      "t",
      1,
      10,
      [],
      { column: "name", direction: "desc" },
      true,
    );

    expect(dataSql(fake.query)[0]).toContain('ORDER BY "name" DESC, ROWID ASC');
  });

  it("keeps primary key ordering and a driver default order unchanged", async () => {
    const withKey = fakeDriver(
      [column("id", { isPrimaryKey: true, primaryKeyOrdinal: 1 })],
      { rowIdentity: ["ctid"], defaultOrderBy: 'ORDER BY "id"' },
    );
    await service(withKey.driver).getPage(
      "c1",
      "db",
      "s",
      "t",
      1,
      10,
      [],
      { column: "id", direction: "asc" },
      true,
    );
    expect(dataSql(withKey.query)[0]).toContain('ORDER BY "id" ASC LIMIT');
    expect(withKey.getTableRowIdentityOrderExpressions).not.toHaveBeenCalled();

    const withDefault = fakeDriver([column("name")], {
      rowIdentity: ["ctid"],
      defaultOrderBy: 'ORDER BY "name"',
    });
    await service(withDefault.driver).getPage(
      "c1",
      "db",
      "s",
      "t",
      1,
      10,
      [],
      null,
      true,
    );
    expect(dataSql(withDefault.query)[0]).toContain('ORDER BY "name" LIMIT');
  });

  it("uses the row identity for every page of an export without a primary key", async () => {
    const fake = fakeDriver([column("name")], { rowIdentity: ["rowid"] });
    fake.query
      .mockResolvedValueOnce({
        columns: ["name"],
        rows: [{ __col_0: "a" }, { __col_0: "a" }],
      })
      .mockResolvedValueOnce({ columns: ["name"], rows: [{ __col_0: "a" }] });

    const chunks = [];
    for await (const chunk of service(fake.driver).exportAll(
      "c1",
      "db",
      "s",
      "t",
      2,
      { column: "name", direction: "asc" },
    )) {
      chunks.push(chunk);
    }

    expect(chunks.flatMap((chunk) => chunk.rows)).toHaveLength(3);
    expect(dataSql(fake.query)).toHaveLength(2);
    for (const sql of dataSql(fake.query)) {
      expect(sql).toContain('ORDER BY "name" ASC, rowid ASC');
    }
  });
});

describe("driver row identity order expressions", () => {
  const drivers: SQLiteCoreDriver[] = [];
  afterEach(async () => {
    await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
  });

  it("pages a SQLite rowid table sorted by a duplicate value without skipping rows", async () => {
    const directory = await createProjectTempDir("sqlite-stable-order");
    const driver = new SQLiteCoreDriver({
      id: "sqlite-stable-order",
      name: "sqlite-stable-order",
      type: "sqlite",
      filePath: join(directory, "db.sqlite"),
    });
    drivers.push(driver);
    await driver.connect();
    await driver.query("CREATE TABLE items (name TEXT, label TEXT)");
    await driver.query("CREATE TABLE shadowed (rowid TEXT, name TEXT)");
    await driver.query("CREATE VIEW items_view AS SELECT name FROM items");
    for (let index = 0; index < 9; index += 1) {
      await driver.query("INSERT INTO items (name, label) VALUES (?, ?)", [
        "same",
        `row-${index}`,
      ]);
    }

    const itemColumns = await driver.describeColumns("main", "main", "items");
    await expect(
      driver.getTableRowIdentityOrderExpressions(
        "main",
        "main",
        "items",
        itemColumns,
      ),
    ).resolves.toEqual(["rowid"]);
    await expect(
      driver.getTableRowIdentityOrderExpressions(
        "main",
        "main",
        "shadowed",
        await driver.describeColumns("main", "main", "shadowed"),
      ),
    ).resolves.toEqual(["_rowid_"]);
    await expect(
      driver.getTableRowIdentityOrderExpressions(
        "main",
        "main",
        "items_view",
        await driver.describeColumns("main", "main", "items_view"),
      ),
    ).resolves.toEqual([]);

    const reader = service(driver);
    const labels: unknown[] = [];
    for (let page = 1; page <= 3; page += 1) {
      const result = await reader.getPage(
        "c1",
        "main",
        "main",
        "items",
        page,
        3,
        [],
        { column: "name", direction: "asc" },
        true,
      );
      labels.push(...result.rows.map((row) => row.label));
    }
    expect(labels).toEqual(
      Array.from({ length: 9 }, (_, index) => `row-${index}`),
    );
  });

  it("uses tableoid and ctid only for PostgreSQL relations that have them", async () => {
    const driver = new PostgresDriver({
      id: "pg",
      name: "pg",
      type: "pg",
      host: "localhost",
      database: "db",
    });
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ relkind: "p" }] })
      .mockResolvedValueOnce({ rows: [{ relkind: "v" }] });
    Object.assign(driver, { requirePool: () => ({ query }) });

    await expect(
      driver.getTableRowIdentityOrderExpressions("db", "public", "t"),
    ).resolves.toEqual(["tableoid", "ctid"]);
    await expect(
      driver.getTableRowIdentityOrderExpressions("db", "public", "v"),
    ).resolves.toEqual([]);
    expect(query).toHaveBeenCalledWith(expect.stringContaining("relkind"), [
      "public",
      "t",
    ]);
  });

  it("uses ROWID only for Oracle heap tables", async () => {
    const driver = new OracleDriver({
      id: "oracle",
      name: "oracle",
      type: "oracle",
      host: "localhost",
      database: "db",
    });
    const close = vi.fn();
    const execute = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ CNT: 1 }] })
      .mockResolvedValueOnce({ rows: [{ CNT: 0 }] });
    Object.assign(driver, {
      getConnection: async () => ({ execute, close }),
    });

    await expect(
      driver.getTableRowIdentityOrderExpressions("db", "HR", "T"),
    ).resolves.toEqual(["ROWID"]);
    await expect(
      driver.getTableRowIdentityOrderExpressions("db", "HR", "V"),
    ).resolves.toEqual([]);
    expect(close).toHaveBeenCalledTimes(2);
  });

  it.each([
    {
      indexes: [],
      sorted: "ORDER BY `db`.`t`.`name` DESC, `db`.`t`.`label` ASC LIMIT",
      unsorted: "ORDER BY `db`.`t`.`name` ASC, `db`.`t`.`label` ASC LIMIT",
    },
    {
      indexes: [
        { name: "nullable", columns: ["name"], unique: true, primary: false },
        { name: "code", columns: ["code"], unique: true, primary: false },
      ],
      sorted: "ORDER BY `db`.`t`.`name` DESC, `db`.`t`.`code` ASC LIMIT",
      unsorted: "ORDER BY `db`.`t`.`code` ASC LIMIT",
    },
  ])("adds a MySQL tie-breaker without a primary key (unique indexes: $indexes.length)", async ({
    indexes,
    sorted,
    unsorted,
  }) => {
    const driver = new MySQLDriver({
      id: "mysql",
      name: "mysql",
      type: "mysql",
      host: "localhost",
      database: "db",
    });
    const columns = [
      column("name", { nativeType: "varchar(50)" }),
      column("label", { nativeType: "varchar(50)" }),
      column("body", { nativeType: "longtext" }),
      column("doc", { category: "json", nativeType: "json" }),
      column("shape", { category: "spatial", nativeType: "geometry" }),
      ...(indexes.length > 0 ? [column("code", { nullable: false })] : []),
    ].filter((entry) => indexes.length === 0 || entry.name !== "label");
    const query = vi.fn(async (_sql: string) => ({ columns: [], rows: [] }));
    Object.assign(driver, {
      describeColumns: async () => columns,
      getIndexes: async () => indexes,
      query,
    });

    for (const sort of [{ column: "name", direction: "desc" as const }, null]) {
      await driver.readTablePage({
        database: "db",
        schema: "",
        table: "t",
        page: 1,
        pageSize: 10,
        filters: [],
        sort,
        skipCount: true,
      });
    }

    const [sortedSql, defaultSql] = query.mock.calls.map(([sql]) => sql);
    expect(sortedSql).toContain(sorted);
    expect(defaultSql).toContain(unsorted);
  });

  it("appends MSSQL comparable columns after a non-key sort column once", async () => {
    const driver = new MSSQLDriver({
      id: "mssql",
      name: "mssql",
      type: "mssql",
      host: "localhost",
      database: "db",
    });
    const columns = [
      column("name", { nativeType: "nvarchar(50)" }),
      column("notes", { nativeType: "ntext" }),
      column("amount", { category: "decimal", nativeType: "decimal(10,2)" }),
    ];
    const query = vi.fn(async (_sql: string) => ({ columns: [], rows: [] }));
    Object.assign(driver, { describeColumns: async () => columns, query });

    await service(driver).getPage(
      "c1",
      "db",
      "dbo",
      "t",
      1,
      10,
      [],
      { column: "name", direction: "asc" },
      true,
    );

    expect(query.mock.calls[0][0]).toContain(
      "ORDER BY [db].[dbo].[t].[name] ASC, [db].[dbo].[t].[amount] ASC OFFSET",
    );
  });
});
