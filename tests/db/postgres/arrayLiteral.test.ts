import type { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import { applyChangesTransactional } from "../../../src/extension/table/tableMutationExecution";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { jsonArrayLiteralToPgArrayLiteral } from "../../../src/extension/utils/postgresArrayLiteral";
import { parsePostgresArrayLiteral } from "../../../src/extension/utils/postgresArrayParser";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { rowsFromQuery } from "../../support/liveDbHarness";

describe("B03 PostgreSQL array literals (live)", () => {
  let config: ConnectionConfig;
  let driver: PostgresDriver;
  let read: TableReadService;
  const schema = `audit_b03_${process.pid}_${Date.now().toString(36)}`;
  const table = "array_elements";
  const qualified = `"${schema}"."${table}"`;
  const manager = { getDriver: () => driver, getConnection: () => config };

  beforeAll(async () => {
    config = await resolveConnectionSeed("postgres");
    driver = new PostgresDriver(config);
    await driver.connect();
    await driver.query(`CREATE SCHEMA "${schema}"`);
    await driver.query(
      `CREATE TABLE ${qualified}(id int PRIMARY KEY, a text[])`,
    );
    read = new TableReadService(manager as never);
  });

  afterAll(async () => {
    if (!driver) return;
    try {
      await driver.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    } finally {
      await driver.disconnect();
    }
  });

  const arrayColumn = async () => {
    const columns = await read.getColumns(
      config.id,
      config.database ?? "",
      schema,
      table,
    );
    const column = columns.find((entry) => entry.name === "a");
    if (!column) throw new Error("Missing array column");
    return column;
  };

  it("A: reads lower bounds and round-trips native dimension-prefixed literals", async () => {
    const [row] = rowsFromQuery(
      await driver.query(
        "SELECT array_fill(7,ARRAY[2],ARRAY[0]) AS a, array_fill(7,ARRAY[2],ARRAY[0])::text AS literal",
      ),
    );
    expect(row?.a).toEqual(["7", "7"]);
    expect(row?.literal).toBe("[0:1]={7,7}");
    const column = { ...(await arrayColumn()), nativeType: "integer[]" };
    expect(driver.formatOutputValue(row?.literal, column)).toBe("[7,7]");
    const [roundTrip] = rowsFromQuery(
      await driver.query(
        "SELECT $1::int[] AS a, array_dims($1::int[]) AS dims",
        [driver.coerceInputValue(row?.literal, column)],
      ),
    );
    expect(roundTrip?.a).toEqual(["7", "7"]);
    expect(roundTrip?.dims).toBe("[0:1]");
  });

  it("B: nested quoted braces, quotes, backslashes and NULL agree with server JSON", async () => {
    const expected = [
      ["}", '{"\\'],
      [null, "NULL"],
    ];
    const column = await arrayColumn();
    const [row] = rowsFromQuery(
      await driver.query(
        `SELECT a, array_to_json(a)::text AS control FROM (SELECT ARRAY[ARRAY['}', $1], ARRAY[NULL,'NULL']]::text[] AS a) s`,
        ['{"\\'],
      ),
    );
    expect(row?.a).toEqual(expected);
    expect(JSON.parse(String(row?.control))).toEqual(expected);
    const [roundTrip] = rowsFromQuery(
      await driver.query(
        "SELECT $1::text[] AS a, array_to_json($1::text[])::text AS control",
        [
          driver.coerceInputValue(
            driver.formatOutputValue(row?.a, column),
            column,
          ),
        ],
      ),
    );
    expect(roundTrip?.a).toEqual(expected);
    expect(JSON.parse(String(roundTrip?.control))).toEqual(expected);
  });

  it.each([
    [1, ["a\nb", "a\tb", "a\rb", "\b\f\v", String.raw`\n`]],
    [
      2,
      [
        ["}", '{"\\'],
        [null, "NULL"],
      ],
    ],
  ])("C/B: structured JSON apply and optimistic originals survive two edits, row %i", async (id, expected) => {
    await driver.query(`INSERT INTO ${qualified} VALUES ($1, ARRAY['old'])`, [
      id,
    ]);
    const database = config.database ?? "";
    const columns = await read.getColumns(config.id, database, schema, table);
    for (const values of [expected, expected]) {
      const page = await read.getPage(
        config.id,
        database,
        schema,
        table,
        1,
        10,
        [{ column: "id", operator: "eq", value: String(id) }],
      );
      const result = await applyChangesTransactional(
        manager as never,
        config.id,
        database,
        schema,
        table,
        [
          {
            primaryKeys: { id },
            changes: { a: JSON.stringify(values) },
            originalValues: { a: page.rows[0].a },
          },
        ],
        columns,
      );
      expect(result).toMatchObject({
        success: true,
        rowOutcomes: [{ status: "applied" }],
      });
      expect(result.warning).toBeUndefined();
      const [row] = rowsFromQuery(
        await driver.query(
          `SELECT a, array_to_json(a)::text AS control FROM ${qualified} WHERE id=$1`,
          [id],
        ),
      );
      expect(row?.a).toEqual(values);
      expect(JSON.parse(String(row?.control))).toEqual(values);
    }
    // The same optimistic check must still detect a genuine concurrent edit.
    const page = await read.getPage(config.id, database, schema, table, 1, 10, [
      { column: "id", operator: "eq", value: String(id) },
    ]);
    await driver.query(
      `UPDATE ${qualified} SET a=ARRAY['concurrent'] WHERE id=$1`,
      [id],
    );
    const conflict = await applyChangesTransactional(
      manager as never,
      config.id,
      database,
      schema,
      table,
      [
        {
          primaryKeys: { id },
          changes: { a: JSON.stringify(expected) },
          originalValues: { a: page.rows[0].a },
        },
      ],
      columns,
    );
    expect(conflict.success).toBe(false);
    expect(
      rowsFromQuery(
        await driver.query(`SELECT a FROM ${qualified} WHERE id=$1`, [id]),
      )[0]?.a,
    ).toEqual(["concurrent"]);
  });

  it("returns exact controls in read-only queries, exact numeric/bigint tokens and typed booleans", async () => {
    const readonly = new PostgresDriver({ ...config, readOnly: true });
    await readonly.connect();
    try {
      const controls = ["a\nb", "a\tb", "a\rb", String.raw`\n`];
      const [row] = rowsFromQuery(
        await readonly.query(
          "SELECT $1::text[] AS controls, ARRAY[9223372036854775807,-9223372036854775808]::bigint[] AS big, ARRAY[12345678901234567890.1234567890,13000.0]::numeric[] AS nums, ARRAY[true,false,NULL] AS bools, ARRAY['t','f',NULL]::text[] AS texts",
          [controls],
        ),
      );
      expect(row?.controls).toEqual(controls);
      expect(row?.big).toEqual(["9223372036854775807", "-9223372036854775808"]);
      expect(row?.nums).toEqual(["12345678901234567890.1234567890", "13000.0"]);
      expect(row?.bools).toEqual([true, false, null]);
      expect(row?.texts).toEqual(["t", "f", null]);
    } finally {
      await readonly.disconnect();
    }
  });

  it("round-trips numeric/bigint JSON edits without JS Number rounding", async () => {
    const column = await arrayColumn();
    const numbers = "[12345678901234567890.1234567890,13000.0]";
    const bigints = "[9223372036854775807,-9223372036854775808]";
    const numericColumn = { ...column, nativeType: "numeric[]" };
    const bigintColumn = { ...column, nativeType: "bigint[]" };
    const [row] = rowsFromQuery(
      await driver.query("SELECT $1::numeric[] AS nums, $2::bigint[] AS big", [
        driver.coerceInputValue(numbers, numericColumn),
        driver.coerceInputValue(bigints, bigintColumn),
      ]),
    );
    expect(driver.formatOutputValue(row?.nums, numericColumn)).toBe(numbers);
    expect(driver.formatOutputValue(row?.big, bigintColumn)).toBe(bigints);
  });

  it.each([
    ["[2147483647:2147483647]={x}", false],
    ["[2147483646:2147483647]={x,y}", false],
    ["[2147483646:2147483646]={x}", true],
    ["[-2147483648:-2147483648]={x}", true],
    ["[-2147483648:-2147483647]={x,y}", true],
    ["[-2147483649:-2147483649]={x}", false],
    ["[-2147483648:-1]={x}", false],
    ["[0 :1]={x,y}", false],
    ["[0: 1]={x,y}", false],
    ["[0:1 ]={x,y}", false],
    ["[ 0:1]={x,y}", false],
    [" \t[0:1] \n= \t{x,y} \n", true],
    ["[+0:+1]={x,y}", true],
  ])("bounds grammar agrees with PostgreSQL: %s (valid=%s)", async (literal, valid) => {
    // Only scalar text results: server validation is independent of our parser.
    const query = driver.query(
      "SELECT array_dims($1::text[]) AS dims, array_to_json($1::text[])::text AS elements",
      [literal],
    );
    if (valid) {
      const [row] = rowsFromQuery(await query);
      expect(parsePostgresArrayLiteral(literal)).toEqual(
        JSON.parse(String(row?.elements)),
      );
      expect(jsonArrayLiteralToPgArrayLiteral(literal)).toBe(literal.trim());
    } else {
      await expect(query).rejects.toThrow();
      expect(() => parsePostgresArrayLiteral(literal)).toThrow();
      expect(() => jsonArrayLiteralToPgArrayLiteral(literal)).toThrow();
    }
  });

  it("numeric-looking text originals support two edits and still reject concurrent changes", async () => {
    const id = 3;
    const initial = ["01", "-01", "-.5"];
    await driver.query(`INSERT INTO ${qualified} VALUES ($1, $2::text[])`, [
      id,
      initial,
    ]);
    const database = config.database ?? "";
    const columns = await read.getColumns(config.id, database, schema, table);
    for (const expected of [["fixed"], initial]) {
      const page = await read.getPage(
        config.id,
        database,
        schema,
        table,
        1,
        10,
        [{ column: "id", operator: "eq", value: String(id) }],
      );
      // Exercise the strict converter on actual page originals, before asserting
      // their display, so the regression reproduces the reported apply failure.
      const result = await applyChangesTransactional(
        manager as never,
        config.id,
        database,
        schema,
        table,
        [
          {
            primaryKeys: { id },
            changes: { a: JSON.stringify(expected) },
            originalValues: { a: page.rows[0].a },
          },
        ],
        columns,
      );
      expect(result).toMatchObject({
        success: true,
        rowOutcomes: [{ status: "applied" }],
      });
      expect(result.warning).toBeUndefined();
      const refreshed = await read.getPage(
        config.id,
        database,
        schema,
        table,
        1,
        10,
        [{ column: "id", operator: "eq", value: String(id) }],
      );
      expect(refreshed.rows[0].a).toBe(JSON.stringify(expected));
    }
    const page = await read.getPage(config.id, database, schema, table, 1, 10, [
      { column: "id", operator: "eq", value: String(id) },
    ]);
    await driver.query(
      `UPDATE ${qualified} SET a=ARRAY['concurrent'] WHERE id=$1`,
      [id],
    );
    const result = await applyChangesTransactional(
      manager as never,
      config.id,
      database,
      schema,
      table,
      [
        {
          primaryKeys: { id },
          changes: { a: '["fixed"]' },
          originalValues: { a: page.rows[0].a },
        },
      ],
      columns,
    );
    expect(result.success).toBe(false);
    expect(
      rowsFromQuery(
        await driver.query(`SELECT a FROM ${qualified} WHERE id=$1`, [id]),
      )[0]?.a,
    ).toEqual(["concurrent"]);
  });

  it("uses actual enum array metadata to retain numeric-looking labels as JSON strings", async () => {
    const enumType = `"${schema}"."numeric"`;
    await driver.query(
      `CREATE TYPE ${enumType} AS ENUM ('01','-01','-.5','t','f')`,
    );
    await driver.query(
      `CREATE TABLE "${schema}".enum_elements(id int PRIMARY KEY, a ${enumType}[])`,
    );
    await driver.query(
      `INSERT INTO "${schema}".enum_elements VALUES (1, ARRAY['01','-01','-.5','t','f']::${enumType}[])`,
    );
    const page = await read.getPage(
      config.id,
      config.database ?? "",
      schema,
      "enum_elements",
      1,
      10,
      [],
    );
    expect(
      page.columns.find((column) => column.name === "a")?.nativeType,
    ).toContain(schema);
    expect(page.rows[0].a).toBe('["01","-01","-.5","t","f"]');
  });

  it("identifies enum int8 arrays from the catalog even when search_path hides their schema", async () => {
    const enumType = `"${schema}".int8`;
    const enumTable = "enum_int8_elements";
    const qualifiedEnumTable = `"${schema}"."${enumTable}"`;
    const initial = ["01", "-01", "-.5"];
    await driver.query(
      `CREATE TYPE ${enumType} AS ENUM ('01','-01','-.5','fixed')`,
    );
    await driver.query(
      `CREATE TABLE ${qualifiedEnumTable}(id int PRIMARY KEY, a ${enumType}[], n numeric[], b pg_catalog.int8[])`,
    );
    await driver.query(
      `INSERT INTO ${qualifiedEnumTable} VALUES (1, $1::${enumType}[], '{12345678901234567890.1234567890,13000.0}', '{9223372036854775807,-9223372036854775808}')`,
      [initial],
    );
    const scopedDriver = new PostgresDriver(config);
    await scopedDriver.connect();
    try {
      const pool = (scopedDriver as unknown as { pool: Pool }).pool;
      // Initialize every physical connection (production pool max=5), so catalog,
      // page reads, UPDATE transaction and verification all use this search_path.
      // A dedicated driver is disconnected below; no session settings leak out.
      const clients = await Promise.all(
        Array.from({ length: 5 }, () => pool.connect()),
      );
      try {
        await Promise.all(
          clients.map(async (client) => {
            await client.query(`SET search_path TO "${schema}", pg_catalog`);
            const result = await client.query("SHOW search_path");
            expect(result.rows[0].search_path).toBe(`${schema}, pg_catalog`);
          }),
        );
      } finally {
        for (const client of clients) client.release();
      }
      // Show that format_type alone really loses the user type's namespace.
      const [catalog] = rowsFromQuery(
        await scopedDriver.query(
          "SELECT pg_catalog.format_type(a.atttypid,a.atttypmod) AS formatted FROM pg_catalog.pg_attribute a WHERE a.attrelid=$1::regclass AND a.attname='a'",
          [qualifiedEnumTable],
        ),
      );
      expect(catalog?.formatted).toBe("int8[]");
      const scopedManager = {
        getDriver: () => scopedDriver,
        getConnection: () => config,
      };
      const scopedRead = new TableReadService(scopedManager as never);
      const database = config.database ?? "";
      const columns = await scopedRead.getColumns(
        config.id,
        database,
        schema,
        enumTable,
      );
      const getPage = () =>
        scopedRead.getPage(config.id, database, schema, enumTable, 1, 10, []);
      const apply = (original: unknown, elements: string[]) =>
        applyChangesTransactional(
          scopedManager as never,
          config.id,
          database,
          schema,
          enumTable,
          [
            {
              primaryKeys: { id: 1 },
              changes: { a: JSON.stringify(elements) },
              originalValues: { a: original },
            },
          ],
          columns,
        );
      for (const elements of [["fixed"], initial]) {
        const page = await getPage();
        // Use actual originals before checking their display to reproduce the
        // strict-JSON regression, rather than failing only on metadata text.
        const result = await apply(page.rows[0].a, elements);
        expect(result).toMatchObject({
          success: true,
          rowOutcomes: [{ status: "applied" }],
        });
        expect(result.warning).toBeUndefined();
        const refreshed = await getPage();
        expect(refreshed.rows[0].a).toBe(JSON.stringify(elements));
        expect(refreshed.rows[0].n).toBe(
          "[12345678901234567890.1234567890,13000.0]",
        );
        expect(refreshed.rows[0].b).toBe(
          "[9223372036854775807,-9223372036854775808]",
        );
      }
      expect(columns.find((column) => column.name === "a")?.nativeType).toBe(
        `${schema}.int8[]`,
      );
      expect(columns.find((column) => column.name === "b")?.nativeType).toBe(
        "bigint[]",
      );
      const stale = await getPage();
      await scopedDriver.query(
        `UPDATE ${qualifiedEnumTable} SET a=ARRAY['fixed']::${enumType}[] WHERE id=1`,
      );
      const conflict = await apply(stale.rows[0].a, initial);
      expect(conflict.success).toBe(false);
      expect((await getPage()).rows[0].a).toBe('["fixed"]');
    } finally {
      await scopedDriver.disconnect();
    }
  });

  it("native escape/NULL grammar matches PostgreSQL, including multidimensional bounds", async () => {
    const literal = String.raw`[-1:0][0:3]={{null,"null",N\ULL,a\,b},{\{,\},"",a\ b}}`;
    const [row] = rowsFromQuery(
      await driver.query(
        "SELECT $1::text[] AS a, array_to_json($1::text[])::text AS control",
        [literal],
      ),
    );
    const expected = [
      [null, "null", "NULL", "a,b"],
      ["{", "}", "", "a b"],
    ];
    expect(row?.a).toEqual(expected);
    expect(JSON.parse(String(row?.control))).toEqual(expected);
  });

  it("writes object elements as proper JSON text with exact numbers and nested arrays", async () => {
    const input =
      '[{"n":12345678901234567890.00,"items":[null,{"s":"a\\nb"}]}]';
    const column = await arrayColumn();
    const [row] = rowsFromQuery(
      await driver.query("SELECT ($1::json[])[1]::text AS element", [
        driver.coerceInputValue(input, { ...column, nativeType: "json[]" }),
      ]),
    );
    expect(row?.element).toBe(input.slice(1, -1));
  });
});
