import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { rowsFromQuery } from "../../support/liveDbHarness";

describe("PostgreSQL JSON/Array precision (live)", () => {
  let driver: PostgresDriver;
  let database: string;
  let schema: string;
  let table: string;
  let amountColumn: ColumnTypeMeta;
  let payloadColumn: ColumnTypeMeta;

  beforeAll(async () => {
    const connection = (await resolveConnectionSeed(
      "postgres",
    )) as ConnectionConfig;
    driver = new PostgresDriver(connection);
    await driver.connect();
    const listed = await driver.listDatabases();
    const target = listed[0];
    database = target?.name ?? connection.database ?? "postgres";
    schema = "public";
    table = `rapidb_json_precision_${Date.now().toString(36)}`;
    const qualified = `${driver.quoteIdentifier(schema)}.${driver.quoteIdentifier(table)}`;
    await driver.query(
      `CREATE TABLE ${qualified} (
        id INTEGER PRIMARY KEY,
        payload JSONB NOT NULL,
        amount NUMERIC[] NOT NULL
      )`,
    );
    const columns = await driver.describeColumns(database, schema, table);
    amountColumn = columns.find(
      (column) => column.name === "amount",
    ) as ColumnTypeMeta;
    payloadColumn = columns.find(
      (column) => column.name === "payload",
    ) as ColumnTypeMeta;
    expect(amountColumn).toBeDefined();
    expect(payloadColumn).toBeDefined();
  }, 60_000);

  afterAll(async () => {
    if (!driver) {
      return;
    }
    try {
      const qualified = `${driver.quoteIdentifier(schema)}.${driver.quoteIdentifier(table)}`;
      await driver.query(`DROP TABLE IF EXISTS ${qualified}`);
    } finally {
      await driver.disconnect();
    }
  }, 60_000);

  it("preserves the textual representation of a JSON numeric", async () => {
    const qualified = `${driver.quoteIdentifier(schema)}.${driver.quoteIdentifier(table)}`;
    await driver.query(
      `INSERT INTO ${qualified} (id, payload, amount) VALUES ($1, $2::json, $3)`,
      [1, '{"loan": 13000.0, "rate": 0.95}', "{13000.0,0.95}"],
    );

    const result = await driver.query(
      `SELECT payload, amount FROM ${qualified} WHERE id = 1`,
    );
    const [row] = rowsFromQuery(result);

    const payloadDisplay = driver.formatOutputValue(
      row?.payload,
      payloadColumn,
    );
    const amountDisplay = driver.formatOutputValue(row?.amount, amountColumn);

    expect(typeof payloadDisplay).toBe("string");
    expect(payloadDisplay).toBe('{"loan": 13000.0, "rate": 0.95}');
    expect(typeof amountDisplay).toBe("string");
    expect(amountDisplay).toBe("[13000.0,0.95]");
  });

  it("round-trips a JSON edit through coerceInputValue", async () => {
    const qualified = `${driver.quoteIdentifier(schema)}.${driver.quoteIdentifier(table)}`;
    await driver.query(
      `INSERT INTO ${qualified} (id, payload, amount) VALUES ($1, $2::json, $3)`,
      [2, '{"loan": 0.0, "rate": 0.0}', "{0.0,0.0}"],
    );
    const updated = '{"loan": 9999.0, "rate": 1.25}';
    const coerced = driver.coerceInputValue(updated, payloadColumn);
    expect(coerced).toBe(updated);

    await driver.query(
      `UPDATE ${qualified} SET payload = $1::json WHERE id = 2`,
      [coerced],
    );

    const result = await driver.query(
      `SELECT payload FROM ${qualified} WHERE id = 2`,
    );
    const [row] = rowsFromQuery(result);
    const display = driver.formatOutputValue(row?.payload, payloadColumn);
    expect(display).toBe(updated);
  });

  it("round-trips a numeric array edit through coerceInputValue", async () => {
    const qualified = `${driver.quoteIdentifier(schema)}.${driver.quoteIdentifier(table)}`;
    await driver.query(
      `INSERT INTO ${qualified} (id, payload, amount) VALUES ($1, $2::json, $3)`,
      [3, '{"loan": 0.0, "rate": 0.0}', "{0.0,0.0}"],
    );
    const edited = "[42.0,17.5]";
    const coerced = driver.coerceInputValue(edited, amountColumn);
    expect(coerced).toBe("{42.0,17.5}");

    await driver.query(
      `UPDATE ${qualified} SET amount = $1::numeric[] WHERE id = 3`,
      [coerced],
    );

    const result = await driver.query(
      `SELECT amount FROM ${qualified} WHERE id = 3`,
    );
    const [row] = rowsFromQuery(result);
    const display = driver.formatOutputValue(row?.amount, amountColumn);
    expect(display).toBe(edited);
  });

  it("preserves the trailing zero in a JSON equality filter", async () => {
    const qualified = `${driver.quoteIdentifier(schema)}.${driver.quoteIdentifier(table)}`;
    const condition = driver.buildFilterCondition(
      payloadColumn,
      "eq",
      '{"loan": 13000.0}',
      1,
    );
    expect(condition).toEqual({
      sql: `("payload")::jsonb = $1::jsonb`,
      params: ['{"loan": 13000.0}'],
    });

    await driver.query(
      `INSERT INTO ${qualified} (id, payload, amount) VALUES ($1, $2::json, $3)`,
      [10, '{"loan": 13000.0}', "{13000.0}"],
    );
    const result = await driver.query(
      `SELECT id FROM ${qualified} WHERE ${condition?.sql ?? ""}`,
      condition?.params ?? [],
    );
    const [row] = rowsFromQuery(result);
    expect(row?.id).toBe(10);
  });
});
