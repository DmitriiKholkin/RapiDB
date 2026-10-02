import type { Pool, PoolClient } from "pg";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { rowsFromQuery } from "../../support/liveDbHarness";

describe("B08 PostgreSQL reconstructed DDL (live)", () => {
  let driver: PostgresDriver;
  let database: string;
  const schema = `b08_${process.pid}_${Date.now().toString(36)}" schema`;
  const copySchema = `${schema} copy`;
  const table = 'Child " exact';
  const copy = 'Copy " exact';
  const q = (name: string) => `"${name.replaceAll('"', '""')}"`;
  const qualified = (name: string) => `${q(schema)}.${q(name)}`;
  const qualifiedCopy = `${q(copySchema)}.${q(copy)}`;

  beforeAll(async () => {
    const config = await resolveConnectionSeed("postgres");
    database = config.database ?? "";
    driver = new PostgresDriver(config);
    await driver.connect();
    await driver.query(`CREATE SCHEMA ${q(schema)}`);
    await driver.query(`CREATE SCHEMA ${q(copySchema)}`);
    await driver.query(`CREATE TABLE ${qualified('Parent " exact')} (
      "a" int, "b" int, CONSTRAINT "Parent pk" PRIMARY KEY ("b", "a"))`);
    await driver.query(`CREATE TABLE ${qualified(table)} (
      "id" int, "part" int, "parent a" int, "parent b" int, "score" int NOT NULL,
      CONSTRAINT "PK "" exact" PRIMARY KEY ("part", "id"),
      CONSTRAINT "CHECK "" exact" CHECK ("score" > 0),
      CONSTRAINT "UNIQUE "" exact" UNIQUE ("id", "score") DEFERRABLE INITIALLY IMMEDIATE,
      CONSTRAINT "FK "" exact" FOREIGN KEY ("parent b", "parent a")
        REFERENCES ${qualified('Parent " exact')} ("b", "a") MATCH FULL ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED)`);
    await driver.query(
      `INSERT INTO ${qualified('Parent " exact')} VALUES (10, 20)`,
    );
  });

  afterAll(async () => {
    if (!driver) return;
    try {
      await driver.query(`DROP SCHEMA IF EXISTS ${q(copySchema)} CASCADE`);
      await driver.query(`DROP SCHEMA IF EXISTS ${q(schema)} CASCADE`);
    } finally {
      await driver.disconnect();
    }
  });

  it("restores named CHECK, UNIQUE, composite FK and ordered PK, and enforces each", async () => {
    const ddl = await driver.getCreateTableDDL(database, schema, table);
    expect(ddl).toContain("Not a pg_dump schema backup");
    expect(ddl.match(/PRIMARY KEY/g)).toHaveLength(1);
    expect(ddl).toContain('CONSTRAINT "PK "" exact" PRIMARY KEY (part, id)');
    expect(ddl).toContain(`REFERENCES ${qualified('Parent " exact')}`);
    const declaration = `CREATE TABLE ${qualified(table)}`;
    expect(ddl).toContain(declaration);
    await driver.query(
      ddl.replace(declaration, `CREATE TABLE ${qualifiedCopy}`),
    );
    const constraints = async (name: string) =>
      rowsFromQuery(
        await driver.query(
          `SELECT con.conname, con.contype, pg_get_constraintdef(con.oid, false) AS definition
       FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2 ORDER BY con.conname`,
          [name === copy ? copySchema : schema, name],
        ),
      );
    const original = await constraints(table);
    expect(original.map((row) => row.contype).sort()).toEqual([
      "c",
      "f",
      "p",
      "u",
    ]);
    expect(await constraints(copy)).toEqual(original);
    await driver.query(`INSERT INTO ${qualifiedCopy} VALUES (1, 1, 10, 20, 5)`);
    for (const [values, constraint] of [
      ["2, 2, 10, 20, -1", 'CHECK " exact'],
      ["1, 2, 10, 20, 5", 'UNIQUE " exact'],
      ["2, 2, 99, 99, 5", 'FK " exact'],
      ["1, 1, 10, 20, 6", 'PK " exact'],
    ]) {
      await expect(
        driver.query(`INSERT INTO ${qualifiedCopy} VALUES (${values})`),
      ).rejects.toMatchObject({ constraint });
    }
  });

  it.each([
    "VIEW",
    "MATERIALIZED VIEW",
  ])("quotes exact %s names while keeping native definitions", async (kind) => {
    const name = `${kind} " exact`;
    await driver.query(
      `CREATE ${kind} ${qualified(name)} AS SELECT "a" FROM ${qualified('Parent " exact')}`,
    );
    const ddl = await driver.getCreateTableDDL(database, schema, name);
    expect(ddl).toContain(qualified(name));
    expect(ddl).toContain('"Parent "" exact"');
    await driver.query(`DROP ${kind} ${qualified(name)}`);
    await driver.query(ddl);
    expect(
      rowsFromQuery(await driver.query(`SELECT * FROM ${qualified(name)}`)),
    ).toEqual([{ a: 10 }]);
  });

  it("restores an exclusion constraint and single named PK without leaking search_path", async () => {
    const name = "ranges";
    await driver.query(`CREATE TABLE ${qualified(name)} (
      id int CONSTRAINT single_pk PRIMARY KEY, span int4range,
      CONSTRAINT no_overlap EXCLUDE USING gist (span WITH &&))`);
    const [before] = rowsFromQuery(await driver.query("SHOW search_path"));
    const ddl = await driver.getCreateTableDDL(database, schema, name);
    const [after] = rowsFromQuery(await driver.query("SHOW search_path"));
    expect(after).toEqual(before);
    expect(ddl.match(/PRIMARY KEY/g)).toHaveLength(1);
    expect(ddl).toContain('CONSTRAINT "single_pk" PRIMARY KEY (id)');
    expect(ddl).toContain('CONSTRAINT "no_overlap" EXCLUDE USING gist');
    const restored = `${q(copySchema)}.${q(name)}`;
    await driver.query(
      ddl.replace(
        `CREATE TABLE ${qualified(name)}`,
        `CREATE TABLE ${restored}`,
      ),
    );
    await driver.query(`INSERT INTO ${restored} VALUES (1, '[1,5)')`);
    await expect(
      driver.query(`INSERT INTO ${restored} VALUES (2, '[3,7)')`),
    ).rejects.toMatchObject({ constraint: "no_overlap" });
  });

  it.each([
    "success",
    "catalog query error",
  ])("preserves editor BEGIN/INSERT/UPDATE through DDL %s and COMMIT", async (outcome) => {
    const name = outcome === "success" ? "editor_transaction" : "editor_error";
    await driver.query(
      `CREATE TABLE ${qualified(name)} (id int PRIMARY KEY, value int)`,
    );
    await driver.query(`INSERT INTO ${qualified(name)} VALUES (1, 10)`);
    const config = await resolveConnectionSeed("postgres");
    const editor = new PostgresDriver(config);
    const verifier = new PostgresDriver(config);
    await editor.connect();
    const catalogPids: unknown[] = [];
    const internals = editor as unknown as {
      createPool(
        database: string,
        max?: number,
        applicationName?: string,
      ): Pool;
    };
    const createPool = internals.createPool.bind(editor);
    const poolSpy = vi
      .spyOn(internals, "createPool")
      .mockImplementation((...args) => {
        const pool = createPool(...args);
        const connect = pool.connect.bind(pool);
        vi.spyOn(
          pool as unknown as { connect(): Promise<PoolClient> },
          "connect",
        ).mockImplementation(async () => {
          const client = await connect();
          const [backend] = (
            await client.query("SELECT pg_backend_pid() AS pid")
          ).rows;
          catalogPids.push(backend.pid);
          if (outcome !== "success") {
            const query = client.query.bind(client);
            vi.spyOn(client, "query").mockImplementation(((
              sql: unknown,
              ...params: unknown[]
            ) => {
              // Inject a real server-side catalog SQL error, inside the owned
              // read transaction; don't just reject a mock promise.
              if (
                typeof sql === "string" &&
                sql.includes("FROM pg_attribute a")
              )
                return query("SELECT 1/0");
              return Reflect.apply(query, client, [sql, ...params]);
            }) as PoolClient["query"]);
          }
          return client;
        });
        return pool;
      });
    try {
      await editor.query("BEGIN");
      await editor.query("SET LOCAL search_path TO public, pg_catalog");
      await editor.query(`INSERT INTO ${qualified(name)} VALUES (2, 20)`);
      await editor.query(
        `UPDATE ${qualified(name)} SET value = 11 WHERE id = 1`,
      );
      const state = async () =>
        rowsFromQuery(
          await editor.query(
            `SELECT pg_backend_pid() AS pid, current_setting('search_path') AS path, id, value FROM ${qualified(name)} ORDER BY id`,
          ),
        );
      const before = await state();
      expect(before.map(({ id, value }) => ({ id, value }))).toEqual([
        { id: 1, value: 11 },
        { id: 2, value: 20 },
      ]);
      expect(new Set(before.map((row) => row.pid)).size).toBe(1);
      if (outcome === "success")
        await editor.getCreateTableDDL(database, schema, name);
      else
        await expect(
          editor.getCreateTableDDL(database, schema, name),
        ).rejects.toThrow("division by zero");
      expect(catalogPids).toHaveLength(1);
      expect(catalogPids[0]).not.toBe(before[0].pid);
      // A fresh editor pool has exactly one physical backend. Check its PID
      // as well as pending data, rather than trusting pool transaction affinity.
      expect(await state()).toEqual(before);
      await expect
        .poll(async () =>
          rowsFromQuery(
            await driver.query(
              "SELECT pid FROM pg_stat_activity WHERE application_name = 'RapiDB catalog DDL' AND datname = current_database()",
            ),
          ),
        )
        .toEqual([]);
      await editor.query("COMMIT");
      await verifier.connect();
      expect(
        rowsFromQuery(
          await verifier.query(
            `SELECT id, value FROM ${qualified(name)} ORDER BY id`,
          ),
        ),
      ).toEqual([
        { id: 1, value: 11 },
        { id: 2, value: 20 },
      ]);
    } finally {
      poolSpy.mockRestore();
      await editor.query("ROLLBACK").catch(() => undefined);
      await Promise.all([editor.disconnect(), verifier.disconnect()]);
    }
  });
});
