import { afterEach, describe, expect, it } from "vitest";
import { isMutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import {
  SQLiteCoreDriver,
  splitSQLiteScript,
} from "../../src/extension/dbDrivers/sqliteCore";

let driver: SQLiteCoreDriver | undefined;

afterEach(async () => {
  await driver?.disconnect();
  driver = undefined;
});

async function open() {
  driver = new SQLiteCoreDriver({
    id: "sqlite-trigger-scripts",
    name: "SQLite trigger scripts",
    type: "sqlite",
    filePath: ":memory:",
  });
  await driver.connect();
  return driver;
}

describe("splitSQLiteScript trigger scanning", () => {
  it.each([
    [
      "CREATE TRIGGER t1 AFTER INSERT ON a BEGIN UPDATE a SET x = 1; END; SELECT 1",
      [
        "CREATE TRIGGER t1 AFTER INSERT ON a BEGIN UPDATE a SET x = 1; END",
        "SELECT 1",
      ],
    ],
    [
      "create temp trigger if not exists t2 before delete on a begin delete from b; insert into c values(1); end ; select 2;",
      [
        "create temp trigger if not exists t2 before delete on a begin delete from b; insert into c values(1); end",
        "select 2",
      ],
    ],
    [
      "CREATE TEMPORARY TRIGGER t3 INSERT ON a BEGIN SELECT CASE WHEN new.x THEN 1 ELSE CASE 2 WHEN 2 THEN 3 END END; END; SELECT 3",
      [
        "CREATE TEMPORARY TRIGGER t3 INSERT ON a BEGIN SELECT CASE WHEN new.x THEN 1 ELSE CASE 2 WHEN 2 THEN 3 END END; END",
        "SELECT 3",
      ],
    ],
    [
      "CREATE TRIGGER t4 INSERT ON a WHEN (SELECT CASE WHEN 1 THEN 1 END) BEGIN SELECT 'END;'; SELECT \"BEGIN\"; END; SELECT 4",
      [
        "CREATE TRIGGER t4 INSERT ON a WHEN (SELECT CASE WHEN 1 THEN 1 END) BEGIN SELECT 'END;'; SELECT \"BEGIN\"; END",
        "SELECT 4",
      ],
    ],
    [
      "CREATE TRIGGER begin UPDATE OF begin, end ON begin BEGIN SELECT new.end, new.case; END; SELECT 5",
      [
        "CREATE TRIGGER begin UPDATE OF begin, end ON begin BEGIN SELECT new.end, new.case; END",
        "SELECT 5",
      ],
    ],
    [
      "CREATE TRIGGER t6 INSERT ON a BEGIN -- END;\n SELECT 1; /* END; */ END; -- tail\nSELECT 6",
      [
        "CREATE TRIGGER t6 INSERT ON a BEGIN -- END;\n SELECT 1; /* END; */ END",
        "SELECT 6",
      ],
    ],
    [
      "CREATE TRIGGER t8 BEFORE UPDATE ON a WHEN new.begin > 0 AND begin_flag BEGIN UPDATE a SET end = 1; INSERT INTO a(begin, end) VALUES (CASE WHEN new.end THEN CASE new.begin WHEN 1 THEN 2 END END, 0); END; SELECT 8",
      [
        "CREATE TRIGGER t8 BEFORE UPDATE ON a WHEN new.begin > 0 AND begin_flag BEGIN UPDATE a SET end = 1; INSERT INTO a(begin, end) VALUES (CASE WHEN new.end THEN CASE new.begin WHEN 1 THEN 2 END END, 0); END",
        "SELECT 8",
      ],
    ],
    [
      "EXPLAIN QUERY PLAN SELECT 1; CREATE TRIGGER t9 DELETE ON a BEGIN DELETE FROM b WHERE end = old.end; END; SELECT 9",
      [
        "EXPLAIN QUERY PLAN SELECT 1",
        "CREATE TRIGGER t9 DELETE ON a BEGIN DELETE FROM b WHERE end = old.end; END",
        "SELECT 9",
      ],
    ],
    [
      "SELECT 'CREATE TRIGGER x BEGIN'; CREATE TABLE trigger_like(begin, end); SELECT 7",
      [
        "SELECT 'CREATE TRIGGER x BEGIN'",
        "CREATE TABLE trigger_like(begin, end)",
        "SELECT 7",
      ],
    ],
  ])("splits %j", (sql, expected) => {
    expect(splitSQLiteScript(sql)).toEqual(expected);
  });
});

describe("SQLite scripts containing CREATE TRIGGER", () => {
  it("returns SELECT results after a trigger definition in the same script", async () => {
    const db = await open();
    await db.query(
      "CREATE TABLE trg_rs_items(id INTEGER PRIMARY KEY, qty INTEGER); CREATE TABLE trg_rs_log(item_id INTEGER, note TEXT)",
    );

    const result = await db.query(`
      CREATE TRIGGER trg_rs_after_insert AFTER INSERT ON trg_rs_items
      BEGIN
        INSERT INTO trg_rs_log VALUES (new.id, 'semi;colon END; BEGIN');
        UPDATE trg_rs_items SET qty = qty + 1 WHERE id = new.id;
      END;
      INSERT INTO trg_rs_items VALUES (1, 10);
      SELECT id, qty FROM trg_rs_items;
      SELECT item_id, note FROM trg_rs_log;
    `);

    expect(result.columns).toEqual(["id", "qty"]);
    expect(result.rows).toEqual([{ __col_0: 1, __col_1: 11 }]);
    expect(result.resultSets?.map((set) => set.rows)).toEqual([
      [{ __col_0: 1, __col_1: 11 }],
      [{ __col_0: 1, __col_1: "semi;colon END; BEGIN" }],
    ]);
  });

  it("keeps CASE ... END, comments and RAISE inside a TEMP IF NOT EXISTS trigger body", async () => {
    const db = await open();
    await db.query("CREATE TABLE trg_case_items(id INTEGER, label TEXT)");

    const result = await db.query(`
      CREATE TEMP TRIGGER IF NOT EXISTS trg_case_guard
      BEFORE INSERT ON trg_case_items
      FOR EACH ROW WHEN (CASE WHEN new.id < 0 THEN 1 ELSE 0 END) = 0
      BEGIN
        -- END; inside a line comment
        /* BEGIN ... END; inside a block comment */
        SELECT CASE
          WHEN new.label IS NULL THEN RAISE(ABORT, 'label END; required')
          ELSE CASE WHEN new.label = '' THEN RAISE(FAIL, 'empty') END
        END;
      END;
      INSERT INTO trg_case_items VALUES (1, 'one');
      SELECT count(*) AS total FROM trg_case_items;
    `);

    expect(result.columns).toEqual(["total"]);
    expect(result.rows).toEqual([{ __col_0: 1 }]);

    const stored = await db.query(
      "SELECT sql FROM sqlite_temp_master WHERE name = 'trg_case_guard'",
    );
    expect(String(stored.rows[0]?.__col_0)).toContain(
      "-- END; inside a line comment",
    );
    expect(String(stored.rows[0]?.__col_0)).toMatch(
      /RAISE\(FAIL, 'empty'\) END\s+END;\s+END$/,
    );

    await expect(
      db.query("INSERT INTO trg_case_items VALUES (2, NULL)"),
    ).rejects.toThrow(/label END; required/);
  });

  it("keeps unquoted begin/end column names and nested CASE inside one trigger statement", async () => {
    const db = await open();
    await db.query(
      "CREATE TABLE trg_kw_items(id INTEGER, begin INTEGER, end INTEGER); CREATE TABLE trg_kw_audit(begin INTEGER, end TEXT)",
    );

    const triggerSql = `CREATE TRIGGER trg_kw_update AFTER UPDATE ON trg_kw_items
      WHEN new.begin > 0 AND old.begin IS NOT new.begin
      BEGIN
        UPDATE trg_kw_items SET end = new.begin * 10 WHERE id = new.id;
        INSERT INTO trg_kw_audit(begin, end) VALUES (
          new.begin,
          CASE WHEN new.begin > 1 THEN CASE new.begin WHEN 2 THEN 'two' ELSE 'many' END ELSE 'one' END
        );
      END`;
    const result = await db.query(`
      ${triggerSql};
      INSERT INTO trg_kw_items(id, begin, end) VALUES (1, 0, 0);
      UPDATE trg_kw_items SET begin = 2 WHERE id = 1;
      SELECT id, begin, end FROM trg_kw_items;
      SELECT begin, end FROM trg_kw_audit;
    `);

    expect(result.resultSets?.map((set) => set.rows)).toEqual([
      [{ __col_0: 1, __col_1: 2, __col_2: 20 }],
      [{ __col_0: 2, __col_1: "two" }],
    ]);
    const stored = await db.query(
      "SELECT sql FROM sqlite_master WHERE name = 'trg_kw_update'",
    );
    expect(stored.rows[0]?.__col_0).toBe(triggerSql);
  });

  it("rolls back the whole script when a trigger RAISE aborts a later statement", async () => {
    const db = await open();
    await db.query("CREATE TABLE trg_raise_items(id INTEGER)");

    const error = await db
      .query(`
        INSERT INTO trg_raise_items VALUES (1);
        CREATE TRIGGER trg_raise_block BEFORE INSERT ON trg_raise_items
        WHEN new.id > 1 BEGIN SELECT RAISE(ABORT, 'blocked by trigger'); END;
        INSERT INTO trg_raise_items VALUES (2);
        SELECT * FROM trg_raise_items;
      `)
      .catch((caught: unknown) => caught);

    expect(isMutationNotExecutedError(error)).toBe(true);
    expect(String((error as Error).message)).toMatch(/blocked by trigger/);
    const rows = await db.query("SELECT count(*) FROM trg_raise_items");
    expect(rows.rows[0]?.__col_0).toBe(0);
    const triggers = await db.query(
      "SELECT count(*) FROM sqlite_master WHERE name = 'trg_raise_block'",
    );
    expect(triggers.rows[0]?.__col_0).toBe(0);
  });

  it("returns bounded result sets for trigger scripts when a hard cap is set", async () => {
    const db = await open();
    await db.query("CREATE TABLE trg_cap_items(id INTEGER)");

    const result = await db.query(
      `CREATE TRIGGER "trg begin" AFTER INSERT ON trg_cap_items BEGIN SELECT 1; END;
       INSERT INTO trg_cap_items VALUES (1), (2), (3);
       SELECT id FROM trg_cap_items ORDER BY id`,
      [],
      { hardCap: 2 },
    );

    expect(result.rows).toEqual([{ __col_0: 1 }, { __col_0: 2 }]);
    expect(result.truncated).toBe(true);
  });

  it("returns trigger script result sets through the SQLite worker", async () => {
    const workerDriver = new SQLiteDriver({
      id: "sqlite-trigger-scripts-worker",
      name: "SQLite trigger scripts worker",
      type: "sqlite",
      filePath: ":memory:",
    });
    await workerDriver.connect();
    try {
      const result = await workerDriver.query(`
        CREATE TABLE trg_worker_items(id INTEGER, seen INTEGER DEFAULT 0);
        CREATE TRIGGER trg_worker_seen AFTER INSERT ON trg_worker_items
        BEGIN UPDATE trg_worker_items SET seen = 1 WHERE id = new.id; END;
        INSERT INTO trg_worker_items(id) VALUES (7);
        SELECT id, seen FROM trg_worker_items;
      `);
      expect(result.rows).toEqual([{ __col_0: 7, __col_1: 1 }]);
    } finally {
      await workerDriver.disconnect();
    }
  });
});
