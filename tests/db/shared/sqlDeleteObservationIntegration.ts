import { randomUUID } from "node:crypto";
import { setTimeout as pause } from "node:timers/promises";
import oracledb from "oracledb";
import { expect, it } from "vitest";
import type { IDBDriver } from "../../../src/extension/dbDrivers/types";
import { TableDataService } from "../../../src/extension/tableDataService";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import type { ComposeBackedDbEngineId } from "../../contracts/testingContracts";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";
import { createDriver } from "../../support/liveDbHarness";

export function registerSqlDeleteObservationIntegration(
  engine: ComposeBackedDbEngineId,
) {
  const contents = {
    postgres: "note text, details json",
    mysql: "note varchar(25), details datetime(6)",
    mssql: "note nvarchar(25), details varbinary(10)",
    oracle: "note varchar2(25), details clob",
  }[engine];
  const initialDetails = {
    postgres: `'{"a": 1}'`,
    mysql: "'2024-01-01 00:00:00.123456'",
    mssql: "0x01",
    oracle: "TO_CLOB('original LOB')",
  }[engine];
  const changedDetails = {
    postgres: `'{"a": 2}'`,
    mysql: "'2024-01-01 00:00:00.123457'",
    mssql: "0x0100",
    oracle: "TO_CLOB('different LOB')",
  }[engine];

  async function withTable(
    definition: string,
    test: (fixture: {
      read: () => ReturnType<TableDataService["getPage"]>;
      prepare: (
        page: Awaited<ReturnType<TableDataService["getPage"]>>,
        indexes: number[],
      ) => ReturnType<TableDataService["prepareDeleteRowsPlan"]>;
      execute: TableDataService["executePreparedDeletePlan"];
      sql: (text: string) => ReturnType<IDBDriver["query"]>;
      config: ConnectionConfig;
      qualified: string;
    }) => Promise<void>,
  ) {
    const config = await resolveConnectionSeed(engine);
    // Only this regression uses a dynamic port for independent, disposable DBs.
    const isolatedPort = process.env.RAPIDB_TEST_SQL_DELETE_PORT;
    if (isolatedPort) config.port = Number(isolatedPort);
    const database = config.database ?? "";
    const schema =
      engine === "postgres"
        ? "public"
        : engine === "mssql"
          ? "dbo"
          : engine === "oracle"
            ? (config.username?.toUpperCase() ?? "")
            : "";
    const table =
      `h8_${randomUUID().replaceAll("-", "").slice(0, 18)}`.toUpperCase();
    const scope = [config.id, database, schema, table] as const;
    const driver = createDriver(engine, config);
    const external = createDriver(engine, config);
    const service = new TableDataService({
      getConnection: () => config,
      getDriver: () => driver,
    } as never);
    const qualified = driver.qualifiedTableName(database, schema, table);
    const sql = async (text: string) => {
      const result = await external.query(
        text.replaceAll("<table>", qualified),
      );
      expect(result.error).toBeUndefined();
      return result;
    };
    let created = false;
    try {
      await driver.connect();
      await external.connect();
      await sql(
        `CREATE TABLE <table> (tenant INT NOT NULL, id INT NOT NULL, ${definition},
         PRIMARY KEY (tenant,id))${engine === "mysql" ? " ENGINE=InnoDB" : ""}`,
      );
      created = true;
      const tenant = engine === "oracle" ? "TENANT" : "tenant";
      const id = engine === "oracle" ? "ID" : "id";
      await test({
        read: () =>
          service.getPage(...scope, 1, 25, [], null, false, undefined, true),
        prepare: (page, indexes) =>
          service.prepareDeleteRowsPlan(
            ...scope,
            indexes.map((index) => ({
              [tenant]: page.rows[index][tenant],
              [id]: page.rows[index][id],
            })),
            indexes.map((index) => page.sqlDeleteObservations?.[index] ?? null),
          ),
        execute: (plan) => service.executePreparedDeletePlan(plan),
        sql,
        config,
        qualified,
      });
    } finally {
      try {
        if (created) await sql("DROP TABLE <table>");
      } finally {
        await Promise.all([driver.disconnect(), external.disconnect()]);
      }
    }
  }

  it("rolls back the whole guarded delete if a second client changes a selected row after preview", async () => {
    await withTable(contents, async ({ read, prepare, execute, sql }) => {
      await sql("INSERT INTO <table> (tenant,id,note) VALUES (1,1,NULL)");
      await sql(
        `INSERT INTO <table> (tenant,id,note,details) VALUES (1,2,'same',${initialDetails})`,
      );
      const page = await read();
      expect(page.sqlDeleteObservations?.every(Boolean)).toBe(true);
      const plan = await prepare(page, [0, 1]);
      if (!plan) throw new Error("Missing guarded delete preview");
      await sql(`UPDATE <table> SET details = ${changedDetails} WHERE id = 2`);
      await expect(execute(plan)).rejects.toMatchObject({
        deleteResult: {
          success: false,
          affectedRows: 0,
          changesPossible: false,
          error: expect.stringMatching(
            /row changed, was replaced, or was deleted/i,
          ),
        },
      });
      expect(
        (await sql("SELECT id FROM <table> ORDER BY id")).rows,
      ).toHaveLength(2);

      const refreshed = await prepare(await read(), [0, 1]);
      if (!refreshed) throw new Error("Missing refreshed preview");
      await execute(refreshed);
      expect((await sql("SELECT id FROM <table>")).rows).toEqual([]);
    });
  });

  if (engine === "postgres" || engine === "mssql") {
    it("uses the stored row version to distinguish an identical reinsertion", async () => {
      await withTable(
        `${contents}${engine === "mssql" ? ", rv ROWVERSION NOT NULL" : ""}`,
        async ({ read, prepare, execute, sql }) => {
          const insert =
            "INSERT INTO <table> (tenant,id,note) VALUES (1,1,'same')";
          await sql(insert);
          const plan = await prepare(await read(), [0]);
          if (!plan) throw new Error("Missing guarded delete preview");
          if (engine === "mssql")
            expect(plan.operations[0].sql).toContain("[rv]");
          await sql("DELETE FROM <table> WHERE id = 1");
          await sql(insert);
          await expect(execute(plan)).rejects.toMatchObject({
            deleteResult: {
              success: false,
              affectedRows: 0,
              changesPossible: false,
            },
          });
          expect((await sql("SELECT note FROM <table>")).rows).toEqual([
            { __col_0: "same" },
          ]);
        },
      );
    });
  }
  if (engine === "mysql" || engine === "oracle") {
    it("refuses a row whose non-NULL native value cannot be captured safely", async () => {
      await withTable(
        engine === "mysql" ? "details JSON" : "details CLOB",
        async ({ read, prepare, sql }) => {
          await sql("INSERT INTO <table> (tenant,id) VALUES (1,1)");
          const unsupported =
            engine === "mysql" ? `'{"a":1}'` : "TO_CLOB(RPAD('x',501,'x'))";
          await sql(`INSERT INTO <table> VALUES (1,2,${unsupported})`);
          const page = await read();
          expect(page.rows).toHaveLength(2);
          expect(page.sqlDeleteObservations).toEqual([
            expect.any(Object),
            null,
          ]);
          await expect(prepare(page, [0, 1])).rejects.toThrow(
            /original row snapshot/i,
          );
          expect((await sql("SELECT id FROM <table>")).rows).toHaveLength(2);
        },
      );
    });
  }
  if (engine === "mssql") {
    it("compares legacy LOBs that can change without advancing rowversion", async () => {
      await withTable(
        "note text, rv ROWVERSION NOT NULL",
        async ({ read, prepare, execute, sql }) => {
          await sql(
            "INSERT INTO <table> (tenant,id,note) VALUES (1,1,'before')",
          );
          const plan = await prepare(await read(), [0]);
          if (!plan) throw new Error("Missing guarded delete preview");
          await sql(`DECLARE @ptr varbinary(16);
          SELECT @ptr = TEXTPTR(note) FROM <table>;
          WRITETEXT <table>.note @ptr 'after'`);
          await expect(execute(plan)).rejects.toMatchObject({
            deleteResult: { affectedRows: 0, changesPossible: false },
          });
          expect((await sql("SELECT note FROM <table>")).rows).toEqual([
            { __col_0: "after" },
          ]);
        },
      );
    });
  }
  if (engine === "oracle") {
    it("compares the current LOB after waiting on a concurrent in-place LOB write", async () => {
      await withTable(
        "note CLOB",
        async ({ config, qualified, read, prepare, execute, sql }) => {
          await sql("INSERT INTO <table> VALUES (1,1,TO_CLOB('before'))");
          const plan = await prepare(await read(), [0]);
          if (!plan) throw new Error("Missing guarded delete preview");
          expect(plan.operations[0].oracleDeleteRowLock?.sql).toMatch(
            /FOR UPDATE$/,
          );
          const writer = await oracledb.getConnection({
            user: config.username,
            password: config.password,
            connectString: `${config.host}:${config.port}/${config.serviceName}`,
          });
          try {
            await writer.execute(`DECLARE lob CLOB;
              BEGIN
                SELECT note INTO lob FROM ${qualified} FOR UPDATE;
                DBMS_LOB.WRITE(lob, 6, 1, 'edited');
              END;`);
            const pending = expect(execute(plan)).rejects.toMatchObject({
              deleteResult: {
                success: false,
                affectedRows: 0,
                changesPossible: false,
              },
            });
            // Leave the writer's lock open while the guarded transaction begins.
            await pause(250);
            await writer.commit();
            await pending;
            expect(
              (await sql("SELECT DBMS_LOB.SUBSTR(note, 6, 1) FROM <table>"))
                .rows,
            ).toEqual([{ __col_0: "edited" }]);
          } finally {
            await writer.close();
          }
        },
      );
    });
  }
}
