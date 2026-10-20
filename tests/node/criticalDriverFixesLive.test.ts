import { randomUUID } from "node:crypto";
import { Client } from "@elastic/elasticsearch";
import { MongoClient } from "mongodb";
import mysql from "mysql2/promise";
import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const mysqlPort = Number(process.env.RAPIDB_FIX_MYSQL_PORT);
const esPort = Number(process.env.RAPIDB_FIX_ES_PORT);
const mongoPort = Number(process.env.RAPIDB_FIX_MONGO_PORT);

describe.runIf(Boolean(mysqlPort))("critical MySQL fixes (live)", () => {
  it("preserves executable WHERE, rolls back unfinished scripts, and isolates session state", async () => {
    const config = {
      host: "127.0.0.1",
      port: mysqlPort,
      user: "root",
      password: "rapidb-fix-only",
      database: "rapidb_fix",
    };
    const admin = await mysql.createConnection(config);
    const driver = new MySQLDriver({
      id: "fix",
      name: "fix",
      type: "mysql",
      host: config.host,
      port: mysqlPort,
      username: config.user,
      password: config.password,
      database: config.database,
    });
    const table = `fix_${randomUUID().replaceAll("-", "")}`;
    try {
      await driver.connect();
      await admin.query(
        `CREATE TABLE ${table} (id INT PRIMARY KEY, balance INT) ENGINE=InnoDB`,
      );
      await admin.query(
        `CREATE PROCEDURE ${table}_tx(IN next_balance INT) BEGIN START TRANSACTION; UPDATE ${table} SET balance=next_balance WHERE id=2; END`,
      );
      await admin.query(`INSERT INTO ${table} VALUES (1,100), (2,200)`);
      expect(
        (
          await driver.query(
            `DELETE FROM ${table} /*!50000 WHERE id = 1 */;`,
            undefined,
            { hardCap: 1 },
          )
        ).affectedRows,
      ).toBe(1);
      const [remaining] = await admin.query(`SELECT * FROM ${table}`);
      expect(remaining).toEqual([{ id: 2, balance: 200 }]);

      let expectedBalance = 200;
      for (const hardCap of [undefined, 1]) {
        for (const begin of ["START TRANSACTION", "SET autocommit=0"]) {
          await expect(
            driver.query(
              `${begin}; UPDATE ${table} SET balance=0 WHERE id=2`,
              undefined,
              { hardCap },
            ),
          ).rejects.toThrow(/Unfinished.*rolled back/);
          const [rolledBack] = await admin.query(
            `SELECT balance FROM ${table} WHERE id=2`,
          );
          expect(rolledBack).toEqual([{ balance: expectedBalance }]);
          await driver.runTransaction([
            {
              sql: `UPDATE ${table} SET balance=balance+1 WHERE id=2`,
              params: [],
            },
          ]);
          const [rows] = await admin.query<mysql.RowDataPacket[]>(
            `SELECT balance FROM ${table} WHERE id=2`,
          );
          expectedBalance += 1;
          expect(Number(rows[0].balance)).toBe(expectedBalance);
        }
      }
      await driver.query(
        `START TRANSACTION; UPDATE ${table} SET balance=300 WHERE id=2; COMMIT`,
        undefined,
        { hardCap: 1 },
      );
      const [committed] = await admin.query(
        `SELECT balance FROM ${table} WHERE id=2`,
      );
      expect(committed).toEqual([{ balance: 300 }]);
      for (const hardCap of [undefined, 1]) {
        await expect(
          driver.query(`CALL ${table}_tx(?)`, [0], { hardCap }),
        ).rejects.toThrow(/Unfinished.*rolled back/);
        const [rows] = await admin.query(
          `SELECT balance FROM ${table} WHERE id=2`,
        );
        expect(rows).toEqual([{ balance: 300 }]);
        await driver.query(`SELECT * FROM ${table}`, undefined, {
          hardCap,
          readOnly: true,
        });
        await expect(
          driver.query(
            `START TRANSACTION; UPDATE ${table} SET balance=0 WHERE id=2; SELECT missing_column FROM ${table}`,
            undefined,
            { hardCap },
          ),
        ).rejects.toThrow(/missing_column/);
        const [afterError] = await admin.query(
          `SELECT balance FROM ${table} WHERE id=2`,
        );
        expect(afterError).toEqual([{ balance: 300 }]);
      }
      await expect(
        driver.query(
          `DELETE FROM ${table} WHERE id=2; SELECT 1 /* unfinished`,
          undefined,
          { hardCap: 1 },
        ),
      ).rejects.toThrow(/Unterminated/);
      const [afterParseError] = await admin.query(
        `SELECT balance FROM ${table} WHERE id=2`,
      );
      expect(afterParseError).toEqual([{ balance: 300 }]);
      await driver.query(
        "USE information_schema; SET autocommit=0; SET sql_mode='ANSI'",
      );
      const next = await driver.query(
        "SELECT DATABASE() AS db, @@autocommit AS ac, @@sql_mode AS mode",
        undefined,
        { hardCap: 1 },
      );
      expect(Object.values(next.rows[0])[0]).toBe("rapidb_fix");
      expect(Number(Object.values(next.rows[0])[1])).toBe(1);
      expect(String(Object.values(next.rows[0])[2])).not.toContain("ANSI");
    } finally {
      await driver.disconnect();
      await admin.query(`DROP PROCEDURE IF EXISTS ${table}_tx`);
      await admin.query(`DROP TABLE IF EXISTS ${table}`);
      await admin.end();
    }
  });
});

const column: ColumnTypeMeta = {
  name: "_source",
  type: "object",
  nativeType: "object",
  category: "json",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};

async function checkService(
  driver: ElasticsearchDriver | MongoDBDriver,
  table: string,
) {
  const manager = {
    getConnection: () => ({ id: "fix", readOnly: false }),
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  const idColumn: ColumnTypeMeta = {
    ...column,
    name: "_id",
    category: "text",
    nativeType: "string",
    isPrimaryKey: true,
  };
  const columns = [column, idColumn];
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  const id = (value: string) =>
    driver instanceof MongoDBDriver
      ? { $rapidbMongoId: { type: "string", value } }
      : value;
  await service.insertRow("fix", "rapidb_fix", "", table, {
    _id: "a",
    _source: '{"amount":42}',
  });
  await expect(
    service.insertRow("fix", "rapidb_fix", "", table, {
      _id: "b",
      _source: '{"amount":9007199254740993}',
    }),
  ).rejects.toThrow(/numeric precision/);
  const result = await applyChangesTransactional(
    manager,
    "fix",
    "rapidb_fix",
    "",
    table,
    [
      { primaryKeys: { _id: id("a") }, changes: { _source: '{"amount":1}' } },
      {
        primaryKeys: { _id: id("b") },
        changes: { _source: '{"amount":1000000000000000100}' },
      },
    ],
    columns,
  );
  expect(result.success).toBe(false);
}

describe.runIf(Boolean(esPort))(
  "critical Elasticsearch precision fix (live)",
  () => {
    it("inserts safe JSON and refuses inaccurate insert/batch before writing", async () => {
      const admin = new Client({ node: `http://127.0.0.1:${esPort}` });
      const driver = new ElasticsearchDriver({
        id: "fix",
        name: "fix",
        type: "elasticsearch",
        host: "127.0.0.1",
        port: esPort,
      });
      const table = `fix_${randomUUID().replaceAll("-", "")}`;
      try {
        await driver.connect();
        await admin.indices.create({ index: table });
        await checkService(driver, table);
        const result = await admin.search(
          { index: table, query: { match_all: {} } },
          {
            headers: {
              accept: "application/vnd.elasticsearch+json; compatible-with=8",
              "content-type":
                "application/vnd.elasticsearch+json; compatible-with=8",
            },
          },
        );
        expect(result.hits.hits.map((hit) => hit._source)).toEqual([
          { amount: 42 },
        ]);
        await expect(
          driver.insertRow({
            database: "",
            schema: "",
            table,
            values: { _source: '{"amount":9007199254740993}' },
          }),
        ).rejects.toThrow(/numeric precision/);
        await expect(
          driver.query(
            `PUT /${table}/_doc/unsafe\n{"amount":9007199254740993}`,
          ),
        ).rejects.toThrow(/numeric precision/);
      } finally {
        await driver.disconnect();
        await admin.indices.delete({ index: table }, { ignore: [404] });
        await admin.close();
      }
    });
  },
);

describe.runIf(Boolean(mongoPort))(
  "critical MongoDB precision fix (live)",
  () => {
    it("inserts safe BSON and refuses inaccurate insert/batch before writing", async () => {
      const uri = `mongodb://127.0.0.1:${mongoPort}/rapidb_fix?directConnection=true`;
      const admin = new MongoClient(uri);
      const driver = new MongoDBDriver({
        id: "fix",
        name: "fix",
        type: "mongodb",
        connectionUri: uri,
      });
      const table = `fix_${randomUUID().replaceAll("-", "")}`;
      try {
        await admin.connect();
        await driver.connect();
        await checkService(driver, table);
        const rows = await admin
          .db("rapidb_fix")
          .collection(table)
          .find()
          .toArray();
        expect(rows).toHaveLength(1);
        expect(rows[0]._source).toEqual({ amount: 42 });
        const collection = admin
          .db("rapidb_fix")
          .collection<{ _id: string; payload: unknown }>(table);
        const payload = { amount: Number("1000000000000000100") };
        await collection.insertOne({ _id: "legacy", payload });
        const manager = {
          getConnection: () => ({ readOnly: false }),
          getDriver: () => driver,
        } as unknown as ConnectionManager;
        const result = await applyChangesTransactional(
          manager,
          "fix",
          "rapidb_fix",
          "",
          table,
          [
            {
              primaryKeys: {
                _id: { $rapidbMongoId: { type: "string", value: "legacy" } },
              },
              changes: { payload: '{"amount":42}' },
              originalValues: { payload: JSON.stringify(payload) },
            },
          ],
          [
            { ...column, name: "payload" },
            {
              ...column,
              name: "_id",
              category: "text",
              nativeType: "string",
              isPrimaryKey: true,
            },
          ],
        );
        expect(result.success).toBe(true);
        expect((await collection.findOne({ _id: "legacy" }))?.payload).toEqual({
          amount: 42,
        });
      } finally {
        await driver.disconnect();
        await admin
          .db("rapidb_fix")
          .collection(table)
          .drop()
          .catch(() => undefined);
        await admin.close();
      }
    });
  },
);
