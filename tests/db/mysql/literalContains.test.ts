import type { Pool, PoolConnection, RowDataPacket } from "mysql2/promise";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../../src/extension/dbDrivers/mysql";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

// Opt in against the existing MySQL test seed (no fixture reset required):
// RAPIDB_LIVE_MYSQL_CONTAINS=1 npx vitest run --project db-mysql tests/db/mysql/literalContains.test.ts
describe.runIf(process.env.RAPIDB_LIVE_MYSQL_CONTAINS === "1")(
  "H1 live MySQL literal Contains across SESSION sql_mode",
  () => {
    let driver: MySQLDriver | undefined;
    let pool: Pool | undefined;
    let connection: PoolConnection | undefined;
    let service: TableReadService;
    let database: string;
    let configuredConnectionId: string;
    let originalMode: string | undefined;
    let connectionId: number;
    let tableCreated = false;
    const table = `rapidb_h1_contains_${process.pid}_${Date.now()}`;
    const samples: Array<string | null> = [
      "plain",
      "100% done",
      "a_b",
      String.raw`folder\file`,
      "wow!",
      "single'quote",
      'double"quote',
      "雪😀",
      String.raw`left!%_\!right`,
      "mix!%_\\'\"雪😀end",
      "x' OR 1=1 --",
      "aXb",
      "100done",
      "folder/file",
      "leftXYright",
      "mixABC/quotesend",
      "ordinary string",
      "",
      null,
    ];
    const needles = [
      "%",
      "_",
      "!",
      "\\",
      "'",
      '"',
      "雪😀",
      String.raw`!%_\!`,
      "!%_\\'\"雪😀",
      "x' OR 1=1 --",
      "ordinary",
      "plain",
      "absent%_\\!雪😀",
    ];
    const standardMode = "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";
    const modes = [
      standardMode,
      `${standardMode},NO_BACKSLASH_ESCAPES`,
      `${standardMode},ANSI_QUOTES`,
      `${standardMode},NO_BACKSLASH_ESCAPES,ANSI_QUOTES`,
    ];

    beforeAll(async () => {
      const config = await resolveConnectionSeed("mysql");
      if (!config.database) throw new Error("MySQL test seed needs a database");
      database = config.database;
      configuredConnectionId = config.id;
      driver = new MySQLDriver(config);
      await driver.connect();
      pool = (driver as unknown as { pool: Pool }).pool;
      connection = await pool.getConnection();
      const [session] = await connection.query<RowDataPacket[]>(
        "SELECT @@SESSION.sql_mode AS mode, CONNECTION_ID() AS id",
      );
      if (typeof session[0]?.mode !== "string") {
        throw new Error("MySQL did not return the original SESSION sql_mode");
      }
      originalMode = session[0].mode;
      connectionId = Number(session[0]?.id);
      expect(Number.isFinite(connectionId)).toBe(true);

      // Only pin routing, not SQL/results: metadata uses pool.query(), whereas
      // COUNT/data use pool.getConnection() and the native mysql2 text protocol.
      // Keep this real lease until cleanup so every path sees the same session.
      vi.spyOn(connection, "release").mockImplementation(() => {});
      vi.spyOn(pool, "getConnection").mockResolvedValue(connection);
      vi.spyOn(pool, "query").mockImplementation(
        connection.query.bind(connection),
      );
      const liveDriver = driver;
      service = new TableReadService({
        getConnection: () => config,
        getDriver: () => liveDriver,
      } as never);

      await driver.query(
        `CREATE TABLE \`${table}\` (\`id\` INT PRIMARY KEY, \`value\` VARCHAR(512) CHARACTER SET utf8mb4 NULL)`,
      );
      tableCreated = true;
      // Seed through real prepared statements so quotes/backslashes in fixture
      // data are independent of the server's initial string-literal mode.
      for (const [index, value] of samples.entries()) {
        await connection.execute(
          `INSERT INTO \`${table}\` (\`id\`, \`value\`) VALUES (?, ?)`,
          [index + 1, value],
        );
      }
    });

    afterAll(async () => {
      try {
        if (connection && originalMode !== undefined) {
          await connection.query("SET SESSION sql_mode = ?", [originalMode]);
        }
      } finally {
        try {
          if (connection && tableCreated) {
            await connection.query(`DROP TABLE \`${table}\``);
          }
        } finally {
          vi.restoreAllMocks();
          connection?.release();
          await driver?.disconnect();
        }
      }
    });

    it.each(modes)("returns literal count and rows under %s", async (mode) => {
      if (!driver || !connection || originalMode === undefined) {
        throw new Error("Live MySQL session was not initialized");
      }
      const query = vi.spyOn(driver, "query"); // Call through to the live server.
      try {
        await connection.query("SET SESSION sql_mode = ?", [mode]);
        const [session] = await connection.query<RowDataPacket[]>(
          "SELECT @@SESSION.sql_mode AS mode, CONNECTION_ID() AS id",
        );
        expect(String(session[0]?.mode).split(",").sort()).toEqual(
          mode.split(",").sort(),
        );
        expect(Number(session[0]?.id)).toBe(connectionId);

        for (const needle of needles) {
          query.mockClear();
          const page = await service.getPage(
            configuredConnectionId,
            database,
            database,
            table,
            1,
            100,
            [{ column: "value", operator: "like", value: needle }],
            { column: "id", direction: "asc" },
            false,
          );
          const expected = samples.flatMap((value, index) =>
            value?.includes(needle) ? [{ id: index + 1, value }] : [],
          );
          expect(page.rows, needle).toEqual(expected);
          expect(page.totalCount, needle).toBe(expected.length);
          expect(
            page.rows.some((row) => row.value === null),
            needle,
          ).toBe(false);
          // A failed COUNT falls back to a lower bound. Verify the COUNT really
          // succeeded, including zero matches, rather than accepting fallback.
          expect(query).toHaveBeenCalledTimes(2);
          expect(query.mock.calls[0]?.[0]).toMatch(/^SELECT COUNT\(\*\)/);
          expect(query.mock.calls[1]?.[0]).toMatch(/^SELECT `id`, `value`/);
          expect(
            query.mock.settledResults.every(
              (result) => result.type === "fulfilled",
            ),
          ).toBe(true);
        }
        const [sessionAfterReads] = await connection.query<RowDataPacket[]>(
          "SELECT @@SESSION.sql_mode AS mode, CONNECTION_ID() AS id",
        );
        expect(String(sessionAfterReads[0]?.mode).split(",").sort()).toEqual(
          mode.split(",").sort(),
        );
        expect(Number(sessionAfterReads[0]?.id)).toBe(connectionId);
      } finally {
        query.mockRestore();
        await connection.query("SET SESSION sql_mode = ?", [originalMode]);
        const [restored] = await connection.query<RowDataPacket[]>(
          "SELECT @@SESSION.sql_mode AS mode",
        );
        expect(String(restored[0]?.mode)).toBe(originalMode);
      }
    });
  },
);
