import type { Pool, PoolConnection, RowDataPacket } from "mysql2/promise";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
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
    let checkout: (() => Promise<PoolConnection>) | undefined;
    let service: TableReadService;
    let database: string;
    let configuredConnectionId: string;
    let originalMode: string | undefined;
    let modeForReads: string | undefined;
    let observedModes: string[] = [];
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
      checkout = pool.getConnection.bind(pool);
      const liveDriver = driver;
      service = new TableReadService({
        getConnection: () => config,
        getDriver: () => liveDriver,
      } as never);

      await connection.query(
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

    beforeEach(() => {
      if (!pool || !checkout) throw new Error("MySQL pool was not initialized");
      // Production deliberately discards arbitrary-query sessions. Initialize
      // each real checkout instead of returning one already-destroyed lease.
      const realCheckout = checkout;
      vi.spyOn(pool, "getConnection").mockImplementation(async () => {
        const lease = await realCheckout();
        try {
          if (modeForReads !== undefined) {
            await lease.query("SET SESSION sql_mode = ?", [modeForReads]);
            const [session] = await lease.query<RowDataPacket[]>(
              "SELECT @@SESSION.sql_mode AS mode",
            );
            observedModes.push(String(session[0]?.mode));
          }
          return lease;
        } catch (error) {
          lease.destroy();
          throw error;
        }
      });
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
        modeForReads = mode;
        observedModes = [];

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
        expect(observedModes.length).toBeGreaterThanOrEqual(needles.length * 2);
        for (const observed of observedModes)
          expect(observed.split(",").sort()).toEqual(mode.split(",").sort());
      } finally {
        modeForReads = undefined;
        query.mockRestore();
        await connection.query("SET SESSION sql_mode = ?", [originalMode]);
        const [restored] = await connection.query<RowDataPacket[]>(
          "SELECT @@SESSION.sql_mode AS mode",
        );
        expect(String(restored[0]?.mode)).toBe(originalMode);
      }
    });

    it.each(
      modes,
    )("never matches replacement characters in legacy charsets under %s", async (mode) => {
      if (!connection || originalMode === undefined) {
        throw new Error("Live MySQL session was not initialized");
      }
      const legacyTable = `${table}_legacy`;
      try {
        modeForReads = mode;
        observedModes = [];
        await connection.query("SET SESSION sql_mode = ?", [mode]);
        for (const charset of ["latin1", "utf8mb3"] as const) {
          await connection.query(
            `CREATE TABLE \`${legacyTable}\` (id INT PRIMARY KEY, value VARCHAR(80) CHARACTER SET ${charset} COLLATE ${charset}_general_ci)`,
          );
          try {
            for (const [index, value] of [
              "?",
              "café",
              "CAFÉ",
              "100% done",
              "snow?",
            ].entries()) {
              await connection.execute(
                `INSERT INTO \`${legacyTable}\` (id, value) VALUES (?, ?)`,
                [index + 1, value],
              );
            }
            for (const [needle, expectedIds] of [
              ["😀", []],
              ["snow😀", []],
              ["?", [1, 5]],
              ["café", [2, 3]],
              ["%", [4]],
              ["_", []],
            ] as const) {
              const page = await service.getPage(
                configuredConnectionId,
                database,
                database,
                legacyTable,
                1,
                100,
                [{ column: "value", operator: "like", value: needle }],
                { column: "id", direction: "asc" },
                false,
              );
              expect(
                page.rows.map((row) => row.id),
                `${charset}: ${needle}`,
              ).toEqual(expectedIds);
              expect(page.totalCount, `${charset}: ${needle}`).toBe(
                expectedIds.length,
              );
            }
          } finally {
            await connection.query(`DROP TABLE \`${legacyTable}\``);
          }
        }
        expect(observedModes.length).toBeGreaterThanOrEqual(24);
        for (const observed of observedModes)
          expect(observed.split(",").sort()).toEqual(mode.split(",").sort());
      } finally {
        modeForReads = undefined;
        await connection.query("SET SESSION sql_mode = ?", [originalMode]);
      }
    });
  },
);
