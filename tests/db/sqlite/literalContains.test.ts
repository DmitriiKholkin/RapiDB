import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";
import { TableReadService } from "../../../src/extension/table/tableReadService";

const drivers: SQLiteCoreDriver[] = [];
afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

describe.each([
  "native",
  "worker",
] as const)("H1 real %s SQLite Contains", (mode) => {
  it("matches literal %, _, backslash and combined escape characters without losing ordinary substring matching", async () => {
    const config = {
      id: "h1-sqlite",
      name: "H1 SQLite",
      type: "sqlite" as const,
      filePath: ":memory:",
    };
    const driver =
      mode === "native"
        ? new SQLiteCoreDriver(config)
        : new SQLiteDriver(config);
    drivers.push(driver);
    await driver.connect();
    await driver.query(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)",
    );
    const values = [
      "plain",
      "100% done",
      "a_b",
      String.raw`path\file`,
      "bang!",
      String.raw`left!%_\!right`,
      "leftXXright",
      "aXb",
      "ordinary string",
      "x' OR 1=1 --",
      "2026-10-05T10:20:30Z",
      "雪😀",
      null,
    ];
    for (const [index, value] of values.entries())
      await driver.query("INSERT INTO items VALUES (?, ?)", [index + 1, value]);
    const service = new TableReadService({
      getConnection: () => config,
      getDriver: () => driver,
    } as never);
    for (const needle of [
      "%",
      "_",
      "\\",
      "!",
      String.raw`!%_\!`,
      String.raw`\!`,
      "!%",
      "ordinary",
      "plain",
      "x' OR 1=1 --",
      "2026-10-05T10:20:30Z",
      "雪😀",
      "%_",
    ]) {
      const page = await service.getPage(
        config.id,
        "",
        "main",
        "items",
        1,
        100,
        [{ column: "value", operator: "like", value: needle }],
        { column: "id", direction: "asc" },
      );
      const expected = values.flatMap((value, index) =>
        value?.includes(needle) ? [index + 1] : [],
      );
      expect(
        page.rows.map((row) => row.id),
        needle,
      ).toEqual(expected);
      expect(page.totalCount, needle).toBe(expected.length);
    }
  });
});
