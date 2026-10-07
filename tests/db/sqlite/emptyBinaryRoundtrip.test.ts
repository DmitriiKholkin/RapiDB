import { afterEach, describe, expect, it } from "vitest";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { NULL_SENTINEL } from "../../../src/shared/tableTypes";

describe("SQLite empty binary table edits", () => {
  let driver: SQLiteCoreDriver | undefined;

  afterEach(async () => {
    await driver?.disconnect();
    driver = undefined;
  });

  it("round-trips empty bytes without conflating empty text or NULL", async () => {
    driver = new SQLiteCoreDriver({
      id: "empty-binary-roundtrip",
      name: "Empty binary round trip",
      type: "sqlite",
      filePath: ":memory:",
    });
    await driver.connect();
    await driver.query(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, payload BLOB); INSERT INTO items VALUES (1, X'AB'), (2, X''), (3, ''), (4, NULL)",
    );

    const manager = {
      getConnection: () => ({ id: "empty-binary-roundtrip", type: "sqlite" }),
      getDriver: () => driver,
    } as never;
    const readService = new TableReadService(manager);
    const mutationService = new TableMutationService(manager, readService);
    const readPage = async () =>
      readService.getPage(
        "empty-binary-roundtrip",
        "",
        "main",
        "items",
        1,
        10,
        [],
      );

    const initialPage = await readPage();
    expect(initialPage.rows.map((row) => row.payload)).toEqual([
      "0xab",
      "0x",
      "",
      null,
    ]);

    await mutationService.updateRow(
      "empty-binary-roundtrip",
      "",
      "main",
      "items",
      { id: 1 },
      { payload: "0x" },
    );
    await mutationService.updateRow(
      "empty-binary-roundtrip",
      "",
      "main",
      "items",
      { id: 2 },
      { payload: "" },
    );
    await mutationService.updateRow(
      "empty-binary-roundtrip",
      "",
      "main",
      "items",
      { id: 3 },
      { payload: NULL_SENTINEL },
    );

    const updatedPage = await readPage();
    expect(updatedPage.rows.map((row) => row.payload)).toEqual([
      "0x",
      "",
      null,
      null,
    ]);

    const storageTypes = await driver.query(
      "SELECT id, typeof(payload) AS storage_type FROM items ORDER BY id",
    );
    expect(storageTypes.rows.map((row) => row.__col_1)).toEqual([
      "blob",
      "text",
      "null",
      "null",
    ]);
  });
});
