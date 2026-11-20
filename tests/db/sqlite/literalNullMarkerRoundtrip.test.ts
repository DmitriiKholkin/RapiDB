import { expect, it } from "vitest";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";
import { applyChangesTransactional } from "../../../src/extension/table/tableMutationExecution";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { NULL_SENTINEL } from "../../../src/shared/tableTypes";

it("writes the former NULL marker as SQLite text and preserves explicit SQL NULL", async () => {
  const id = "literal-null-marker";
  const driver = new SQLiteCoreDriver({
    id,
    name: "Literal NULL marker round trip",
    type: "sqlite",
    filePath: ":memory:",
  });
  try {
    await driver.connect();
    await driver.query(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT); INSERT INTO items VALUES (1, NULL)",
    );
    const manager = {
      getConnection: () => ({ id, type: "sqlite" }),
      getDriver: () => driver,
    } as never;
    const reads = new TableReadService(manager);
    const columns = await reads.getColumns(id, "", "main", "items");
    const update = (original: string | null, next: string | null) =>
      applyChangesTransactional(
        manager,
        id,
        "",
        "main",
        "items",
        [
          {
            primaryKeys: { id: 1 },
            changes: { payload: next },
            originalValues: { payload: original },
          },
        ],
        columns,
      );
    const read = () =>
      driver.query(
        "SELECT typeof(payload), hex(payload) FROM items WHERE id = 1",
      );

    expect((await update(null, NULL_SENTINEL)).success).toBe(true);
    expect((await read()).rows[0]).toEqual({
      __col_0: "text",
      __col_1: Buffer.from(NULL_SENTINEL).toString("hex").toUpperCase(),
    });
    expect(
      (await reads.getPage(id, "", "main", "items", 1, 10, [])).rows[0]
        ?.payload,
    ).toBe(NULL_SENTINEL);

    expect((await update(NULL_SENTINEL, null)).success).toBe(true);
    expect((await read()).rows[0]?.__col_0).toBe("null");
  } finally {
    await driver.disconnect();
  }
});
