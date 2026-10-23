import { describe, expect, it } from "vitest";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import {
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

describe("single-row UPDATE cardinality", () => {
  it.each([
    "prepared",
    "atomic",
    "direct",
  ] as const)("%s rolls back if a cached key no longer uniquely identifies a row", async (path) => {
    const config = {
      id: "cardinality",
      name: "cardinality",
      type: "sqlite" as const,
      filePath: ":memory:",
    };
    const driver = new SQLiteCoreDriver(config);
    await driver.connect();
    try {
      await driver.query(
        "CREATE TABLE events (id INTEGER NOT NULL PRIMARY KEY, happened DATE)",
      );
      const columns = await driver.describeColumns("main", "main", "events");
      const manager = {
        getConnection: () => config,
        getDriver: () => driver,
      } as never;
      const prepared = prepareApplyChangesPlan(
        manager,
        config.id,
        "main",
        "main",
        "events",
        [{ primaryKeys: { id: 1 }, changes: { happened: "2026-10-23" } }],
        columns,
      );
      if (!prepared.executable) throw new Error("Expected UPDATE plan");
      // An external migration can remove a key without notifying an open panel.
      await driver.query(
        "DROP TABLE events; CREATE TABLE events (id INTEGER NOT NULL, happened DATE); INSERT INTO events VALUES (1, '2020-01-01'), (1, '2020-01-02')",
      );
      if (path === "direct") {
        const service = new TableMutationService(manager, {
          getColumns: async () => columns,
        });
        await expect(
          service.updateRow(
            config.id,
            "main",
            "main",
            "events",
            { id: 1 },
            { happened: "2026-10-23" },
          ),
        ).rejects.toThrow(/affected 2 row\(s\); expected 1/);
      } else {
        const result =
          path === "prepared"
            ? await executePreparedApplyPlan(manager, prepared.plan)
            : await executeAtomicSqlApplyPlan(manager, prepared.plan, []);
        expect(result.success).toBe(false);
        expect(result.error).toMatch(/affected 2 row\(s\); expected 1/);
      }
      const stored = await driver.query(
        "SELECT happened FROM events ORDER BY happened",
      );
      expect(stored.rows.map((row) => row.__col_0)).toEqual([
        "2020-01-01",
        "2020-01-02",
      ]);
    } finally {
      await driver.disconnect();
    }
  });
});
