import { types as pgTypes } from "pg";
import { describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { buildUpdateRowSql } from "../../src/extension/table/updateSql";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import {
  buildPendingRestoreState,
  restorePendingEditsSafely,
} from "../../src/webview/components/table/tableViewHelpers";

const config = {
  id: "float-transport",
  name: "Float transport",
  type: "pg" as const,
};
const idColumn: ColumnTypeMeta = {
  name: "id",
  type: "integer",
  nativeType: "integer",
  category: "integer",
  nullable: false,
  isPrimaryKey: true,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};

describe("PostgreSQL special float webview transport", () => {
  it.each([
    "real",
    "double precision",
  ])("preserves %s special values through JSON and CAS", async (nativeType) => {
    const driver = new PostgresDriver(config);
    const floatColumn: ColumnTypeMeta = {
      ...idColumn,
      name: "f",
      type: nativeType,
      nativeType,
      category: "float",
      nullable: true,
      isPrimaryKey: false,
    };
    const columns = [idColumn, floatColumn];
    const parser = pgTypes.getTypeParser(nativeType === "real" ? 700 : 701);
    const specials = ["NaN", "Infinity", "-Infinity"];
    vi.spyOn(driver, "describeColumns").mockResolvedValue(columns);
    vi.spyOn(driver, "query").mockResolvedValue({
      columns: ["id", "f"],
      rows: [
        ...specials.map((value, index) => ({
          __col_0: index + 1,
          __col_1: parser(value),
        })),
        { __col_0: 4, __col_1: null },
        { __col_0: 5, __col_1: 1.25 },
      ],
      rowCount: 5,
      executionTimeMs: 1,
    });
    const service = new TableReadService({
      getConnection: () => config,
      getDriver: () => driver,
    } as never);
    const page = await service.getPage(
      config.id,
      "db",
      "public",
      "floats",
      1,
      25,
      [],
      null,
      true,
    );
    const rows = JSON.parse(JSON.stringify(page)).rows as Record<
      string,
      unknown
    >[];
    expect(rows.map((row) => row.f)).toEqual([...specials, null, 1.25]);

    for (const [index, special] of specials.entries()) {
      const row = rows[index];
      const update = buildUpdateRowSql(
        driver,
        "db",
        "public",
        "floats",
        { id: row.id },
        { f: "5" },
        columns,
        { f: row.f },
      );
      expect(update?.sql).not.toContain("IS NULL");
      expect(update?.params).toEqual(["5", row.id, special]);
      expect(
        driver.checkPersistedEdit(floatColumn, special, {
          persistedValue: row.f,
        })?.ok,
      ).toBe(true);

      const draft = buildPendingRestoreState(
        new Map([[index, new Map([["f", "5"]])]]),
        rows,
        ["id"],
      );
      const restored = restorePendingEditsSafely(draft, rows, ["id"]);
      expect(restored.pendingEdits.size).toBe(1);
      const changedRows = rows.map((value, rowIndex) =>
        rowIndex === index ? { ...value, f: null } : value,
      );
      const conflicted = restorePendingEditsSafely(draft, changedRows, ["id"]);
      expect(conflicted.pendingEdits.size).toBe(0);
      expect(conflicted.unresolved).toHaveLength(1);
    }

    const nullUpdate = buildUpdateRowSql(
      driver,
      "db",
      "public",
      "floats",
      { id: 4 },
      { f: "5" },
      columns,
      { f: rows[3].f },
    );
    expect(nullUpdate?.sql).toContain('"f" IS NULL');
    expect(nullUpdate?.params).toEqual(["5", 4]);
  });
});
