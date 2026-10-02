import { describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import {
  applyChangesTransactional,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

const columns: ColumnTypeMeta[] = [
  {
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
  },
  {
    name: "amount",
    type: "numeric(4,2)",
    nativeType: "numeric(4,2)",
    category: "decimal",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];

function setup(hooks = false) {
  const driver = new PostgresDriver({
    id: "prevalidation",
    name: "prevalidation",
    type: "pg",
  });
  const preview = vi.spyOn(driver, "materializePreviewSql");
  const query = vi.spyOn(driver, "query").mockResolvedValue({
    rows: [],
    columns: [],
    rowCount: 1,
    executionTimeMs: 0,
  });
  const transaction = vi.spyOn(driver, "runTransaction").mockResolvedValue();
  const coerce = vi.spyOn(driver, "coerceInputValue");
  const hookUpdate = vi.fn();
  const hookInsert = vi.fn();
  if (hooks)
    Object.assign(driver, { updateRows: hookUpdate, insertRow: hookInsert });
  const manager = {
    getDriver: () => driver,
    getConnection: () => ({ id: "prevalidation", type: "pg" }),
  };
  const service = new TableMutationService(manager as never, {
    getColumns: async () => columns,
  });
  return {
    driver,
    preview,
    query,
    transaction,
    coerce,
    hookUpdate,
    hookInsert,
    manager,
    service,
  };
}

describe("B01 raw persisted-edit prevalidation", () => {
  it.each([
    false,
    true,
  ])("rejects the entire batch before coercion, preview or writes (hooks %s)", async (hooks) => {
    const s = setup(hooks);
    const result = await applyChangesTransactional(
      s.manager as never,
      "prevalidation",
      "db",
      "public",
      "items",
      [
        { primaryKeys: { id: 1 }, changes: { amount: "2.34" } },
        { primaryKeys: { id: 2 }, changes: { amount: "1.239" } },
        { primaryKeys: { id: 3 }, changes: { amount: "999" } },
      ],
      columns,
    );
    expect(result).toMatchObject({
      success: false,
      failedRows: [1, 2],
      rowOutcomes: [
        { rowIndex: 0, success: false, status: "skipped" },
        {
          rowIndex: 1,
          success: false,
          status: "prevalidation_failed",
          columns: ["amount"],
          message: expect.stringContaining("2 fractional digits"),
        },
        {
          rowIndex: 2,
          success: false,
          status: "prevalidation_failed",
          columns: ["amount"],
          message: expect.stringContaining("precision 4"),
        },
      ],
    });
    for (const spy of [
      s.preview,
      s.query,
      s.transaction,
      s.coerce,
      s.hookUpdate,
    ])
      expect(spy).not.toHaveBeenCalled();
  });
  it.each([
    false,
    true,
  ])("public update/insert and insert preparation cannot bypass diagnostics (hooks %s)", async (hooks) => {
    const s = setup(hooks);
    for (const run of [
      () =>
        s.service.updateRow(
          "prevalidation",
          "db",
          "public",
          "items",
          { id: 1 },
          { amount: "1.239" },
        ),
      () =>
        s.service.insertRow("prevalidation", "db", "public", "items", {
          amount: "1.239",
        }),
      () =>
        s.service.prepareInsertRow("prevalidation", "db", "public", "items", {
          amount: "1.239",
        }),
    ])
      await expect(run()).rejects.toMatchObject({
        status: "prevalidation_failed",
        columns: ["amount"],
        message: expect.stringContaining("2 fractional digits"),
      });
    for (const spy of [
      s.preview,
      s.query,
      s.transaction,
      s.coerce,
      s.hookUpdate,
      s.hookInsert,
    ])
      expect(spy).not.toHaveBeenCalled();
  });
  it("ignores undefined and unknown changes and omitted identity/default values", async () => {
    const s = setup();
    const check = vi.spyOn(s.driver, "checkPersistedEdit");
    const prepared = prepareApplyChangesPlan(
      s.manager as never,
      "prevalidation",
      "db",
      "public",
      "items",
      [{ primaryKeys: {}, changes: { amount: undefined, unknown: "1.239" } }],
      columns,
    );
    expect(prepared).toMatchObject({
      executable: false,
      result: { success: true, rowOutcomes: [{ status: "skipped" }] },
    });
    await expect(
      s.service.prepareInsertRow("prevalidation", "db", "public", "items", {}),
    ).resolves.toMatchObject({ operation: { params: [] } });
    expect(check).not.toHaveBeenCalled();
  });

  it("honors ok:false independently of shouldVerify and reports every failed column", () => {
    const s = setup();
    vi.spyOn(s.driver, "checkPersistedEdit").mockReturnValue({
      ok: false,
      shouldVerify: true,
    });
    const result = prepareApplyChangesPlan(
      s.manager as never,
      "prevalidation",
      "db",
      "public",
      "items",
      [{ primaryKeys: { id: 1 }, changes: { id: "2", amount: "0.12" } }],
      columns,
    );
    expect(result).toMatchObject({
      executable: false,
      result: {
        success: false,
        rowOutcomes: [
          {
            rowIndex: 0,
            status: "prevalidation_failed",
            success: false,
            columns: ["id", "amount"],
            message: expect.stringContaining('Column "amount"'),
          },
        ],
      },
    });
    expect(s.coerce).not.toHaveBeenCalled();
    expect(s.preview).not.toHaveBeenCalled();
  });
});
