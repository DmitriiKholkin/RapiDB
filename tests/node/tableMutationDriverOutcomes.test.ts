import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import type {
  ColumnTypeMeta,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import { DriverUpdateRowsError } from "../../src/extension/dbDrivers/updateRowOutcomes";
import type { RowUpdate } from "../../src/extension/table/tableDataContracts";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";

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
    name: "value",
    type: "text",
    nativeType: "text",
    category: "text",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];

const updates = (ids: number[]): RowUpdate[] =>
  ids.map((id) => ({ primaryKeys: { id }, changes: { value: `next-${id}` } }));

function createHarness(updateRows: IDBDriver["updateRows"]) {
  const driver = {
    updateRows,
    checkPersistedEdit: () => undefined,
    coerceInputValue: (value: unknown) => value,
    buildMutationPreviewStatement: () => "UPDATE items",
    qualifiedTableName: () => "items",
  } as unknown as IDBDriver;
  const manager = {
    getConnection: () => ({ id: "conn", readOnly: false }),
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  const prepared = prepareApplyChangesPlan(
    manager,
    "conn",
    "db",
    "",
    "items",
    updates([1, 2]),
    columns,
  );
  if (!prepared.executable) throw new Error("Expected an executable plan");
  return { manager, plan: prepared.plan };
}

describe("driver-mode UPDATE row reconciliation", () => {
  it("does not assign partial aggregate counts to rows by position", async () => {
    const { manager, plan } = createHarness(async () => ({ affectedRows: 1 }));

    const result = await executePreparedApplyPlan(manager, plan);

    expect(result).toMatchObject({
      success: false,
      changesPossible: true,
      outcomeUnknown: true,
      rowOutcomes: [
        { rowIndex: 0, success: false, status: "unknown" },
        { rowIndex: 1, success: false, status: "unknown" },
      ],
    });
  });

  it("treats a missing affectedRows count as unknown instead of all-applied", async () => {
    const { manager, plan } = createHarness(
      async () => ({ affectedRows: undefined }) as never,
    );

    const result = await executePreparedApplyPlan(manager, plan);

    expect(result.success).toBe(false);
    expect(result.changesPossible).toBe(true);
    expect(result.outcomeUnknown).toBe(true);
    expect(result.rowOutcomes?.map(({ status }) => status)).toEqual([
      "unknown",
      "unknown",
    ]);
  });

  it("honors per-row applied and not-applied outcomes", async () => {
    const { manager, plan } = createHarness(async () => ({
      affectedRows: 1,
      updateRowOutcomes: [
        { rowIndex: 0, status: "applied" },
        { rowIndex: 1, status: "not_applied" },
      ],
    }));

    const result = await executePreparedApplyPlan(manager, plan);

    expect(result).toMatchObject({
      success: false,
      changesPossible: true,
      outcomeUnknown: false,
      failedRows: [1],
      rowOutcomes: [
        { rowIndex: 0, success: true, status: "applied" },
        { rowIndex: 1, success: false, status: "not_applied" },
      ],
    });
  });

  it("retains identities from sequential writes completed before an error", async () => {
    const { manager, plan } = createHarness(async () => {
      throw new DriverUpdateRowsError("Connection lost", {
        affectedRows: 1,
        updateRowOutcomes: [
          { rowIndex: 0, status: "applied" },
          { rowIndex: 1, status: "unknown" },
        ],
      });
    });

    const result = await executePreparedApplyPlan(manager, plan);

    expect(result).toMatchObject({
      success: false,
      changesPossible: true,
      outcomeUnknown: true,
      rowOutcomes: [
        { rowIndex: 0, success: true, status: "applied" },
        { rowIndex: 1, success: false, status: "unknown" },
      ],
    });
  });

  it("maps driver request indexes around skipped plan rows", async () => {
    const updateRows = vi.fn(async () => ({
      affectedRows: 1,
      updateRowOutcomes: [
        { rowIndex: 0, status: "applied" as const },
        { rowIndex: 1, status: "unknown" as const },
      ],
    }));
    const { manager } = createHarness(updateRows);
    const prepared = prepareApplyChangesPlan(
      manager,
      "conn",
      "db",
      "",
      "items",
      [
        { primaryKeys: { id: 1 }, changes: { value: "next-1" } },
        { primaryKeys: { id: 2 }, changes: {} },
        { primaryKeys: { id: 3 }, changes: { value: "next-3" } },
      ],
      columns,
    );
    if (!prepared.executable) throw new Error("Expected an executable plan");

    const result = await executePreparedApplyPlan(manager, prepared.plan);

    expect(updateRows).toHaveBeenCalledWith({
      database: "db",
      schema: "",
      table: "items",
      updates: [
        expect.objectContaining({ primaryKeys: { id: 1 } }),
        expect.objectContaining({ primaryKeys: { id: 3 } }),
      ],
    });
    expect(result.rowOutcomes).toMatchObject([
      { rowIndex: 0, status: "applied" },
      { rowIndex: 1, status: "skipped" },
      { rowIndex: 2, status: "unknown" },
    ]);
  });
});
