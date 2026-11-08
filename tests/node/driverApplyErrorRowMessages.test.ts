import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type {
  ColumnTypeMeta,
  DriverMutationResult,
} from "../../src/extension/dbDrivers/types";
import { DriverUpdateRowsError } from "../../src/extension/dbDrivers/updateRowOutcomes";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";

const textColumn: ColumnTypeMeta = {
  name: "name",
  type: "string",
  nativeType: "string",
  category: "text",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};
const idColumn: ColumnTypeMeta = {
  ...textColumn,
  name: "_id",
  nullable: false,
  isPrimaryKey: true,
};
const GENERIC_NOT_APPLIED =
  "This row was not updated; it may have changed or been deleted since it was loaded.";
const LIMIT_ERROR =
  "DynamoDB applies multi-row edits in a single transaction, which is limited to 100 items.";

function rowUpdate(id: string) {
  return {
    primaryKeys: { _id: { $rapidbMongoId: { type: "string", value: id } } },
    changes: { name: `new-${id}` },
  };
}

async function apply(outcome: () => Promise<DriverMutationResult>) {
  const config = { id: "driver-apply", name: "Driver", type: "mongodb" };
  const driver = new MongoDBDriver(config as never);
  Object.assign(driver, { connected: true });
  vi.spyOn(driver, "updateRows").mockImplementation(outcome);
  const manager = { getDriver: () => driver, getConnection: () => config };
  return applyChangesTransactional(
    manager as never,
    "driver-apply",
    "db",
    "",
    "items",
    [rowUpdate("a"), rowUpdate("b")],
    [idColumn, textColumn],
  );
}

describe("DDB-4 driver apply errors are reported on each unapplied row", () => {
  it("shows the driver error instead of a conflict hint when no row was applied", async () => {
    const result = await apply(async () => {
      throw new DriverUpdateRowsError(LIMIT_ERROR, {
        affectedRows: 0,
        updateRowOutcomes: [
          { rowIndex: 0, status: "not_applied" },
          { rowIndex: 1, status: "not_applied" },
        ],
      });
    });
    expect(result).toMatchObject({
      success: false,
      error: LIMIT_ERROR,
      failedRows: [0, 1],
      changesPossible: false,
    });
    for (const outcome of result.rowOutcomes ?? []) {
      expect(outcome).toMatchObject({ success: false, status: "not_applied" });
      expect(outcome.message).toContain(LIMIT_ERROR);
      expect(outcome.message).not.toBe(GENERIC_NOT_APPLIED);
    }
  });

  it("keeps the conflict hint for plain not-applied rows and partial failures", async () => {
    const conflict = await apply(async () => ({
      affectedRows: 0,
      updateRowOutcomes: [
        { rowIndex: 0, status: "not_applied" },
        { rowIndex: 1, status: "not_applied" },
      ],
    }));
    expect(conflict.rowOutcomes?.map((outcome) => outcome.message)).toEqual([
      GENERIC_NOT_APPLIED,
      GENERIC_NOT_APPLIED,
    ]);

    const partial = await apply(async () => {
      throw new DriverUpdateRowsError("socket closed", {
        affectedRows: 1,
        updateRowOutcomes: [
          { rowIndex: 0, status: "applied" },
          { rowIndex: 1, status: "not_applied" },
        ],
      });
    });
    expect(partial.rowOutcomes?.[1]).toMatchObject({
      status: "not_applied",
      message: GENERIC_NOT_APPLIED,
    });
  });
});
