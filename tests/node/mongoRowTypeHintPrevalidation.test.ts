import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import {
  applyChangesTransactional,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";

const sampledBinaryColumn: ColumnTypeMeta = {
  name: "payload",
  type: "binData",
  nativeType: "binData",
  category: "binary",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};
const idColumn: ColumnTypeMeta = {
  ...sampledBinaryColumn,
  name: "_id",
  type: "string",
  nativeType: "string",
  category: "text",
  nullable: false,
  isPrimaryKey: true,
};
const primaryKeys = {
  _id: { $rapidbMongoId: { type: "string", value: "row-1" } },
};

function setup() {
  const config = {
    id: "mongo-row-hints",
    name: "Mongo Row Hints",
    type: "mongodb",
  } as const;
  const driver = new MongoDBDriver(config);
  const updateOne = vi
    .fn()
    .mockResolvedValue({ acknowledged: true, matchedCount: 1 });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => ({ updateOne }) }) },
  });
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
  };
  return { manager, updateOne, columns: [idColumn, sampledBinaryColumn] };
}

describe("AUD-028 Mongo per-row type hints during prevalidation", () => {
  it("accepts a non-hex string for a row whose field is a plain string", async () => {
    const s = setup();
    const updates = [
      {
        primaryKeys,
        changes: { payload: "0xnothex" },
        originalTypes: {
          payload: { category: "text" as const, nativeType: "string" },
        },
      },
    ];
    expect(
      prepareApplyChangesPlan(
        s.manager as never,
        "mongo-row-hints",
        "db",
        "",
        "items",
        updates,
        s.columns,
      ),
    ).toMatchObject({ executable: true });

    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-row-hints",
      "db",
      "",
      "items",
      updates,
      s.columns,
    );
    expect(result).toMatchObject({ success: true });
    expect(s.updateOne).toHaveBeenCalledOnce();
    expect(JSON.stringify(s.updateOne.mock.calls[0])).toContain(
      '"payload":"0xnothex"',
    );
  });

  it("still rejects malformed hex for rows without a string hint", () => {
    const s = setup();
    for (const originalTypes of [
      undefined,
      { payload: { category: "binary" as const, nativeType: "binData" } },
      { payload: { category: "other" as const, nativeType: "null" } },
    ]) {
      expect(
        prepareApplyChangesPlan(
          s.manager as never,
          "mongo-row-hints",
          "db",
          "",
          "items",
          [{ primaryKeys, changes: { payload: "0xnothex" }, originalTypes }],
          s.columns,
        ),
      ).toMatchObject({
        executable: false,
        result: {
          success: false,
          rowOutcomes: [
            {
              rowIndex: 0,
              status: "prevalidation_failed",
              columns: ["payload"],
            },
          ],
        },
      });
    }
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  function prepare(
    s: ReturnType<typeof setup>,
    update: Record<string, unknown>,
    columns = s.columns,
  ) {
    return prepareApplyChangesPlan(
      s.manager as never,
      "mongo-row-hints",
      "db",
      "",
      "items",
      [{ primaryKeys, ...update } as never],
      columns,
    );
  }

  const prevalidationFailed = {
    executable: false,
    result: {
      success: false,
      rowOutcomes: [
        { rowIndex: 0, status: "prevalidation_failed", columns: ["payload"] },
      ],
    },
  };

  it("rejects malformed hex when the sample is a string but the row hint is binData", () => {
    const s = setup();
    const stringColumn: ColumnTypeMeta = {
      ...sampledBinaryColumn,
      type: "string",
      nativeType: "string",
      category: "text",
    };
    expect(
      prepare(
        s,
        {
          changes: { payload: "0xnothex" },
          originalValues: { payload: "0x00" },
          originalTypes: {
            payload: { category: "binary", nativeType: "binData" },
          },
        },
        [idColumn, stringColumn],
      ),
    ).toMatchObject(prevalidationFailed);
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  it("does not let extra hint keys lift read-only or DBPointer protection", () => {
    const s = setup();
    const readOnlyPointer: ColumnTypeMeta = {
      ...sampledBinaryColumn,
      type: "dbPointer",
      nativeType: "dbPointer",
      category: "other",
      isReadOnly: true,
    };
    expect(
      prepare(
        s,
        {
          changes: { payload: "plain" },
          originalTypes: {
            payload: {
              category: "text",
              nativeType: "string",
              isReadOnly: false,
            },
          },
        },
        [idColumn, readOnlyPointer],
      ),
    ).toMatchObject(prevalidationFailed);

    const stringColumn: ColumnTypeMeta = {
      ...sampledBinaryColumn,
      type: "string",
      nativeType: "string",
      category: "text",
    };
    expect(
      prepare(
        s,
        {
          changes: { payload: "plain" },
          originalTypes: {
            payload: { category: "other", nativeType: "dbPointer" },
          },
        },
        [idColumn, stringColumn],
      ),
    ).toMatchObject(prevalidationFailed);
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  it("applies a string-hinted edit that carries originalValues", async () => {
    const s = setup();
    const updates = [
      {
        primaryKeys,
        changes: { payload: "0xnothex" },
        originalValues: { payload: "old text" },
        originalTypes: {
          payload: { category: "text" as const, nativeType: "string" },
        },
      },
    ];
    expect(prepare(s, updates[0])).toMatchObject({ executable: true });
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-row-hints",
      "db",
      "",
      "items",
      updates,
      s.columns,
    );
    expect(result).toMatchObject({ success: true });
    expect(s.updateOne).toHaveBeenCalledOnce();
    const [filter, update] = s.updateOne.mock.calls[0];
    expect(JSON.stringify(filter)).toContain('"old text"');
    expect(JSON.stringify(update)).toContain('"payload":"0xnothex"');
  });
});
