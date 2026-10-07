import { Binary } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import {
  applyChangesTransactional,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";

const binaryColumn: ColumnTypeMeta = {
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
  ...binaryColumn,
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
const malformedHex = [
  "0xA",
  "0xABC",
  "0XABC",
  "\\xABC",
  "\\XABC",
  "0xGG",
  "0XGG",
  "\\xGG",
  "\\XGG",
  "0xAB CD",
  "0xAB!",
  ' "0xA" ',
];

function setup(column = binaryColumn) {
  const config = {
    id: "mongo-binary",
    name: "Mongo Binary",
    type: "mongodb",
  } as const;
  const driver = new MongoDBDriver(config);
  const updateOne = vi
    .fn()
    .mockResolvedValue({ acknowledged: true, matchedCount: 1 });
  const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => ({ updateOne, insertOne }) }) },
  });
  const columns = [idColumn, column];
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
  };
  const service = new TableMutationService(manager as never, {
    getColumns: async () => columns,
  });
  return { driver, columns, manager, service, updateOne, insertOne };
}

describe("R10 Mongo binary input versus tolerant output", () => {
  it.each(
    malformedHex,
  )("rejects explicit malformed hex %j in coercion", (value) => {
    const { driver } = setup();
    expect(() => driver.coerceInputValue(value, binaryColumn)).toThrow(
      /Invalid hex/,
    );
    expect(driver.checkPersistedEdit(binaryColumn, value)).toMatchObject({
      ok: false,
      shouldVerify: false,
      message: expect.stringMatching(/Invalid hex/),
    });
    const output = value === ' "0xA" ' ? "0xA" : value;
    expect(driver.formatOutputValue(value, binaryColumn)).toBe(output);
    expect(driver.coerceOriginalValue(value, binaryColumn)).toBe(output);
  });

  it.each(
    malformedHex,
  )("rejects %j for the whole Apply batch before preview or $set", async (value) => {
    const s = setup();
    const coerce = vi.spyOn(s.driver, "coerceInputValue");
    const preview = vi.spyOn(s.driver, "buildMutationPreviewStatement");
    const update = vi.spyOn(s.driver, "updateRows");
    const updates = [
      { primaryKeys, changes: { payload: "0xABCD" } },
      { primaryKeys, changes: { payload: value } },
    ];
    const expected = {
      success: false,
      failedRows: [1],
      rowOutcomes: [
        { rowIndex: 0, status: "skipped" },
        {
          rowIndex: 1,
          status: "prevalidation_failed",
          columns: ["payload"],
          message: expect.stringMatching(/Invalid hex/),
        },
      ],
    };
    expect(
      prepareApplyChangesPlan(
        s.manager as never,
        "mongo-binary",
        "db",
        "",
        "items",
        updates,
        s.columns,
      ),
    ).toMatchObject({
      executable: false,
      result: expected,
    });
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-binary",
      "db",
      "",
      "items",
      updates,
      s.columns,
    );
    expect(result).toMatchObject(expected);
    expect(result.changesPossible).not.toBe(true);
    expect(result.outcomeUnknown).not.toBe(true);
    for (const spy of [coerce, preview, update, s.updateOne, s.insertOne])
      expect(spy).not.toHaveBeenCalled();
  });

  it.each(
    malformedHex,
  )("prevalidates %j through public UPDATE/INSERT preparation", async (value) => {
    const s = setup({ ...binaryColumn, nativeType: "binData(128)" });
    const coerce = vi.spyOn(s.driver, "coerceInputValue");
    const preview = vi.spyOn(s.driver, "buildMutationPreviewStatement");
    for (const run of [
      () =>
        s.service.updateRow("mongo-binary", "db", "", "items", primaryKeys, {
          payload: value,
        }),
      () =>
        s.service.insertRow("mongo-binary", "db", "", "items", {
          payload: value,
        }),
      () =>
        s.service.prepareInsertRow("mongo-binary", "db", "", "items", {
          payload: value,
        }),
    ]) {
      await expect(run()).rejects.toMatchObject({
        status: "prevalidation_failed",
        columns: ["payload"],
        message: expect.stringMatching(/Invalid hex/),
      });
    }
    for (const spy of [coerce, preview, s.updateOne, s.insertOne])
      expect(spy).not.toHaveBeenCalled();
  });

  it.each([
    ["0xABCD", "abcd", 128],
    ["0XABCD", "abcd", 128],
    ["\\xABCD", "abcd", 128],
    ["\\XABCD", "abcd", 128],
    ["ABCD", "abcd", 128],
    ["1234", "1234", 128],
    ["q80=", "abcd", 128],
    ['BinData(2, "q80=")', "abcd", 2],
    ["0x", "", 128],
    ["0X", "", 128],
    ["\\x", "", 128],
    ["\\X", "", 128],
  ] as const)("sends valid %j through Apply as Binary", async (value, hex, subtype) => {
    const column = { ...binaryColumn, nativeType: "binData(128)" };
    const s = setup(column);
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-binary",
      "db",
      "",
      "items",
      [{ primaryKeys, changes: { payload: value } }],
      s.columns,
    );
    expect(result).toMatchObject({
      success: true,
      rowOutcomes: [{ status: "applied" }],
    });
    const saved = s.updateOne.mock.calls[0][1].$set.payload;
    expect(saved).toBeInstanceOf(Binary);
    expect(saved.sub_type).toBe(subtype);
    expect(Buffer.from(saved.buffer)).toEqual(Buffer.from(hex, "hex"));
    const displayed = s.driver.formatOutputValue(saved, column);
    // Hex display carries its subtype in column metadata, not in the text.
    const roundtrip = s.driver.coerceInputValue(displayed, {
      ...column,
      bsonSubtype: subtype,
    }) as Binary;
    expect(roundtrip).toBeInstanceOf(Binary);
    expect(roundtrip.sub_type).toBe(subtype);
    expect(Buffer.from(roundtrip.buffer)).toEqual(Buffer.from(hex, "hex"));
  });

  it("preserves bsonSubtype metadata for bare hex, base64 and empty hex", async () => {
    const s = setup({
      ...binaryColumn,
      nativeType: "binData(2)",
      bsonSubtype: 128,
    });
    for (const payload of ["ABCD", "q80=", "0x"]) {
      await s.service.updateRow(
        "mongo-binary",
        "db",
        "",
        "items",
        primaryKeys,
        {
          payload,
        },
      );
      const saved = s.updateOne.mock.calls.at(-1)?.[1].$set.payload;
      expect(saved).toBeInstanceOf(Binary);
      expect(saved.sub_type).toBe(128);
    }
  });

  it.each([
    "plain text",
    "ABC",
    "123",
    "",
    NULL_SENTINEL,
  ])("still permits non-explicit-hex type editing %j", async (value) => {
    const s = setup();
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-binary",
      "db",
      "",
      "items",
      [{ primaryKeys, changes: { payload: value } }],
      s.columns,
    );
    expect(result.success).toBe(true);
    expect(s.updateOne.mock.calls[0][1].$set.payload).toBe(
      value === NULL_SENTINEL ? null : value,
    );
  });

  it.each([
    "0xA",
    "0xGG",
    "\\xGG",
  ])("can edit another field while binary original values contain %j", async (value) => {
    const s = setup();
    s.columns.push({ ...idColumn, name: "title", isPrimaryKey: false });
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-binary",
      "db",
      "",
      "items",
      [
        {
          primaryKeys,
          changes: { title: "after" },
          originalValues: { payload: value, title: "before" },
        },
      ],
      s.columns,
    );
    expect(result.success).toBe(true);
    expect(s.updateOne).toHaveBeenCalledOnce();
    expect(s.updateOne.mock.calls[0][0]).toMatchObject({
      payload: value,
      title: "before",
    });
    expect(s.updateOne.mock.calls[0][1]).toEqual({ $set: { title: "after" } });
  });

  it.each([
    "0xA",
    "0xGG",
    "\\xGG",
  ])("permits literal %j in string fields and original-value predicates", async (value) => {
    const s = setup();
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-binary",
      "db",
      "",
      "items",
      [
        {
          primaryKeys,
          changes: { payload: "ordinary string" },
          originalValues: { payload: value },
        },
      ],
      s.columns,
    );
    expect(result.success).toBe(true);
    expect(s.updateOne.mock.calls[0][0].payload).toBe(value);
    expect(s.driver.formatOutputValue(value, binaryColumn)).toBe(value);

    const text = setup({
      ...binaryColumn,
      nativeType: "string",
      type: "string",
      category: "text",
    });
    await text.service.updateRow(
      "mongo-binary",
      "db",
      "",
      "items",
      primaryKeys,
      { payload: value },
    );
    expect(text.updateOne.mock.calls[0][1].$set.payload).toBe(value);
  });
});
