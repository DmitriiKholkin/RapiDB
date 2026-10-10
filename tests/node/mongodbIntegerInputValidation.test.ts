import { Int32, Long } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";

const column: ColumnTypeMeta = {
  name: "amount",
  type: "int",
  nativeType: "int",
  category: "integer",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};
const primaryKeys = {
  _id: { $rapidbMongoId: { type: "string", value: "row-1" } },
};
function setup(nativeType: string) {
  const config = {
    id: "mongo-integer",
    name: "Mongo Integer",
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
  const columns = [
    {
      ...column,
      name: "_id",
      nativeType: "string",
      category: "text" as const,
      isPrimaryKey: true,
    },
    { ...column, nativeType },
  ];
  const manager = { getDriver: () => driver, getConnection: () => config };
  const service = new TableMutationService(manager as never, {
    getColumns: async () => columns,
  });
  return { driver, updateOne, insertOne, columns, manager, service };
}

describe.each([
  "int",
  "int32",
  "long",
  "int64",
])("MongoDB %s integer input", (nativeType) => {
  const int32 = nativeType === "int" || nativeType === "int32";
  const min = int32 ? -2147483648n : -9223372036854775808n;
  const max = int32 ? 2147483647n : 9223372036854775807n;
  const meta = { ...column, nativeType };

  it.each([
    min.toString(),
    max.toString(),
    "+00042",
    "-00042",
    "-0",
    ' "42" ',
    "0",
  ])("converts %j exactly without changing BSON type", async (input) => {
    const s = setup(nativeType);
    const saved = s.driver.coerceInputValue(input, meta) as Int32 | Long;
    expect(saved).toBeInstanceOf(int32 ? Int32 : Long);
    const expected = BigInt(input === ' "42" ' ? "42" : input).toString();
    expect(saved.toString()).toBe(expected);
    expect(s.driver.checkPersistedEdit(meta, input)).toBeNull();
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-integer",
      "db",
      "",
      "items",
      [{ primaryKeys, changes: { amount: input } }],
      s.columns,
    );
    expect(result.success).toBe(true);
    expect(s.updateOne.mock.calls[0][1].$set.amount.toString()).toBe(expected);
  });

  it.each([
    (min - 1n).toString(),
    (max + 1n).toString(),
    "9".repeat(500),
  ])("rejects overflow %j before any preview or batch writes", async (input) => {
    const s = setup(nativeType);
    expect(() => s.driver.coerceInputValue(input, meta)).toThrow(
      /out of range/,
    );
    expect(s.driver.checkPersistedEdit(meta, input)).toMatchObject({
      ok: false,
      shouldVerify: false,
    });
    const preview = vi.spyOn(s.driver, "buildMutationPreviewStatement");
    const coerce = vi.spyOn(s.driver, "coerceInputValue");
    const result = await applyChangesTransactional(
      s.manager as never,
      "mongo-integer",
      "db",
      "",
      "items",
      [
        { primaryKeys, changes: { amount: "1" } },
        { primaryKeys, changes: { amount: input } },
      ],
      s.columns,
    );
    expect(result).toMatchObject({
      success: false,
      failedRows: [1],
      rowOutcomes: [
        { status: "skipped" },
        {
          status: "prevalidation_failed",
          columns: ["amount"],
          message: expect.stringMatching(/out of range/),
        },
      ],
    });
    for (const run of [
      () =>
        s.service.updateRow("mongo-integer", "db", "", "items", primaryKeys, {
          amount: input,
        }),
      () =>
        s.service.insertRow("mongo-integer", "db", "", "items", {
          amount: input,
        }),
      () =>
        s.service.prepareInsertRow("mongo-integer", "db", "", "items", {
          amount: input,
        }),
    ])
      await expect(run()).rejects.toMatchObject({
        status: "prevalidation_failed",
        columns: ["amount"],
      });
    for (const spy of [preview, coerce, s.updateOne, s.insertOne])
      expect(spy).not.toHaveBeenCalled();
    expect(s.driver.coerceOriginalValue(input, meta)).toBe(input);
    expect(s.driver.formatOutputValue(input, meta)).toBe(input);
  });

  it("preserves non-integer strings, null and original mixed-type values", () => {
    const s = setup(nativeType);
    for (const input of ["plain text", "1.5", "", null])
      expect(s.driver.coerceInputValue(input, meta)).toBe(input);
    expect(s.driver.coerceInputValue(NULL_SENTINEL, meta)).toBeNull();
    expect(s.driver.coerceOriginalValue(NULL_SENTINEL, meta)).toBeNull();
  });
});
