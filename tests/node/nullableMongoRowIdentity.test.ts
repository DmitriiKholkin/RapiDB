import { ObjectId } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const columns: ColumnTypeMeta[] = [
  {
    name: "_id",
    type: "null",
    nativeType: "null",
    category: "text",
    nullable: true,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
  {
    name: "value",
    type: "string",
    nativeType: "string",
    category: "text",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];

describe("MongoDB row identity guards", () => {
  it("rejects absent or incomplete hints before preview or hooks instead of guessing a sampled ObjectId type", async () => {
    const driver = new MongoDBDriver({
      id: "mongo",
      name: "Mongo",
      type: "mongodb",
    });
    const sampledColumns = columns.map((column) =>
      column.name === "_id"
        ? { ...column, nativeType: "objectId", type: "objectId" }
        : column,
    );
    const manager = {
      getConnection: () => ({ id: "mongo" }),
      getDriver: () => driver,
    } as never;
    const service = new TableMutationService(manager, {
      getColumns: async () => sampledColumns,
    });
    const preview = vi.spyOn(driver, "buildMutationPreviewStatement");
    const update = vi
      .spyOn(driver, "updateRows")
      .mockResolvedValue({ affectedRows: 1 });
    const remove = vi.spyOn(driver, "deleteRows").mockResolvedValue({
      affectedRows: 1,
      rowOutcomes: [
        {
          rowIndex: 0,
          primaryKeys: { _id: "507f1f77bcf86cd799439011" },
          success: true,
          status: "deleted",
        },
      ],
    });
    const hex = "507f1f77bcf86cd799439011";
    const valid = { _id: { $rapidbMongoId: { type: "string", value: hex } } };
    for (const id of [
      hex,
      { $rapidbMongoId: undefined },
      { $rapidbMongoId: null },
      { $rapidbMongoId: {} },
      { $rapidbMongoId: { type: undefined, value: hex } },
      { $rapidbMongoId: { type: null, value: hex } },
      { $rapidbMongoId: { type: "string" } },
    ]) {
      const invalid = { _id: id };
      expect(() =>
        prepareApplyChangesPlan(
          manager,
          "mongo",
          "db",
          "",
          "items",
          [
            { primaryKeys: valid, changes: { value: "safe" } },
            { primaryKeys: invalid, changes: { value: "unsafe" } },
          ],
          sampledColumns,
        ),
      ).toThrow(/type hint/);
      await expect(
        service.updateRow("mongo", "db", "", "items", invalid, {
          value: "unsafe",
        }),
      ).rejects.toThrow(/type hint/);
      await expect(
        service.deleteRows("mongo", "db", "", "items", [valid, invalid]),
      ).rejects.toThrow(/type hint/);
    }
    expect(preview).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    expect(remove).not.toHaveBeenCalled();
    await service.updateRow("mongo", "db", "", "items", valid, {
      value: "safe",
    });
    expect(update).toHaveBeenCalledWith(
      expect.objectContaining({
        updates: [{ primaryKeys: { _id: hex }, changes: { value: "safe" } }],
      }),
    );
    await service.deleteRows("mongo", "db", "", "items", [valid]);
    expect(remove).toHaveBeenCalledWith(
      expect.objectContaining({ primaryKeyValuesList: [{ _id: hex }] }),
    );
  });

  it("checks supported ID hints per row even when sampled metadata has native type null", async () => {
    const driver = new MongoDBDriver({
      id: "mongo",
      name: "Mongo",
      type: "mongodb",
    });
    const manager = {
      getConnection: () => ({ id: "mongo" }),
      getDriver: () => driver,
    } as never;
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    const preview = vi.spyOn(driver, "buildMutationPreviewStatement");
    for (const hint of [
      null,
      { type: "string", value: null },
      { type: "unsupported", value: "12" },
      { type: "objectId", value: "bad" },
    ]) {
      const primaryKeys = {
        _id: hint === null ? null : { $rapidbMongoId: hint },
      };
      expect(() =>
        prepareApplyChangesPlan(
          manager,
          "mongo",
          "db",
          "",
          "items",
          [{ primaryKeys, changes: { value: "new" } }],
          columns,
        ),
      ).toThrow();
      await expect(
        service.prepareDeleteRowsPlan("mongo", "db", "", "items", [
          primaryKeys,
        ]),
      ).rejects.toThrow();
    }
    expect(preview).not.toHaveBeenCalled();
    for (const [type, value] of [
      ["string", "null"],
      ["string", ""],
      ["objectId", "507f1f77bcf86cd799439011"],
    ]) {
      const primaryKeys = { _id: { $rapidbMongoId: { type, value } } };
      const prepared = prepareApplyChangesPlan(
        manager,
        "mongo",
        "db",
        "",
        "items",
        [{ primaryKeys, changes: { value: "new" } }],
        columns,
      );
      expect(prepared.executable).toBe(true);
      if (!prepared.executable) throw new Error("Expected an executable plan");
      const expected = type === "objectId" ? new ObjectId(value) : value;
      expect(prepared.plan.updates[0].primaryKeys).toEqual({ _id: expected });
      const deleted = await service.prepareDeleteRowsPlan(
        "mongo",
        "db",
        "",
        "items",
        [primaryKeys],
      );
      expect(deleted?.primaryKeyValuesList).toEqual([{ _id: expected }]);
    }
  });

  it("rejects missing native IDs across the whole direct driver batch before any write", async () => {
    const driver = new MongoDBDriver({
      id: "mongo",
      name: "Mongo",
      type: "mongodb",
    });
    // These guards must run even before a collection is acquired.
    for (const _id of [null, undefined]) {
      await expect(
        driver.updateRows({
          database: "db",
          schema: "",
          table: "items",
          updates: [
            { primaryKeys: { _id: "safe" }, changes: { value: "new" } },
            { primaryKeys: { _id }, changes: { value: "unsafe" } },
          ],
        }),
      ).rejects.toThrow("requires a complete _id");
      await expect(
        driver.deleteRows({
          database: "db",
          schema: "",
          table: "items",
          primaryKeyValuesList: [{ _id: "safe" }, { _id }],
        }),
      ).rejects.toThrow("requires a complete _id");
    }
  });
});
