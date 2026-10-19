import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type {
  DriverUpdateRowsRequest,
  MongoValueTypeHint,
} from "../../src/extension/dbDrivers/types";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const unsupportedNames = ["a.b", ".a", "a.", "$literal", "$where"];
const primaryKeys = {
  _id: { $rapidbMongoId: { type: "string", value: "test" } },
};
const textHint: MongoValueTypeHint = {
  nativeType: "string",
  category: "text",
};
type Update = DriverUpdateRowsRequest["updates"][number];

function setup() {
  const config = {
    id: "mongo-literal-fields",
    name: "Mongo literal fields",
    type: "mongodb",
  } as const;
  const driver = new MongoDBDriver(config);
  const document = {
    _id: "test",
    "a.b": "old",
    a: { b: "old" },
    $literal: "old",
    plain: "old",
    price$usd: "old",
  };
  const updateOne = vi
    .fn()
    .mockResolvedValue({ acknowledged: true, matchedCount: 1 });
  const cursor = {
    limit: vi.fn().mockReturnThis(),
    sort: vi.fn().mockReturnThis(),
    skip: vi.fn().mockReturnThis(),
    toArray: async () => [document],
  };
  const find = vi.fn(() => cursor);
  const collection = vi.fn(() => ({ find, updateOne }));
  Object.assign(driver, {
    connected: true,
    client: {
      db: () => ({
        collection,
        listCollections: () => ({
          toArray: async () => [
            { name: "items", type: "collection", options: {} },
          ],
        }),
      }),
    },
  });
  const manager = { getDriver: () => driver, getConnection: () => config };
  const apply = async (updates: Update[], omitUnsupportedMetadata = false) => {
    const columns = await driver.describeColumns("db", "", "items");
    return applyChangesTransactional(
      manager as never,
      config.id,
      "db",
      "",
      "items",
      updates,
      omitUnsupportedMetadata
        ? columns.filter((column) => !column.isReadOnly)
        : columns,
    );
  };
  const request = (updates: Update[]): DriverUpdateRowsRequest => ({
    database: "db",
    schema: "",
    table: "items",
    updates,
  });
  return {
    driver,
    document,
    updateOne,
    collection,
    cursor,
    find,
    apply,
    request,
    manager,
  };
}

describe("MongoDB Table Viewer literal field edit safety", () => {
  it("marks unsupported literal columns read-only without hiding their values", async () => {
    const { driver, document } = setup();
    const columns = await driver.describeColumns("db", "", "items");
    for (const name of ["a.b", "$literal"]) {
      expect(columns.find((column) => column.name === name)).toMatchObject({
        name,
        isReadOnly: true,
      });
      expect(document[name as keyof typeof document]).toBe("old");
    }
    for (const name of ["plain", "a", "price$usd"])
      expect(
        columns.find((column) => column.name === name)?.isReadOnly,
      ).not.toBe(true);
  });

  it.each([
    false,
    true,
  ])("marks page fallback columns read-only after an empty schema sample (client fallback: %j)", async (clientFallback) => {
    const s = setup();
    s.find.mockReturnValueOnce({
      ...s.cursor,
      toArray: async () => [],
    });
    if (clientFallback)
      s.cursor.sort.mockImplementation(() => {
        throw new Error("server unavailable");
      });
    const page = await s.driver.readTablePage({
      database: "db",
      schema: "",
      table: "items",
      page: 1,
      pageSize: 25,
      filters: [],
      sort: null,
      skipCount: true,
    });
    for (const name of ["a.b", "$literal"])
      expect(
        page.columns.find((column) => column.name === name)?.isReadOnly,
      ).toBe(true);
    expect(page.rows[0]["a.b"]).toBe("old");
  });

  it.each(
    unsupportedNames,
  )("rejects stale writable metadata for %j", async (name) => {
    const { driver } = setup();
    const [sample] = await driver.describeColumns("db", "", "items");
    expect(
      driver.checkPersistedEdit({ ...sample, name, isReadOnly: false }, "NEW"),
    ).toMatchObject({
      ok: false,
      shouldVerify: false,
      message: expect.stringContaining("read-only"),
    });
  });

  it.each([
    "a.b",
    "$literal",
  ])("prevalidates the whole Viewer batch before any preview for %j", async (name) => {
    const s = setup();
    const preview = vi.spyOn(s.driver, "buildMutationPreviewStatement");
    const result = await s.apply([
      { primaryKeys, changes: { plain: "NEW" } },
      {
        primaryKeys,
        changes: { [name]: "NEW" },
        originalValues: { [name]: "old" },
        originalTypes: { [name]: textHint },
      },
    ]);
    expect(result).toMatchObject({
      success: false,
      failedRows: [1],
      rowOutcomes: [{ status: "skipped" }, { status: "prevalidation_failed" }],
    });
    expect(preview).not.toHaveBeenCalled();
    expect(s.updateOne).not.toHaveBeenCalled();
    expect(s.document).toMatchObject({
      "a.b": "old",
      a: { b: "old" },
      plain: "old",
      $literal: "old",
    });
  });

  describe.each([
    "changes",
    "originalValues",
    "originalTypes",
  ] as const)("%s input", (source) => {
    function unsafeUpdate(name: string): Update {
      return {
        primaryKeys: { _id: "test" },
        changes: { plain: "NEW" },
        [source]: { [name]: source === "originalTypes" ? textHint : "NEW" },
      };
    }

    it.each(
      unsupportedNames,
    )("rejects raw mixed batches before opening a collection or writing for %j", async (name) => {
      const s = setup();
      await expect(
        s.driver.updateRows(
          s.request([
            { primaryKeys: { _id: "test" }, changes: { plain: "NEW" } },
            unsafeUpdate(name),
          ]),
        ),
      ).rejects.toThrow(/literal field.*read-only/);
      expect(s.collection).not.toHaveBeenCalled();
      expect(s.updateOne).not.toHaveBeenCalled();
    });

    it.each(
      unsupportedNames,
    )("rejects %j before serializing an executable preview", (name) => {
      const s = setup();
      const serialize = vi.spyOn(
        s.driver as unknown as { serializeMongosh(value: unknown): string },
        "serializeMongosh",
      );
      expect(() =>
        s.driver.buildMutationPreviewStatement(
          "update",
          "db",
          "",
          "items",
          unsafeUpdate(name),
        ),
      ).toThrow(/literal field.*read-only/);
      expect(serialize).not.toHaveBeenCalled();
      expect(s.updateOne).not.toHaveBeenCalled();
    });

    it.each([
      "a.b",
      "$literal",
    ])("rejects unsampled %j through the Viewer apply path before writes", async (name) => {
      const s = setup();
      await expect(
        s.apply(
          [
            { primaryKeys, changes: { plain: "NEW" } },
            { ...unsafeUpdate(name), primaryKeys },
          ],
          true,
        ),
      ).rejects.toThrow(/literal field.*read-only/);
      expect(s.updateOne).not.toHaveBeenCalled();
    });
  });

  it.each([
    "null",
    "undefined",
    "missing",
  ])("rejects unsupported originalTypes-only %s guards", async (nativeType) => {
    const s = setup();
    for (const name of ["a.b", "$literal"]) {
      const update = {
        primaryKeys: { _id: "test" },
        changes: { plain: "NEW" },
        originalTypes: { [name]: { nativeType, category: "other" as const } },
      };
      await expect(s.driver.updateRows(s.request([update]))).rejects.toThrow(
        /read-only/,
      );
      expect(() =>
        s.driver.buildMutationPreviewStatement(
          "update",
          "db",
          "",
          "items",
          update,
        ),
      ).toThrow(/read-only/);
    }
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  it.each([
    "a.b",
    "$literal",
  ])("rejects unsampled %j before a single-row service drops the field", async (name) => {
    const s = setup();
    const columns = await s.driver.describeColumns("db", "", "items");
    const updateRows = vi.spyOn(s.driver, "updateRows");
    const service = new TableMutationService(s.manager as never, {
      getColumns: async () => columns.filter((column) => !column.isReadOnly),
    });
    await expect(
      service.updateRow(
        "mongo-literal-fields",
        "db",
        "",
        "items",
        primaryKeys,
        {
          plain: "NEW",
          [name]: "NEW",
        },
      ),
    ).rejects.toThrow(/literal field.*read-only/);
    expect(updateRows).not.toHaveBeenCalled();
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  it("rejects even undefined unsupported field entries in direct requests", async () => {
    const s = setup();
    await expect(
      s.driver.updateRows(
        s.request([
          { primaryKeys: { _id: "test" }, changes: { "a.b": undefined } },
        ]),
      ),
    ).rejects.toThrow(/read-only/);
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  it.each([
    "plain",
    "price$usd",
  ])("ordinary field %j edits retain optimistic guards and preview", async (name) => {
    const s = setup();
    const preview = vi.spyOn(s.driver, "buildMutationPreviewStatement");
    const result = await s.apply([
      {
        primaryKeys,
        changes: { [name]: "NEW" },
        originalValues: { [name]: "old" },
        originalTypes: { [name]: textHint },
      },
    ]);
    expect(result.success).toBe(true);
    expect(s.updateOne).toHaveBeenCalledExactlyOnceWith(
      { _id: { $eq: "test" }, [name]: "old" },
      { $set: { [name]: "NEW" } },
    );
    expect(preview.mock.results[0].value).toContain(".updateOne(");
  });

  it("allows replacing an ordinary object field containing literal nested keys", async () => {
    const s = setup();
    const value = { "b.c": "NEW", $literal: "NEW" };
    const result = await s.apply([
      { primaryKeys, changes: { a: JSON.stringify(value) } },
    ]);
    expect(result.success).toBe(true);
    expect(s.updateOne).toHaveBeenCalledExactlyOnceWith(
      { _id: { $eq: "test" } },
      { $set: { a: value } },
    );
  });

  it("does not block legitimate dotted paths in the Query Editor", async () => {
    const s = setup();
    await s.driver.query(
      'db.items.updateOne({ _id: "test" }, { $set: { "a.b": "NEW" } })',
    );
    expect(s.updateOne).toHaveBeenCalledExactlyOnceWith(
      { _id: "test" },
      { $set: { "a.b": "NEW" } },
    );
  });
});
