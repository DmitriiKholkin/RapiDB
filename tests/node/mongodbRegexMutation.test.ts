import { BSON, BSONRegExp } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { TableReadService } from "../../src/extension/table/tableReadService";

function harness(pattern: string, options: string) {
  const config = {
    id: "regex-mutation",
    name: "Regex",
    type: "mongodb" as const,
  };
  const driver = new MongoDBDriver(config);
  let wire = BSON.serialize({
    _id: "one",
    pattern: new BSONRegExp(pattern, options),
  });
  const sameWireValue = (a: unknown, b: unknown) =>
    Buffer.from(BSON.serialize({ value: a })).equals(
      Buffer.from(BSON.serialize({ value: b })),
    );
  const updateOne = vi.fn(
    async (
      criteria: Record<string, unknown>,
      update: { $set: Record<string, unknown> },
    ) => {
      const decoded = BSON.deserialize(BSON.serialize(criteria), {
        bsonRegExp: true,
      });
      const stored = BSON.deserialize(wire, { bsonRegExp: true });
      const matches =
        sameWireValue(decoded.pattern.$eq, stored.pattern) &&
        sameWireValue(decoded.$expr.$and[0].$eq[1].$literal, stored.pattern);
      if (matches) wire = BSON.serialize({ ...stored, ...update.$set });
      return {
        acknowledged: true,
        matchedCount: matches ? 1 : 0,
        modifiedCount: matches ? 1 : 0,
      };
    },
  );
  const insertOne = vi.fn(async (value: Record<string, unknown>) => {
    wire = BSON.serialize(value);
    return { acknowledged: true, insertedId: "two" };
  });
  const find = (_criteria: unknown, options: BSON.DeserializeOptions) => {
    const cursor = {
      sort: () => cursor,
      skip: () => cursor,
      limit: () => cursor,
      toArray: async () => [BSON.deserialize(wire, options)],
    };
    return cursor;
  };
  Object.assign(driver, {
    requireDb: () => ({
      collection: () => ({
        find,
        updateOne,
        insertOne,
        countDocuments: async () => 1,
      }),
    }),
    isView: async () => false,
  });
  const manager = {
    getConnection: () => config,
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  const read = new TableReadService(manager);
  return {
    driver,
    manager,
    read,
    updateOne,
    insertOne,
    stored: () => BSON.deserialize(wire, { bsonRegExp: true }),
  };
}

describe("MongoDB table regex mutation wire roundtrip", () => {
  it.each([
    "/after/g",
    "/after/y",
    "/after/d",
    "/after/v",
    "/after/z",
    "/unclosed",
    "/bad\0value/i",
  ])("rejects invalid regex %j before updating or inserting BSON", async (value) => {
    const h = harness("before", "i");
    const page = await h.read.getPage(
      "regex-mutation",
      "qa",
      "qa",
      "items",
      1,
      10,
      [],
      null,
      true,
    );
    expect(() =>
      prepareApplyChangesPlan(
        h.manager,
        "regex-mutation",
        "qa",
        "qa",
        "items",
        [
          {
            primaryKeys: {
              _id: { $rapidbMongoId: { type: "string", value: "one" } },
            },
            changes: { pattern: value },
            originalValues: { pattern: page.rows[0].pattern },
            originalTypes: page.mongoValueTypes?.[0],
          },
        ],
        page.columns,
      ),
    ).toThrow(/MongoDB regex literal/);
    const service = new TableMutationService(h.manager, {
      getColumns: async () => page.columns,
    });
    await expect(
      service.prepareInsertRow("regex-mutation", "qa", "qa", "items", {
        _id: "two",
        pattern: value,
      }),
    ).rejects.toThrow(/MongoDB regex literal/);
    expect(h.updateOne).not.toHaveBeenCalled();
    expect(h.insertOne).not.toHaveBeenCalled();
    expect(h.stored().pattern).toEqual(new BSONRegExp("before", "i"));
  });

  it.each(
    [
      ["a.b", "s"],
      ["^a.b$", "su"],
      [" a b ", "ix"],
      ["(?i)word", "x"],
      ["before", "l"],
      ["a/b\\c", "m"],
      ["a\nb", ""],
    ].flatMap(([pattern, flags]) =>
      ["direct", "preview"].map((mode) => [pattern, flags, mode]),
    ),
  )("preserves original and new BSON pattern/options (%j, %s, %s)", async (pattern, flags, mode) => {
    const h = harness(pattern, flags);
    const page = await h.read.getPage(
      "regex-mutation",
      "qa",
      "qa",
      "items",
      1,
      10,
      [],
      null,
      true,
    );
    const replacement = new BSONRegExp("replacement", flags);
    const prepared = prepareApplyChangesPlan(
      h.manager,
      "regex-mutation",
      "qa",
      "qa",
      "items",
      [
        {
          primaryKeys: {
            _id: { $rapidbMongoId: { type: "string", value: "one" } },
          },
          changes: { pattern: `/replacement/${flags}` },
          originalValues: { pattern: page.rows[0].pattern },
          originalTypes: page.mongoValueTypes?.[0],
        },
      ],
      page.columns,
    );
    if (!prepared.executable) throw new Error(JSON.stringify(prepared.result));
    if (mode === "preview") {
      await h.driver.query(prepared.plan.previewStatements.join("\n"));
    } else {
      expect(
        await executePreparedApplyPlan(h.manager, prepared.plan),
      ).toMatchObject({ success: true });
    }
    expect(h.updateOne).toHaveBeenCalledTimes(1);
    expect(h.stored().pattern).toEqual(replacement);
    const refreshed = await h.read.getPage(
      "regex-mutation",
      "qa",
      "qa",
      "items",
      1,
      10,
      [],
      null,
      true,
    );
    expect(refreshed.rows[0].pattern).toBe(`/replacement/${flags}`);
  });

  it.each([
    "direct",
    "preview",
  ])("preserves native regex flags when inserting from a table draft (%s)", async (mode) => {
    const h = harness("a.b", "s");
    const page = await h.read.getPage(
      "regex-mutation",
      "qa",
      "qa",
      "items",
      1,
      10,
      [],
      null,
      true,
    );
    const service = new TableMutationService(h.manager, {
      getColumns: async () => page.columns,
    });
    const plan = await service.prepareInsertRow(
      "regex-mutation",
      "qa",
      "qa",
      "items",
      { _id: "two", pattern: "/(?i)new.pattern/sux" },
    );
    if (mode === "preview") {
      await h.driver.query(plan.previewStatements.join("\n"));
    } else {
      await service.executePreparedInsertPlan(plan);
    }
    expect(h.insertOne).toHaveBeenCalledTimes(1);
    expect(h.stored().pattern).toEqual(
      new BSONRegExp("(?i)new.pattern", "sux"),
    );
  });
});
