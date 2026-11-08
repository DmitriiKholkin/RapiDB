import { BSON, BSONRegExp } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

describe("MongoDB native regex decoding and EJSON export", () => {
  it.each([
    false,
    true,
  ])("preserves BSON regex flags through wire decoding and table export, fallback=%s", async (fallback) => {
    const documents = [
      {
        _id: "one",
        pattern: new BSONRegExp("^ a b $", "ix"),
        nested: { regex: new BSONRegExp("a.b", "su") },
      },
      { _id: "two", pattern: new BSONRegExp("x y", "mx") },
    ];
    const wire = documents.map((document) => BSON.serialize(document));
    const find = vi.fn(
      (
        _criteria: unknown,
        options: { bsonRegExp?: boolean; promoteValues?: boolean },
      ) => {
        const cursor = {
          limit: (_count: number) => cursor,
          skip: (_offset: number) => cursor,
          sort: (_sort: unknown) => cursor,
          toArray: async () =>
            wire.map((bytes) =>
              BSON.deserialize(bytes, {
                bsonRegExp: options.bsonRegExp,
                promoteValues: options.promoteValues,
              }),
            ),
        };
        return cursor;
      },
    );
    const driver = new MongoDBDriver({
      id: "regex-export-qa",
      name: "Regex QA",
      type: "mongodb",
    });
    Object.assign(driver, {
      requireDb: () => ({ collection: () => ({ find }) }),
      isView: async () => false,
      ...(fallback
        ? {
            buildMongoFilterCriteria: () => {
              throw new Error("force native-page fallback");
            },
          }
        : {}),
    });
    const page = await driver.readTablePage({
      database: "qa",
      schema: "qa",
      table: "regex",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: null,
      skipCount: true,
    });
    expect(find).toHaveBeenCalledTimes(2);
    expect(
      find.mock.calls.every(([, options]) => options.bsonRegExp === true),
    ).toBe(true);
    expect(page.rows[0].pattern).toBe("/^ a b $/ix");
    expect(page.rows[1].pattern).toBe("/x y/mx");
    const exported = driver.formatTableExportChunk(page);
    expect(exported.rows).toEqual(
      documents.map((document) =>
        BSON.EJSON.serialize(document, { relaxed: false }),
      ),
    );
    expect(
      BSON.EJSON.parse(JSON.stringify(exported.rows), { relaxed: false }),
    ).toEqual(documents);
  });
});
