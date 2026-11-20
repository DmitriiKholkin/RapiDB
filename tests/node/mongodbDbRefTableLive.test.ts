import { randomUUID } from "node:crypto";
import { DBRef, Long, MongoClient, ObjectId } from "mongodb";
import { expect, it } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";

const uri = process.env.RAPIDB_MONGO_DBREF_TEST_URL;

it.skipIf(!uri)(
  "preserves DBRef IDs through table reads, edits, and optimistic matching in MongoDB",
  async () => {
    const endpoint = new URL(uri ?? "");
    if (endpoint.protocol !== "mongodb:" || endpoint.hostname !== "127.0.0.1") {
      throw new Error("Use a disposable loopback MongoDB for this test.");
    }
    const database = `rapidb_dbref_${randomUUID().replaceAll("-", "")}`;
    const config = {
      id: database,
      name: "DBRef verification",
      type: "mongodb",
      mongoConnectionMode: "uri",
      connectionUri: endpoint.toString(),
      database,
    } as const;
    const client = new MongoClient(endpoint.toString(), {
      serverSelectionTimeoutMS: 4_000,
    });
    const driver = new MongoDBDriver(config);
    const manager = { getDriver: () => driver, getConnection: () => config };
    const table = client
      .db(database)
      .collection<{ _id: string; ref: DBRef }>("items", {
        promoteValues: false,
      });
    const hex = "507f1f77bcf86cd799439011";
    const oid = new ObjectId(hex);
    const makeRef = (id: unknown) =>
      new DBRef("other", id as ObjectId, "related", {
        revision: Long.fromString("9007199254740993"),
      });
    const referenceDocument = ($id: unknown) => ({
      $ref: "other",
      $id,
      $db: "related",
      revision: { $numberLong: "9007199254740993" },
    });

    try {
      await client.connect();
      await table.insertMany([
        { _id: "oid", ref: makeRef(oid) },
        { _id: "string", ref: makeRef(hex) },
      ]);
      const storedId = async (key: string): Promise<unknown> =>
        (await table.findOne({ _id: key }))?.ref.oid;
      expect(await storedId("oid")).toEqual(oid);
      expect(await storedId("string")).toBe(hex);

      await driver.connect();
      const page = await driver.readTablePage({
        database,
        schema: "",
        table: "items",
        page: 1,
        pageSize: 25,
        sort: null,
        filters: [],
        skipCount: false,
      });
      // Only the JSON-safe table text and hints survive the webview boundary.
      const { rows, mongoValueTypes } = JSON.parse(
        JSON.stringify(page),
      ) as typeof page;
      const apply = (index: number, ref: string) =>
        applyChangesTransactional(
          manager as never,
          config.id,
          database,
          "",
          "items",
          [
            {
              primaryKeys: {
                _id: {
                  $rapidbMongoId: { type: "string", value: rows[index]._id },
                },
              },
              changes: { ref },
              originalValues: { ref: rows[index].ref },
              originalTypes: mongoValueTypes?.[index],
            },
          ],
          page.columns,
        );

      const updateCollection = ($id: unknown) =>
        JSON.stringify({ ...referenceDocument($id), $ref: "updated" });
      // The old baseline coerced this stored string's ID to ObjectId, so a
      // legitimate update to the reference could never match the row.
      expect.soft(await apply(1, updateCollection("different"))).toMatchObject({
        success: true,
        rowOutcomes: [{ status: "applied" }],
      });
      expect
        .soft(rows.map((row) => row.ref))
        .toEqual([
          JSON.stringify(referenceDocument({ $oid: hex })),
          JSON.stringify(referenceDocument({ $string: hex })),
        ]);

      // Change a regular ObjectId reference to the identically spelled string.
      expect(
        await apply(0, JSON.stringify(referenceDocument({ $string: hex }))),
      ).toMatchObject({ success: true, rowOutcomes: [{ status: "applied" }] });
      expect(await storedId("oid")).toBe(hex);
      expect(await storedId("string")).toBe("different");
      expect((await table.findOne({ _id: "oid" }))?.ref).toEqual(makeRef(hex));

      // Reusing the pre-edit ObjectId baseline is now a real conflict.
      expect(
        await apply(0, JSON.stringify(referenceDocument({ $oid: hex }))),
      ).toMatchObject({
        success: false,
        conflictRows: [0],
        rowOutcomes: [{ status: "not_applied" }],
      });
      expect(await storedId("oid")).toBe(hex);

      const raw = await driver.query(
        `db.getSiblingDB(${JSON.stringify(database)}).items.find({_id:'oid'})`,
      );
      expect(raw.rows[0].__col_1).toBe(
        JSON.stringify({ $ref: "other", $id: hex, $db: "related" }),
      );
      expect(raw.exportRows?.[0]?.__col_1).toEqual(makeRef(hex));
      expect.assert(raw.serializeJsonExportRow);
      expect(
        JSON.parse(
          raw.serializeJsonExportRow([["ref", raw.exportRows?.[0]?.__col_1]]),
        ),
      ).toEqual({ ref: referenceDocument(hex) });
    } finally {
      await driver.disconnect();
      try {
        await client.db(database).dropDatabase();
      } finally {
        await client.close();
      }
    }
  },
  20_000,
);
