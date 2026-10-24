import { randomUUID } from "node:crypto";
import { Client } from "@elastic/elasticsearch";
import { Long, MongoClient } from "mongodb";
import { expect, it } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

const liveSuite = process.env.RAPIDB_LIVE_NOSQL === "1";
const mongoUri =
  process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL ??
  (liveSuite
    ? "mongodb://mongo_admin:mongo_pass123@127.0.0.1:27017/admin"
    : undefined);
const elasticEndpoint =
  process.env.RAPIDB_ES_IDENTITY_TEST_URL ??
  (liveSuite ? "http://127.0.0.1:9200" : undefined);

it.skipIf(!mongoUri)(
  "deletes only the exact multiline MongoDB filter match on a real server",
  async () => {
    const uri = mongoUri ?? "";
    const database = `rapidb_text_${randomUUID().replaceAll("-", "")}`;
    const client = new MongoClient(uri);
    const driver = new MongoDBDriver({
      id: database,
      name: database,
      type: "mongodb",
      connectionUri: uri,
      database,
    });
    try {
      await client.connect();
      await driver.connect();
      const collection = client.db(database).collection("items");
      await collection.insertMany([
        { selected: true, text: "first\n// stored text\nlast" },
        { selected: false, text: "first\nlast" },
      ]);
      await driver.query(
        "db.items.deleteMany({text: `first\n// stored text\nlast`})",
      );
      expect(await collection.countDocuments({ selected: true })).toBe(0);
      expect(await collection.countDocuments({ selected: false })).toBe(1);
      const integerCollection = client
        .db(database)
        .collection<{ _id: Long }>("items");
      await integerCollection.insertMany([
        { _id: new Long(1) },
        { _id: new Long(2) },
      ]);
      await expect(
        driver.query(
          'db.items.deleteOne({_id:NumberLong("2")}); db.items.deleteOne({_id:18446744073709551617n})',
        ),
      ).rejects.toThrow(/int64.*range/i);
      expect(
        await integerCollection.countDocuments({
          _id: { $in: [new Long(1), new Long(2)] },
        }),
      ).toBe(2);
    } finally {
      await driver.disconnect();
      await client.db(database).dropDatabase();
      await client.close();
    }
  },
);

it.skipIf(!elasticEndpoint)(
  "does not turn an Elasticsearch document path into an index deletion on a real server",
  async () => {
    const endpoint = elasticEndpoint ?? "";
    const index = `rapidb-path-${randomUUID()}`;
    const client = new Client({
      node: endpoint,
      headers: {
        accept: "application/vnd.elasticsearch+json; compatible-with=8",
        "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
      },
    });
    const driver = new ElasticsearchDriver({
      id: index,
      name: index,
      type: "elasticsearch",
      endpoint,
    });
    try {
      await client.indices.create({ index });
      await client.index({ index, id: "keep", document: { keep: true } });
      await driver.connect();
      for (const id of ["..", "%2e%2e", ".%2E"]) {
        await expect(
          driver.query(
            `DELETE /${index}/_doc/keep\nDELETE /${index}/_doc/${id}`,
          ),
        ).rejects.toThrow(/path/i);
        expect(await client.indices.exists({ index })).toBe(true);
        expect(await client.exists({ index, id: "keep" })).toBe(true);
      }
    } finally {
      await driver.disconnect();
      await client.indices.delete({ index }, { ignore: [404] });
      await client.close();
    }
  },
);
