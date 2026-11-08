import { randomUUID } from "node:crypto";
import { Client } from "@elastic/elasticsearch";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";

const endpoint = process.env.RAPIDB_ES_IDENTITY_TEST_URL;

describe.skipIf(!endpoint)(
  "Elasticsearch filtered paging — live small result window",
  () => {
    const client = new Client({
      node: endpoint ?? "http://localhost:9200",
      maxRetries: 0,
      headers: {
        accept: "application/vnd.elasticsearch+json; compatible-with=8",
        "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
      },
    });
    const indices: string[] = [];
    const drivers: ElasticsearchDriver[] = [];

    afterEach(async () => {
      await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
      for (const index of indices.splice(0))
        await client.indices.delete({ index });
    });
    afterAll(() => client.close());

    async function fixture(count: number, window: number) {
      const index = `rapidb-filtered-paging-${randomUUID()}`;
      await client.indices.create({
        index,
        settings: {
          number_of_shards: 2,
          number_of_replicas: 0,
          max_result_window: window,
        },
      });
      indices.push(index);
      const seeded = await client.bulk({
        refresh: true,
        operations: Array.from({ length: count }, (_, ordinal) => [
          { index: { _index: index, _id: `doc-${ordinal}` } },
          { ordinal },
        ]).flat(),
      });
      expect(seeded.errors).toBe(false);
      const driver = new ElasticsearchDriver({
        id: "es-filtered-paging-live",
        name: "ES filtered paging live",
        type: "elasticsearch",
        endpoint,
      });
      drivers.push(driver);
      await driver.connect();
      return { driver, index };
    }

    it.each([
      1, 3,
    ])("returns exact filtered/sorted pages after multiple window-%i batches", async (window) => {
      const { driver, index } = await fixture(17, window);
      const page = await driver.readTablePage({
        database: "default",
        schema: "indices",
        table: index,
        page: 2,
        pageSize: 2,
        skipCount: false,
        filters: [
          { column: "_id", operator: "in", value: "doc-1,doc-9,doc-16" },
        ],
        sort: { column: "_id", direction: "desc" },
      });
      expect(page.rows.map((row) => row._id)).toEqual(["doc-1"]);
      expect(page.totalCount).toBe(3);
      expect(JSON.parse(String(page.rows[0]._source))).toEqual({ ordinal: 1 });
      expect(page.rows[0]._identity).toEqual(expect.any(String));
    }, 30000);

    it("fails closed above the full-index cap even for a one-row filter and a window of 3", async () => {
      const { driver, index } = await fixture(10001, 3);
      await expect(
        driver.readTablePage({
          database: "default",
          schema: "indices",
          table: index,
          page: 1,
          pageSize: 1,
          skipCount: true,
          filters: [{ column: "_id", operator: "eq", value: "doc-1" }],
          sort: null,
        }),
      ).rejects.toThrow("10000-row safety limit");
    }, 30000);
  },
);
