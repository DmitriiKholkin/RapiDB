import { randomUUID } from "node:crypto";
import { Client } from "@elastic/elasticsearch";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";

const endpoint = process.env.RAPIDB_ES_IDENTITY_TEST_URL;
describe.skipIf(!endpoint)(
  "Elasticsearch document identity — live ES 8",
  () => {
    const indices: string[] = [];
    const drivers: ElasticsearchDriver[] = [];
    const client = new Client({
      node: endpoint ?? "http://localhost:9200",
      headers: {
        accept: "application/vnd.elasticsearch+json; compatible-with=8",
        "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
      },
    });

    afterEach(async () => {
      await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
      for (const index of indices.splice(0))
        await client.indices.delete({ index });
    });
    afterAll(() => client.close());

    async function readKeys(
      driver: ElasticsearchDriver,
      table: string,
      tenant = "selected",
      materialized = false,
    ) {
      const page = await driver.readTablePage({
        database: "default",
        schema: "indices",
        table,
        page: 1,
        pageSize: 10,
        filters: [],
        sort: materialized ? { column: "_id", direction: "asc" } : null,
        skipCount: false,
      });
      const row = page.rows.find(
        (entry) => JSON.parse(String(entry._source)).tenant === tenant,
      );
      expect(row).toBeDefined();
      if (!row) throw new Error("Expected the selected row");
      const keys = Object.fromEntries(
        page.columns
          .filter((column) => column.isPrimaryKey)
          .map((column) => [column.name, row[column.name]]),
      );
      expect(
        Object.values(keys).every(
          (value) => value !== null && value !== undefined,
        ),
      ).toBe(true);
      return keys;
    }

    async function fixture() {
      const index = `rapidb-identity-${randomUUID()}`;
      await client.indices.create({
        index,
        settings: { number_of_shards: 3, number_of_replicas: 0 },
      });
      indices.push(index);
      const shard = async (routing: string) =>
        (await client.searchShards({ index, routing })).shards[0][0].shard;
      const defaultShard = await shard("same-id");
      let routing = "";
      for (let i = 0; i < 100; i++) {
        if ((await shard(`tenant-${i}`)) !== defaultShard) {
          routing = `tenant-${i}`;
          break;
        }
      }
      expect(routing).not.toBe("");
      await client.index({
        index,
        id: "same-id",
        document: { tenant: "default" },
        refresh: true,
      });
      await client.index({
        index,
        id: "same-id",
        routing,
        document: { tenant: "selected" },
        refresh: true,
      });
      const driver = new ElasticsearchDriver({
        id: "es-identity-live",
        name: "ES identity live",
        type: "elasticsearch",
        endpoint,
      });
      drivers.push(driver);
      await driver.connect();
      const primaryKeys = await readKeys(driver, index);
      return { index, routing, driver, primaryKeys };
    }

    it("deleting the selected routed row never deletes its default-routed twin", async () => {
      const { index, routing, driver, primaryKeys } = await fixture();
      const result = await driver.deleteRows({
        database: "default",
        schema: "indices",
        table: index,
        primaryKeyValuesList: [primaryKeys],
      });
      expect(result.affectedRows).toBe(1);
      expect(await client.exists({ index, id: "same-id" })).toBe(true);
      expect(await client.exists({ index, id: "same-id", routing })).toBe(
        false,
      );
    }, 30000);

    it("normal non-routing rows can still be edited and deleted with refreshed identities", async () => {
      const { index, routing, driver } = await fixture();
      const keys = await readKeys(driver, index, "default");
      const update = await driver.updateRows({
        database: "default",
        schema: "indices",
        table: index,
        updates: [
          {
            primaryKeys: keys,
            changes: { _source: '{"tenant":"edited"}' },
            originalValues: { _source: '{"tenant":"default"}' },
          },
        ],
      });
      expect(update.affectedRows).toBe(1);
      expect((await client.get({ index, id: "same-id" }))._source).toEqual({
        tenant: "edited",
      });
      const fresh = await readKeys(driver, index, "edited");
      await driver.deleteRows({
        database: "default",
        schema: "indices",
        table: index,
        primaryKeyValuesList: [fresh],
      });
      expect(await client.exists({ index, id: "same-id" })).toBe(false);
      expect(
        (await client.get({ index, id: "same-id", routing }))._source,
      ).toEqual({ tenant: "selected" });
    }, 30000);

    it.each([
      "partial",
      "replace",
      "conditional",
    ])("edits the routed twin only (%s), including materialized page reads", async (mode) => {
      const { index, routing, driver } = await fixture();
      const keys = await readKeys(driver, index, "selected", true);
      const changes =
        mode === "partial"
          ? { tenant: "edited" }
          : { _source: '{"tenant":"edited"}' };
      const originalValues =
        mode === "conditional"
          ? { _source: '{"tenant":"selected"}' }
          : undefined;
      const result = await driver.updateRows({
        database: "default",
        schema: "indices",
        table: index,
        updates: [{ primaryKeys: keys, changes, originalValues }],
      });
      expect(result.affectedRows).toBe(1);
      expect((await client.get({ index, id: "same-id" }))._source).toEqual({
        tenant: "default",
      });
      expect(
        (await client.get({ index, id: "same-id", routing }))._source,
      ).toEqual({ tenant: "edited" });
    }, 30000);

    it("alias previews pin the selected concrete index even after the alias moves", async () => {
      const { index, routing, driver } = await fixture();
      const alias = `${index}-alias`;
      await client.indices.putAlias({ index, name: alias });
      const keys = await readKeys(driver, alias);
      const nextIndex = `${index}-next`;
      await client.indices.create({
        index: nextIndex,
        settings: { number_of_replicas: 0 },
      });
      indices.push(nextIndex);
      await client.index({
        index: nextIndex,
        id: "same-id",
        routing,
        document: { tenant: "next" },
        refresh: true,
      });
      await client.indices.updateAliases({
        actions: [
          { remove: { index, alias } },
          { add: { index: nextIndex, alias } },
        ],
      });
      const preview = driver.buildMutationPreviewStatement(
        "delete",
        "default",
        "indices",
        alias,
        { primaryKeys: keys },
      );
      expect(preview).toContain(`/${index}/_doc/`);
      await driver.query(preview);
      expect(await client.exists({ index, id: "same-id", routing })).toBe(
        false,
      );
      expect(await client.exists({ index, id: "same-id" })).toBe(true);
      expect(
        (await client.get({ index: nextIndex, id: "same-id", routing }))
          ._source,
      ).toEqual({ tenant: "next" });
    }, 30000);

    it("multi-index alias writes use the selected concrete index, not its write index", async () => {
      const { index, routing, driver } = await fixture();
      const otherIndex = `${index}-other`;
      await client.indices.create({
        index: otherIndex,
        settings: { number_of_replicas: 0 },
      });
      indices.push(otherIndex);
      await client.index({
        index: otherIndex,
        id: "same-id",
        routing,
        document: { tenant: "other" },
        refresh: true,
      });
      const alias = `${index}-alias`;
      await client.indices.updateAliases({
        actions: [
          { add: { index, alias } },
          { add: { index: otherIndex, alias, is_write_index: true } },
        ],
      });
      const keys = await readKeys(driver, alias);
      await driver.deleteRows({
        database: "default",
        schema: "indices",
        table: alias,
        primaryKeyValuesList: [keys],
      });
      expect(await client.exists({ index, id: "same-id", routing })).toBe(
        false,
      );
      expect(await client.exists({ index, id: "same-id" })).toBe(true);
      expect(
        (await client.get({ index: otherIndex, id: "same-id", routing }))
          ._source,
      ).toEqual({ tenant: "other" });
    }, 30000);

    it.each([
      "delete",
      "update",
    ])("rejects a stale %s identity after a concurrent replacement", async (operation) => {
      const { index, routing, driver, primaryKeys } = await fixture();
      await client.index({
        index,
        id: "same-id",
        routing,
        document: { tenant: "concurrent" },
        refresh: true,
      });
      const base = { database: "default", schema: "indices", table: index };
      await expect(
        operation === "delete"
          ? driver.deleteRows({
              ...base,
              primaryKeyValuesList: [primaryKeys],
            })
          : driver.updateRows({
              ...base,
              updates: [
                { primaryKeys, changes: { _source: '{"tenant":"edited"}' } },
              ],
            }),
      ).rejects.toThrow(/version_conflict/);
      expect(
        (await client.get({ index, id: "same-id", routing }))._source,
      ).toEqual({ tenant: "concurrent" });
      expect((await client.get({ index, id: "same-id" }))._source).toEqual({
        tenant: "default",
      });
    }, 30000);

    it("rejects missing and routing-stripped identities without touching either twin", async () => {
      const { index, routing, driver, primaryKeys } = await fixture();
      const [payload, signature] = String(primaryKeys._identity).split(".");
      const stripped = JSON.parse(Buffer.from(payload, "base64url").toString());
      stripped[5] = null;
      for (const keys of [
        { _id: "same-id" },
        {
          _id: "same-id",
          _identity: `${Buffer.from(JSON.stringify(stripped)).toString("base64url")}.${signature}`,
        },
      ]) {
        await expect(
          driver.deleteRows({
            database: "default",
            schema: "indices",
            table: index,
            primaryKeyValuesList: [keys],
          }),
        ).rejects.toThrow(/Refresh/);
      }
      expect(
        (await client.get({ index, id: "same-id", routing }))._source,
      ).toEqual({ tenant: "selected" });
      expect((await client.get({ index, id: "same-id" }))._source).toEqual({
        tenant: "default",
      });
    }, 30000);
  },
);
