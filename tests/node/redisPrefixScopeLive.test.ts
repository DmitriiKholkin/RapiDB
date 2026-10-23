import { randomUUID } from "node:crypto";
import { createClient } from "redis";
import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { TableReadService } from "../../src/extension/table/tableReadService";

const url = process.env.RAPIDB_REDIS_PREFIX_AUDIT_URL;

describe.skipIf(!url)("Redis literal prefix isolation on a real server", () => {
  it.each([
    ["tenant*", "tenant-other"],
    ["tenant?", "tenantX"],
    ["tenant[ab]", "tenanta"],
    ["tenant[^a]", "tenantb"],
    ["tenant\\x", "tenantx"],
    ["tenant\\*?[]", "tenant-other"],
  ])("reads and exports only %s, then edits/deletes only its key", async (suffix, neighbor) => {
    const id = `rapidb-prefix-audit-${randomUUID()}-`;
    const prefix = `${id}${suffix}`;
    const ownKey = `${prefix}:one`;
    const otherKey = `${id}${neighbor}:one`;
    const client = createClient({ url, socket: { reconnectStrategy: false } });
    const driver = new RedisDriver({
      id,
      name: id,
      type: "redis",
      connectionUri: url,
    });
    try {
      await client.connect();
      await driver.connect();
      await client.set(ownKey, "own");
      await client.set(otherKey, "neighbor");
      const groups = await driver.listObjects("db0");
      expect(groups.some((group) => group.name === prefix)).toBe(true);
      for (const sort of [
        null,
        { column: "value", direction: "asc" as const },
      ]) {
        const page = await driver.readTablePage({
          database: "db0",
          schema: "",
          table: prefix,
          page: 1,
          pageSize: 25,
          filters: [],
          sort,
          skipCount: false,
        });
        expect(page.rows).toEqual([{ key: ownKey, value: "own", ttl: null }]);
        expect(page.totalCount).toBe(1);
        const service = new TableReadService({
          getConnection: () => ({ id, type: "redis" }),
          getDriver: () => driver,
        } as unknown as ConnectionManager);
        const keys: unknown[] = [];
        for await (const chunk of service.exportAll(
          id,
          "db0",
          "",
          prefix,
          1,
          sort,
        )) {
          keys.push(...chunk.rows.map((row) => row.key));
        }
        expect(keys).toEqual([ownKey]);
      }
      await driver.updateRows({
        database: "db0",
        schema: "",
        table: prefix,
        updates: [
          {
            primaryKeys: { key: ownKey },
            changes: { value: "edited" },
            originalValues: { value: "own" },
          },
        ],
      });
      expect(await client.get(ownKey)).toBe("edited");
      expect(await client.get(otherKey)).toBe("neighbor");
      await driver.deleteRows({
        database: "db0",
        schema: "",
        table: prefix,
        primaryKeyValuesList: [{ key: ownKey }],
      });
      expect(await client.get(ownKey)).toBeNull();
      expect(await client.get(otherKey)).toBe("neighbor");
    } finally {
      await driver.disconnect();
      if (client.isReady) await client.del([ownKey, otherKey]);
      if (client.isOpen) client.destroy();
    }
  });
});
