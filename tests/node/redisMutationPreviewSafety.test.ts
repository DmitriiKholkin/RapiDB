import { randomUUID } from "node:crypto";
import { createClient } from "redis";
import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";

describe("Redis update descriptions", () => {
  it.each([
    { key: 'destination"; DEL victim\n', ttl: 30 },
    { value: '["member"]' },
    { key: "other", value: "new", ttl: -1 },
    { ttl: 42 },
    { value: null },
  ])("does not expose replayable destructive commands: %j", async (changes) => {
    const driver = new RedisDriver({
      id: "preview",
      name: "preview",
      type: "redis",
    });
    const client = {
      type: vi.fn().mockResolvedValue("list"),
      sendCommand: vi.fn().mockRejectedValue(new Error("unknown command")),
    };
    Object.assign(driver, {
      getDatabaseClient: async () => client,
      client,
      connected: true,
    });
    const data = {
      primaryKeys: { key: 'source\0"; SET victim broken' },
      changes,
    };
    const sync = driver.buildMutationPreviewStatement(
      "update",
      "db0",
      "",
      "",
      data,
    );
    client.type.mockResolvedValue("string");
    const [asyncPreview] = await driver.buildMutationPreviewStatements(
      "update",
      "db0",
      "",
      "",
      data,
    );
    expect(asyncPreview).toBe(sync);
    expect(JSON.parse(sync)).toMatchObject({
      source: data.primaryKeys.key,
      preview: expect.stringContaining("not an executable command"),
      conditions: expect.stringContaining("target must not exist"),
    });
    await expect(driver.query(sync)).rejects.toThrow();
    expect(client.sendCommand).toHaveBeenCalledTimes(1);
    const args = client.sendCommand.mock.calls[0][0] as string[];
    expect(args[0]).toMatch(/^\{/);
  });

  it("states TTL behavior, including empty collection deletion", () => {
    const driver = new RedisDriver({
      id: "preview",
      name: "preview",
      type: "redis",
    });
    const preview = (changes: Record<string, unknown>) =>
      JSON.parse(
        driver.buildMutationPreviewStatement("update", "db0", "", "", {
          primaryKeys: { key: "key" },
          changes,
        }),
      );
    expect(preview({ value: "[]" })).toMatchObject({
      ttl: "keep existing TTL",
      value: {
        action: expect.stringContaining("empty collections delete the source"),
      },
    });
    expect(preview({ ttl: -1 }).ttl).toBe("persist");
    expect(preview({ ttl: 42 }).ttl).toBe("set to 42 seconds");
  });

  it.each([
    { key: "stream:renamed" },
    { ttl: 60 },
  ])("warns that stream contents are not checked for %j", async (changes) => {
    const driver = new RedisDriver({
      id: "preview",
      name: "preview",
      type: "redis",
    });
    const client = {
      type: vi.fn().mockResolvedValue("stream"),
      sendCommand: vi.fn().mockResolvedValue(1),
    };
    Object.assign(driver, { getDatabaseClient: async () => client });
    const data = { primaryKeys: { key: "stream:source" }, changes };
    const sync = driver.buildMutationPreviewStatement(
      "update",
      "db0",
      "",
      "",
      data,
    );
    const [asyncPreview] = await driver.buildMutationPreviewStatements(
      "update",
      "db0",
      "",
      "",
      data,
    );
    expect(asyncPreview).toBe(sync);
    expect(JSON.parse(sync).conditions).toContain(
      "Stream contents are not checked for changes",
    );
    await expect(
      driver.updateRows({
        database: "db0",
        schema: "",
        table: "stream",
        updates: [
          { ...data, originalValues: { value: "old stream snapshot" } },
        ],
      }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(client.sendCommand).toHaveBeenCalledWith(
      expect.arrayContaining(["EVAL", "stream", "1"]),
    );
  });
});

it.skipIf(!process.env.RAPIDB_REDIS_QUERY_AUDIT_URL)(
  "renames and changes TTL on a stream despite a stale supplied value snapshot",
  async () => {
    const url = process.env.RAPIDB_REDIS_QUERY_AUDIT_URL;
    const source = `rapidb-stream-preview:${randomUUID()}`;
    const target = `${source}:renamed`;
    const client = createClient({ url, socket: { reconnectStrategy: false } });
    const driver = new RedisDriver({
      id: source,
      name: source,
      type: "redis",
      connectionUri: url,
    });
    try {
      await client.connect();
      await driver.connect();
      await client.xAdd(source, "*", { entry: "new content" });
      const update = (key: string, changes: Record<string, unknown>) =>
        driver.updateRows({
          database: "db0",
          schema: "",
          table: "stream",
          updates: [
            {
              primaryKeys: { key },
              changes,
              originalValues: { value: "stale snapshot" },
            },
          ],
        });
      await expect(update(source, { key: target })).resolves.toMatchObject({
        affectedRows: 1,
      });
      await expect(update(target, { ttl: 60 })).resolves.toMatchObject({
        affectedRows: 1,
      });
      expect(await client.xLen(target)).toBe(1);
      expect(await client.ttl(target)).toBeGreaterThan(0);
    } finally {
      await driver.disconnect();
      if (client.isReady) await client.del([source, target]);
      if (client.isOpen) client.destroy();
    }
  },
);
