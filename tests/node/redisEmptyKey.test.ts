import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";

function setup() {
  const driver = new RedisDriver({ id: "empty", name: "empty", type: "redis" });
  const client = {
    type: vi.fn().mockResolvedValue("string"),
    sendCommand: vi.fn().mockResolvedValue(1),
    del: vi.fn().mockResolvedValue(1),
    set: vi.fn().mockResolvedValue("OK"),
  };
  Object.assign(driver, { getDatabaseClient: async () => client });
  return { driver, client };
}

const scope = { database: "db0", schema: "", table: "some-prefix" };

describe("Redis empty key identity", () => {
  it.each([
    "\ud800",
    "\udfff",
    "prefix:\ud800:tail",
  ])("rejects a non-round-trippable key %j in every mutation and preview path", async (key) => {
    const { driver, client } = setup();
    const error = /losslessly UTF-8/;
    await expect(
      driver.insertRow({ ...scope, values: { key, value: "v" } }),
    ).rejects.toThrow(error);
    await expect(
      driver.deleteRows({ ...scope, primaryKeyValuesList: [{ key }] }),
    ).rejects.toThrow(error);
    for (const [source, target] of [
      [key, "valid"],
      ["valid", key],
    ]) {
      const data = { primaryKeys: { key: source }, changes: { key: target } };
      await expect(
        driver.updateRows({ ...scope, updates: [data] }),
      ).rejects.toThrow(error);
      expect(() =>
        driver.buildMutationPreviewStatement("update", "db0", "", "", data),
      ).toThrow(error);
      await expect(
        driver.buildMutationPreviewStatements("update", "db0", "", "", data),
      ).rejects.toThrow(error);
    }
    expect(() =>
      driver.buildMutationPreviewStatement("insert", "db0", "", "", {
        values: { key },
      }),
    ).toThrow(error);
    await expect(
      driver.buildMutationPreviewStatements("insert", "db0", "", "", {
        values: { key },
      }),
    ).rejects.toThrow(error);
    const data = { primaryKeyValuesList: [{ key }] };
    expect(() =>
      driver.buildMutationPreviewStatement("delete", "db0", "", "", data),
    ).toThrow(error);
    await expect(
      driver.buildMutationPreviewStatements("delete", "db0", "", "", data),
    ).rejects.toThrow(error);
    expect(client.type).not.toHaveBeenCalled();
    expect(client.set).not.toHaveBeenCalled();
    expect(client.del).not.toHaveBeenCalled();
    expect(client.sendCommand).not.toHaveBeenCalled();
  });

  it("inserts and deletes the exact empty key without adding the table prefix", async () => {
    const { driver, client } = setup();
    await expect(
      driver.insertRow({ ...scope, values: { key: "", value: "v" } }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(client.set).toHaveBeenCalledWith("", "v", { NX: true });
    await expect(
      driver.deleteRows({ ...scope, primaryKeyValuesList: [{ key: "" }] }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(client.del).toHaveBeenCalledWith("");
    expect(
      driver.buildMutationPreviewStatement("insert", "db0", "", "", {
        values: { key: "", value: "v" },
      }),
    ).toContain('""');
  });

  it.each([
    ["", "other"],
    ["other", ""],
    ["", ""],
  ])("updates exact source %j and target %j", async (source, target) => {
    const { driver, client } = setup();
    const changes = source === target ? { ttl: 30 } : { key: target };
    const data = { primaryKeys: { key: source }, changes };
    await expect(
      driver.updateRows({ ...scope, updates: [data] }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(client.type).toHaveBeenCalledWith(source);
    expect(client.sendCommand.mock.calls[0][0].slice(3, 5)).toEqual([
      source,
      target,
    ]);
    const preview = JSON.parse(
      driver.buildMutationPreviewStatement("update", "db0", "", "", data),
    );
    expect(preview).toMatchObject({ source, target });
  });

  it("validates value previews for an empty source", async () => {
    const { driver, client } = setup();
    client.type.mockResolvedValue("stream");
    await expect(
      driver.buildMutationPreviewStatements("update", "db0", "", "", {
        primaryKeys: { key: "" },
        changes: { value: "new" },
      }),
    ).rejects.toThrow(/stream values are read-only/i);
    expect(client.type).toHaveBeenCalledWith("");
  });

  it.each([
    undefined,
    null,
    0,
    false,
    Buffer.from([0xff]),
    {},
  ])("does not accept non-string key %j", async (key) => {
    const { driver, client } = setup();
    await expect(
      driver.insertRow({ ...scope, values: { key, value: "v" } }),
    ).rejects.toThrow();
    await expect(
      driver.deleteRows({ ...scope, primaryKeyValuesList: [{ key }] }),
    ).rejects.toThrow();
    await expect(
      driver.updateRows({
        ...scope,
        updates: [{ primaryKeys: { key: "" }, changes: { key } }],
      }),
    ).rejects.toThrow();
    await expect(
      driver.updateRows({
        ...scope,
        updates: [{ primaryKeys: { key }, changes: { ttl: 30 } }],
      }),
    ).resolves.toMatchObject({ affectedRows: 0 });
    expect(client.set).not.toHaveBeenCalled();
    expect(client.del).not.toHaveBeenCalled();
    expect(client.sendCommand).not.toHaveBeenCalled();
  });
});
