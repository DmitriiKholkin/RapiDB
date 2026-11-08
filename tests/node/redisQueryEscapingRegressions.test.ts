import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { DriverUpdateRowsError } from "../../src/extension/dbDrivers/updateRowOutcomes";

function queryFixture() {
  const driver = new RedisDriver({
    id: "redis-g5",
    name: "Redis G5",
    type: "redis",
    host: "localhost",
  });
  const sendCommand = vi.fn().mockResolvedValue("OK");
  Object.assign(driver, { client: { sendCommand }, connected: true });
  return { driver, sendCommand };
}

function sentCommands(sendCommand: ReturnType<typeof vi.fn>) {
  return sendCommand.mock.calls.map(([args]) => args);
}

describe("Redis query tokenizer follows redis-cli quoting", () => {
  it.each([
    [
      String.raw`SET path 'C:\temp\new'`,
      ["SET", "path", String.raw`C:\temp\new`],
    ],
    [String.raw`SET re a\d+`, ["SET", "re", String.raw`a\d+`]],
    [String.raw`SET k 'it\'s'`, ["SET", "k", "it's"]],
    [String.raw`SET k 'a\b\\c'`, ["SET", "k", String.raw`a\b\\c`]],
    [String.raw`SET k "a\"b\\c\n\t\a"`, ["SET", "k", 'a"b\\c\n\t\x07']],
    [String.raw`SET k "\x41\x7e\xZZ"`, ["SET", "k", "A~xZZ"]],
  ])("tokenizes %s", async (query, expected) => {
    const { driver, sendCommand } = queryFixture();
    await driver.query(query);
    expect(sentCommands(sendCommand)).toEqual([expected]);
  });

  it("sends raw bytes for double-quoted \\xHH escapes above 0x7f", async () => {
    const { driver, sendCommand } = queryFixture();
    await driver.query(String.raw`SET k "a\xff\x00b"`);
    expect(sentCommands(sendCommand)).toEqual([
      ["SET", "k", Buffer.from([0x61, 0xff, 0x00, 0x62])],
    ]);
  });

  it("splits statements with the same quoting rules as the tokenizer", async () => {
    const { driver, sendCommand } = queryFixture();
    await driver.query(String.raw`SET a 'x\';y'; SET b "p\";q"; SET c r\;s`);
    expect(sentCommands(sendCommand)).toEqual([
      ["SET", "a", "x';y"],
      ["SET", "b", 'p";q'],
      ["SET", "c", "r\\"],
      ["S"],
    ]);
  });

  it("treats a single-quoted backslash before a closing quote literally", async () => {
    const { driver, sendCommand } = queryFixture();
    await driver.query(String.raw`SET k 'a\\'; GET k'`);
    expect(sentCommands(sendCommand)).toEqual([
      ["SET", "k", String.raw`a\'; GET k`],
    ]);
    await expect(driver.query(String.raw`SET k 'a\\'; GET k`)).rejects.toThrow(
      /unterminated/,
    );
  });
});

describe("Redis trailing statement terminators", () => {
  it.each([
    ["GET k;;;", true, [["GET", "k"]]],
    ['GET "k;";', true, [["GET", "k;"]]],
    ["GET 'k;'", true, [["GET", "k;"]]],
    [
      "GET k; SET k v;",
      false,
      [
        ["GET", "k"],
        ["SET", "k", "v"],
      ],
    ],
  ] as const)("classifies and executes %s consistently", async (query, allowed, commands) => {
    const { driver, sendCommand } = queryFixture();
    expect(driver.getCapabilities().readOnlyQueryGuard?.(query).allowed).toBe(
      allowed,
    );
    await driver.query(query);
    expect(sentCommands(sendCommand)).toEqual(commands);
  });

  it("returns an empty result for terminator-only input", async () => {
    const { driver, sendCommand } = queryFixture();
    expect(driver.getCapabilities().readOnlyQueryGuard?.(" ;; ").allowed).toBe(
      false,
    );
    await expect(driver.query(" ;; ")).resolves.toMatchObject({ rowCount: 0 });
    expect(sendCommand).not.toHaveBeenCalled();
  });
});

describe("Redis edit values with unpaired surrogates", () => {
  function editFixture(type: string) {
    const driver = new RedisDriver({
      id: "redis-g5-edit",
      name: "Redis G5 edit",
      type: "redis",
      host: "localhost",
    });
    const client = {
      type: vi.fn().mockResolvedValue(type),
      hLen: vi.fn().mockResolvedValue(1),
      lLen: vi.fn().mockResolvedValue(1),
      strLen: vi.fn().mockResolvedValue(1),
      withTypeMapping: () => ({ get: async () => Buffer.from("old") }),
      sendCommand: vi.fn().mockResolvedValue(1),
    };
    Object.assign(driver, {
      client,
      connected: true,
      getDatabaseClient: async () => client,
    });
    return { driver, client };
  }

  const lone = String.fromCharCode(0xd800);

  it.each([
    ["string", `bad${lone}`],
    ["hash", JSON.stringify({ field: lone })],
    ["hash", JSON.stringify({ [lone]: "value" })],
    ["list", JSON.stringify([`x${lone}`])],
  ])("rejects a %s value before sending it and reports not applied", async (type, value) => {
    const { driver, client } = editFixture(type);
    const error = await driver
      .updateRows({
        database: "db0",
        schema: "",
        table: "audit",
        updates: [{ primaryKeys: { key: "audit:k" }, changes: { value } }],
      })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(DriverUpdateRowsError);
    expect((error as Error).message).toMatch(/unpaired UTF-16 surrogate/);
    expect((error as DriverUpdateRowsError).result.updateRowOutcomes).toEqual([
      { rowIndex: 0, status: "not_applied" },
    ]);
    expect(client.sendCommand).not.toHaveBeenCalled();
  });

  it("rejects an inserted value before previewing or sending it", async () => {
    const { driver, client } = editFixture("string");
    const set = vi.fn().mockResolvedValue("OK");
    Object.assign(client, { set });
    const values = { key: "audit:new", value: `bad${lone}` };
    expect(() =>
      driver.buildMutationPreviewStatement("insert", "db0", "", "audit", {
        values,
      }),
    ).toThrow(/unpaired UTF-16 surrogate/);
    await expect(
      driver.insertRow({
        database: "db0",
        schema: "",
        table: "audit",
        values,
      }),
    ).rejects.toThrow(/unpaired UTF-16 surrogate/);
    expect(set).not.toHaveBeenCalled();
    await driver.insertRow({
      database: "db0",
      schema: "",
      table: "audit",
      values: { key: "audit:new", value: "ok 😀" },
    });
    expect(set).toHaveBeenCalledWith("audit:new", "ok 😀", { NX: true });
  });

  it("still sends well-formed astral characters", async () => {
    const { driver, client } = editFixture("string");
    await driver.updateRows({
      database: "db0",
      schema: "",
      table: "audit",
      updates: [{ primaryKeys: { key: "audit:k" }, changes: { value: "😀" } }],
    });
    expect(client.sendCommand).toHaveBeenCalledTimes(1);
    expect(client.sendCommand.mock.calls[0]?.[0][9]).toBe('"😀"');
  });
});
