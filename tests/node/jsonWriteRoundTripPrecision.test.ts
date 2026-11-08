import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import {
  isRoundTripJsonNumberToken,
  parseJsonForWrite,
} from "../../src/extension/utils/jsonWriteSafety";

const exactBinaryIntegers = ["1000000000000000128", "1152921504606846976"];
const column = {
  name: "_source",
  nativeType: "object",
  category: "json",
} as ColumnTypeMeta;

function redisHarness(kind: string) {
  const driver = new RedisDriver({
    id: "round-trip",
    name: "Round trip",
    type: "redis",
  });
  const client = {
    type: vi.fn().mockResolvedValue(kind),
    hLen: vi.fn().mockResolvedValue(1),
    lLen: vi.fn().mockResolvedValue(1),
    sCard: vi.fn().mockResolvedValue(1),
    zCard: vi.fn().mockResolvedValue(1),
    sendCommand: vi.fn().mockResolvedValue(1),
  };
  Object.assign(driver, { getDatabaseClient: async () => client });
  return { driver, client };
}

describe("JSON writes re-serialized with JSON.stringify", () => {
  it.each(
    exactBinaryIntegers,
  )("keeps %s for exact-binary (Mongo) writes but rejects it for JSON text writes", (token) => {
    const text = `{"amount":${token}}`;
    expect(JSON.stringify(JSON.parse(text))).not.toContain(token);
    expect(parseJsonForWrite(text)).toEqual(JSON.parse(text));
    expect(() => parseJsonForWrite(text, isRoundTripJsonNumberToken)).toThrow(
      /numeric precision/,
    );
    const mongo = new MongoDBDriver({
      id: "mongo",
      name: "mongo",
      type: "mongodb",
    });
    expect(mongo.coerceInputValue(text, column)).toEqual(JSON.parse(text));
  });

  it.each([
    ["1E+23", 1e23],
    ["-0.0", -0],
    ["9007199254740992", 9007199254740992],
    ["13000.0", 13000],
  ])("still accepts %s for JSON text writes", (token, value) => {
    expect(
      parseJsonForWrite(`{"v":${token}}`, isRoundTripJsonNumberToken),
    ).toEqual({ v: value });
  });

  it("rejects 9007199254740993.0 for JSON text writes", () => {
    expect(() =>
      parseJsonForWrite('{"v":9007199254740993.0}', isRoundTripJsonNumberToken),
    ).toThrow(/numeric precision/);
  });

  it.each(
    exactBinaryIntegers,
  )("rejects Elasticsearch REST writes and queries with %s before SDK calls", async (token) => {
    const es = new ElasticsearchDriver({
      id: "es",
      name: "es",
      type: "elasticsearch",
    });
    const index = vi.fn();
    const search = vi.fn();
    Object.assign(es, { connected: true, client: { index, search } });
    await expect(
      es.query(`PUT /items/_doc/a\n{"amount":${token}}`),
    ).rejects.toThrow(/numeric precision/);
    await expect(
      es.query(`POST /items/_search\n{"query":{"term":{"amount":${token}}}}`),
    ).rejects.toThrow(/numeric precision/);
    expect(index).not.toHaveBeenCalled();
    expect(search).not.toHaveBeenCalled();
  });

  it.each([
    ["hash", '{"account":1000000000000000128}'],
    ["list", "[1152921504606846976]"],
    ["set", "[1000000000000000128]"],
    ["zset", '[{"value":1000000000000000128,"score":1}]'],
  ])("rejects Redis %s edits that would store rewritten digits", async (kind, value) => {
    const { driver, client } = redisHarness(kind);
    await expect(
      driver.updateRows({
        database: "db0",
        schema: "",
        table: "audit",
        updates: [{ primaryKeys: { key: "audit:source" }, changes: { value } }],
      }),
    ).rejects.toThrow(/numeric precision/);
    expect(client.sendCommand).not.toHaveBeenCalled();
  });
});
