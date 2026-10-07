import type { QueryArrayConfig } from "pg";
import { Query } from "pg";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";

const drift = vi.hoisted(() => ({
  pgPackage: { version: "8.21.0" },
  mutate: undefined as ((query: object) => void) | undefined,
}));

vi.mock("pg/package.json", () => ({ default: drift.pgPackage }));
vi.mock("pg", async () => {
  const actual = await vi.importActual<typeof import("pg")>("pg");
  // All protocol behavior remains the installed pg implementation. Only its
  // initial shape is changed to simulate dependency/native implementation drift.
  class Query extends actual.Query {
    constructor(config: QueryArrayConfig, callback: () => void) {
      super(config, callback);
      drift.mutate?.(this);
    }
  }
  return { ...actual, Query };
});

afterEach(() => {
  drift.pgPackage.version = "8.21.0";
  drift.mutate = undefined;
  for (const method of [
    "_checkForMultirow",
    "handleRowDescription",
    "handleDataRow",
    "handleCommandComplete",
    "handleReadyForQuery",
    "submit",
  ])
    Reflect.deleteProperty(Query.prototype, method);
});

function harness() {
  const client = { query: vi.fn(), release: vi.fn() };
  const driver = new PostgresDriver({
    id: "pg-compatibility",
    name: "PG",
    type: "pg",
    host: "localhost",
  });
  (driver as unknown as { pool: unknown }).pool = {
    connect: vi.fn(async () => client),
    totalCount: 0,
    idleCount: 1,
    waitingCount: 0,
  };
  return { driver, client };
}

async function expectNotSubmitted(detail: string) {
  const { driver, client } = harness();
  await expect(
    driver.query("UPDATE items SET active = true RETURNING id", undefined, {
      hardCap: 2,
    }),
  ).rejects.toThrow(
    `Cannot safely bound PostgreSQL query results: incompatible pg ${drift.pgPackage.version} (${detail}`,
  );
  expect(client.query).not.toHaveBeenCalled();
  expect(client.release).toHaveBeenCalledExactlyOnceWith(true);
}

describe("PostgreSQL bounded adapter compatibility", () => {
  it("rejects unvalidated pg versions before submitting SQL, with no buffered fallback", async () => {
    drift.pgPackage.version = "9.0.0";
    await expectNotSubmitted("unvalidated version");
  });

  it.each([
    "_checkForMultirow",
    "handleRowDescription",
    "handleDataRow",
    "handleCommandComplete",
    "handleReadyForQuery",
    "submit",
  ])("rejects a missing Query.%s hook before submitting SQL", async (method) => {
    Object.defineProperty(Query.prototype, method, {
      configurable: true,
      value: undefined,
    });
    await expectNotSubmitted(`missing Query.${method}`);
  });

  it.each([
    "_result",
    "_results",
    "rows",
    "fields",
    "rowAsArray",
    "command",
    "rowCount",
  ])("rejects an incompatible %s field before submitting SQL", async (field) => {
    drift.mutate = (query) => {
      const target = field.startsWith("_")
        ? query
        : Reflect.get(query, "_result");
      Reflect.deleteProperty(target, field);
    };
    await expectNotSubmitted("unexpected _result/_results shape");
  });

  it("rejects the native Query shape before submitting SQL", async () => {
    drift.mutate = (query) => {
      Reflect.deleteProperty(query, "_result");
      Reflect.deleteProperty(query, "_results");
      Reflect.set(query, "_arrayMode", true);
      Reflect.set(query, "_emitRowEvents", false);
    };
    await expectNotSubmitted("unexpected _result/_results shape");
  });

  it.each([
    "addFields",
    "parseRow",
    "addRow",
    "addCommandComplete",
  ])("rejects a missing Result.%s method before submitting SQL", async (method) => {
    drift.mutate = (query) =>
      Reflect.set(Reflect.get(query, "_result"), method, undefined);
    await expectNotSubmitted(`missing Result.${method}`);
  });

  it("rejects a renamed accumulation switch before submitting SQL", async () => {
    Object.defineProperty(Query.prototype, "handleRowDescription", {
      configurable: true,
      value() {},
    });
    await expectNotSubmitted("row-buffering probe failed:");
  });

  it("rejects an ignored accumulation switch before submitting SQL", async () => {
    Object.defineProperty(Query.prototype, "handleDataRow", {
      configurable: true,
      value(this: Query, message: { fields: string[] }) {
        const result = Reflect.get(this, "_result");
        const row = result.parseRow(message.fields);
        this.emit("row", row, result);
        result.addRow(row);
      },
    });
    await expectNotSubmitted("row-buffering probe failed:");
  });
});
