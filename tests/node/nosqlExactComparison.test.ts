import { Decimal128, Long } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import {
  applyFilters,
  applySort,
} from "../../src/extension/dbDrivers/nosqlUtils";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import {
  compareNumericTokens,
  NUMERIC_COMPARISON_LIMITS,
} from "../../src/shared/numericNormalization";

const numeric = [{ name: "value", category: "decimal" as const }];
const request: DriverTablePageRequest = {
  database: "db0",
  schema: "",
  table: "default",
  page: 1,
  pageSize: 100,
  filters: [],
  sort: { column: "value", direction: "asc" },
  skipCount: false,
};
const values = [
  "9007199254740993",
  "9007199254740992",
  "-9007199254740992",
  "-9007199254740993",
  "0.10000000000000000002",
  "0.10000000000000000001",
  "-1.00000000000000000001",
  "-1.00000000000000000002",
  "1.00000000000000000002e-100",
  "1.00000000000000000001e-100",
];
const ordered = [
  values[3],
  values[2],
  values[7],
  values[6],
  values[9],
  values[8],
  values[5],
  values[4],
  values[1],
  values[0],
];

describe("B08 exact typed client comparisons", () => {
  it.each([
    ["9007199254740993", "9007199254740992", 1],
    ["-9007199254740993", "-9007199254740992", -1],
    ["1.00000000000000000001", "1.00000000000000000002", -1],
    ["-1.00000000000000000001", "-1.00000000000000000002", 1],
    ["+0001.2300e+002", "123", 0],
    [".00100", "1e-3", 0],
    ["1.", "1.0", 0],
    ["+001.e+3", "1000", 0],
    ["-001.e-3", "-.001", 0],
    ["$1,234.50", "1234.5", 0],
    ["CHF 1'234.50", "1234.5", 0],
    ["($1,234.50)", "-1234.5", 0],
    ["-0e999999999999999999999", "+0", 0],
    ["1e999999999999999999999", "9e999999999999999999998", 1],
    ["1e-999999999999999999999", "9e-999999999999999999998", -1],
    ["NaN", "1", null],
  ] as const)("compares %s and %s exactly", (a, b, expected) => {
    expect(compareNumericTokens(a, b)).toBe(expected);
    expect(compareNumericTokens(b, a)).toBe(
      expected === null ? null : expected === 0 ? 0 : -expected,
    );
  });

  it.each([
    "1.2.",
    "1.2.e3",
    ".1.",
    "1..",
    "1..e3",
    "1.e",
    "1.e+",
    "$1.2.",
    "1,234.5.e3",
  ])("rejects malformed decimal %s without repairing it in range filters", (malformed) => {
    expect(compareNumericTokens(malformed, "0")).toBeNull();
    expect(compareNumericTokens("0", malformed)).toBeNull();
    for (const operator of ["gt", "gte", "lt", "lte"] as const) {
      expect(
        applyFilters(
          [{ value: malformed }],
          [{ column: "value", operator, value: "0" }],
          numeric,
        ),
      ).toEqual([]);
      expect(
        applyFilters(
          [{ value: "0" }],
          [{ column: "value", operator, value: malformed }],
          numeric,
        ),
      ).toEqual([]);
    }
    expect(
      applyFilters(
        [{ value: malformed }],
        [{ column: "value", operator: "between", value: ["-10000", "10000"] }],
        numeric,
      ),
    ).toEqual([]);
    for (const bounds of [
      [malformed, "10000"],
      ["-10000", malformed],
    ] as [string, string][]) {
      expect(
        applyFilters(
          [{ value: "0" }],
          [{ column: "value", operator: "between", value: bounds }],
          numeric,
        ),
      ).toEqual([]);
    }
  });

  it("canonicalizes long valid significands with bounded scans, not suffix-regex retries", () => {
    const long = `1${"0".repeat(40_000)}1`;
    const originalReplace = String.prototype.replace;
    const originalCharCodeAt = String.prototype.charCodeAt;
    let inspections = 0;
    const replace = vi
      .spyOn(String.prototype, "replace")
      .mockImplementation(function (this: string, ...args) {
        // Deterministically fail the old quadratic algorithm, without wall-clock
        // assertions or waiting for regexp backtracking on adversarial digits.
        if (
          args[0] instanceof RegExp &&
          !args[0].source.startsWith("^") &&
          args[0].source.endsWith("+$")
        ) {
          throw new Error("Unanchored suffix-run regexp is not a linear scan");
        }
        return Reflect.apply(originalReplace, this, args);
      });
    const charCodeAt = vi
      .spyOn(String.prototype, "charCodeAt")
      .mockImplementation(function (this: string, index) {
        inspections += 1;
        if (inspections > 10 * long.length)
          throw new Error("Zero scans exceeded linear work budget");
        return originalCharCodeAt.call(this, index);
      });
    let results: (number | null)[];
    try {
      results = [
        compareNumericTokens(long, `${long}0e-1`),
        compareNumericTokens(long, "1e40001"),
        compareNumericTokens(`1${"0".repeat(40_000)}`, "1e40000"),
        compareNumericTokens(`0.${"0".repeat(40_000)}1`, "1e-40001"),
      ];
    } finally {
      replace.mockRestore();
      charCodeAt.mockRestore();
    }
    expect(results).toEqual([0, 1, 0, 0]);
    expect(inspections).toBeLessThanOrEqual(10 * long.length);
  });

  it.each([
    " ",
    "\t",
    "\u00a0",
    "$",
    "€",
    "\u{1ECB0}",
  ])("normalizes long %j affix runs with bounded Unicode scans, including filters/sorts", (affix) => {
    const run = affix.repeat(20_000);
    const interior = `1${run}1`;
    const suffix = `11${run}`;
    const rows = [{ value: interior }, { value: suffix }, { value: "11" }];
    const originalReplace = String.prototype.replace;
    const originalCharCodeAt = String.prototype.charCodeAt;
    const budget = 20 * (interior.length + suffix.length);
    let inspections = 0;
    const replace = vi
      .spyOn(String.prototype, "replace")
      .mockImplementation(function (this: string, ...args) {
        if (
          args[0] instanceof RegExp &&
          !args[0].source.startsWith("^") &&
          args[0].source.endsWith("+$")
        ) {
          throw new Error(
            "Unanchored suffix-run regexp retries interior affixes",
          );
        }
        return Reflect.apply(originalReplace, this, args);
      });
    const charCodeAt = vi
      .spyOn(String.prototype, "charCodeAt")
      .mockImplementation(function (this: string, index) {
        inspections += 1;
        if (inspections > budget)
          throw new Error("Affix scans exceeded linear work budget");
        return originalCharCodeAt.call(this, index);
      });
    let results: (number | null)[];
    let filtered: Record<string, unknown>[];
    let sorted: Record<string, unknown>[];
    try {
      results = [
        compareNumericTokens(interior, "11"),
        compareNumericTokens(suffix, "11"),
      ];
      filtered = applyFilters(
        rows,
        [{ column: "value", operator: "gte", value: "11" }],
        numeric,
      );
      sorted = applySort(rows, request.sort, numeric);
    } finally {
      replace.mockRestore();
      charCodeAt.mockRestore();
    }
    expect(results).toEqual([0, 0]);
    expect(filtered).toEqual(rows);
    expect(sorted).toEqual(rows);
    expect(inspections).toBeLessThanOrEqual(budget);
  });

  it("rejects oversized raw inputs before normalization or BigInt conversion", () => {
    const oversized = "1".repeat(NUMERIC_COMPARISON_LIMITS.maxTokenLength + 1);
    const trim = vi.spyOn(String.prototype, "trim");
    const bigint = vi.spyOn(globalThis, "BigInt");
    let results: (number | null)[];
    let trimCalls: number;
    let bigintCalls: number;
    try {
      results = [
        compareNumericTokens(oversized, "1"),
        compareNumericTokens("1", oversized),
      ];
      trimCalls = trim.mock.calls.length;
      bigintCalls = bigint.mock.calls.length;
    } finally {
      trim.mockRestore();
      bigint.mockRestore();
    }
    expect(results).toEqual([null, null]);
    expect(trimCalls).toBe(0);
    expect(bigintCalls).toBe(0);
    expect(
      compareNumericTokens(
        "1".repeat(NUMERIC_COMPARISON_LIMITS.maxTokenLength),
        "1e65535",
      ),
    ).toBe(1);
  });

  it("bounds significant exponent digits before BigInt, preserving zero-padded exponents", () => {
    const exponent = "9".repeat(NUMERIC_COMPARISON_LIMITS.maxExponentDigits);
    expect(compareNumericTokens(`1e${exponent}`, `2e${exponent}`)).toBe(-1);
    const bigint = vi.spyOn(globalThis, "BigInt");
    let result: number | null;
    let calls: number;
    try {
      result = compareNumericTokens(`1e${exponent}9`, `-1e-${exponent}9`);
      calls = bigint.mock.calls.length;
    } finally {
      bigint.mockRestore();
    }
    expect(result).toBeNull();
    expect(calls).toBe(0);
    expect(compareNumericTokens(`1e+${"0".repeat(10_000)}2`, "100")).toBe(0);
    expect(compareNumericTokens(`1e-${"0".repeat(10_000)}2`, ".01")).toBe(0);
  });

  it("sorts exactly, keeps null ordering, and excludes null/malformed ranges", () => {
    const rows = [...values.map((value) => ({ value })), { value: null }, {}];
    expect(
      applySort(rows, request.sort, numeric).map((row) => row.value),
    ).toEqual([null, undefined, ...ordered]);
    expect(
      applySort(rows, { column: "value", direction: "desc" }, numeric).map(
        (row) => row.value,
      ),
    ).toEqual([...ordered.slice().reverse(), null, undefined]);
    for (const [operator, expected] of [
      ["gt", [values[0]]],
      ["gte", [values[0], values[1]]],
      ["lt", values.slice(2)],
      ["lte", values.slice(1)],
    ] as const) {
      expect(
        applyFilters(
          rows,
          [{ column: "value", operator, value: values[1] }],
          numeric,
        ).map((row) => row.value),
      ).toEqual(expected);
    }
    expect(
      applyFilters(
        rows,
        [
          {
            column: "value",
            operator: "between",
            value: ["9007199254740992", "9007199254740992"],
          },
        ],
        numeric,
      ),
    ).toEqual([{ value: values[1] }]);
    expect(
      applyFilters(
        [{ value: null }, {}, { value: "NaN" }],
        [{ column: "value", operator: "between", value: ["-1", "1"] }],
        numeric,
      ),
    ).toEqual([]);
  });

  it("keeps eq/neq/in string semantics and does not infer text or missing metadata", () => {
    const rows = [{ value: "01.00" }, { value: "1" }];
    expect(
      applyFilters(
        rows,
        [{ column: "value", operator: "eq", value: "1" }],
        numeric,
      ),
    ).toEqual([rows[1]]);
    expect(
      applyFilters(
        rows,
        [{ column: "value", operator: "neq", value: "1" }],
        numeric,
      ),
    ).toEqual([rows[0]]);
    expect(
      applyFilters(
        rows,
        [{ column: "value", operator: "in", value: "1,2" }],
        numeric,
      ),
    ).toEqual([rows[1]]);
    for (const category of ["text", "other", undefined] as const) {
      const columns = category ? [{ name: "value", category }] : [];
      const textRows = [
        "2",
        "10",
        "001",
        "2026-01-01T00:00:00Z",
        "2025-12-31T23:30:00-02:00",
      ].map((value) => ({ value }));
      expect(applySort(textRows, request.sort, columns)).toEqual(
        [...textRows].sort((a, b) => a.value.localeCompare(b.value)),
      );
      expect(
        applyFilters(
          textRows,
          [{ column: "value", operator: "gt", value: "10" }],
          columns,
        ),
      ).toContainEqual({ value: "2" });
    }
  });

  it("only interprets dates in temporal metadata", () => {
    const rows = ["2026-01-01T00:00:00Z", "2025-12-31T23:30:00-02:00"].map(
      (value) => ({ value }),
    );
    expect(
      applySort(rows, request.sort, [{ name: "value", category: "datetime" }]),
    ).toEqual(rows);
    expect(
      applySort(rows, request.sort, [{ name: "value", category: "text" }]),
    ).toEqual([...rows].reverse());
  });

  it("does not allocate an exponent-sized decimal expansion", () => {
    const exponent = "9".repeat(5000);
    expect(compareNumericTokens(`1e${exponent}`, `2e${exponent}`)).toBe(-1);
    expect(compareNumericTokens(`-2e-${exponent}`, `-1e-${exponent}`)).toBe(-1);
  });
});

function redis(valuesByKey: Record<string, string>) {
  const driver = new RedisDriver({
    id: "b08",
    name: "B08",
    type: "redis",
    host: "localhost",
  });
  const client = {
    scan: vi
      .fn()
      .mockResolvedValue({ cursor: "0", keys: Object.keys(valuesByKey) }),
    type: vi.fn().mockResolvedValue("string"),
    get: vi.fn(async (key: string) => valuesByKey[key]),
    ttl: vi.fn().mockResolvedValue(-1),
  };
  Object.assign(driver, {
    connected: true,
    client,
    databaseClients: new Map([[0, { promise: Promise.resolve(client) }]]),
  });
  return driver;
}

function mongo(documents: Record<string, unknown>[], emptySchema = false) {
  const driver = new MongoDBDriver({
    id: "b08",
    name: "B08",
    type: "mongodb",
    database: "db0",
  });
  const find = vi.fn(() => {
    const cursor = {
      limit: vi.fn(() => cursor),
      sort: vi.fn(() => {
        throw new Error("server BSON coercion unavailable");
      }),
      toArray: vi.fn(async () =>
        emptySchema && find.mock.calls.length === 1 ? [] : documents,
      ),
    };
    return cursor;
  });
  Object.assign(driver, {
    requireDb: () => ({ collection: () => ({ find }) }),
    isView: async () => false,
  });
  return { driver, find };
}

function dynamo(items: Record<string, unknown>[]) {
  const driver = new DynamoDBDriver({
    id: "b08",
    name: "B08",
    type: "dynamodb",
    awsRegion: "us-east-1",
  });
  const send = vi.fn(
    async (command: {
      constructor: { name: string };
      input?: Record<string, unknown>;
    }) => {
      if (command.constructor.name === "DescribeTableCommand")
        return {
          Table: {
            KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
            AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
          },
        };
      if (command.constructor.name === "GetItemCommand")
        return { Item: items[0] };
      return { Items: items };
    },
  );
  Object.assign(driver, { connected: true, client: { send } });
  return { driver, send };
}

describe("B08 actual driver fallback paths with SDK-boundary fixtures", () => {
  it("Redis sorts numeric string values exactly and range-filters adjacent int64s", async () => {
    const driver = redis(
      Object.fromEntries(values.map((value, i) => [`row:${i}`, value])),
    );
    const page = await driver.readTablePage(request);
    expect(page.rows.map((row) => row.value)).toEqual(ordered);
    expect(
      page.columns.find((column) => column.name === "value")?.category,
    ).toBe("decimal");
    const filtered = await driver.readTablePage({
      ...request,
      filters: [{ column: "value", operator: "gt", value: values[1] }],
    });
    expect(filtered.rows.map((row) => row.value)).toEqual([values[0]]);
  });

  it("Redis keeps numeric-looking keys textual and mixed value inference stable after filters", async () => {
    const driver = redis({
      "2": "9007199254740993",
      "10": "9007199254740992",
      "001": "text",
    });
    const page = await driver.readTablePage({
      ...request,
      filters: [{ column: "key", operator: "is_not_null" }],
      sort: { column: "key", direction: "asc" },
    });
    expect(page.rows.map((row) => row.key)).toEqual(["001", "10", "2"]);
    expect(page.columns.find((column) => column.name === "key")?.category).toBe(
      "text",
    );
    const filtered = await driver.readTablePage({
      ...request,
      filters: [{ column: "value", operator: "eq", value: "9007199254740993" }],
    });
    expect(
      filtered.columns.find((column) => column.name === "value")?.category,
    ).toBe("other");
  });

  it("Mongo server failure falls back to exact BSON Long/Decimal128 sorting and ranges", async () => {
    const documents = values.map((value, i) => ({
      _id: String(i),
      value: i < 4 ? Long.fromString(value) : Decimal128.fromString(value),
    }));
    const { driver, find } = mongo(documents);
    expect(
      (await driver.readTablePage(request)).rows.map((row) =>
        String(row.value).toLowerCase(),
      ),
    ).toEqual(ordered);
    expect(find.mock.calls.length).toBe(3);
    const page = await driver.readTablePage({
      ...request,
      filters: [{ column: "value", operator: "gt", value: values[1] }],
    });
    expect(page.rows.map((row) => row.value)).toEqual([values[0]]);
  });

  it.each([
    false,
    true,
  ])("Mongo text IDs remain text with empty schema=%s", async (emptySchema) => {
    const { driver } = mongo(
      ["2", "10", "001", "2026-01-01"].map((_id) => ({ _id })),
      emptySchema,
    );
    const page = await driver.readTablePage({
      ...request,
      sort: { column: "_id", direction: "asc" },
    });
    expect(page.rows.map((row) => row._id)).toEqual([
      "001",
      "10",
      "2",
      "2026-01-01",
    ]);
    expect(page.columns.find((column) => column.name === "_id")?.category).toBe(
      "text",
    );
  });

  it("Mongo mixed BSON/string rows do not numeric-coerce strings and use a transitive text sort", async () => {
    const { driver } = mongo([
      { _id: "a", value: Long.fromString("2") },
      { _id: "b", value: "11" },
      { _id: "c", value: Long.fromString("10") },
    ]);
    const page = await driver.readTablePage(request);
    expect(page.rows.map((row) => row.value)).toEqual(["10", "11", "2"]);
    const filtered = await driver.readTablePage({
      ...request,
      filters: [{ column: "value", operator: "gt", value: "2" }],
    });
    expect(filtered.rows.map((row) => row.value)).toEqual(["10"]);
  });

  it("Mongo empty schema infers native numeric families without inferring numeric strings", async () => {
    const { driver } = mongo(
      [
        { _id: "2", value: Decimal128.fromString("10.00000000000000000001") },
        { _id: "10", value: Long.fromString("2") },
      ],
      true,
    );
    const page = await driver.readTablePage(request);
    expect(page.rows.map((row) => row.value)).toEqual([
      "2",
      "10.00000000000000000001",
    ]);
    expect(
      page.columns.find((column) => column.name === "value")?.category,
    ).toBe("decimal");
    expect(page.columns.find((column) => column.name === "_id")?.category).toBe(
      "text",
    );
  });

  it("Dynamo materialization preserves numeric N precision and textual S identifiers", async () => {
    const { driver } = dynamo(
      values.map((value, i) => ({ id: { S: String(i) }, value: { N: value } })),
    );
    expect(
      (await driver.readTablePage(request)).rows.map((row) => row.value),
    ).toEqual(ordered);
    expect(
      (
        await driver.readTablePage({
          ...request,
          filters: [
            {
              column: "value",
              operator: "between",
              value: [values[5], values[5]],
            },
          ],
        })
      ).rows.map((row) => row.value),
    ).toEqual([values[5]]);
    const text = dynamo(
      ["2", "10", "001"].map((value) => ({
        id: { S: value },
        value: { S: value },
      })),
    );
    const page = await text.driver.readTablePage(request);
    expect(page.rows.map((row) => row.value)).toEqual(["001", "10", "2"]);
    expect(
      page.columns.find((column) => column.name === "value")?.category,
    ).toBe("text");
  });

  it("Dynamo streaming client filters exclude textual impostors in numeric columns", async () => {
    const { driver, send } = dynamo([
      {
        id: { S: "high" },
        value: { N: "0.10000000000000000002" },
        payload: { L: [{ S: "yes" }] },
      },
      {
        id: { S: "low" },
        value: { N: "0.10000000000000000001" },
        payload: { L: [{ S: "yes" }] },
      },
      {
        id: { S: "text" },
        value: { S: "0.10000000000000000003" },
        payload: { L: [{ S: "yes" }] },
      },
    ]);
    const page = await driver.readTablePage({
      ...request,
      sort: null,
      skipCount: true,
      filters: [
        { column: "payload", operator: "like", value: "yes" },
        { column: "value", operator: "gt", value: "0.10000000000000000001" },
      ],
    });
    expect(page.rows.map((row) => row.id)).toEqual(["high"]);
    // The L member predicate remains client-only; numeric comparison must
    // still reject the low N and the numerically larger textual S impostor.
    const scans = send.mock.calls
      .map(([command]) => command)
      .filter((command) => command.constructor.name === "ScanCommand");
    const filteredScans = scans.filter(
      (command) => command.input?.FilterExpression !== undefined,
    );
    expect(filteredScans).toHaveLength(1);
    expect(filteredScans[0].input).toMatchObject({
      TableName: request.table,
      FilterExpression: "#n0 > :v0",
    });
    expect(filteredScans[0].input?.ExpressionAttributeNames).toEqual({
      "#n0": "value",
    });
  });

  it.each([
    ["9007199254740993", 1],
    ["9007199254740992", 0],
  ] as const)("Dynamo GetItem fallback range-compares exact int64 %s", async (value, matches) => {
    const { driver, send } = dynamo([
      {
        id: { S: "key" },
        value: { N: value },
        payload: { L: [{ S: "yes" }] },
      },
    ]);
    const page = await driver.readTablePage({
      ...request,
      sort: null,
      skipCount: true,
      filters: [
        { column: "id", operator: "eq", value: "key" },
        { column: "payload", operator: "like", value: "yes" },
        { column: "value", operator: "gt", value: "9007199254740992" },
      ],
    });
    expect(page.rows).toHaveLength(matches);
    const getItems = send.mock.calls
      .map(([command]) => command)
      .filter((command) => command.constructor.name === "GetItemCommand");
    expect(getItems).toHaveLength(1);
    expect(getItems[0].input).toEqual({
      TableName: request.table,
      Key: { id: { S: "key" } },
    });
    // GetItem cannot apply residual predicates; L membership and exact int64
    // range comparison therefore remain client-side for both adjacent Ns.
    expect(getItems[0].input?.FilterExpression).toBeUndefined();
  });

  it("Elasticsearch fallback keeps IDs textual, including numeric/date-shaped IDs", async () => {
    const driver = new ElasticsearchDriver({
      id: "b08",
      name: "B08",
      type: "elasticsearch",
      host: "localhost",
    });
    const ids = [
      "2",
      "10",
      "001",
      "2026-01-01T00:00:00Z",
      "2025-12-31T23:30:00-02:00",
    ];
    Object.assign(driver, {
      connected: true,
      client: {
        search: vi.fn().mockResolvedValue({
          hits: {
            total: ids.length,
            hits: ids.map((_id) => ({ _id, _source: {} })),
          },
        }),
      },
    });
    const page = await driver.readTablePage({
      ...request,
      sort: { column: "_id", direction: "asc" },
    });
    expect(page.rows.map((row) => row._id)).toEqual(
      [...ids].sort((a, b) => a.localeCompare(b)),
    );
    expect(page.columns.find((column) => column.name === "_id")?.category).toBe(
      "text",
    );
    expect(
      (
        await driver.readTablePage({
          ...request,
          sort: null,
          filters: [{ column: "_id", operator: "gt", value: "10" }],
        })
      ).rows.map((row) => row._id),
    ).toContain("2");
  });
});
