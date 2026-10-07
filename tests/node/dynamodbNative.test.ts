import { marshall, NumberValueImpl, unmarshall } from "@aws-sdk/util-dynamodb";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import type {
  ColumnTypeMeta,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import {
  parseDynamoDbNativeQueryInput,
  parseDynamoDbNativeQueryInputs,
} from "../../src/shared/dynamodbNative";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";

const emptyBinaryCellMarker = "0x (empty binary)";

const config: ConnectionConfig = {
  id: "conn-ddb",
  name: "Dynamo",
  type: "dynamodb",
  awsRegion: "us-east-1",
};

function createColumns(): ColumnTypeMeta[] {
  return [
    {
      name: "tenant_id",
      type: "string",
      nativeType: "string",
      category: "text",
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      primaryKeyRole: "partition",
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "neq", "like", "in"],
      valueSemantics: "plain",
    },
    {
      name: "user_id",
      type: "string",
      nativeType: "string",
      category: "text",
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 2,
      primaryKeyRole: "sort",
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "neq", "like", "in", "gte", "lte"],
      valueSemantics: "plain",
    },
    {
      name: "email",
      type: "string",
      nativeType: "string",
      category: "text",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "neq", "like", "in", "is_null", "is_not_null"],
      valueSemantics: "plain",
    },
    {
      name: "age",
      type: "number",
      nativeType: "number",
      category: "float",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: [
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "between",
        "in",
        "is_null",
        "is_not_null",
      ],
      valueSemantics: "plain",
    },
  ];
}

function createMapColumn(): ColumnTypeMeta {
  return {
    name: "address",
    type: "map",
    nativeType: "map",
    category: "json",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "neq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

type CreateDriverOptions = {
  columns?: ColumnTypeMeta[];
  describeTable?: {
    KeySchema: Array<{
      AttributeName: string;
      KeyType: "HASH" | "RANGE";
    }>;
    AttributeDefinitions: Array<{
      AttributeName: string;
      AttributeType: "S" | "N" | "B";
    }>;
    GlobalSecondaryIndexes?: Array<{
      IndexName: string;
      KeySchema: Array<{
        AttributeName: string;
        KeyType: "HASH" | "RANGE";
      }>;
      Projection: { ProjectionType: "ALL" };
    }>;
  };
};

function createDriver(options: CreateDriverOptions = {}) {
  const columns = options.columns ?? createColumns();
  const describeTable = options.describeTable ?? {
    KeySchema: [
      { AttributeName: "tenant_id", KeyType: "HASH" },
      { AttributeName: "user_id", KeyType: "RANGE" },
    ],
    AttributeDefinitions: [
      { AttributeName: "tenant_id", AttributeType: "S" },
      { AttributeName: "user_id", AttributeType: "S" },
      { AttributeName: "email", AttributeType: "S" },
    ],
    GlobalSecondaryIndexes: [
      {
        IndexName: "email-index",
        KeySchema: [
          { AttributeName: "email", KeyType: "HASH" },
          { AttributeName: "user_id", KeyType: "RANGE" },
        ],
        Projection: { ProjectionType: "ALL" },
      },
    ],
  };

  const queuedResponses: unknown[] = [];
  const driver = new DynamoDBDriver(config);
  const clientSend = vi.fn(
    async (command: {
      constructor: { name: string };
      input?: Record<string, unknown>;
    }) => {
      if (command.constructor.name === "ListTablesCommand") {
        return { TableNames: ["users"] };
      }
      if (command.constructor.name === "DescribeTableCommand") {
        return { Table: describeTable };
      }
      return queuedResponses.shift() ?? {};
    },
  );

  const driverState = driver as unknown as {
    client: { send: typeof clientSend } | null;
    connected: boolean;
    describeColumns: ReturnType<typeof vi.fn>;
  };
  driverState.connected = true;
  driverState.client = { send: clientSend };
  driverState.describeColumns = vi.fn(async () => columns);

  return {
    driver,
    clientSend,
    queueResponses: (...responses: unknown[]) => {
      queuedResponses.push(...responses);
    },
  };
}

function toJsonSafeValue(value: unknown): unknown {
  if (Buffer.isBuffer(value)) {
    return value.toString("base64");
  }
  if (Array.isArray(value)) {
    return value.map((entry) => toJsonSafeValue(entry));
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        toJsonSafeValue(entry),
      ]),
    );
  }
  return value;
}

function commandInputs(
  clientSend: ReturnType<typeof vi.fn>,
  commandName: string,
): Array<Record<string, unknown>> {
  return clientSend.mock.calls
    .map(([command]) => command)
    .filter((command) => command.constructor.name === commandName)
    .map((command) => command.input as Record<string, unknown>);
}

describe("DynamoDB lossless marker filter inputs (R9)", () => {
  const literal = emptyBinaryCellMarker;
  const escapedLiteral = `\\${literal}`;
  const nextLiteral = `\\${literal}`;
  const escapedNextLiteral = `\\\\${literal}`;
  const request = {
    database: "us-east-1",
    schema: "us-east-1",
    table: "users",
    page: 1,
    pageSize: 25,
    sort: null,
    skipCount: true,
  };

  function createMarkerDriver(
    extraItems: Array<{
      tenant_id: string;
      user_id: string;
      email: unknown;
    }> = [],
  ) {
    const fixture = createDriver();
    const items = [
      { tenant_id: "tenant", user_id: literal, email: literal },
      { tenant_id: "tenant", user_id: nextLiteral, email: nextLiteral },
      { tenant_id: "tenant", user_id: "binary", email: Buffer.alloc(0) },
      { tenant_id: "tenant", user_id: "partial", email: `prefix\\0x suffix` },
      { tenant_id: "tenant", user_id: "slashes", email: "path\\\\leaf" },
      ...extraItems,
    ];
    const originalSend = fixture.clientSend.getMockImplementation();
    fixture.clientSend.mockImplementation(async (command) => {
      const input = command.input as Record<string, unknown>;
      if (command.constructor.name === "GetItemCommand") {
        const key = unmarshall(input.Key as Parameters<typeof unmarshall>[0]);
        const item = items.find(
          (candidate) =>
            candidate.tenant_id === key.tenant_id &&
            candidate.user_id === key.user_id,
        );
        return item ? { Item: marshall(item) } : {};
      }
      if (
        command.constructor.name === "ScanCommand" ||
        command.constructor.name === "QueryCommand"
      ) {
        const expression = String(input.FilterExpression ?? "");
        const contains = /^contains\((#n\d+), (:v\d+)\)$/.exec(expression);
        const between = /^(#n\d+) BETWEEN (:v\d+) AND (:v\d+)$/.exec(
          expression,
        );
        const range = /^(#n\d+) (>=|>|<=|<) (:v\d+)$/.exec(expression);
        const names = (input.ExpressionAttributeNames ?? {}) as Record<
          string,
          string
        >;
        const values = unmarshall(
          (input.ExpressionAttributeValues ?? {}) as Parameters<
            typeof unmarshall
          >[0],
        );
        const matches = items.filter((item) => {
          if (contains) {
            const haystack = item[names[contains[1]] as keyof typeof item];
            const needle = values[contains[2]];
            if (typeof haystack === "string")
              return haystack.includes(String(needle));
            if (haystack instanceof Set || Array.isArray(haystack)) {
              return [...haystack].some(
                (member) => typeof member === "string" && member === needle,
              );
            }
            return false;
          }
          if (between || range) {
            const condition = between ?? range;
            if (!condition) return false;
            const actual = item[names[condition[1]] as keyof typeof item];
            if (typeof actual !== "string") return false;
            const compare = (bound: unknown) =>
              Buffer.compare(
                Buffer.from(actual, "utf8"),
                Buffer.from(String(bound), "utf8"),
              );
            if (between)
              return (
                compare(values[between[2]]) >= 0 &&
                compare(values[between[3]]) <= 0
              );
            const cmp = compare(values[condition[3]]);
            return condition[2] === ">"
              ? cmp > 0
              : condition[2] === ">="
                ? cmp >= 0
                : condition[2] === "<"
                  ? cmp < 0
                  : cmp <= 0;
          }
          return true;
        });
        return { Items: matches.map((item) => marshall(item)) };
      }
      return originalSend?.(command) ?? {};
    });
    return fixture;
  }

  it.each([
    "native",
    "streaming",
    "materialized",
  ] as const)("gets each adjacent escaped S key from the actual request (%s)", async (mode) => {
    const { driver, clientSend } = createMarkerDriver();
    for (const [raw, display] of [
      [literal, escapedLiteral],
      [nextLiteral, escapedNextLiteral],
    ]) {
      const page = await driver.readTablePage({
        ...request,
        sort:
          mode === "materialized"
            ? { column: "email", direction: "asc" }
            : null,
        filters: [
          { column: "tenant_id", operator: "eq", value: "tenant" },
          { column: "user_id", operator: "eq", value: display },
          ...(mode === "streaming"
            ? [
                {
                  column: "email",
                  operator: "ilike",
                  value: display,
                } satisfies FilterExpression,
              ]
            : []),
        ],
      });
      expect(page.rows).toEqual([
        expect.objectContaining({ user_id: display, email: display }),
      ]);
      expect(commandInputs(clientSend, "GetItemCommand").at(-1)?.Key).toEqual(
        marshall({ tenant_id: "tenant", user_id: raw }),
      );
      expect(driver.coerceInputValue(display, createColumns()[1])).toBe(raw);
    }
  });

  it.each([
    [escapedLiteral, [escapedLiteral, escapedNextLiteral]],
    [escapedNextLiteral, [escapedNextLiteral]],
    [literal, [escapedLiteral, escapedNextLiteral]],
    ["empty binary", [escapedLiteral, escapedNextLiteral]],
    ["0x", [escapedLiteral, escapedNextLiteral, "partial"]],
    ["\\0x", [escapedNextLiteral, "partial"]],
    ["\\\\0x", []],
    ["path\\\\", ["slashes"]],
  ] as const)("keeps native Contains / streaming / materialized parity for %s", async (needle, expectedKeys) => {
    const results: string[][] = [];
    for (const mode of [
      "native",
      "streaming",
      "materialized",
      "materialized-like",
    ] as const) {
      const { driver, clientSend } = createMarkerDriver();
      const operator =
        mode === "native" || mode === "materialized-like" ? "like" : "ilike";
      const page = await driver.readTablePage({
        ...request,
        skipCount: mode !== "materialized" && mode !== "materialized-like",
        sort:
          mode === "materialized-like"
            ? { column: "email", direction: "asc" }
            : null,
        filters: [{ column: "email", operator, value: needle }],
      });
      results.push(page.rows.map((row) => String(row.user_id)).sort());
      if (mode === "native") {
        const input = commandInputs(clientSend, "ScanCommand")[0];
        const expectedNeedle =
          needle === escapedLiteral
            ? literal
            : needle === escapedNextLiteral
              ? nextLiteral
              : needle;
        expect(input.ExpressionAttributeValues).toEqual({
          ":v0": { S: expectedNeedle },
        });
        expect(
          driver.buildFilterCondition(createColumns()[2], "like", needle, 1)
            ?.params,
        ).toEqual([expectedNeedle]);
      }
    }
    expect(results).toEqual(
      Array.from({ length: 4 }, () => [...expectedKeys].sort()),
    );
  });

  it.each([
    { operator: "eq", value: escapedLiteral, expected: [escapedLiteral] },
    {
      operator: "eq",
      value: escapedNextLiteral,
      expected: [escapedNextLiteral],
    },
    { operator: "eq", value: literal, expected: [escapedLiteral] },
    {
      operator: "neq",
      value: escapedLiteral,
      expected: [escapedNextLiteral, "binary", "partial", "slashes"],
    },
    {
      operator: "in",
      value: `${escapedLiteral},${escapedNextLiteral}`,
      expected: [escapedLiteral, escapedNextLiteral],
    },
    {
      operator: "between",
      value: [escapedNextLiteral, escapedNextLiteral],
      expected: [escapedNextLiteral],
    },
    {
      operator: "gte",
      value: escapedNextLiteral,
      expected: [escapedNextLiteral, "partial", "slashes"],
    },
  ] satisfies Array<{
    operator: FilterExpression["operator"];
    value: string | [string, string];
    expected: string[];
  }>)("uses logical inputs in client $operator without S/B collisions", async ({
    operator,
    value,
    expected,
  }) => {
    for (const skipCount of [false, true]) {
      const { driver } = createMarkerDriver();
      const page = await driver.readTablePage({
        ...request,
        skipCount,
        filters: [
          { column: "email", operator, value } as FilterExpression,
          { column: "user_id", operator: "ilike", value: "" },
        ],
      });
      expect(page.rows.map((row) => row.user_id).sort()).toEqual(
        [...expected].sort(),
      );
    }
  });

  it("decodes native range/list parameters, with binary decoding only in native binary context", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses({ Items: [] }, { Items: [] });
    await driver.readTablePage({
      ...request,
      filters: [
        { column: "tenant_id", operator: "eq", value: "tenant" },
        {
          column: "user_id",
          operator: "between",
          value: [escapedLiteral, escapedNextLiteral],
        },
        {
          column: "email",
          operator: "in",
          value: `${escapedLiteral},${escapedNextLiteral}`,
        },
      ],
    });
    expect(
      commandInputs(clientSend, "QueryCommand")[0].ExpressionAttributeValues,
    ).toEqual({
      ":v0": { S: "tenant" },
      ":v1": { S: literal },
      ":v2": { S: nextLiteral },
      ":v3": { S: literal },
      ":v4": { S: nextLiteral },
    });
    const textColumn = createColumns()[2];
    const binaryColumn = {
      ...textColumn,
      nativeType: "binary",
      category: "binary" as const,
    };
    for (const column of [textColumn, binaryColumn]) {
      expect(
        driver.buildFilterCondition(column, "eq", escapedLiteral, 1)?.params,
      ).toEqual([literal]);
      expect(
        driver.buildFilterCondition(
          column,
          "between",
          [escapedLiteral, escapedNextLiteral],
          1,
        )?.params,
      ).toEqual([literal, nextLiteral]);
      expect(
        driver.buildFilterCondition(
          column,
          "in",
          `${escapedLiteral},${escapedNextLiteral}`,
          1,
        )?.params,
      ).toEqual([literal, nextLiteral]);
    }
    expect(
      driver.buildFilterCondition(textColumn, "eq", literal, 1)?.params,
    ).toEqual([literal]);
    expect(
      driver.buildFilterCondition(binaryColumn, "eq", literal, 1)?.params,
    ).toEqual([Buffer.alloc(0)]);
    const state = driver as unknown as {
      coerceFilterParameter: (
        column: ColumnTypeMeta | undefined,
        value: string,
      ) => unknown;
    };
    expect(state.coerceFilterParameter(undefined, literal)).toBe(literal);
    expect(state.coerceFilterParameter(undefined, escapedLiteral)).toBe(
      literal,
    );
  });

  it.each([
    {
      operator: "between",
      value: [escapedLiteral, escapedNextLiteral],
      expected: [escapedLiteral, escapedNextLiteral],
    },
    {
      operator: "gt",
      value: escapedLiteral,
      expected: [escapedNextLiteral, "partial", "slashes"],
    },
    {
      operator: "gte",
      value: escapedNextLiteral,
      expected: [escapedNextLiteral, "partial", "slashes"],
    },
    { operator: "lt", value: escapedNextLiteral, expected: [escapedLiteral] },
    { operator: "lte", value: escapedLiteral, expected: [escapedLiteral] },
    {
      operator: "between",
      value: ["\ue000", "😀"],
      expected: ["bmp", "astral"],
    },
  ] satisfies Array<{
    operator: FilterExpression["operator"];
    value: string | [string, string];
    expected: string[];
  }>)("matches native UTF-8 string $operator results, not locale order", async ({
    operator,
    value,
    expected,
  }) => {
    for (const mode of [
      "native",
      "materialized",
      "streaming-client",
      "materialized-client",
    ] as const) {
      const { driver } = createMarkerDriver(
        value[0] === "\ue000"
          ? [
              { tenant_id: "tenant", user_id: "bmp", email: "\ue000" },
              { tenant_id: "tenant", user_id: "astral", email: "😀" },
            ]
          : [],
      );
      if (mode.endsWith("client")) {
        // A binary first sample makes the predicate client-only; escaped
        // bounds still identify S and must compare the unfiltered raw S rows.
        vi.spyOn(driver, "describeColumns").mockResolvedValue(
          createColumns().map((column) =>
            column.name === "email"
              ? { ...column, nativeType: "binary", category: "binary" }
              : column,
          ),
        );
      }
      const page = await driver.readTablePage({
        ...request,
        skipCount: mode !== "materialized-client",
        sort:
          mode === "materialized"
            ? { column: "email", direction: "asc" }
            : null,
        filters: [{ column: "email", operator, value } as FilterExpression],
      });
      expect(page.rows.map((row) => row.user_id).sort()).toEqual(
        [...expected].sort(),
      );
    }
  });

  it.each([
    [
      escapedLiteral,
      [
        escapedLiteral,
        escapedNextLiteral,
        "ss-marker",
        "list-marker",
        "list-mixed",
      ],
    ],
    [escapedNextLiteral, [escapedNextLiteral, "ss-next", "list-next"]],
    ["member", ["ss-member", "list-member", "string-member"]],
    ["0x", [escapedLiteral, escapedNextLiteral, "partial"]],
  ] as const)("retains native SS/L string-member Contains matches with S sampling for %s", async (needle, expected) => {
    for (const mode of [
      "native",
      "materialized-like",
      "streaming-ilike",
      "materialized-ilike",
    ] as const) {
      const { driver } = createMarkerDriver([
        {
          tenant_id: "tenant",
          user_id: "ss-marker",
          email: new Set([literal]),
        },
        {
          tenant_id: "tenant",
          user_id: "ss-next",
          email: new Set([nextLiteral]),
        },
        { tenant_id: "tenant", user_id: "list-marker", email: [literal] },
        { tenant_id: "tenant", user_id: "list-next", email: [nextLiteral] },
        {
          tenant_id: "tenant",
          user_id: "list-mixed",
          email: [Buffer.alloc(0), literal],
        },
        {
          tenant_id: "tenant",
          user_id: "list-binary",
          email: [Buffer.from(literal)],
        },
        {
          tenant_id: "tenant",
          user_id: "ss-member",
          email: new Set(["member"]),
        },
        { tenant_id: "tenant", user_id: "list-member", email: ["member", 1] },
        {
          tenant_id: "tenant",
          user_id: "string-member",
          email: "prefix member suffix",
        },
        {
          tenant_id: "tenant",
          user_id: "list-substring",
          email: ["prefix member suffix"],
        },
        { tenant_id: "tenant", user_id: "number-set", email: new Set([1]) },
      ]);
      const page = await driver.readTablePage({
        ...request,
        skipCount: mode !== "materialized-ilike",
        sort:
          mode === "materialized-like"
            ? { column: "user_id", direction: "asc" }
            : null,
        filters: [
          {
            column: "email",
            operator: mode.endsWith("ilike") ? "ilike" : "like",
            value: needle,
          },
        ],
      });
      expect(page.rows.map((row) => row.user_id).sort()).toEqual(
        [...expected].sort(),
      );
    }
  });

  it.each([
    "string",
    "string set",
    "list",
  ] as const)("uses raw SS/L Contains membership independently of %s sampled metadata", async (nativeType) => {
    for (const [needle, expected] of [
      [
        literal,
        [
          escapedLiteral,
          escapedNextLiteral,
          "ss-marker",
          "list-marker",
          "list-mixed",
        ],
      ],
      [
        escapedLiteral,
        [
          escapedLiteral,
          escapedNextLiteral,
          "ss-marker",
          "list-marker",
          "list-mixed",
        ],
      ],
      [escapedNextLiteral, [escapedNextLiteral, "ss-next", "list-next"]],
      ["0x", [escapedLiteral, escapedNextLiteral, "partial"]],
      ["empty binary", [escapedLiteral, escapedNextLiteral]],
    ] as const) {
      for (const operator of ["like", "ilike"] as const) {
        for (const materialized of [false, true]) {
          const { driver, clientSend } = createMarkerDriver([
            {
              tenant_id: "tenant",
              user_id: "ss-marker",
              email: new Set([literal]),
            },
            {
              tenant_id: "tenant",
              user_id: "ss-next",
              email: new Set([nextLiteral]),
            },
            { tenant_id: "tenant", user_id: "list-marker", email: [literal] },
            { tenant_id: "tenant", user_id: "list-next", email: [nextLiteral] },
            {
              tenant_id: "tenant",
              user_id: "list-mixed",
              email: [Buffer.alloc(0), literal, 1],
            },
            {
              tenant_id: "tenant",
              user_id: "list-binary",
              email: [Buffer.alloc(0), Buffer.from(literal)],
            },
            {
              tenant_id: "tenant",
              user_id: "binary-set",
              email: new Set([Buffer.from(literal)]),
            },
            { tenant_id: "tenant", user_id: "number-set", email: new Set([1]) },
          ]);
          vi.spyOn(driver, "describeColumns").mockResolvedValue(
            createColumns().map((column) =>
              column.name === "email"
                ? {
                    ...column,
                    type: nativeType,
                    nativeType,
                    category: nativeType === "string" ? "text" : "array",
                  }
                : column,
            ),
          );
          const page = await driver.readTablePage({
            ...request,
            skipCount: !materialized,
            sort: materialized ? { column: "user_id", direction: "asc" } : null,
            filters: [{ column: "email", operator, value: needle }],
          });
          expect(page.rows.map((row) => row.user_id).sort()).toEqual(
            [...expected].sort(),
          );
          if (nativeType !== "string") {
            // Exercise client filtering of all raw rows, not just a native
            // pre-filtered subset which could hide membership rejections.
            expect(
              commandInputs(clientSend, "ScanCommand")[0].FilterExpression,
            ).toBeUndefined();
          }
        }
      }
    }
  });

  it.each([
    false,
    true,
  ])("keeps empty B separate from escaped S with binary metadata (skipCount=%s)", async (skipCount) => {
    const binaryColumn = {
      ...createColumns()[2],
      nativeType: "binary",
      category: "binary" as const,
    };
    for (const [input, expected] of [
      [literal, "binary"],
      [escapedLiteral, escapedLiteral],
      [escapedNextLiteral, escapedNextLiteral],
    ]) {
      const { driver, queueResponses } = createDriver({
        columns: [...createColumns().slice(0, 2), binaryColumn],
      });
      queueResponses({
        Items: [
          marshall({ user_id: literal, email: literal }),
          marshall({ user_id: nextLiteral, email: nextLiteral }),
          marshall({ user_id: "binary", email: Buffer.alloc(0) }),
        ],
      });
      const page = await driver.readTablePage({
        ...request,
        skipCount,
        filters: [{ column: "email", operator: "eq", value: input }],
      });
      expect(page.rows.map((row) => row.user_id)).toEqual([expected]);
    }
    const { driver, queueResponses } = createDriver({
      columns: [...createColumns().slice(0, 2), binaryColumn],
    });
    queueResponses({
      Items: [
        marshall({ user_id: literal, email: literal }),
        marshall({ user_id: nextLiteral, email: nextLiteral }),
        marshall({ user_id: "binary", email: Buffer.alloc(0) }),
      ],
    });
    const page = await driver.readTablePage({
      ...request,
      skipCount,
      filters: [
        {
          column: "email",
          operator: "in",
          value: `${literal},${escapedLiteral}`,
        },
      ],
    });
    expect(page.rows.map((row) => row.user_id).sort()).toEqual(
      ["binary", escapedLiteral].sort(),
    );
  });

  it.each([
    "string",
    "string set",
    "list",
    "binary",
    "map",
  ] as const)("uses only actual S substrings and SS/L string members with %s metadata", async (nativeType) => {
    const items = [
      { tenant_id: "tenant", user_id: "s-hex", email: "0xdead" },
      {
        tenant_id: "tenant",
        user_id: "b-hex",
        email: Buffer.from("dead", "hex"),
      },
      {
        tenant_id: "tenant",
        user_id: "map",
        email: { payload: "0xdead", text: "needle", count: 123, flag: true },
      },
      { tenant_id: "tenant", user_id: "number", email: 123 },
      { tenant_id: "tenant", user_id: "boolean", email: true },
      { tenant_id: "tenant", user_id: "null", email: null },
      { tenant_id: "tenant", user_id: "s-number", email: "123" },
      { tenant_id: "tenant", user_id: "s-boolean", email: "true" },
      { tenant_id: "tenant", user_id: "s-null", email: "null" },
      { tenant_id: "tenant", user_id: "s-needle", email: "has needle" },
      {
        tenant_id: "tenant",
        user_id: "ss-needle",
        email: new Set(["0x", "0xdead", "needle", "1", "true", "null"]),
      },
      {
        tenant_id: "tenant",
        user_id: "l-needle",
        email: ["0x", "0xdead", "needle", "1", "true", "null"],
      },
      {
        tenant_id: "tenant",
        user_id: "l-mixed",
        email: [Buffer.from("dead", "hex"), 123, true, null, "0x", "needle"],
      },
      {
        tenant_id: "tenant",
        user_id: "binary-set",
        email: new Set([Buffer.from("dead", "hex")]),
      },
      { tenant_id: "tenant", user_id: "number-set", email: new Set([123]) },
      {
        tenant_id: "tenant",
        user_id: "l-non-strings",
        email: [
          Buffer.from("dead", "hex"),
          123,
          true,
          null,
          { text: "needle" },
        ],
      },
    ];
    for (const [needle, expected] of [
      [
        "0x",
        [
          escapedLiteral,
          escapedNextLiteral,
          "partial",
          "s-hex",
          "ss-needle",
          "l-needle",
          "l-mixed",
        ],
      ],
      ["0xdead", ["s-hex", "ss-needle", "l-needle"]],
      ["dead", ["s-hex"]],
      ["needle", ["s-needle", "ss-needle", "l-needle", "l-mixed"]],
      ["1", ["s-number", "ss-needle", "l-needle"]],
      ["true", ["s-boolean", "ss-needle", "l-needle"]],
      ["null", ["s-null", "ss-needle", "l-needle"]],
    ] as const) {
      // Independent native evaluator consumes the actual S request needle.
      const native = createMarkerDriver(items);
      const nativePage = await native.driver.readTablePage({
        ...request,
        filters: [{ column: "email", operator: "like", value: needle }],
      });
      expect(commandInputs(native.clientSend, "ScanCommand")[0]).toMatchObject({
        FilterExpression: "contains(#n0, :v0)",
        ExpressionAttributeNames: { "#n0": "email" },
        ExpressionAttributeValues: { ":v0": { S: needle } },
      });
      const expectedKeys = [...expected].sort();
      expect(nativePage.rows.map((row) => row.user_id).sort()).toEqual(
        expectedKeys,
      );
      for (const operator of ["like", "ilike"] as const) {
        for (const materialized of [false, true]) {
          const { driver } = createMarkerDriver(items);
          vi.spyOn(driver, "describeColumns").mockResolvedValue(
            createColumns().map((column) =>
              column.name === "email"
                ? {
                    ...column,
                    type: nativeType,
                    nativeType,
                    category:
                      nativeType === "string"
                        ? "text"
                        : nativeType === "binary"
                          ? "binary"
                          : nativeType === "map"
                            ? "json"
                            : "array",
                  }
                : column,
            ),
          );
          const page = await driver.readTablePage({
            ...request,
            skipCount: !materialized,
            filters: [
              { column: "email", operator, value: needle },
              { column: "user_id", operator: "ilike", value: "" },
            ],
          });
          expect(page.rows.map((row) => row.user_id).sort()).toEqual(
            expectedKeys,
          );
        }
      }
    }
  });

  it("uses known raw S when categories are absent, never an untyped display string", () => {
    const { driver } = createMarkerDriver();
    const state = driver as unknown as {
      formatDynamoRowForDisplay: (
        row: Record<string, unknown>,
      ) => Record<string, unknown>;
      tableRowCategories: WeakMap<
        Record<string, unknown>,
        Map<string, unknown>
      >;
      applyDynamoFilters: (
        rows: Record<string, unknown>[],
        filters: FilterExpression[],
        columns: ColumnTypeMeta[],
      ) => Record<string, unknown>[];
    };
    const rows = [
      "0xdead",
      Buffer.from("dead", "hex"),
      { text: "0xdead" },
      123,
      true,
      null,
      literal,
      new Set([literal]),
      [Buffer.alloc(0), literal],
    ].map((email) => state.formatDynamoRowForDisplay({ email }));
    for (const row of rows) state.tableRowCategories.delete(row);
    expect(
      state.applyDynamoFilters(
        rows,
        [{ column: "email", operator: "like", value: "0x" }],
        createColumns(),
      ),
    ).toEqual([rows[0], rows[6]]);
    expect(
      state.applyDynamoFilters(
        rows,
        [{ column: "email", operator: "like", value: escapedLiteral }],
        createColumns(),
      ),
    ).toEqual(rows.slice(6));
    expect(
      state.applyDynamoFilters(
        [{ email: "0xdead" }],
        [{ column: "email", operator: "like", value: "0x" }],
        createColumns(),
      ),
    ).toEqual([]);
  });

  it.each([
    false,
    true,
  ])("never searches the empty B display token as a Contains haystack (skipCount=%s)", async (skipCount) => {
    const binaryColumn = {
      ...createColumns()[2],
      nativeType: "binary",
      category: "binary" as const,
    };
    for (const needle of ["0x", "empty binary", literal, escapedLiteral]) {
      const { driver, queueResponses } = createDriver({
        columns: [...createColumns().slice(0, 2), binaryColumn],
      });
      queueResponses({
        Items: [
          marshall({ user_id: "string", email: literal }),
          marshall({ user_id: "binary", email: Buffer.alloc(0) }),
        ],
      });
      const page = await driver.readTablePage({
        ...request,
        skipCount,
        filters: [{ column: "email", operator: "like", value: needle }],
      });
      expect(page.rows.map((row) => row.user_id)).toEqual(["string"]);
    }
  });
});

describe("parseDynamoDbNativeQueryInput", () => {
  it("renders native query numbers as exact scalars with numeric metadata", async () => {
    const { driver, queueResponses } = createDriver();
    queueResponses({
      Items: [
        {
          amount: { N: "9007199254740993.1250" },
          count: { N: "9007199254740993" },
        },
      ],
    });
    const result = await driver.query(JSON.stringify({ TableName: "users" }));
    expect(result.rows).toEqual([
      { __col_0: "9007199254740993.1250", __col_1: "9007199254740993" },
    ]);
    expect(result.columnMeta).toEqual([
      { category: "decimal" },
      { category: "integer" },
    ]);
  });

  it("preserves prototype-like keys and user objects resembling raw numeric tokens", () => {
    const { driver } = createDriver();
    const text =
      '{"__proto__":{"n":9007199254740993},"nested":{"__rapidbRawNumber":true,"raw":"123","keep":"data"}}';
    const value = driver.coerceInputValue(text, createMapColumn());
    expect(Object.hasOwn(value as object, "__proto__")).toBe(true);
    expect(driver.formatOutputValue(value, createMapColumn())).toBe(text);
  });

  it("guards nested special values even when another row supplied string metadata", () => {
    const { driver } = createDriver();
    const column = {
      ...createMapColumn(),
      nativeType: "string",
      category: "text" as const,
    };
    const displayed = driver.formatOutputValue(
      { nested: new Set(["a"]) },
      column,
    );
    expect(displayed).toMatch(/^Read-only DynamoDB value/);
    expect(() => driver.coerceInputValue(displayed, column)).toThrow(
      "read-only",
    );
  });

  it("parses the empty-binary marker independently of sampled metadata", () => {
    const { driver } = createDriver();
    const binaryColumn: ColumnTypeMeta = {
      ...createMapColumn(),
      name: "payload",
      type: "binary",
      nativeType: "binary",
      category: "binary",
    };
    const textColumn: ColumnTypeMeta = {
      ...binaryColumn,
      name: "note",
      type: "string",
      nativeType: "string",
      category: "text",
    };

    expect(driver.coerceInputValue("0x", binaryColumn)).toEqual(
      Buffer.alloc(0),
    );
    expect(driver.coerceInputValue(emptyBinaryCellMarker, textColumn)).toEqual(
      Buffer.alloc(0),
    );
    expect(driver.coerceInputValue("", binaryColumn)).toBe("");
    expect(driver.coerceInputValue("0xA", binaryColumn)).toBe("0xA");
    expect(driver.coerceInputValue("0x", textColumn)).toBe("0x");

    const escapedMarker = `\\${emptyBinaryCellMarker}`;
    expect(driver.formatOutputValue(emptyBinaryCellMarker, textColumn)).toBe(
      escapedMarker,
    );
    expect(driver.coerceInputValue(escapedMarker, binaryColumn)).toBe(
      emptyBinaryCellMarker,
    );
    expect(
      driver.coerceInputValue(`\\\\${emptyBinaryCellMarker}`, textColumn),
    ).toBe(escapedMarker);
  });

  it.each(
    ["map", "list", "binary", "string set", "number", "string"].flatMap(
      (sampleType) =>
        [
          { actual: "string", value: "hello", expected: "hello" },
          {
            actual: "JSON-looking string",
            value: '{"n":9007199254740993}',
            expected: '{"n":9007199254740993}',
          },
          {
            actual: "set-looking string",
            value: "<<'literal'>>",
            expected: "<<'literal'>>",
          },
          {
            actual: "binary-looking string",
            value: "0xdeadbeef",
            expected: "0xdeadbeef",
          },
          {
            actual: "exact number",
            value: NumberValueImpl.from("9007199254740993.1250"),
            expected: "9007199254740993.1250",
          },
          { actual: "boolean", value: true, expected: true },
          { actual: "null", value: null, expected: null },
          { actual: "missing", value: undefined, expected: null },
          {
            actual: "string set",
            value: new Set(["a", "b"]),
            expected: "<<'a', 'b'>>",
          },
          {
            actual: "number set",
            value: new Set([NumberValueImpl.from("9007199254740993.1250")]),
            expected: "<<9007199254740993.1250>>",
          },
          {
            actual: "binary set",
            value: new Set([Uint8Array.from([0xde, 0xad])]),
            expected: "<<0xdead>>",
          },
          {
            actual: "binary",
            value: Uint8Array.from([0xde, 0xad]),
            expected: "0xdead",
          },
          {
            actual: "map",
            value: { n: NumberValueImpl.from("9007199254740993.1250") },
            expected: '{"n":9007199254740993.1250}',
          },
          {
            actual: "list",
            value: ["hello", NumberValueImpl.from("9007199254740993.1250")],
            expected: '["hello",9007199254740993.1250]',
          },
        ].map((testCase) => ({ sampleType, ...testCase })),
    ),
  )("formats actual $actual independently of sampled $sampleType metadata", ({
    sampleType,
    value,
    expected,
  }) => {
    const { driver } = createDriver();
    const category =
      sampleType === "map"
        ? "json"
        : sampleType === "list" || sampleType.endsWith(" set")
          ? "array"
          : sampleType === "binary"
            ? "binary"
            : sampleType === "number"
              ? "decimal"
              : "text";
    expect(
      driver.formatOutputValue(value, {
        ...createMapColumn(),
        type: sampleType,
        nativeType: sampleType,
        category,
      }),
    ).toEqual(expected);
  });

  it.each([
    new Set(["a"]),
    Buffer.from([1, 2]),
    new Set([NumberValueImpl.from("9007199254740993")]),
  ])("blocks lossy nested set/binary table edits (%s)", (special) => {
    const { driver } = createDriver();
    const column = createMapColumn();
    const displayed = driver.formatOutputValue({ nested: [special] }, column);
    expect(displayed).toMatch(/^Read-only DynamoDB value/);
    expect(() => driver.coerceInputValue(displayed, column)).toThrow(
      "read-only",
    );
  });

  it("parses a single raw AWS request body", () => {
    expect(
      parseDynamoDbNativeQueryInput(
        JSON.stringify({ TableName: "users", ConsistentRead: true }),
      ),
    ).toEqual({ TableName: "users", ConsistentRead: true });
  });

  it("parses multiple consecutive raw AWS request bodies", () => {
    expect(
      parseDynamoDbNativeQueryInputs(
        [
          JSON.stringify({ TableName: "users", Limit: 1 }, null, 2),
          JSON.stringify({ TableName: "users", Limit: 2 }, null, 2),
        ].join("\n\n"),
      ),
    ).toEqual([
      { TableName: "users", Limit: 1 },
      { TableName: "users", Limit: 2 },
    ]);
  });

  it("rejects empty, malformed, and legacy envelope payloads", () => {
    expect(() => parseDynamoDbNativeQueryInput("   ")).toThrow(
      /cannot be empty/i,
    );
    expect(() => parseDynamoDbNativeQueryInput("{")).toThrow(
      /must be valid JSON/i,
    );
    expect(() =>
      parseDynamoDbNativeQueryInput(
        JSON.stringify({ operation: "Scan", input: { TableName: "users" } }),
      ),
    ).toThrow(/remove the legacy operation wrapper/i);
  });
});

describe("DynamoDBDriver native API", () => {
  it.each([
    '{"TableName":}',
    '{"TableName":}{"TableName":"users"}',
    '{"TableName":"users"}{"TableName":}',
    '{"TableName":"users"}{"TableName":',
  ])("wraps malformed JSON and executes no part of %s", async (text) => {
    const { driver, clientSend } = createDriver();
    expect(() => parseDynamoDbNativeQueryInputs(text)).toThrow(
      /^DynamoDB native query text must be valid JSON\./,
    );
    await expect(driver.query(text)).rejects.toThrow(
      /^DynamoDB native query text must be valid JSON\./,
    );
    expect(clientSend).not.toHaveBeenCalled();
  });

  it("executes native JSON queries from the editor", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses({
      Items: [
        marshall({
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        }),
      ],
    });

    const result = await driver.query(JSON.stringify({ TableName: "users" }), [
      "Scan",
    ]);

    const scanInputs = commandInputs(clientSend, "ScanCommand");
    expect(scanInputs).toHaveLength(1);
    expect(scanInputs[0]).toMatchObject({ TableName: "users" });
    expect(result.rowCount).toBe(1);
    expect(result.columns).toEqual(
      expect.arrayContaining(["email", "tenant_id", "user_id"]),
    );
  });

  it("infers UpdateItem when the editor action is still the generic default", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses({ Attributes: marshall({ tenant_id: "tenant-1" }) });

    const result = await driver.query(
      JSON.stringify({
        TableName: "users",
        Key: marshall({ tenant_id: "tenant-1", user_id: "user-1" }),
        UpdateExpression: "SET #email = :email",
        ExpressionAttributeNames: { "#email": "email" },
        ExpressionAttributeValues: {
          ":email": marshall({ value: "updated@example.com" }).value,
        },
        ReturnValues: "ALL_NEW",
      }),
      ["Query"],
    );

    expect(commandInputs(clientSend, "UpdateItemCommand")).toHaveLength(1);
    expect(result.rowCount).toBe(1);
    expect(result.affectedRows).toBe(1);
  });

  it("executes multiple consecutive request bodies with the selected action", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-1",
            email: "one@example.com",
          }),
        ],
      },
      {
        Items: [
          marshall({
            tenant_id: "tenant-2",
            user_id: "user-2",
            email: "two@example.com",
          }),
        ],
      },
    );

    const result = await driver.query(
      [
        JSON.stringify({ TableName: "users", Limit: 1 }, null, 2),
        JSON.stringify({ TableName: "users", Limit: 2 }, null, 2),
      ].join("\n\n"),
      ["Scan"],
    );

    const scanInputs = commandInputs(clientSend, "ScanCommand");
    expect(scanInputs).toHaveLength(2);
    expect(scanInputs[0]).toMatchObject({ TableName: "users", Limit: 1 });
    expect(scanInputs[1]).toMatchObject({ TableName: "users", Limit: 2 });
    expect(result.rowCount).toBe(2);
    expect(result.rows.flatMap((row) => Object.values(row))).toEqual(
      expect.arrayContaining([
        "tenant-1",
        "user-1",
        "one@example.com",
        "tenant-2",
        "user-2",
        "two@example.com",
      ]),
    );
  });

  it("dispatches batch and transaction native commands from selected actions", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {
        Responses: {
          users: [marshall({ tenant_id: "tenant-1", user_id: "user-1" })],
          audit: [marshall({ id: "event-1" })],
        },
      },
      {
        Responses: [
          {
            Item: marshall({ tenant_id: "tenant-2", user_id: "user-2" }),
          },
        ],
      },
      {
        UnprocessedItems: {
          users: [
            {
              DeleteRequest: {
                Key: marshall({ tenant_id: "tenant-4", user_id: "user-4" }),
              },
            },
          ],
        },
      },
      {},
    );

    const batchGetResult = await driver.query(
      JSON.stringify({
        RequestItems: {
          users: {
            Keys: [marshall({ tenant_id: "tenant-1", user_id: "user-1" })],
          },
          audit: {
            Keys: [marshall({ id: "event-1" })],
          },
        },
      }),
      ["batchGetItemCommand"],
    );

    const transactGetResult = await driver.query(
      JSON.stringify({
        TransactItems: [
          {
            Get: {
              TableName: "users",
              Key: marshall({ tenant_id: "tenant-2", user_id: "user-2" }),
            },
          },
        ],
      }),
      ["transactgetitems"],
    );

    const batchWriteResult = await driver.query(
      JSON.stringify({
        RequestItems: {
          users: [
            {
              PutRequest: {
                Item: marshall({ tenant_id: "tenant-3", user_id: "user-3" }),
              },
            },
            {
              DeleteRequest: {
                Key: marshall({ tenant_id: "tenant-4", user_id: "user-4" }),
              },
            },
          ],
        },
      }),
      ["BatchWriteItemCommand"],
    );

    const transactWriteResult = await driver.query(
      JSON.stringify({
        TransactItems: [
          {
            Put: {
              TableName: "users",
              Item: marshall({ tenant_id: "tenant-5", user_id: "user-5" }),
            },
          },
          {
            Delete: {
              TableName: "users",
              Key: marshall({ tenant_id: "tenant-6", user_id: "user-6" }),
            },
          },
        ],
      }),
      ["TransactWriteItems"],
    );

    expect(commandInputs(clientSend, "BatchGetItemCommand")).toHaveLength(1);
    expect(commandInputs(clientSend, "TransactGetItemsCommand")).toHaveLength(
      1,
    );
    expect(commandInputs(clientSend, "BatchWriteItemCommand")).toHaveLength(1);
    expect(commandInputs(clientSend, "TransactWriteItemsCommand")).toHaveLength(
      1,
    );
    expect(batchGetResult.rowCount).toBe(2);
    expect(transactGetResult.rowCount).toBe(1);
    expect(batchWriteResult.affectedRows).toBe(1);
    expect(transactWriteResult.affectedRows).toBe(2);
    expect(
      [...batchGetResult.rows, ...transactGetResult.rows].flatMap((row) =>
        Object.values(row),
      ),
    ).toEqual(
      expect.arrayContaining([
        "users",
        "audit",
        "event-1",
        "tenant-1",
        "user-1",
        "tenant-2",
        "user-2",
      ]),
    );
  });

  it("rejects saved legacy PartiQL queries", async () => {
    const { driver } = createDriver();

    await expect(
      driver.query('SELECT * FROM "users" WHERE "tenant_id" = \'tenant-1\''),
    ).rejects.toThrow(/PartiQL is no longer supported/i);
  });

  it("treats a bare table request body as Scan", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses({ Items: [] });

    await expect(
      driver.query(JSON.stringify({ TableName: "users" })),
    ).resolves.toMatchObject({ rowCount: 0 });
    expect(commandInputs(clientSend, "ScanCommand")).toHaveLength(1);
  });

  it("builds native JSON previews for insert, update, and delete mutations", () => {
    const { driver } = createDriver();

    const insertPreview = JSON.parse(
      driver.buildMutationPreviewStatement(
        "insert",
        "us-east-1",
        "us-east-1",
        "users",
        {
          values: {
            tenant_id: "tenant-1",
            user_id: "user-1",
            email: "person@example.com",
          },
        },
      ),
    ) as Record<string, unknown>;
    expect(insertPreview).toMatchObject({ TableName: "users" });
    expect(insertPreview).not.toHaveProperty("operation");

    const updatePreview = JSON.parse(
      driver.buildMutationPreviewStatement(
        "update",
        "us-east-1",
        "us-east-1",
        "users",
        {
          primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
          changes: { email: "next@example.com" },
        },
      ),
    ) as Record<string, unknown>;
    expect(updatePreview).toMatchObject({
      TableName: "users",
      UpdateExpression: "SET #u0 = :u0",
    });

    const deletePreviews = driver
      .buildMutationPreviewStatement(
        "delete",
        "us-east-1",
        "us-east-1",
        "users",
        {
          primaryKeyValuesList: [
            { tenant_id: "tenant-1", user_id: "user-1" },
            { tenant_id: "tenant-2", user_id: "user-2" },
          ],
        },
      )
      .split("\n\n")
      .map((entry) => JSON.parse(entry) as Record<string, unknown>);
    expect(deletePreviews).toHaveLength(2);
    expect(deletePreviews[0]).toMatchObject({ TableName: "users" });
    expect(deletePreviews[1]).toMatchObject({ TableName: "users" });
    expect(deletePreviews[0]).not.toHaveProperty("operation");
    expect(deletePreviews[1]).not.toHaveProperty("operation");
  });

  it("coerces edited DynamoDB values into native types before building previews", async () => {
    const { driver } = createDriver();
    const mutationService = new TableMutationService(
      {
        getConnection: () => ({ id: "conn-ddb" }),
        getDriver: () => driver,
      } as never,
      {
        getColumns: async () => [
          ...createColumns(),
          {
            name: "active",
            type: "boolean",
            nativeType: "boolean",
            category: "boolean",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["eq", "neq", "is_null", "is_not_null"],
            valueSemantics: "boolean",
          },
          {
            name: "profile",
            type: "map",
            nativeType: "map",
            category: "json",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["like", "is_null", "is_not_null"],
            valueSemantics: "plain",
          },
          {
            name: "tags",
            type: "string set",
            nativeType: "string set",
            category: "array",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["like", "is_null", "is_not_null"],
            valueSemantics: "plain",
          },
          {
            name: "history",
            type: "list",
            nativeType: "list",
            category: "array",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["like", "is_null", "is_not_null"],
            valueSemantics: "plain",
          },
          {
            name: "payload",
            type: "binary",
            nativeType: "binary",
            category: "binary",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: false,
            filterOperators: ["is_null", "is_not_null"],
            valueSemantics: "plain",
          },
        ],
      },
    );

    const plan = await mutationService.prepareInsertRow(
      "conn-ddb",
      "us-east-1",
      "us-east-1",
      "users",
      {
        tenant_id: "tenant-1",
        user_id: "user-1",
        age: "31",
        active: "true",
        profile: '{"tier":"pro","visits":3}',
        tags: '["alpha","beta"]',
        history: '[1,"two",true]',
        payload: "0xdeadbeef",
      },
    );

    expect(plan.mode).toBe("driver");
    expect(plan.values).toEqual({
      tenant_id: "tenant-1",
      user_id: "user-1",
      age: NumberValueImpl.from("31"),
      active: true,
      profile: { tier: "pro", visits: NumberValueImpl.from("3") },
      tags: new Set(["alpha", "beta"]),
      history: [NumberValueImpl.from("1"), "two", true],
      payload: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
    });

    const preview = JSON.parse(plan.previewStatements[0] ?? "{}") as {
      ConditionExpression?: string;
      ExpressionAttributeNames?: Record<string, unknown>;
      Item?: Record<string, unknown>;
    };
    expect(preview.ConditionExpression).toBe(
      "attribute_not_exists(#k0) AND attribute_not_exists(#k1)",
    );
    expect(preview.ExpressionAttributeNames).toEqual({
      "#k0": "tenant_id",
      "#k1": "user_id",
    });
    expect(preview.Item).toEqual(
      toJsonSafeValue(
        marshall(
          {
            tenant_id: "tenant-1",
            user_id: "user-1",
            age: 31,
            active: true,
            profile: { tier: "pro", visits: 3 },
            tags: new Set(["alpha", "beta"]),
            history: [1, "two", true],
            payload: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
          },
          { removeUndefinedValues: true },
        ),
      ),
    );
  });

  it("edits, marshals, and reloads empty binary without changing text or NULL", async () => {
    const payloadColumn: ColumnTypeMeta = {
      ...createMapColumn(),
      name: "payload",
      type: "binary",
      nativeType: "binary",
      category: "binary",
      filterable: false,
      filterOperators: ["is_null", "is_not_null"],
    };
    const columns = [...createColumns(), payloadColumn];
    const { driver, clientSend, queueResponses } = createDriver({ columns });
    queueResponses(
      { Attributes: marshall({ payload: Buffer.alloc(0) }) },
      { Attributes: marshall({ payload: "" }) },
      { Attributes: marshall({ email: "0x" }) },
      { Attributes: marshall({ email: emptyBinaryCellMarker }) },
      { Attributes: marshall({ email: null }) },
    );
    const mutationService = new TableMutationService(
      {
        getConnection: () => ({ id: "conn-ddb" }),
        getDriver: () => driver,
      } as never,
      { getColumns: async () => columns },
    );
    const keys = { tenant_id: "tenant-1", user_id: "user-1" };
    const emptyBinaryDisplay = driver.formatOutputValue(
      Buffer.alloc(0),
      payloadColumn,
    );
    expect(emptyBinaryDisplay).toBe(emptyBinaryCellMarker);

    await mutationService.updateRow(
      "conn-ddb",
      "us-east-1",
      "us-east-1",
      "users",
      keys,
      { payload: String(emptyBinaryDisplay) },
    );
    await mutationService.updateRow(
      "conn-ddb",
      "us-east-1",
      "us-east-1",
      "users",
      keys,
      { payload: "" },
    );
    await mutationService.updateRow(
      "conn-ddb",
      "us-east-1",
      "us-east-1",
      "users",
      keys,
      { email: "0x" },
    );
    const literalMarkerDisplay = driver.formatOutputValue(
      emptyBinaryCellMarker,
      columns.find((column) => column.name === "email") as ColumnTypeMeta,
    );
    expect(literalMarkerDisplay).toBe(`\\${emptyBinaryCellMarker}`);
    await mutationService.updateRow(
      "conn-ddb",
      "us-east-1",
      "us-east-1",
      "users",
      keys,
      { email: String(literalMarkerDisplay) },
    );
    await mutationService.updateRow(
      "conn-ddb",
      "us-east-1",
      "us-east-1",
      "users",
      keys,
      { email: NULL_SENTINEL },
    );

    const updates = commandInputs(clientSend, "UpdateItemCommand");
    expect(updates).toHaveLength(5);
    const updateValues = updates.map(
      (update) => update.ExpressionAttributeValues as Record<string, unknown>,
    );
    const binaryAttribute = updateValues[0][":u0"] as never;
    expect(binaryAttribute).toEqual({ B: Buffer.alloc(0) });
    const reloadedBinary = unmarshall({ payload: binaryAttribute }).payload;
    expect(driver.formatOutputValue(reloadedBinary, payloadColumn)).toBe(
      emptyBinaryCellMarker,
    );
    expect(updateValues[1][":u0"]).toEqual({ S: "" });
    expect(updateValues[2][":u0"]).toEqual({ S: "0x" });
    expect(updateValues[3][":u0"]).toEqual({ S: emptyBinaryCellMarker });
    expect(updateValues[4][":u0"]).toEqual({ NULL: true });
  });

  it("saves an empty binary cell when the first sampled row is string-typed", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    const request = {
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 25,
      filters: [],
      sort: null,
      skipCount: true,
    };
    const firstRow = {
      tenant_id: "tenant-1",
      user_id: "user-1",
      payload: "ordinary text",
    };
    const binaryRow = {
      tenant_id: "tenant-1",
      user_id: "user-2",
      payload: Buffer.alloc(0),
    };
    queueResponses(
      { Items: [marshall(firstRow), marshall(binaryRow)] },
      { Attributes: marshall(binaryRow) },
      { Items: [marshall(firstRow), marshall(binaryRow)] },
    );

    const page = await driver.readTablePage(request);
    const payloadColumn = page.columns.find(
      (column) => column.name === "payload",
    );
    expect(payloadColumn?.nativeType).toBe("string");
    expect(page.rows[1]?.payload).toBe(emptyBinaryCellMarker);

    const mutationService = new TableMutationService(
      {
        getConnection: () => ({ id: "conn-ddb" }),
        getDriver: () => driver,
      } as never,
      { getColumns: async () => page.columns },
    );
    await mutationService.updateRow(
      "conn-ddb",
      "us-east-1",
      "us-east-1",
      "users",
      { tenant_id: binaryRow.tenant_id, user_id: binaryRow.user_id },
      { payload: String(page.rows[1]?.payload) },
    );

    const update = commandInputs(clientSend, "UpdateItemCommand")[0];
    const updateValues = update?.ExpressionAttributeValues as
      | Record<string, unknown>
      | undefined;
    expect(updateValues?.[":u0"]).toEqual({ B: Buffer.alloc(0) });

    const reloadedPage = await driver.readTablePage(request);
    expect(reloadedPage.rows[1]?.payload).toBe(emptyBinaryCellMarker);
  });

  it("executes native item operations for insert, update, and delete", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {},
      { Attributes: marshall({ tenant_id: "tenant-1" }) },
      { Attributes: marshall({ tenant_id: "tenant-1" }) },
      {},
    );

    await expect(
      driver.insertRow({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        values: {
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        },
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        updates: [
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
            changes: { email: "next@example.com" },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    await expect(
      driver.deleteRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        primaryKeyValuesList: [
          { tenant_id: "tenant-1", user_id: "user-1" },
          { tenant_id: "tenant-2", user_id: "user-2" },
        ],
      }),
    ).resolves.toMatchObject({
      affectedRows: 1,
      rowOutcomes: [{ status: "deleted" }, { status: "notfound" }],
    });

    const putInputs = commandInputs(clientSend, "PutItemCommand");
    expect(putInputs[0]).toMatchObject({
      TableName: "users",
      ConditionExpression:
        "attribute_not_exists(#k0) AND attribute_not_exists(#k1)",
      ExpressionAttributeNames: {
        "#k0": "tenant_id",
        "#k1": "user_id",
      },
      Item: marshall(
        {
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        },
        { removeUndefinedValues: true },
      ),
    });

    const updateInputs = commandInputs(clientSend, "UpdateItemCommand");
    expect(updateInputs[0]).toMatchObject({
      TableName: "users",
      Key: marshall(
        { tenant_id: "tenant-1", user_id: "user-1" },
        { removeUndefinedValues: true },
      ),
      UpdateExpression: "SET #u0 = :u0",
      ConditionExpression: "attribute_exists(#k0) AND attribute_exists(#k1)",
      ReturnValues: "ALL_NEW",
      ExpressionAttributeNames: {
        "#u0": "email",
        "#k0": "tenant_id",
        "#k1": "user_id",
      },
      ExpressionAttributeValues: {
        ":u0": marshall(
          { value: "next@example.com" },
          { removeUndefinedValues: true },
        ).value,
      },
    });

    const deleteInputs = commandInputs(clientSend, "DeleteItemCommand");
    expect(deleteInputs).toHaveLength(2);
    expect(deleteInputs[0]).toMatchObject({
      TableName: "users",
      Key: marshall(
        { tenant_id: "tenant-1", user_id: "user-1" },
        { removeUndefinedValues: true },
      ),
      ConditionExpression: "attribute_exists(#k0) AND attribute_exists(#k1)",
      ReturnValues: "ALL_OLD",
      ExpressionAttributeNames: {
        "#k0": "tenant_id",
        "#k1": "user_id",
      },
    });
  });

  it("uses TransactWriteItems for multi-row table updates", async () => {
    const { driver, clientSend } = createDriver();

    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        updates: [
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
            changes: { email: "one@example.com" },
            originalValues: { email: "old-one@example.com" },
          },
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-2" },
            changes: { email: "two@example.com" },
            originalValues: { email: "old-two@example.com" },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 2 });

    const transaction = commandInputs(clientSend, "TransactWriteItemsCommand");
    expect(transaction).toHaveLength(1);
    expect(transaction[0]?.TransactItems).toHaveLength(2);
    expect(transaction[0]).toEqual(
      expect.objectContaining({
        TransactItems: expect.arrayContaining([
          expect.objectContaining({
            Update: expect.objectContaining({
              ConditionExpression: expect.stringContaining("#o0 = :o0"),
            }),
          }),
        ]),
      }),
    );
  });

  it("reports an atomic DynamoDB conditional failure for every transaction row", async () => {
    const { driver, clientSend } = createDriver();
    clientSend.mockImplementation(async (command) => {
      if (command.constructor.name === "DescribeTableCommand") {
        return {
          Table: {
            KeySchema: [
              { AttributeName: "tenant_id", KeyType: "HASH" },
              { AttributeName: "user_id", KeyType: "RANGE" },
            ],
            AttributeDefinitions: [
              { AttributeName: "tenant_id", AttributeType: "S" },
              { AttributeName: "user_id", AttributeType: "S" },
            ],
          },
        };
      }
      throw Object.assign(new Error("Conditional check failed"), {
        name: "TransactionCanceledException",
        CancellationReasons: [
          { Code: "ConditionalCheckFailed" },
          { Code: "None" },
        ],
      });
    });

    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        updates: [
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
            changes: { email: "one@example.com" },
          },
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-2" },
            changes: { email: "two@example.com" },
          },
        ],
      }),
    ).resolves.toEqual({
      affectedRows: 0,
      updateRowOutcomes: [
        { rowIndex: 0, status: "not_applied" },
        { rowIndex: 1, status: "not_applied" },
      ],
    });
    expect(commandInputs(clientSend, "TransactWriteItemsCommand")).toHaveLength(
      1,
    );
  });

  it.each([
    "ValidationError",
    "ProvisionedThroughputExceeded",
    "TransactionConflict",
    "MixedConditionalValidation",
    "MissingReasons",
  ])("preserves %s cancellation diagnostics without claiming any write committed", async (reason) => {
    const { driver, clientSend } = createDriver();
    const error = Object.assign(new Error(`Transaction cancelled: ${reason}`), {
      name: "TransactionCanceledException",
      ...(reason === "MissingReasons"
        ? {}
        : {
            CancellationReasons:
              reason === "MixedConditionalValidation"
                ? [
                    { Code: "ConditionalCheckFailed" },
                    { Code: "ValidationError" },
                  ]
                : [{ Code: reason }, { Code: "None" }],
          }),
    });
    const originalSend = clientSend.getMockImplementation();
    if (!originalSend)
      throw new Error("Expected DynamoDB fixture implementation");
    clientSend.mockImplementation(async (command) => {
      if (command.constructor.name === "TransactWriteItemsCommand") throw error;
      return originalSend(command);
    });
    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "",
        table: "users",
        updates: [
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
            changes: { email: "one@example.com" },
          },
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-2" },
            changes: { email: "two@example.com" },
          },
        ],
      }),
    ).rejects.toMatchObject({
      message: error.message,
      cause: error,
      result: {
        affectedRows: 0,
        updateRowOutcomes: [
          { rowIndex: 0, status: "not_applied" },
          { rowIndex: 1, status: "not_applied" },
        ],
      },
    });
  });

  it("does not invent partial successes for an uncertain DynamoDB transaction", async () => {
    const { driver, clientSend } = createDriver();
    clientSend.mockImplementation(async (command) => {
      if (command.constructor.name === "DescribeTableCommand") {
        return {
          Table: {
            KeySchema: [
              { AttributeName: "tenant_id", KeyType: "HASH" },
              { AttributeName: "user_id", KeyType: "RANGE" },
            ],
            AttributeDefinitions: [
              { AttributeName: "tenant_id", AttributeType: "S" },
              { AttributeName: "user_id", AttributeType: "S" },
            ],
          },
        };
      }
      throw new Error("Connection lost");
    });

    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "users",
        updates: [
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-1" },
            changes: { email: "one@example.com" },
          },
          {
            primaryKeys: { tenant_id: "tenant-1", user_id: "user-2" },
            changes: { email: "two@example.com" },
          },
        ],
      }),
    ).rejects.toMatchObject({
      result: {
        affectedRows: 0,
        updateRowOutcomes: [
          { rowIndex: 0, status: "unknown" },
          { rowIndex: 1, status: "unknown" },
        ],
      },
    });
  });

  it("aliases reserved key names in mutation conditions", async () => {
    const { driver, clientSend, queueResponses } = createDriver({
      columns: [
        {
          name: "namespace",
          type: "string",
          nativeType: "string",
          category: "text",
          nullable: false,
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          primaryKeyRole: "partition",
          isForeignKey: false,
          filterable: true,
          filterOperators: ["eq", "neq", "like", "in"],
          valueSemantics: "plain",
        },
        {
          name: "key",
          type: "string",
          nativeType: "string",
          category: "text",
          nullable: false,
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          primaryKeyRole: "sort",
          isForeignKey: false,
          filterable: true,
          filterOperators: ["eq", "neq", "like", "in"],
          valueSemantics: "plain",
        },
        {
          name: "value",
          type: "string",
          nativeType: "string",
          category: "text",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          filterable: true,
          filterOperators: [
            "eq",
            "neq",
            "like",
            "in",
            "is_null",
            "is_not_null",
          ],
          valueSemantics: "plain",
        },
      ],
      describeTable: {
        KeySchema: [
          { AttributeName: "namespace", KeyType: "HASH" },
          { AttributeName: "key", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "namespace", AttributeType: "S" },
          { AttributeName: "key", AttributeType: "S" },
          { AttributeName: "value", AttributeType: "S" },
        ],
      },
    });

    queueResponses(
      {},
      {
        Attributes: marshall({
          namespace: "limits",
          key: "max_login_attempts",
        }),
      },
      {
        Attributes: marshall({
          namespace: "limits",
          key: "max_login_attempts",
        }),
      },
      {},
    );

    await expect(
      driver.insertRow({
        database: "us-east-1",
        schema: "us-east-1",
        table: "ConfigStore",
        values: {
          namespace: "limits",
          key: "max_login_attempts",
          value: "111",
        },
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    await expect(
      driver.updateRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "ConfigStore",
        updates: [
          {
            primaryKeys: {
              namespace: "limits",
              key: "max_login_attempts",
            },
            changes: { value: "111" },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    await expect(
      driver.deleteRows({
        database: "us-east-1",
        schema: "us-east-1",
        table: "ConfigStore",
        primaryKeyValuesList: [
          { namespace: "limits", key: "max_login_attempts" },
        ],
      }),
    ).resolves.toMatchObject({
      affectedRows: 1,
      rowOutcomes: [{ status: "deleted" }],
    });

    const putInputs = commandInputs(clientSend, "PutItemCommand");
    expect(putInputs[0]).toMatchObject({
      TableName: "ConfigStore",
      ConditionExpression:
        "attribute_not_exists(#k0) AND attribute_not_exists(#k1)",
      ExpressionAttributeNames: {
        "#k0": "namespace",
        "#k1": "key",
      },
    });

    const updateInputs = commandInputs(clientSend, "UpdateItemCommand");
    expect(updateInputs[0]).toMatchObject({
      TableName: "ConfigStore",
      ConditionExpression: "attribute_exists(#k0) AND attribute_exists(#k1)",
      ExpressionAttributeNames: {
        "#u0": "value",
        "#k0": "namespace",
        "#k1": "key",
      },
    });

    const deleteInputs = commandInputs(clientSend, "DeleteItemCommand");
    expect(deleteInputs[0]).toMatchObject({
      TableName: "ConfigStore",
      ConditionExpression: "attribute_exists(#k0) AND attribute_exists(#k1)",
      ExpressionAttributeNames: {
        "#k0": "namespace",
        "#k1": "key",
      },
    });
  });

  it("uses GetItem planning when the full primary key is provided", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {
        Item: marshall({
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        }),
      },
      {
        Item: marshall({
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        }),
      },
    );

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 25,
      filters: [
        { column: "tenant_id", operator: "eq", value: "tenant-1" },
        { column: "user_id", operator: "eq", value: "user-1" },
      ],
      sort: null,
      skipCount: false,
    });

    const getItemInputs = commandInputs(clientSend, "GetItemCommand");
    expect(getItemInputs).toHaveLength(2);
    expect(getItemInputs[0]).toMatchObject({
      TableName: "users",
      Key: marshall(
        { tenant_id: "tenant-1", user_id: "user-1" },
        { removeUndefinedValues: true },
      ),
    });
    expect(page.totalCount).toBe(1);
    expect(page.rows).toEqual([
      expect.objectContaining({
        tenant_id: "tenant-1",
        user_id: "user-1",
      }),
    ]);
  });

  it("uses Query planning for partition-key filters and reuses cursor state across pages", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    queueResponses(
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-1",
            email: "one@example.com",
          }),
        ],
        LastEvaluatedKey: marshall(
          { tenant_id: "tenant-1", user_id: "user-1" },
          { removeUndefinedValues: true },
        ),
      },
      { Count: 2 },
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-2",
            email: "two@example.com",
          }),
        ],
      },
    );

    const request = {
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      pageSize: 1,
      filters: [
        { column: "tenant_id", operator: "eq", value: "tenant-1" },
        { column: "email", operator: "like", value: "example" },
      ] satisfies FilterExpression[],
      sort: { column: "user_id", direction: "asc" } as const,
      skipCount: false,
    };

    const page1 = await driver.readTablePage({ ...request, page: 1 });
    const page2 = await driver.readTablePage({ ...request, page: 2 });

    const queryInputs = commandInputs(clientSend, "QueryCommand");
    expect(queryInputs).toHaveLength(3);
    expect(queryInputs[0]).toMatchObject({
      TableName: "users",
      Limit: 1,
      ScanIndexForward: true,
      KeyConditionExpression: "#n0 = :v0",
      FilterExpression: "contains(#n1, :v1)",
      ExpressionAttributeNames: {
        "#n0": "tenant_id",
        "#n1": "email",
      },
    });
    expect(queryInputs[1]).toMatchObject({
      TableName: "users",
      Select: "COUNT",
    });
    expect(queryInputs[2]).toMatchObject({
      TableName: "users",
      Limit: 1,
      ExclusiveStartKey: marshall(
        { tenant_id: "tenant-1", user_id: "user-1" },
        { removeUndefinedValues: true },
      ),
    });
    expect(page1.totalCount).toBe(2);
    expect(page2.totalCount).toBe(2);
    expect(page1.rows[0]).toEqual(
      expect.objectContaining({ user_id: "user-1" }),
    );
    expect(page2.rows[0]).toEqual(
      expect.objectContaining({ user_id: "user-2" }),
    );
  });

  it("fills a logical page when DynamoDB filters out evaluated items", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    const skippedCursor = marshall(
      { tenant_id: "tenant-1", user_id: "skipped" },
      { removeUndefinedValues: true },
    );
    queueResponses(
      { Items: [], LastEvaluatedKey: skippedCursor },
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "matched",
            email: "matched@example.com",
          }),
        ],
      },
    );

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 1,
      filters: [
        { column: "tenant_id", operator: "eq", value: "tenant-1" },
        { column: "email", operator: "like", value: "example.com" },
      ],
      sort: { column: "user_id", direction: "asc" },
      skipCount: true,
    });

    const queryInputs = commandInputs(clientSend, "QueryCommand");
    expect(queryInputs).toHaveLength(2);
    expect(queryInputs[1]).toMatchObject({
      Limit: 1,
      ExclusiveStartKey: skippedCursor,
    });
    expect(page.rows).toEqual([
      expect.objectContaining({ user_id: "matched" }),
    ]);
  });

  it("bounds cursor sessions and saved page starts", () => {
    const { driver } = createDriver();
    const state = driver as unknown as {
      cursorCache: Map<
        string,
        {
          pageStarts: Map<number, Record<string, unknown> | undefined>;
          terminalPage: number | null;
        }
      >;
      getCursorSession: (key: string) => {
        pageStarts: Map<number, Record<string, unknown> | undefined>;
        terminalPage: number | null;
      };
      setCursorPageStart: (
        session: {
          pageStarts: Map<number, Record<string, unknown> | undefined>;
          terminalPage: number | null;
        },
        page: number,
        cursor: Record<string, unknown>,
      ) => void;
    };

    for (let index = 0; index < 101; index += 1) {
      state.getCursorSession(`session-${index}`);
    }
    expect(state.cursorCache.size).toBe(100);
    expect(state.cursorCache.has("session-0")).toBe(false);

    const session = state.getCursorSession("session-100");
    for (let page = 2; page <= 102; page += 1) {
      state.setCursorPageStart(session, page, { id: { S: String(page) } });
    }
    expect(session.pageStarts.size).toBe(100);
    expect(session.pageStarts.has(1)).toBe(true);
    expect(session.pageStarts.has(2)).toBe(false);
    expect(session.pageStarts.has(102)).toBe(true);
  });

  it("avoids full materialization for client-side filters when skipCount is enabled", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    const driverState = driver as unknown as {
      materializeReadPlanRows: ReturnType<typeof vi.fn>;
    };
    driverState.materializeReadPlanRows = vi.fn(async () => {
      throw new Error("materialization should not be used");
    });

    queueResponses({
      Items: [
        marshall({
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "alice@example.com",
        }),
        marshall({
          tenant_id: "tenant-1",
          user_id: "user-2",
          email: "bob@sample.org",
        }),
      ],
      LastEvaluatedKey: marshall(
        { tenant_id: "tenant-1", user_id: "user-2" },
        { removeUndefinedValues: true },
      ),
    });

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 1,
      filters: [
        { column: "tenant_id", operator: "eq", value: "tenant-1" },
        { column: "email", operator: "ilike", value: "example.com" },
      ],
      sort: { column: "user_id", direction: "asc" },
      skipCount: true,
    });

    expect(driverState.materializeReadPlanRows).not.toHaveBeenCalled();
    const queryInputs = commandInputs(clientSend, "QueryCommand");
    expect(queryInputs).toHaveLength(1);
    expect(queryInputs[0]).toMatchObject({
      TableName: "users",
      Limit: 200,
      ScanIndexForward: true,
      KeyConditionExpression: "#n0 = :v0",
      ExpressionAttributeNames: {
        "#n0": "tenant_id",
      },
    });
    expect(page.totalCount).toBe(0);
    expect(page.rows).toEqual([
      expect.objectContaining({
        tenant_id: "tenant-1",
        user_id: "user-1",
        email: "alice@example.com",
      }),
    ]);
  });

  it("uses Scan for non-key map Contains without searching display serialization", async () => {
    const { driver, clientSend, queueResponses } = createDriver();
    const driverState = driver as unknown as {
      describeColumns: ReturnType<typeof vi.fn>;
    };
    driverState.describeColumns = vi.fn(async () => [
      ...createColumns(),
      createMapColumn(),
    ]);
    queueResponses(
      {
        Items: [
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-1",
            address: {
              country: "RU",
              lon: 37.6173,
              city: "Moscow",
              lat: 55.7558,
            },
          }),
          marshall({
            tenant_id: "tenant-1",
            user_id: "user-2",
            address: {
              country: "DE",
              lon: 13.405,
              city: "Berlin",
              lat: 52.52,
            },
          }),
        ],
      },
      { Count: 2 },
    );

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: "users",
      page: 1,
      pageSize: 25,
      filters: [
        {
          column: "address",
          operator: "like",
          value: '{"country":"RU","lon":37.6173,"city":"Moscow","lat":55.7558}',
        },
      ],
      sort: null,
      skipCount: false,
    });

    const scanInputs = commandInputs(clientSend, "ScanCommand");
    expect(scanInputs).toHaveLength(1);
    expect(scanInputs[0]).toMatchObject({
      TableName: "users",
    });
    expect(page.rows).toEqual([]);
  });
});
