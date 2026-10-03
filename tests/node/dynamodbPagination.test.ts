import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import {
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
} from "../../src/extension/dbDrivers/timeout";
import type {
  ColumnTypeMeta,
  DriverTablePageRequest,
} from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";

type Response = {
  Items?: Record<string, AttributeValue>[];
  LastEvaluatedKey?: Record<string, AttributeValue>;
  Count?: number;
};

const columns: ColumnTypeMeta[] = ["tenant_id", "user_id", "email"].map(
  (name, index) => ({
    name,
    type: "string",
    nativeType: "string",
    category: "text",
    nullable: index === 2,
    isPrimaryKey: index < 2,
    ...(index < 2
      ? {
          primaryKeyOrdinal: index + 1,
          primaryKeyRole: index === 0 ? "partition" : "sort",
        }
      : {}),
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "ilike"],
    valueSemantics: "plain",
  }),
);

function key(id: string): Record<string, AttributeValue> {
  return marshall({ tenant_id: "tenant", user_id: id });
}

function response(id: string, hasMore = true): Response {
  return {
    Items: [marshall({ tenant_id: "tenant", user_id: id, email: "match" })],
    ...(hasMore ? { LastEvaluatedKey: key(id) } : {}),
  };
}

function createHarness(
  responses: Response[] | ((input: Record<string, unknown>) => Response),
  onRead?: (options?: { abortSignal?: AbortSignal }) => void,
) {
  const driver = new DynamoDBDriver({
    id: "ddb",
    name: "DynamoDB",
    type: "dynamodb",
    awsRegion: "us-east-1",
  });
  const send = vi.fn(
    async (
      command: {
        constructor: { name: string };
        input: Record<string, unknown>;
      },
      _options?: { abortSignal?: AbortSignal },
    ) => {
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
      onRead?.(_options);
      const next =
        typeof responses === "function"
          ? responses(command.input)
          : responses.shift();
      if (!next) throw new Error("Unexpected SDK read");
      return next;
    },
  );
  const state = driver as unknown as {
    connected: boolean;
    client: { send: typeof send };
  };
  state.connected = true;
  state.client = { send };
  vi.spyOn(driver, "describeColumns").mockResolvedValue(columns);
  const service = new TableReadService({
    getConnection: () => ({ driver }),
    getDriver: () => driver,
  } as unknown as ConnectionManager);
  const reads = () =>
    send.mock.calls.filter(([command]) =>
      ["ScanCommand", "QueryCommand"].includes(command.constructor.name),
    );
  return { driver, service, send, reads };
}

const modes = ["Scan", "Query"] as const;
const directions = ["asc", "desc"] as const;

function createOrderedHarness() {
  return createHarness((input) => {
    if (input.Select === "COUNT") return { Count: 5 };
    const ids = ["1", "2", "3", "4", "5"];
    if (input.ScanIndexForward === false) ids.reverse();
    const cursor = input.ExclusiveStartKey as
      | Record<string, AttributeValue>
      | undefined;
    const index = cursor ? ids.indexOf(cursor.user_id.S ?? "") + 1 : 0;
    return index < ids.length
      ? response(ids[index], index < ids.length - 1)
      : { Items: [] };
  });
}

function observeCursorMemory(driver: DynamoDBDriver) {
  const state = driver as unknown as {
    cursorCache: Map<string, { cursorPages: Map<string, number> }>;
    trackReadCursor: (
      cursor: Record<string, AttributeValue> | undefined,
      history: { recent: Map<string, true> },
      onCursor?: (signature: string) => void,
    ) => Record<string, AttributeValue> | undefined;
  };
  const original = state.trackReadCursor.bind(driver);
  let maxRecent = 0;
  let maxSession = 0;
  vi.spyOn(state, "trackReadCursor").mockImplementation(
    (cursor, history, onCursor) => {
      const result = original(cursor, history, onCursor);
      maxRecent = Math.max(maxRecent, history.recent.size);
      for (const session of state.cursorCache.values()) {
        maxSession = Math.max(maxSession, session.cursorPages.size);
      }
      return result;
    },
  );
  return () => ({ maxRecent, maxSession });
}

function request(
  mode: (typeof modes)[number],
  pageSize = 500,
): DriverTablePageRequest {
  return {
    database: "us-east-1",
    schema: "us-east-1",
    table: "users",
    page: 1,
    pageSize,
    filters:
      mode === "Query"
        ? [{ column: "tenant_id", operator: "eq", value: "tenant" }]
        : [],
    sort: null,
    skipCount: true,
  };
}

async function exportChunks(
  service: TableReadService,
  req: DriverTablePageRequest,
) {
  const chunks: Array<{
    columns: ColumnTypeMeta[];
    rows: Record<string, unknown>[];
  }> = [];
  for await (const chunk of service.exportAll(
    "ddb",
    req.database,
    req.schema,
    req.table,
    req.pageSize,
    req.sort,
    req.filters,
    req.signal,
  )) {
    chunks.push(chunk);
  }
  return chunks;
}

describe("B03 DynamoDB logical pages and service export", () => {
  it.each(directions)("exports after %s browsing", async (direction) => {
    const { driver, service } = createOrderedHarness();
    const req = {
      ...request("Query", 2),
      sort: { column: "user_id", direction },
    };
    await driver.readTablePage(req);
    const reverse = direction === "asc" ? "desc" : "asc";
    const chunks = await exportChunks(service, {
      ...req,
      sort: { column: "user_id", direction: reverse },
    });
    expect(
      chunks.flatMap((chunk) => chunk.rows.map((row) => row.user_id)),
    ).toEqual(
      reverse === "asc" ? ["1", "2", "3", "4", "5"] : ["5", "4", "3", "2", "1"],
    );
  });

  it.each(directions)("switches %s order on page 2", async (direction) => {
    const { driver, reads } = createOrderedHarness();
    const req = {
      ...request("Query", 2),
      sort: { column: "user_id", direction },
    };
    await driver.readTablePage(req);
    const reverse = direction === "asc" ? "desc" : "asc";
    const page = await driver.readTablePage({
      ...req,
      page: 2,
      sort: { column: "user_id", direction: reverse },
    });
    expect(page.rows.map((row) => row.user_id)).toEqual(
      reverse === "asc" ? ["3", "4"] : ["3", "2"],
    );
    expect(reads()[2][0].input.ExclusiveStartKey).toBeUndefined();
  });

  it("shares COUNT and default ASC cursors across equivalent orders", async () => {
    const { driver, reads } = createOrderedHarness();
    const req = { ...request("Query", 2), skipCount: false };
    const first = await driver.readTablePage(req);
    const second = await driver.readTablePage({
      ...req,
      page: 2,
      sort: { column: "user_id", direction: "asc" },
    });
    const descending = await driver.readTablePage({
      ...req,
      sort: { column: "user_id", direction: "desc" },
    });
    expect([
      first.totalCount,
      second.totalCount,
      descending.totalCount,
    ]).toEqual([5, 5, 5]);
    expect(second.rows.map((row) => row.user_id)).toEqual(["3", "4"]);
    expect(descending.rows.map((row) => row.user_id)).toEqual(["5", "4"]);
    expect(
      reads().filter(([command]) => command.input.Select === "COUNT"),
    ).toHaveLength(1);
    expect(reads()).toHaveLength(7);
  });

  it.each(modes)("exports byte-limited %s responses", async (mode) => {
    const { service, reads } = createHarness([
      response("first"),
      response("second", false),
    ]);
    const chunks = await exportChunks(service, request(mode));
    expect(
      chunks.flatMap((chunk) => chunk.rows.map((row) => row.user_id)),
    ).toEqual(["first", "second"]);
    expect(reads()).toHaveLength(2);
    expect(reads().map(([command]) => command.constructor.name)).toEqual([
      `${mode}Command`,
      `${mode}Command`,
    ]);
    expect(reads()[0][0].input.FilterExpression).toBeUndefined();
    if (mode === "Query") {
      expect(reads()[0][0].input.KeyConditionExpression).toBeDefined();
    }
    expect(reads()[1][0].input).toMatchObject({
      Limit: 499,
      ExclusiveStartKey: key("first"),
    });
  });

  it.each(modes)("fills native %s pages", async (mode) => {
    const { driver, reads } = createHarness([
      response("1"),
      response("2"),
      response("3"),
      response("4", false),
    ]);
    const req = request(mode, 3);
    const first = await driver.readTablePage(req);
    const second = await driver.readTablePage({ ...req, page: 2 });
    const exhausted = await driver.readTablePage({ ...req, page: 3 });
    expect(first.rows.map((row) => row.user_id)).toEqual(["1", "2", "3"]);
    expect(second.rows.map((row) => row.user_id)).toEqual(["4"]);
    expect(exhausted.rows).toEqual([]);
    expect(reads().map(([command]) => command.input.Limit)).toEqual([
      3, 2, 1, 3,
    ]);
    expect(reads()[3][0].input.ExclusiveStartKey).toEqual(key("3"));
  });

  it.each(modes)("exports many truncated %s responses", async (mode) => {
    const ids = Array.from({ length: 257 }, (_, index) => String(index));
    const { service, reads } = createHarness(
      ids.map((id, index) => response(id, index < ids.length - 1)),
    );
    const chunks = await exportChunks(service, request(mode, 25));
    expect(chunks.map((chunk) => chunk.rows.length)).toEqual([
      ...Array.from({ length: 10 }, () => 25),
      7,
    ]);
    expect(
      chunks.flatMap((chunk) => chunk.rows.map((row) => row.user_id)),
    ).toEqual(ids);
    expect(reads()).toHaveLength(257);
    reads().forEach(([command], index) => {
      expect(command.input.Limit).toBe(25 - (index % 25));
      expect(command.input.ExclusiveStartKey).toEqual(
        index === 0 ? undefined : key(ids[index - 1]),
      );
    });
  });

  it.each(modes)("continues filtered empty %s responses", async (mode) => {
    const emptyResponses = Array.from({ length: 150 }, (_, index) => ({
      Items: [],
      LastEvaluatedKey: key(`skipped-${index}`),
    }));
    const { service, reads } = createHarness([
      ...emptyResponses,
      response("first"),
      { Items: [], LastEvaluatedKey: key("between") },
      response("second"),
      { Items: [], LastEvaluatedKey: key("tail") },
      { Items: [] },
    ]);
    const req = request(mode, 2);
    req.filters.push({ column: "email", operator: "eq", value: "match" });
    const chunks = await exportChunks(service, req);
    expect(chunks.map((chunk) => chunk.rows.map((row) => row.user_id))).toEqual(
      [["first", "second"]],
    );
    expect(reads()).toHaveLength(155);
    expect(
      reads().every(
        ([command]) => command.input.FilterExpression !== undefined,
      ),
    ).toBe(true);
  });

  it.each(modes)("bounds sparse %s cursor history", async (mode) => {
    let calls = 0;
    const { driver, service } = createHarness(() => {
      return calls++ < 4096
        ? { Items: [], LastEvaluatedKey: key(`skipped-${calls}`) }
        : response("matched", false);
    });
    const memory = observeCursorMemory(driver);
    const req = request(mode, 2);
    req.filters.push({ column: "email", operator: "eq", value: "match" });
    const chunks = await exportChunks(service, req);
    expect(
      chunks.flatMap((chunk) => chunk.rows.map((row) => row.user_id)),
    ).toEqual(["matched"]);
    expect(calls).toBe(4097);
    expect(memory()).toEqual({ maxRecent: 256, maxSession: 256 });
  });

  it.each(modes)("exhausts empty %s selections", async (mode) => {
    const { service, reads } = createHarness([
      { Items: [], LastEvaluatedKey: key("skipped-1") },
      { LastEvaluatedKey: key("skipped-2") },
      { Items: [], LastEvaluatedKey: {} },
    ]);
    const req = request(mode);
    req.filters.push({ column: "email", operator: "eq", value: "match" });
    const chunks = await exportChunks(service, req);
    expect(chunks).toEqual([{ columns: expect.any(Array), rows: [] }]);
    expect(reads()).toHaveLength(3);
  });

  it.each(modes)("supports %s page jumps and rereads", async (mode) => {
    const ids = ["1", "2", "3", "4", "5"];
    const { driver, reads } = createHarness((input) => {
      const cursor = input.ExclusiveStartKey as
        | Record<string, AttributeValue>
        | undefined;
      const index = cursor ? ids.indexOf(cursor.user_id.S ?? "") + 1 : 0;
      return response(ids[index], index < ids.length - 1);
    });
    const req = request(mode, 2);
    for (const [page, ids] of [
      [3, ["5"]],
      [1, ["1", "2"]],
      [2, ["3", "4"]],
    ] as const) {
      const result = await driver.readTablePage({ ...req, page });
      expect(result.rows.map((row) => row.user_id)).toEqual(ids);
    }
    expect((await driver.readTablePage({ ...req, page: 4 })).rows).toEqual([]);
    expect(reads()).toHaveLength(9);
  });

  it.each(modes)("allows concurrent %s page reads", async (mode) => {
    const { driver } = createHarness((input) => {
      const cursor = input.ExclusiveStartKey as
        | Record<string, AttributeValue>
        | undefined;
      return cursor ? response("second", false) : response("first");
    });
    const req = request(mode, 2);
    const pages = await Promise.all([
      driver.readTablePage(req),
      driver.readTablePage(req),
    ]);
    expect(pages.map((page) => page.rows.map((row) => row.user_id))).toEqual([
      ["first", "second"],
      ["first", "second"],
    ]);
  });

  it.each(modes)("rejects reordered repeated %s cursors", async (mode) => {
    const cursor = key("same");
    const { service, reads } = createHarness([
      { Items: [], LastEvaluatedKey: cursor },
      {
        Items: [],
        LastEvaluatedKey: {
          user_id: cursor.user_id,
          tenant_id: cursor.tenant_id,
        },
      },
    ]);
    await expect(exportChunks(service, request(mode))).rejects.toThrow(
      /cursor repeated/i,
    );
    expect(reads()).toHaveLength(2);
  });

  it.each(modes)("rejects %s cycles within a page", async (mode) => {
    const { driver, reads } = createHarness([
      { Items: [], LastEvaluatedKey: key("a") },
      { Items: [], LastEvaluatedKey: key("b") },
      { Items: [], LastEvaluatedKey: key("a") },
    ]);
    await expect(driver.readTablePage(request(mode))).rejects.toThrow(
      /cursor repeated/i,
    );
    expect(reads()).toHaveLength(3);
  });

  it.each(modes)("rejects %s cycles across export chunks", async (mode) => {
    const { service, reads } = createHarness([
      response("a"),
      response("b"),
      response("c"),
      response("a"),
    ]);
    await expect(exportChunks(service, request(mode, 2))).rejects.toThrow(
      /cursor repeated/i,
    );
    expect(reads()).toHaveLength(4);
  });

  it.each(modes)("rejects non-advancing %s page starts", async (mode) => {
    const { driver, reads } = createHarness([
      response("same"),
      response("same"),
    ]);
    const req = request(mode, 1);
    await driver.readTablePage(req);
    await expect(driver.readTablePage({ ...req, page: 2 })).rejects.toThrow(
      /cursor repeated/i,
    );
    expect(reads()).toHaveLength(2);
  });

  it.each(modes)("exports %s past cursor history eviction", async (mode) => {
    const ids = Array.from({ length: 301 }, (_, index) => String(index));
    const { service, reads } = createHarness(
      ids.map((id, index) => response(id, index < ids.length - 1)),
    );
    const chunks = await exportChunks(service, request(mode, 1));
    expect(
      chunks.flatMap((chunk) => chunk.rows.map((row) => row.user_id)),
    ).toEqual(ids);
    expect(reads()).toHaveLength(301);
  });

  it.each(modes)("rejects long %s cursor cycles", async (mode) => {
    let calls = 0;
    const { service } = createHarness((input) => {
      if (++calls > 1500)
        throw new Error("Cycle detection did not stop reading");
      const cursor = input.ExclusiveStartKey as
        | Record<string, AttributeValue>
        | undefined;
      const previous = Number(cursor?.user_id.S ?? -1);
      // A non-cyclic prefix followed by a cycle longer than the page cache.
      return response(String(previous === 250 ? 1 : previous + 1));
    });
    await expect(exportChunks(service, request(mode, 1))).rejects.toThrow(
      /cursor repeated/i,
    );
    expect(calls).toBeGreaterThan(250);
    expect(calls).toBeLessThan(1500);
  });

  it.each(
    modes.flatMap((mode) =>
      ["page", "count", "clientFilter", "materialize"].map((path) => ({
        mode,
        path,
      })),
    ),
  )("bounds history and stops long $mode $path cycles", async ({
    mode,
    path,
  }) => {
    let calls = 0;
    const { driver } = createHarness((input) => {
      if (path === "count" && input.Select !== "COUNT") {
        return response("row", false);
      }
      if (++calls > 5000)
        throw new Error("Cycle detection did not stop reading");
      const cursor = input.ExclusiveStartKey as
        | Record<string, AttributeValue>
        | undefined;
      const previous = Number(cursor?.user_id.S ?? -1);
      return {
        Items: [],
        Count: 0,
        LastEvaluatedKey: key(String(previous === 700 ? 1 : previous + 1)),
      };
    });
    const memory = observeCursorMemory(driver);
    const req = request(mode, 2);
    if (path === "count") req.skipCount = false;
    if (path === "clientFilter") {
      req.filters.push({ column: "email", operator: "ilike", value: "match" });
    }
    if (path === "materialize")
      req.sort = { column: "email", direction: "asc" };
    await expect(driver.readTablePage(req)).rejects.toThrow(/cursor repeated/i);
    expect(calls).toBeGreaterThan(700);
    expect(calls).toBeLessThan(5000);
    expect(memory().maxRecent).toBe(256);
    expect(memory().maxSession).toBeLessThanOrEqual(256);
  });

  it.each(modes)("counts all short %s responses in browsing", async (mode) => {
    const { driver, reads } = createHarness([
      response("first"),
      response("second", false),
      { Count: 1, LastEvaluatedKey: key("first") },
      { Count: 1 },
    ]);
    const page = await driver.readTablePage({
      ...request(mode),
      skipCount: false,
      sort: mode === "Query" ? { column: "user_id", direction: "desc" } : null,
    });
    expect(page.rows.map((row) => row.user_id)).toEqual(["first", "second"]);
    expect(page.totalCount).toBe(2);
    expect(reads()).toHaveLength(4);
    expect(
      reads()
        .slice(2)
        .every(([command]) => command.input.Select === "COUNT"),
    ).toBe(true);
    if (mode === "Query") {
      expect(reads()[0][0].input.ScanIndexForward).toBe(false);
      expect(reads()[1][0].input.ScanIndexForward).toBe(false);
    }
  });

  it.each(modes)("rejects %s cycles in the browsing count", async (mode) => {
    const { driver, reads } = createHarness([
      response("row", false),
      { Count: 0, LastEvaluatedKey: key("a") },
      { Count: 0, LastEvaluatedKey: key("b") },
      { Count: 0, LastEvaluatedKey: key("a") },
    ]);
    await expect(
      driver.readTablePage({ ...request(mode), skipCount: false }),
    ).rejects.toThrow(/cursor repeated/i);
    expect(reads()).toHaveLength(4);
  });

  it.each(modes)("stops %s requests on cancellation", async (mode) => {
    const controller = new AbortController();
    const reason = new DOMException("Cancelled", "AbortError");
    const { service, reads } = createHarness([response("first")], (options) => {
      expect(options?.abortSignal).toBe(controller.signal);
      controller.abort(reason);
    });
    const req = { ...request(mode), signal: controller.signal };
    await expect(exportChunks(service, req)).rejects.toBe(reason);
    expect(reads()).toHaveLength(1);
  });

  it.each(modes)("allows cancelled %s page retries", async (mode) => {
    const controller = new AbortController();
    let calls = 0;
    const { driver, reads } = createHarness(
      [
        response("first"),
        response("second"),
        response("first"),
        response("second", false),
      ],
      () => {
        if (++calls === 2) controller.abort();
      },
    );
    const req = request(mode, 3);
    await expect(
      driver.readTablePage({ ...req, signal: controller.signal }),
    ).rejects.toMatchObject({ name: "AbortError" });
    const retry = await driver.readTablePage(req);
    expect(retry.rows.map((row) => row.user_id)).toEqual(["first", "second"]);
    expect(reads()).toHaveLength(4);
  });

  it.each(
    modes.flatMap((mode) =>
      ["abort", "deadline"].map((failure) => ({ mode, failure })),
    ),
  )("isolates a concurrent $mode $failure from retry", async ({
    mode,
    failure,
  }) => {
    const controller = new AbortController();
    const deadline = Date.now() + 1000;
    const clock = vi.spyOn(Date, "now");
    const { driver, send, reads } = createHarness((input) => {
      const cursor = input.ExclusiveStartKey as
        | Record<string, AttributeValue>
        | undefined;
      return !cursor
        ? response("first")
        : cursor.user_id.S === "first"
          ? response("second")
          : response("third", false);
    });
    let completeRead: ((value: Response) => void) | undefined;
    let notifyStarted: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      notifyStarted = resolve;
    });
    const originalSend = send.getMockImplementation();
    send.mockImplementation(async (command, options) => {
      if (
        options?.abortSignal === controller.signal &&
        command.input.ExclusiveStartKey
      ) {
        return new Promise<Response>((resolve) => {
          completeRead = resolve;
          notifyStarted?.();
        });
      }
      return originalSend?.(command, options) ?? {};
    });
    const req = request(mode, 3);
    const pending = driver.readTablePage({
      ...req,
      signal: controller.signal,
      ...(failure === "deadline" ? { deadline } : {}),
    });
    const rejection =
      failure === "abort"
        ? expect(pending).rejects.toMatchObject({ name: "AbortError" })
        : expect(pending).rejects.toThrow(/deadline exceeded/i);
    await started;
    const retry = await driver.readTablePage(req);
    expect(retry.rows.map((row) => row.user_id)).toEqual([
      "first",
      "second",
      "third",
    ]);
    if (failure === "abort") controller.abort();
    else clock.mockReturnValue(deadline);
    completeRead?.(response("second"));
    await rejection;
    expect((await driver.readTablePage({ ...req, page: 2 })).rows).toEqual([]);
    expect(reads()).toHaveLength(5);
    const reread = await driver.readTablePage(req);
    expect(reread.rows.map((row) => row.user_id)).toEqual([
      "first",
      "second",
      "third",
    ]);
  });

  it.each(modes)("checks deadlines between %s responses", async (mode) => {
    const deadline = Date.now() + 1000;
    const now = vi.spyOn(Date, "now");
    const { driver, reads } = createHarness([response("first")], () => {
      now.mockReturnValue(deadline);
    });
    await expect(
      driver.readTablePage({ ...request(mode), deadline }),
    ).rejects.toThrow(/deadline exceeded/i);
    expect(reads()).toHaveLength(1);
  });

  it("rejects pre-cancelled and expired native reads before any SDK call", async () => {
    const { driver, send } = createHarness([]);
    const signal = AbortSignal.abort();
    await expect(
      driver.readTablePage({ ...request("Scan"), signal }),
    ).rejects.toMatchObject({ name: "AbortError" });
    await expect(
      driver.readTablePage({ ...request("Scan"), deadline: Date.now() - 1 }),
    ).rejects.toThrow(/deadline exceeded/i);
    expect(send).not.toHaveBeenCalled();
  });

  it("cancels real metadata sampling before following its short-response cursor", async () => {
    const controller = new AbortController();
    const reason = new DOMException("Cancelled", "AbortError");
    const { driver, reads } = createHarness([response("sample")], (options) => {
      expect(options?.abortSignal).toBe(controller.signal);
      controller.abort(reason);
    });
    vi.mocked(driver.describeColumns).mockRestore();
    await expect(
      driver.readTablePage({ ...request("Scan"), signal: controller.signal }),
    ).rejects.toBe(reason);
    expect(reads()).toHaveLength(1);
  });

  it("checks cancellation between exported logical chunks", async () => {
    const { service, reads } = createHarness([response("first")]);
    const controller = new AbortController();
    const stream = service.exportAll(
      "ddb",
      "us-east-1",
      "us-east-1",
      "users",
      1,
      null,
      [],
      controller.signal,
    );
    expect((await stream.next()).value?.rows).toHaveLength(1);
    controller.abort();
    await expect(stream.next()).rejects.toMatchObject({ name: "AbortError" });
    expect(reads()).toHaveLength(1);
  });

  it("forwards the timeout wrapper's signal and stops after an in-flight deadline", async () => {
    vi.useFakeTimers();
    try {
      const { driver, send, reads } = createHarness([response("first")]);
      let completeRead: ((value: Response) => void) | undefined;
      let readSignal: AbortSignal | undefined;
      const originalSend = send.getMockImplementation();
      send.mockImplementation(async (command, options) => {
        if (command.constructor.name === "DescribeTableCommand") {
          return originalSend?.(command, options) ?? {};
        }
        readSignal = options?.abortSignal;
        return new Promise<Response>((resolve) => {
          completeRead = resolve;
        });
      });
      const wrapped = createTimeoutAwareDriver(driver, () =>
        createDriverTimeoutSettingsSnapshot({ dbOperationTimeoutSeconds: 1 }),
      );
      const pending = wrapped.readTablePage(request("Scan"));
      const rejection = expect(pending).rejects.toMatchObject({
        name: "DriverTimeoutError",
      });
      await vi.advanceTimersByTimeAsync(0);
      expect(readSignal?.aborted).toBe(false);
      await vi.advanceTimersByTimeAsync(1000);
      await rejection;
      expect(readSignal?.aborted).toBe(true);
      completeRead?.(response("late"));
      await vi.advanceTimersByTimeAsync(0);
      expect(reads()).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
