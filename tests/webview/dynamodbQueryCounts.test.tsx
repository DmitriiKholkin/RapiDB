import { act, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { QueryPanelController } from "../../src/extension/panels/queryPanelController";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import type { QueryResultCounts } from "../../src/shared/webviewContracts";
import { useQueryViewController } from "../../src/webview/components/query/useQueryViewController";
import { QueryModeTableGrid } from "../../src/webview/components/table/grid/QueryModeTableGrid";
import { useQueryStore } from "../../src/webview/store";
import { dispatchWindowMessage } from "./testUtils";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: ({ count }: { count: number }) => ({
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: index,
        start: index * 26,
        end: (index + 1) * 26,
      })),
    getTotalSize: () => count * 26,
    scrollToIndex: () => undefined,
  }),
}));

const connection: ConnectionConfig = {
  id: "local-dynamo-fixture",
  name: "DynamoDB fixture",
  type: "dynamodb",
  awsRegion: "us-east-1",
};
const table = { TableName: "widgets" };
const firstKey = { id: { S: "widget-one" } };
const secondKey = { id: { S: "widget-two" } };
const put = { ...table, Item: firstKey };
const update = {
  ...table,
  Key: firstKey,
  UpdateExpression: "SET color = :color",
  ExpressionAttributeValues: { ":color": { S: "blue" } },
};
const remove = { ...table, Key: firstKey, ReturnValues: "NONE" };
const removeWithOld = { ...remove, ReturnValues: "ALL_OLD" };
const batchPut = { PutRequest: { Item: firstKey } };
const batchDelete = { DeleteRequest: { Key: secondKey } };
const batch = { RequestItems: { widgets: [batchPut, batchDelete] } };
const conditionCheck = {
  ConditionCheck: {
    ...table,
    Key: secondKey,
    ConditionExpression: "attribute_exists(id)",
  },
};

function Results() {
  const { status, result } = useQueryViewController({
    panelId: "dynamo-fixture",
    connectionId: connection.id,
    editorLanguage: "json",
    formatOnOpen: false,
    initialIsBookmarked: false,
    initialQueryText: "",
  });
  return <QueryModeTableGrid status={status} result={result} />;
}

async function execute(
  requests: object[],
  responses: Array<object | Error>,
  rowLimit = 10,
) {
  const send = vi.fn(async (_command: object) => {
    const response = responses.shift();
    if (!response) throw new Error("Missing fixture response");
    if (response instanceof Error) throw response;
    return response;
  });
  const driver = new DynamoDBDriver(connection);
  // The actual driver parses and dispatches commands; only AWS I/O is stubbed.
  Object.assign(driver, { client: { send }, connected: true });
  const identity = {
    connectionId: connection.id,
    operationId: "dynamo-query-fixture",
  };
  const postMessage = vi.fn((message: unknown) => {
    dispatchWindowMessage(JSON.parse(JSON.stringify(message)));
  });
  const controller = new QueryPanelController(
    {
      getConnection: () => connection,
      isConnected: () => true,
      getQueryRowLimit: () => rowLimit,
      getDriver: () => driver,
      getDriverCapabilities: () => ({ boundedQueryResults: true }),
      addToHistory: vi.fn(async () => undefined),
      refreshSchemaCache: vi.fn(),
    } as never,
    {
      getActiveConnectionId: () => connection.id,
      getInitialConnectionId: () => connection.id,
      getLastQueryResult: () => null,
      postMessage,
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    },
  );
  render(<Results />);
  await act(async () => {
    useQueryStore.getState().setRunning(identity);
    await controller.handleMessage({
      type: "executeQuery",
      payload: {
        ...identity,
        queryText: requests
          .map((request) => JSON.stringify(request))
          .join("\n"),
      },
    });
  });
  expect(postMessage).toHaveBeenCalledExactlyOnceWith({
    type: "queryResult",
    payload: expect.objectContaining(identity),
  });
  return {
    result: useQueryStore.getState().result,
    commands: send.mock.calls.map(([command]) => command.constructor.name),
  };
}

function expectCounts(
  result: unknown,
  expected: Omit<QueryResultCounts, "rowCountKind">,
) {
  expect(result).toMatchObject({ rowCountKind: "returned", ...expected });
  for (const key of [
    "affectedRows",
    "affectedRowsUnknown",
    "affectedRowsKind",
  ] as const) {
    if (expected[key] === undefined) expect(result).not.toHaveProperty(key);
  }
}

describe("DynamoDB query counts across the extension/webview bridge", () => {
  it.each([
    ["PutItemCommand", put],
    ["UpdateItemCommand", update],
  ])("shows the confirmed %s count even without return attributes", async (command, request) => {
    const { result, commands } = await execute([request], [{}]);
    expect(commands).toEqual([command]);
    expectCounts(result, { rowCount: 0, affectedRows: 1 });
    expect(screen.getByText(/^1 row affected ·/)).toBeTruthy();
    expect(screen.queryByText(/0 rows affected/)).toBeNull();
  });

  it("shows returned attributes separately from the total across puts, even when display is truncated", async () => {
    const { result } = await execute(
      [
        { ...put, ReturnValues: "ALL_OLD" },
        put,
        { ...put, Item: secondKey, ReturnValues: "ALL_OLD" },
      ],
      [{ Attributes: firstKey }, {}, { Attributes: secondKey }],
      1,
    );
    expectCounts(result, { rowCount: 2, affectedRows: 3 });
    expect(result).toMatchObject({
      rows: [{ __col_0: "widget-one" }],
      truncatedAt: 1,
    });
    expect(screen.getByText("widget-one")).toBeTruthy();
    expect(
      screen.getByText(
        /1 of 2 rows returned \(display truncated\) · 3 rows affected/,
      ),
    ).toBeTruthy();
  });

  it("keeps a delete without old attributes distinct from a confirmed zero", async () => {
    const { result, commands } = await execute([remove], [{}]);
    expect(commands).toEqual(["DeleteItemCommand"]);
    expectCounts(result, { rowCount: 0, affectedRowsUnknown: true });
    expect(screen.getByText(/Affected row count unavailable/)).toBeTruthy();
    expect(screen.queryByText(/0 rows affected/)).toBeNull();
  });

  it.each([
    {},
    { Attributes: {} },
  ])("reports a known zero when ALL_OLD returns no item (%j)", async (response) => {
    const { result } = await execute([removeWithOld], [response]);
    expectCounts(result, { rowCount: 0, affectedRows: 0 });
    expect(screen.getByText(/^0 rows affected ·/)).toBeTruthy();
  });

  it("reports a known delete when ALL_OLD returns the item", async () => {
    const { result } = await execute(
      [removeWithOld],
      [{ Attributes: firstKey }],
    );
    expectCounts(result, { rowCount: 1, affectedRows: 1 });
    expect(screen.getByText("widget-one")).toBeTruthy();
    expect(screen.getByText(/1 row returned · 1 row affected/)).toBeTruthy();
  });

  it("keeps returned delete rows when another delete in the same script has an unknown count", async () => {
    const { result } = await execute(
      [removeWithOld, { ...remove, Key: secondKey }],
      [{ Attributes: firstKey }, {}],
    );
    expectCounts(result, { rowCount: 1, affectedRowsUnknown: true });
    expect(screen.getByText("widget-one")).toBeTruthy();
    expect(
      screen.getByText(/1 row returned · Affected row count unavailable/),
    ).toBeTruthy();
  });

  it("labels an empty GetItem as a read, not a zero-row write", async () => {
    const { result, commands } = await execute(
      [{ ...table, Key: firstKey }],
      [{}],
    );
    expect(commands).toEqual(["GetItemCommand"]);
    expectCounts(result, { rowCount: 0 });
    expect(screen.getByText(/^0 rows returned ·/)).toBeTruthy();
    expect(screen.queryByText(/affected/i)).toBeNull();
  });

  it("warns about unprocessed read keys without claiming an affected count", async () => {
    const keys = { widgets: { Keys: [firstKey] } };
    const { result } = await execute(
      [{ RequestItems: keys }],
      [{ UnprocessedKeys: keys }],
    );
    expectCounts(result, { rowCount: 0 });
    expect(screen.getByText(/^0 rows returned/)).toBeTruthy();
    expect(screen.getByRole("status").textContent).toContain(
      "did not process 1 key",
    );
    expect(screen.queryByText(/affected|successfully/)).toBeNull();
  });

  it("keeps DynamoDB scan metadata separate from the returned and affected counts", async () => {
    const { result } = await execute(
      [{ ...table, Select: "COUNT" }],
      [{ Count: 4, ScannedCount: 4, LastEvaluatedKey: firstKey }],
    );
    expectCounts(result, { rowCount: 0 });
    expect(screen.getByText(/^0 rows returned/)).toBeTruthy();
    expect(
      screen.getByLabelText("DynamoDB response metadata").textContent,
    ).toContain('"Count": 4');
    expect(screen.getByText(/Use LastEvaluatedKey/)).toBeTruthy();
    expect(screen.queryByText(/affected/i)).toBeNull();
  });

  it("shows what completed before a later write fails without sending remaining requests", async () => {
    const { result, commands } = await execute(
      [put, { ...put, Item: secondKey }, put],
      [{}, new Error("Connection interrupted")],
    );
    expect(commands).toEqual(["PutItemCommand", "PutItemCommand"]);
    expectCounts(result, { rowCount: 0, affectedRows: 1 });
    expect(screen.getByRole("alert").textContent).toContain(
      "Its outcome is unknown and the remaining requests were not sent.",
    );
    expect(
      screen.getByText(
        /0 rows returned · 1 row affected in completed requests/,
      ),
    ).toBeTruthy();
    expect(screen.queryByText("Query executed successfully")).toBeNull();
  });

  it("does not report success when the first write is rejected", async () => {
    const { commands } = await execute(
      [put, { ...put, Item: secondKey }],
      [new Error("Write rejected")],
    );
    expect(commands).toEqual(["PutItemCommand"]);
    expect(screen.getByText(/Write rejected/)).toBeTruthy();
    expect(screen.queryByText(/affected|successfully/)).toBeNull();
  });

  it.each([
    { unprocessed: [batchDelete], processed: 1 },
    { unprocessed: [batchPut, batchDelete], processed: 0 },
  ])("shows only the $processed processed batch writes and explicitly warns about the rest", async ({
    unprocessed,
    processed,
  }) => {
    const { result } = await execute(
      [batch],
      [{ UnprocessedItems: { widgets: unprocessed } }],
    );
    expectCounts(result, {
      rowCount: 0,
      affectedRows: processed,
      affectedRowsKind: "processed-writes",
    });
    expect(
      screen.getByText(new RegExp(`\\b${processed} write requests? processed`)),
    ).toBeTruthy();
    expect(screen.getByRole("status").textContent).toContain(
      `did not process ${unprocessed.length}`,
    );
    expect(
      screen.getByLabelText("DynamoDB response metadata").textContent,
    ).toContain('"UnprocessedItems"');
    expect(screen.queryByText(/rows? affected/)).toBeNull();
    expect(screen.queryByText("Query executed successfully")).toBeNull();
  });

  it("keeps unprocessed metadata and an accurate count across batch responses before a failure", async () => {
    const { result, commands } = await execute(
      [batch, batch, batch, batch],
      [
        { UnprocessedItems: { widgets: [batchDelete] } },
        {},
        new Error("Connection interrupted"),
      ],
    );
    expect(commands).toEqual(Array(3).fill("BatchWriteItemCommand"));
    expectCounts(result, {
      rowCount: 0,
      affectedRows: 3,
      affectedRowsKind: "processed-writes",
    });
    expect(screen.getByText(/3 write requests processed/)).toBeTruthy();
    const alert = screen.getByRole("alert").textContent;
    expect(alert).toContain("2 of 4 batch requests returned a response");
    expect(alert).toContain("did not process 1 write request");
    expect(
      screen.getByLabelText("DynamoDB response metadata").textContent,
    ).toContain('"UnprocessedItems"');
    expect(screen.queryByText("Query executed successfully")).toBeNull();
  });

  it("labels a complete batch as processed requests, not proof that the delete found an item", async () => {
    const { result } = await execute([batch], [{}]);
    expectCounts(result, {
      rowCount: 0,
      affectedRows: 2,
      affectedRowsKind: "processed-writes",
    });
    expect(screen.getByText(/^2 write requests processed ·/)).toBeTruthy();
    expect(screen.queryByRole("status")).toBeNull();
  });

  it.each([
    { items: [conditionCheck], writes: 0 },
    { items: [conditionCheck, { Put: put }], writes: 1 },
  ])("does not count transaction condition checks as writes ($writes writes)", async ({
    items,
    writes,
  }) => {
    const { result, commands } = await execute(
      [{ TransactItems: items }],
      [{}],
    );
    expect(commands).toEqual(["TransactWriteItemsCommand"]);
    expectCounts(result, {
      rowCount: 0,
      affectedRows: writes,
      affectedRowsKind: "processed-writes",
    });
    expect(
      screen.getByText(new RegExp(`\\b${writes} write requests? processed`)),
    ).toBeTruthy();
  });
});
