import { act, render, screen } from "@testing-library/react";
import { ErrorReply } from "redis";
import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { QueryPanelController } from "../../src/extension/panels/queryPanelController";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { useQueryViewController } from "../../src/webview/components/query/useQueryViewController";
import { QueryModeTableGrid } from "../../src/webview/components/table/grid/QueryModeTableGrid";
import { useQueryStore } from "../../src/webview/store";
import { dispatchWindowMessage } from "./testUtils";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: () => ({
    getVirtualItems: () => [{ index: 0, key: 0, start: 0, end: 26 }],
    getTotalSize: () => 26,
    scrollToIndex: () => undefined,
  }),
}));

const connection: ConnectionConfig = {
  id: "redis-fixture",
  name: "Redis fixture",
  type: "redis",
  host: "localhost",
};

function Results() {
  const { status, result } = useQueryViewController({
    panelId: "redis-fixture",
    connectionId: connection.id,
    initialIsBookmarked: false,
    initialQueryText: "",
  });
  return <QueryModeTableGrid status={status} result={result} />;
}

describe("Redis transaction warnings in the Query Editor", () => {
  it.each([
    {
      name: "partial EXEC",
      queryText: "MULTI; SET a 1; INCR text; EXEC",
      responses: [
        "OK",
        "QUEUED",
        "QUEUED",
        ["OK", new ErrorReply("ERR integer")],
      ],
      displayed: ["OK", "QUEUED", "QUEUED", ["OK", "(error) ERR integer"]],
      warning: /EXEC at statement 4 returned errors.*does not roll back/,
    },
    {
      name: "WATCH-aborted EXEC",
      queryText: "WATCH a; MULTI; SET a 1; EXEC",
      responses: ["OK", "OK", "QUEUED", null],
      displayed: ["OK", "OK", "QUEUED", null],
      warning: /not applied because EXEC at statement 4 returned nil/,
    },
  ])("displays a warning and complete replies for $name", async ({
    queryText,
    responses,
    displayed,
    warning,
  }) => {
    const replies = responses.values();
    const client = {
      sendCommand: vi.fn(async () => replies.next().value),
      withAbortSignal: () => client,
    };
    const driver = new RedisDriver(connection);
    Object.assign(driver, {
      client,
      connected: true,
      openClient: () => ({ promise: Promise.resolve(client) }),
      cancelClientEntry: vi.fn(),
    });
    const setLastQueryResult = vi.fn();
    const controller = new QueryPanelController(
      {
        getConnection: () => connection,
        isConnected: () => true,
        getQueryRowLimit: () => 10,
        getDriver: () => driver,
        addToHistory: vi.fn(async () => undefined),
        refreshSchemaCache: vi.fn(),
      } as never,
      {
        getActiveConnectionId: () => connection.id,
        getInitialConnectionId: () => connection.id,
        getLastQueryResult: () => null,
        postMessage: (message) =>
          dispatchWindowMessage(JSON.parse(JSON.stringify(message))),
        setActiveConnectionId: vi.fn(),
        setLastQueryResult,
        syncTitle: vi.fn(),
      },
    );

    const identity = { connectionId: connection.id, operationId: "redis-exec" };
    render(<Results />);
    await act(async () => {
      useQueryStore.getState().setRunning(identity);
      await controller.handleMessage({
        type: "executeQuery",
        payload: { ...identity, queryText },
      });
    });
    const json = JSON.stringify(displayed);
    const banner = screen.getByRole("status");
    expect(banner.textContent).toContain("Query warning:");
    expect(banner.textContent).toMatch(warning);
    expect(screen.getByText(json)).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
    expect(screen.queryByLabelText("DynamoDB response metadata")).toBeNull();
    expect(setLastQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ __col_0: json }] }),
    );
    expect(setLastQueryResult.mock.calls[0][0]).not.toHaveProperty("partial");
  });
});
