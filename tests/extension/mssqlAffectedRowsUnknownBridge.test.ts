import { describe, expect, it, vi } from "vitest";
import type { QueryResult } from "../../src/extension/dbDrivers/types";
import { QueryPanelController } from "../../src/extension/panels/queryPanelController";

vi.mock("vscode", () => ({
  window: { showWarningMessage: vi.fn() },
  env: { clipboard: { readText: vi.fn(async () => "") } },
}));

describe("MSSQL affected-row uncertainty through the query panel", () => {
  it.each([
    "no-rowset",
    "rowsets",
    "partial",
  ] as const)("preserves the marker through real formatting/JSON bridge and keeps history SQL-only: %s", async (mode) => {
    const sets =
      mode === "no-rowset"
        ? []
        : [
            {
              id: "result-1",
              columns: ["v"],
              rows: [{ __col_0: 1 }],
              rowCount: 1,
              ...(mode === "partial" ? { partial: true } : {}),
            },
          ];
    const result: QueryResult = {
      columns: sets[0]?.columns ?? [],
      rows: sets[0]?.rows ?? [],
      rowCount: sets[0]?.rowCount ?? 0,
      affectedRowsUnknown: true,
      ...(sets.length ? { resultSets: sets } : {}),
      ...(mode === "partial" ? { error: "later failure" } : {}),
      executionTimeMs: 3,
    };
    const driver = { query: vi.fn(async () => result) };
    const history = vi.fn(async () => undefined);
    const connectionManager = {
      getConnection: () => ({
        id: "mssql-nested",
        name: "MSSQL",
        type: "mssql",
      }),
      isConnected: () => true,
      getQueryRowLimit: () => 10,
      getDriver: () => driver,
      getDriverCapabilities: () => ({ boundedQueryResults: true }),
      addToHistory: history,
      refreshSchemaCache: vi.fn(),
    };
    const view = {
      getActiveConnectionId: () => "mssql-nested",
      getInitialConnectionId: () => "mssql-nested",
      getLastQueryResult: () => null,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );
    const queryText = "EXEC p; UPDATE audited SET x=1";
    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText, connectionId: "mssql-nested" },
    });
    // Real webview transport drops undefined properties via JSON serialization.
    const messages = JSON.parse(JSON.stringify(view.postMessage.mock.calls));
    expect(messages).toContainEqual([
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({
          affectedRowsUnknown: true,
          rowCount: result.rowCount,
          rows: result.rows,
          ...(mode === "partial" ? { error: "later failure" } : {}),
        }),
      }),
    ]);
    if (sets.length)
      expect(view.setLastQueryResult).toHaveBeenCalledWith(
        expect.objectContaining({
          resultSets: [
            expect.objectContaining({ rowCount: 1, rows: [{ __col_0: 1 }] }),
          ],
        }),
      );
    expect(history).toHaveBeenCalledExactlyOnceWith("mssql-nested", queryText);
  });
});
