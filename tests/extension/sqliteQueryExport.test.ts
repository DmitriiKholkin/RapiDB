import { readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { QueryPanelCachedResult } from "../../src/extension/panels/queryPanelController";
import { createProjectTempDir } from "../runtime/tempDirectories";

const connection = {
  id: "sqlite-query-export",
  name: "SQLite query export",
  type: "sqlite" as const,
  filePath: ":memory:",
};
const mixedQuery = "SELECT id, value FROM mixed ORDER BY id";

describe("SQLite query panel file export", () => {
  let driver: SQLiteDriver;
  let directory: string;
  let outputPath: string;
  const showSaveDialog = vi.fn(async () => ({ fsPath: outputPath }));
  const showWarningMessage = vi.fn(
    async (): Promise<string | undefined> => undefined,
  );
  const showErrorMessage = vi.fn();

  beforeEach(async () => {
    showSaveDialog.mockClear();
    showWarningMessage.mockClear();
    showErrorMessage.mockClear();
    directory = await createProjectTempDir("extension-host", "sqlite-export-");
    vi.doMock("vscode", () => ({
      Uri: { file: (fsPath: string) => ({ fsPath }) },
      ProgressLocation: { Notification: 15 },
      workspace: { getConfiguration: () => ({ get: () => false }) },
      window: {
        showSaveDialog,
        showWarningMessage,
        showErrorMessage,
        showInformationMessage: vi.fn(),
        withProgress: async (
          _options: unknown,
          task: (
            progress: unknown,
            token: { onCancellationRequested: () => { dispose: () => void } },
          ) => Promise<unknown>,
        ) =>
          task({}, { onCancellationRequested: () => ({ dispose: () => {} }) }),
      },
    }));
    showSaveDialog.mockImplementation(async () => ({ fsPath: outputPath }));
    driver = new SQLiteDriver(connection);
    await driver.connect();
    await driver.query(`
      CREATE TABLE mixed (id INTEGER PRIMARY KEY, value);
      INSERT INTO mixed VALUES
        (1, 9007199254740993), (2, '9007199254740993'),
        (3, -9223372036854775808), (4, '-9223372036854775808'),
        (5, 42), (6, '42'), (7, 1.25), (8, NULL);
    `);
  });

  afterEach(async () => {
    try {
      await driver?.disconnect();
      if (directory) await rm(directory, { recursive: true, force: true });
    } finally {
      vi.doUnmock("vscode");
      vi.resetModules();
    }
  });

  async function execute(queryText: string, rowLimit = 100) {
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    let cached: QueryPanelCachedResult | null = null;
    const postMessage = vi.fn((_message: unknown) => {});
    const controller = new QueryPanelController(
      {
        getConnection: () => connection,
        isConnected: () => driver.isConnected(),
        getDriver: () => driver,
        getDriverCapabilities: () => driver.getCapabilities(),
        getQueryRowLimit: () => rowLimit,
        addToHistory: vi.fn(async () => undefined),
        refreshSchemaCache: vi.fn(),
      } as never,
      {
        getActiveConnectionId: () => connection.id,
        getInitialConnectionId: () => connection.id,
        getLastQueryResult: () => cached,
        setLastQueryResult: (result) => {
          cached = result;
        },
        postMessage,
        setActiveConnectionId: vi.fn(),
        syncTitle: vi.fn(),
      },
    );
    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText, connectionId: connection.id },
    });
    const result = postMessage.mock.calls.at(-1)?.[0];
    expect(result).toMatchObject({ type: "queryResult" });
    expect(result).not.toHaveProperty("payload.error");
    // The extension-to-webview message must contain no bigint values.
    expect(() => JSON.stringify(result)).not.toThrow();
    return { controller, result, getCached: () => cached };
  }

  it("keeps the display and CSV unchanged while exporting identical INTEGER and TEXT differently in JSON", async () => {
    const uncapped = await driver.query(
      "SELECT typeof(value), value FROM mixed WHERE id <= ? ORDER BY id",
      [2],
    );
    expect(uncapped.rows).toEqual([
      { __col_0: "integer", __col_1: "9007199254740993" },
      { __col_0: "text", __col_1: "9007199254740993" },
    ]);
    expect(uncapped.jsonExportCellOverrides).toEqual([
      { __col_1: 9007199254740993n },
      undefined,
    ]);
    const { controller, result } = await execute(mixedQuery);
    expect(result).toMatchObject({
      payload: {
        columnMeta: [{ category: "integer" }, { category: null }],
        rows: [
          { __col_1: "9007199254740993" },
          { __col_1: "9007199254740993" },
          { __col_1: "-9223372036854775808" },
          { __col_1: "-9223372036854775808" },
          { __col_1: 42 },
          { __col_1: "42" },
          { __col_1: 1.25 },
          { __col_1: null },
        ],
        truncated: false,
      },
    });
    outputPath = join(directory, "mixed.json");
    await controller.handleMessage({ type: "exportResultsJSON" });
    expect(showErrorMessage).not.toHaveBeenCalled();
    expect(await readFile(outputPath, "utf8")).toBe(
      "[\n" +
        '{"id":1,"value":9007199254740993},\n' +
        '{"id":2,"value":"9007199254740993"},\n' +
        '{"id":3,"value":-9223372036854775808},\n' +
        '{"id":4,"value":"-9223372036854775808"},\n' +
        '{"id":5,"value":42},\n' +
        '{"id":6,"value":"42"},\n' +
        '{"id":7,"value":1.25},\n' +
        '{"id":8,"value":null}\n' +
        "]\n",
    );

    outputPath = join(directory, "mixed.csv");
    await controller.handleMessage({ type: "exportResultsCSV" });
    expect(showErrorMessage).not.toHaveBeenCalled();
    expect(await readFile(outputPath, "utf8")).toBe(
      "id,value\n" +
        "1,9007199254740993\n2,9007199254740993\n" +
        "3,'-9223372036854775808\n4,'-9223372036854775808\n" +
        "5,42\n6,42\n7,1.25\n8,(NULL)\n",
    );
    expect(showWarningMessage).not.toHaveBeenCalled();
  });

  it("never caches or writes the single-result truncation probe", async () => {
    const { controller, result, getCached } = await execute(mixedQuery, 2);
    expect(result).toMatchObject({
      payload: {
        rowCount: 8,
        truncated: true,
        truncatedAt: 2,
      },
    });
    expect(getCached()?.jsonExportCellOverrides).toEqual([
      { __col_1: 9007199254740993n },
      undefined,
    ]);
    outputPath = join(directory, "retained.json");
    showWarningMessage.mockResolvedValueOnce("Export displayed rows");
    await controller.handleMessage({ type: "exportResultsJSON" });
    expect(showErrorMessage).not.toHaveBeenCalled();
    expect(await readFile(outputPath, "utf8")).toBe(
      '[\n{"id":1,"value":9007199254740993},\n' +
        '{"id":2,"value":"9007199254740993"}\n]\n',
    );
  });

  it("exports only retained rows of the selected result set after sorting and reordering", async () => {
    const { controller, result } = await execute(
      `SELECT 9223372036854775807 AS first; ${mixedQuery}; SELECT 9 AS last`,
      3,
    );
    expect(result).toMatchObject({
      payload: {
        resultSets: [
          { rowCount: 1, truncated: false, truncatedAt: 1 },
          { rowCount: 8, truncated: true, truncatedAt: 2 },
          { rowCount: 1, truncated: true, truncatedAt: 0, rows: [] },
        ],
      },
    });
    outputPath = join(directory, "first.json");
    await controller.handleMessage({ type: "exportResultsJSON" });
    expect(await readFile(outputPath, "utf8")).toBe(
      '[\n{"first":9223372036854775807}\n]\n',
    );
    expect(showWarningMessage).not.toHaveBeenCalled();

    outputPath = join(directory, "selected.json");
    const payload = {
      resultSetIndex: 1,
      sort: [{ id: "__col_0", desc: true }],
      columnOrder: ["__col_1", "__col_0"],
    };
    showWarningMessage.mockResolvedValueOnce(undefined);
    await controller.handleMessage({ type: "exportResultsJSON", payload });
    expect(showSaveDialog).toHaveBeenCalledTimes(1);

    showWarningMessage.mockResolvedValueOnce("Export displayed rows");
    await controller.handleMessage({ type: "exportResultsJSON", payload });
    expect(showWarningMessage).toHaveBeenLastCalledWith(
      expect.stringContaining("first 2 displayed rows"),
      { modal: true },
      "Export displayed rows",
    );
    expect(showErrorMessage).not.toHaveBeenCalled();
    expect(await readFile(outputPath, "utf8")).toBe(
      '[\n{"value":"9007199254740993","id":2},\n' +
        '{"value":9007199254740993,"id":1}\n]\n',
    );
  });
});
