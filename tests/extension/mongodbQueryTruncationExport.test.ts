import { readdir, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { QueryResult } from "../../src/extension/dbDrivers/types";
import type { QueryPanelCachedResult } from "../../src/extension/panels/queryPanelController";
import { createProjectTempDir } from "../runtime/tempDirectories";

type MongoDocument = Record<string, unknown>;
type ReadMethod = "find" | "aggregate";
type ExportFormat = "csv" | "json";

const HARD_CAP = 10_000;
const DEFAULT_LIMIT = 100;
const EXPORT_CONFIRMATION = "Export displayed rows";
const READ_METHODS: ReadMethod[] = ["find", "aggregate"];
const EXPORT_FORMATS: ExportFormat[] = ["csv", "json"];

function makeDocuments(count: number, probeIndex?: number): MongoDocument[] {
  return Array.from({ length: count }, (_, index) => ({
    _id: `id-${index}`,
    value: `value-${index}`,
    ...(index === probeIndex ? { probe_only: "probe-not-exported" } : {}),
  }));
}

function readQuery(method: ReadMethod, limit?: number): string {
  const query = method === "find" ? "db.c.find({})" : "db.c.aggregate([])";
  return limit === undefined ? query : `${query}.limit(${limit})`;
}

function createVscodeMock(outputPath: string) {
  return {
    showWarningMessage: vi.fn(
      async (): Promise<string | undefined> => undefined,
    ),
    showSaveDialog: vi.fn(async () => ({ fsPath: outputPath })),
    showInformationMessage: vi.fn(),
    showErrorMessage: vi.fn(),
    withProgress: vi.fn(
      async (
        _options: unknown,
        task: (
          progress: unknown,
          token: { onCancellationRequested: () => { dispose: () => void } },
        ) => Promise<unknown>,
      ) =>
        task(
          {},
          { onCancellationRequested: vi.fn(() => ({ dispose: vi.fn() })) },
        ),
    ),
  };
}

describe("MongoDB query truncation through controller and file export", () => {
  let tempDir: string;
  let ui: ReturnType<typeof createVscodeMock>;

  beforeEach(async () => {
    vi.resetModules();
    tempDir = await createProjectTempDir(
      "extension-host",
      "mongodb-query-truncation-",
    );
    ui = createVscodeMock(join(tempDir, "query-results.out"));
    vi.doMock("vscode", () => ({
      Uri: { file: (fsPath: string) => ({ fsPath }) },
      ProgressLocation: { Notification: 15 },
      window: ui,
    }));
  });

  afterEach(async () => {
    try {
      await rm(tempDir, { recursive: true, force: true });
    } finally {
      vi.doUnmock("vscode");
      vi.resetModules();
    }
  });

  async function createHarness(documents: MongoDocument[]) {
    // Only MongoDB's SDK boundary and VS Code's dialogs are mocked. The driver,
    // formatter, controller, serializers, and atomic file writes are real.
    const { MongoDBDriver } = await import(
      "../../src/extension/dbDrivers/mongodb"
    );
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const { formatQueryResult } = await import(
      "../../src/extension/utils/queryResultFormatting"
    );
    const connection = {
      id: "mongo-truncation",
      name: "Mongo truncation regression",
      type: "mongodb" as const,
      database: "test",
    };
    const driver = new MongoDBDriver(connection);
    const cursorLimit = vi.fn();
    const find = vi.fn(() => {
      let offset = 0;
      let limit = 0;
      let closed = false;
      return {
        sort() {
          return this;
        },
        skip(value: number) {
          offset = value;
          return this;
        },
        limit(value: number) {
          limit = value;
          cursorLimit(value);
          return this;
        },
        async toArray() {
          if (closed) throw new Error("Cursor is closed");
          // MongoDB find().limit(0) is unlimited, not an empty result.
          return documents.slice(
            offset,
            limit === 0 ? undefined : offset + Math.abs(limit),
          );
        },
        close: vi.fn(async () => {
          closed = true;
        }),
      };
    });
    const aggregate = vi.fn((pipeline: MongoDocument[]) => {
      let result = documents;
      let closed = false;
      for (const stage of pipeline) {
        if (typeof stage.$skip === "number") {
          result = result.slice(stage.$skip);
        }
        if (typeof stage.$limit === "number") {
          if (stage.$limit <= 0) throw new Error("$limit must be positive");
          result = result.slice(0, stage.$limit);
        }
      }
      return {
        toArray: vi.fn(async () => {
          if (closed) throw new Error("Cursor is closed");
          return result;
        }),
        close: vi.fn(async () => {
          closed = true;
        }),
      };
    });
    const insertOne = vi.fn(async () => ({
      acknowledged: true,
      insertedId: "written-after-cap",
    }));
    Object.assign(driver, {
      connected: true,
      client: {
        db: () => ({ collection: () => ({ find, aggregate, insertOne }) }),
      },
    });
    const query = vi.spyOn(driver, "query");
    let cached: QueryPanelCachedResult | null = null;
    const view = {
      getActiveConnectionId: () => connection.id,
      getInitialConnectionId: () => connection.id,
      getLastQueryResult: () => cached,
      setLastQueryResult: (result: QueryPanelCachedResult | null) => {
        cached = result;
      },
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      syncTitle: vi.fn(),
    };
    const manager = {
      getConnection: () => connection,
      isConnected: () => true,
      getDriver: () => driver,
      getDriverCapabilities: () => driver.getCapabilities(),
      getQueryRowLimit: () => HARD_CAP,
      addToHistory: vi.fn(async () => undefined),
      refreshSchemaCache: vi.fn(),
    };
    const controller = new QueryPanelController(manager as never, view);

    return {
      controller,
      cursorLimit,
      aggregate,
      find,
      insertOne,
      async execute(queryText: string) {
        await controller.handleMessage({
          type: "executeQuery",
          payload: { queryText, connectionId: connection.id },
        });
        expect(query).toHaveBeenCalledOnce();
        const raw: QueryResult = await query.mock.results[0].value;
        const formatted = formatQueryResult(raw, HARD_CAP);
        expect(view.postMessage).toHaveBeenLastCalledWith({
          type: "queryResult",
          payload: formatted,
        });
        expect(cached).toEqual({
          columns: formatted.columns,
          columnMeta: formatted.columnMeta,
          rows: formatted.rows,
          missingCells: formatted.missingCells,
          truncated: formatted.truncated,
          truncatedAt: formatted.truncatedAt,
        });
        return { raw, formatted };
      },
    };
  }

  function expectReadLimit(
    harness: Awaited<ReturnType<typeof createHarness>>,
    method: ReadMethod,
    limit: number,
  ) {
    if (method === "find") {
      expect(harness.cursorLimit).toHaveBeenCalledExactlyOnceWith(limit);
    } else {
      expect(harness.aggregate).toHaveBeenCalledOnce();
      expect(harness.aggregate.mock.calls[0][0].at(-1)).toEqual({
        $limit: limit,
      });
    }
  }

  async function exportResult(
    controller: Awaited<ReturnType<typeof createHarness>>["controller"],
    format: ExportFormat,
    expectedRows: number,
  ) {
    const outputPath = join(tempDir, `query-results.${format}`);
    ui.showSaveDialog.mockResolvedValue({ fsPath: outputPath });
    await controller.handleMessage({
      type: format === "csv" ? "exportResultsCSV" : "exportResultsJSON",
    });
    expect(ui.showErrorMessage).not.toHaveBeenCalled();
    expect(ui.showSaveDialog).toHaveBeenCalledOnce();
    expect(ui.showInformationMessage).toHaveBeenCalledOnce();
    const content = await readFile(outputPath, "utf8");
    if (format === "json") {
      const exported: MongoDocument[] = JSON.parse(content);
      expect(exported).toHaveLength(expectedRows);
      expect(exported[0]).toMatchObject({ _id: "id-0", value: "value-0" });
    } else {
      const lines = content.trimEnd().split("\n");
      expect(lines).toHaveLength(expectedRows + 1);
      expect(lines[0].split(",")).toContain("_id");
      expect(lines[1]).toContain("id-0");
    }
    expect(content).not.toContain("probe_only");
    expect(content).not.toContain("probe-not-exported");
    return content;
  }

  const truncatedCases = READ_METHODS.flatMap((method) =>
    EXPORT_FORMATS.flatMap((format) =>
      (["confirm", "decline"] as const).flatMap((decision) =>
        [
          { budget: "default", requested: undefined, retained: DEFAULT_LIMIT },
          { budget: "hard cap", requested: 20_000, retained: HARD_CAP },
        ].map((budget) => ({ method, format, decision, ...budget })),
      ),
    ),
  );

  it.each(
    truncatedCases,
  )("$method $budget: $decision truncated $format export and exclude the probe column", async ({
    method,
    format,
    decision,
    requested,
    retained,
  }) => {
    const harness = await createHarness(makeDocuments(12_001, retained));
    const { raw, formatted } = await harness.execute(
      readQuery(method, requested),
    );
    expectReadLimit(harness, method, retained + 1);
    expect(raw.rows).toHaveLength(retained);
    expect(raw.truncated).toBe(true);
    expect(formatted).toMatchObject({ truncated: true, truncatedAt: retained });
    expect(formatted.columns).toEqual(["_id", "value"]);
    ui.showWarningMessage.mockResolvedValue(
      decision === "confirm" ? EXPORT_CONFIRMATION : undefined,
    );

    if (decision === "confirm") {
      await exportResult(harness.controller, format, retained);
    } else {
      await harness.controller.handleMessage({
        type: format === "csv" ? "exportResultsCSV" : "exportResultsJSON",
      });
      expect(ui.showSaveDialog).not.toHaveBeenCalled();
      expect(ui.withProgress).not.toHaveBeenCalled();
      expect(ui.showInformationMessage).not.toHaveBeenCalled();
      expect(await readdir(tempDir)).toEqual([]);
    }
    expect(ui.showWarningMessage).toHaveBeenCalledExactlyOnceWith(
      expect.stringContaining("This query result was truncated"),
      { modal: true },
      EXPORT_CONFIRMATION,
    );
  });

  const completeCases = READ_METHODS.flatMap((method) =>
    EXPORT_FORMATS.flatMap((format) =>
      [
        {
          boundary: "exact default",
          requested: undefined,
          count: 100,
          retained: 100,
          readLimit: 101,
        },
        {
          boundary: "explicit below cap",
          requested: 5,
          count: 12_001,
          retained: 5,
          readLimit: 5,
        },
        {
          boundary: "explicit at cap",
          requested: HARD_CAP,
          count: 12_001,
          retained: HARD_CAP,
          readLimit: HARD_CAP,
        },
        {
          boundary: "exact hard cap",
          requested: 20_000,
          count: HARD_CAP,
          retained: HARD_CAP,
          readLimit: HARD_CAP + 1,
        },
        {
          boundary: "below hard cap",
          requested: 20_000,
          count: HARD_CAP - 1,
          retained: HARD_CAP - 1,
          readLimit: HARD_CAP + 1,
        },
      ].map((boundary) => ({ method, format, ...boundary })),
    ),
  );

  it.each(
    completeCases,
  )("$method $boundary: export complete $format without a false warning", async ({
    method,
    format,
    requested,
    count,
    retained,
    readLimit,
  }) => {
    const harness = await createHarness(makeDocuments(count));
    const { raw, formatted } = await harness.execute(
      readQuery(method, requested),
    );
    expectReadLimit(harness, method, readLimit);
    expect(raw.rows).toHaveLength(retained);
    expect(raw.truncated === true).toBe(false);
    expect(formatted).toMatchObject({
      truncated: false,
      truncatedAt: retained,
    });
    await exportResult(harness.controller, format, retained);
    expect(ui.showWarningMessage).not.toHaveBeenCalled();
  });

  it.each(
    EXPORT_FORMATS,
  )("propagates a statement's truncation below the merge cap to %s export", async (format) => {
    const harness = await createHarness(makeDocuments(101, 100));
    const { raw, formatted } = await harness.execute(
      'db.c.find({}); db.c.insertOne({_id:"written-after-cap"})',
    );
    expect(raw.rows).toHaveLength(101);
    expect(raw.truncated).toBe(true);
    expect(formatted.truncated).toBe(true);
    expect(raw.columns).not.toContain("probe_only");
    expect(harness.insertOne).toHaveBeenCalledOnce();
    ui.showWarningMessage.mockResolvedValue(EXPORT_CONFIRMATION);
    await exportResult(harness.controller, format, 101);
    expect(ui.showWarningMessage).toHaveBeenCalledOnce();
  });

  it.each(
    EXPORT_FORMATS,
  )("flags dropped merge rows for %s export and still executes every later statement", async (format) => {
    const harness = await createHarness(makeDocuments(6_000));
    const { raw, formatted } = await harness.execute(
      'db.c.find({}).limit(6000); db.c.find({}).limit(6000); db.c.find({}).limit(5); db.c.insertOne({_id:"written-after-cap"})',
    );
    expect(harness.find).toHaveBeenCalledTimes(3);
    expect(harness.insertOne).toHaveBeenCalledOnce();
    expect(raw.rows).toHaveLength(HARD_CAP);
    expect(raw.truncated).toBe(true);
    expect(formatted.truncated).toBe(true);
    ui.showWarningMessage.mockResolvedValue(EXPORT_CONFIRMATION);
    await exportResult(harness.controller, format, HARD_CAP);
    expect(ui.showWarningMessage).toHaveBeenCalledOnce();
  });

  it.each(
    EXPORT_FORMATS,
  )("does not falsely flag an exact-cap multistatement %s export", async (format) => {
    const harness = await createHarness(makeDocuments(5_000));
    const { raw, formatted } = await harness.execute(
      'db.c.find({}).limit(5000); db.c.find({}).limit(4999); db.c.insertOne({_id:"written-after-cap"})',
    );
    expect(harness.find).toHaveBeenCalledTimes(2);
    expect(harness.insertOne).toHaveBeenCalledOnce();
    expect(raw.rows).toHaveLength(HARD_CAP);
    expect(raw.truncated === true).toBe(false);
    expect(formatted.truncated).toBe(false);
    await exportResult(harness.controller, format, HARD_CAP);
    expect(ui.showWarningMessage).not.toHaveBeenCalled();
  });
});
