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
      workspace: { getConfiguration: () => ({ get: () => false }) },
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

  async function createHarness(
    documents: MongoDocument[],
    options?: {
      rowLimit?: number;
      command?: (command: MongoDocument, options?: MongoDocument) => unknown;
    },
  ) {
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
        db: () => ({
          command: options?.command,
          collection: () => ({ find, aggregate, insertOne }),
        }),
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
      getQueryRowLimit: () => options?.rowLimit ?? HARD_CAP,
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
        const formatted = formatQueryResult(raw, options?.rowLimit ?? HARD_CAP);
        const { exportRows, serializeJsonExportRow, ...webviewResult } =
          formatted;
        expect(view.postMessage).toHaveBeenLastCalledWith({
          type: "queryResult",
          payload: webviewResult,
        });
        expect(cached).toEqual({
          columns: formatted.columns,
          columnMeta: formatted.columnMeta,
          rows: formatted.rows,
          ...(exportRows ? { exportRows } : {}),
          ...(serializeJsonExportRow ? { serializeJsonExportRow } : {}),
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

  it.each([
    "find",
    "aggregate",
    "multi",
  ] as const)("preserves missing versus null through %s query, reordering, sorting and CSV/JSON export", async (method) => {
    const harness = await createHarness([
      { _id: "b", value: null },
      { _id: "a" },
      { _id: "c", value: "" },
    ]);
    const queryText =
      method === "multi"
        ? "db.c.find({});db.c.aggregate([])"
        : readQuery(method);
    const { raw, formatted } = await harness.execute(queryText);
    const valueIndex = raw.columns.indexOf("value");
    const idIndex = raw.columns.indexOf("_id");
    expect(raw.missingCells).toEqual(
      method === "multi"
        ? [
            { rowIndex: 1, columnIndex: valueIndex },
            { rowIndex: 4, columnIndex: valueIndex },
          ]
        : [{ rowIndex: 1, columnIndex: valueIndex }],
    );
    expect(formatted.missingCells).toEqual(raw.missingCells);
    const payload = {
      columnOrder: [`__col_${valueIndex}`, `__col_${idIndex}`],
      sort: [{ id: `__col_${idIndex}`, desc: false }],
    };
    const csvPath = join(tempDir, "missing.csv");
    ui.showSaveDialog.mockResolvedValue({ fsPath: csvPath });
    await harness.controller.handleMessage({
      type: "exportResultsCSV",
      payload,
    });
    const repeat = method === "multi" ? 2 : 1;
    expect(await readFile(csvPath, "utf8")).toBe(
      "value,_id\n" +
        "MISSING,a\n".repeat(repeat) +
        "(NULL),b\n".repeat(repeat) +
        ",c\n".repeat(repeat),
    );
    const jsonPath = join(tempDir, "missing.json");
    ui.showSaveDialog.mockResolvedValue({ fsPath: jsonPath });
    await harness.controller.handleMessage({
      type: "exportResultsJSON",
      payload,
    });
    expect(JSON.parse(await readFile(jsonPath, "utf8"))).toEqual([
      ...Array.from({ length: repeat }, () => ({ _id: "a" })),
      ...Array.from({ length: repeat }, () => ({ value: null, _id: "b" })),
      ...Array.from({ length: repeat }, () => ({ value: "", _id: "c" })),
    ]);
    expect(ui.showErrorMessage).not.toHaveBeenCalled();
  });

  it.each([
    { docs: [{ _id: "a" }, { _id: "b", value: "MISSING" }], marker: "MISSING" },
    {
      docs: [
        { _id: "a", value: null },
        { _id: "b", value: "(NULL)" },
      ],
      marker: "(NULL)",
    },
  ])("rejects MongoDB CSV marker collision $marker but preserves it in JSON", async ({
    docs,
  }) => {
    const harness = await createHarness(docs);
    await harness.execute(readQuery("find"));
    const csvPath = join(tempDir, "collision.csv");
    ui.showSaveDialog.mockResolvedValue({ fsPath: csvPath });
    await harness.controller.handleMessage({ type: "exportResultsCSV" });
    expect(ui.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("cannot distinguish"),
    );
    expect(await readdir(tempDir)).not.toContain("collision.csv");
    ui.showErrorMessage.mockClear();
    const jsonPath = join(tempDir, "collision.json");
    ui.showSaveDialog.mockResolvedValue({ fsPath: jsonPath });
    await harness.controller.handleMessage({ type: "exportResultsJSON" });
    expect(JSON.parse(await readFile(jsonPath, "utf8"))).toEqual(docs);
    expect(ui.showErrorMessage).not.toHaveBeenCalled();
  });

  it("keeps display and CSV values but exports BSON as canonical Extended JSON", async () => {
    const { BSON, Decimal128, Double, Long, ObjectId, Timestamp } =
      await import("mongodb");
    const id = new ObjectId("507f1f77bcf86cd799439011");
    const date = new Date("2024-01-01T12:34:56.789Z");
    const documents = [
      {
        _id: id,
        date,
        long: Long.fromString("9223372036854775807"),
        decimal: Decimal128.fromString("1.25"),
        precise: Decimal128.fromString("123456789.987654321"),
        payload: {
          items: [new Timestamp({ t: 42, i: 1 }), new Double(1), "123"],
          $rapiDB: { type: "json-null" },
        },
      },
      { _id: "b", date: null, long: null },
      { _id: "c" },
    ];
    const single = await createHarness(documents);
    const { raw: singleRaw, formatted } = await single.execute(
      readQuery("find"),
    );
    const [idColumn, dateColumn, longColumn] = ["_id", "date", "long"].map(
      (name) => `__col_${singleRaw.columns.indexOf(name)}`,
    );
    expect(formatted.rows[0]).toMatchObject({
      [idColumn]: id.toHexString(),
      [dateColumn]: "2024-01-01 12:34:56.789",
      [longColumn]: "9223372036854775807",
    });
    const payload = {
      columnOrder: [longColumn, idColumn, dateColumn],
      sort: [{ id: idColumn, desc: true }],
    };
    const csvPath = join(tempDir, "bson.csv");
    ui.showSaveDialog.mockResolvedValue({ fsPath: csvPath });
    await single.controller.handleMessage({
      type: "exportResultsCSV",
      payload,
    });
    expect(await readFile(csvPath, "utf8")).toBe(
      "long,_id,date\n" +
        "MISSING,c,MISSING\n" +
        "(NULL),b,(NULL)\n" +
        "9223372036854775807,507f1f77bcf86cd799439011,2024-01-01 12:34:56.789\n",
    );

    const multi = await createHarness(documents);
    const { raw: multiRaw } = await multi.execute(
      "db.c.find({});db.c.aggregate([])",
    );
    expect(multiRaw.columnMeta).toEqual(singleRaw.columnMeta);
    for (const [harness, name] of [
      [single, "single"],
      [multi, "multi"],
    ] as const) {
      ui.showSaveDialog.mockResolvedValue({
        fsPath: join(tempDir, `${name}.json`),
      });
      await harness.controller.handleMessage({ type: "exportResultsJSON" });
    }
    const one = await readFile(join(tempDir, "single.json"), "utf8");
    const combined = await readFile(join(tempDir, "multi.json"), "utf8");
    const canonical = documents.map((document) =>
      BSON.EJSON.serialize(document, { relaxed: false }),
    );
    expect(JSON.parse(one)).toEqual(canonical);
    expect(JSON.parse(combined)).toEqual([...canonical, ...canonical]);
    expect(BSON.EJSON.parse(one, { relaxed: false })).toEqual(documents);

    const selectedPath = join(tempDir, "selected.json");
    ui.showSaveDialog.mockResolvedValue({ fsPath: selectedPath });
    await single.controller.handleMessage({
      type: "exportResultsJSON",
      payload,
    });
    const selected = JSON.parse(await readFile(selectedPath, "utf8"));
    expect(selected).toEqual([
      { _id: "c" },
      { long: null, _id: "b", date: null },
      {
        long: { $numberLong: "9223372036854775807" },
        _id: { $oid: id.toHexString() },
        date: { $date: { $numberLong: String(date.getTime()) } },
      },
    ]);
    expect(Object.keys(selected[2])).toEqual(["long", "_id", "date"]);
    expect(ui.showErrorMessage).not.toHaveBeenCalled();
  });

  it.each([
    "foo",
    "(?>foo)",
  ])("preserves BSON command display and exports with pattern %s", async (pattern) => {
    const { BSON, BSONRegExp, Double, Long } = await import("mongodb");
    const response = {
      score: new Double(1),
      small: Long.fromNumber(2),
      large: Long.fromString("9007199254740993"),
      pattern: new BSONRegExp(pattern, "imsux"),
      nested: { items: [new BSONRegExp("a.b", "su")] },
    };
    const bytes = BSON.serialize(response);
    const command = vi.fn((_command, options) =>
      BSON.deserialize(bytes, options),
    );
    const harness = await createHarness([], { command });
    const { raw, formatted } = await harness.execute(
      'db.c.insertOne({_id:"new"});db.adminCommand({ping:1})',
    );
    expect(command).toHaveBeenCalledWith(
      { ping: 1 },
      { useBigInt64: false, promoteValues: false, bsonRegExp: true, raw: true },
    );
    const columnKey = (column: string) =>
      `__col_${raw.columns.indexOf(column)}`;
    for (const [column, value] of Object.entries({
      score: 1,
      small: 2,
      large: "9007199254740993",
      pattern: `/${pattern}/imsux`,
      nested: '{"items":["/a.b/su"]}',
    })) {
      expect(formatted.rows[1][columnKey(column)]).toBe(value);
    }

    const csvPath = join(tempDir, "command.csv");
    ui.showSaveDialog.mockResolvedValue({ fsPath: csvPath });
    await harness.controller.handleMessage({
      type: "exportResultsCSV",
      payload: { columnOrder: ["pattern", "nested"].map(columnKey) },
    });
    expect(await readFile(csvPath, "utf8")).toBe(
      `pattern,nested\nMISSING,MISSING\n/${pattern}/imsux,"{""items"":[""/a.b/su""]}"\n`,
    );

    const outputPath = join(tempDir, "command.json");
    ui.showSaveDialog.mockResolvedValue({ fsPath: outputPath });
    await harness.controller.handleMessage({
      type: "exportResultsJSON",
      payload: {
        sort: [{ id: columnKey("score"), desc: false }],
        columnOrder: raw.columns.map((_, index) => `__col_${index}`).reverse(),
      },
    });
    expect(ui.showErrorMessage).not.toHaveBeenCalled();
    const json = await readFile(outputPath, "utf8");
    expect(JSON.parse(json)).toEqual(
      expect.arrayContaining([
        { acknowledged: true, insertedId: "written-after-cap" },
        BSON.EJSON.serialize(response, { relaxed: false }),
      ]),
    );
    expect(BSON.EJSON.parse(json, { relaxed: false })).toEqual(
      expect.arrayContaining([response]),
    );
  });

  it("serializes only retained rows and selected columns; ambiguous BSON fails only JSON export", async () => {
    const { Long } = await import("mongodb");
    const harness = await createHarness(
      [
        {
          _id: "a",
          long: Long.fromString("9007199254740993"),
          $oid: "literal",
        },
        { _id: "b", $numberLong: "not exported" },
      ],
      { rowLimit: 1 },
    );
    const { raw, formatted } = await harness.execute(readQuery("find"));
    expect(raw.exportRows).toHaveLength(2);
    expect(formatted.exportRows).toHaveLength(1);
    ui.showWarningMessage.mockResolvedValue(EXPORT_CONFIRMATION);

    const csvPath = join(tempDir, "literal.csv");
    ui.showSaveDialog.mockResolvedValue({ fsPath: csvPath });
    await harness.controller.handleMessage({ type: "exportResultsCSV" });
    expect(await readFile(csvPath, "utf8")).toContain("literal");

    const jsonPath = join(tempDir, "literal.json");
    ui.showSaveDialog.mockResolvedValue({ fsPath: jsonPath });
    await harness.controller.handleMessage({ type: "exportResultsJSON" });
    expect(ui.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining('literal document key "$oid"'),
    );
    expect(await readdir(tempDir)).toEqual(["literal.csv"]);

    const longColumn = `__col_${raw.columns.indexOf("long")}`;
    ui.showErrorMessage.mockClear();
    await harness.controller.handleMessage({
      type: "exportResultsJSON",
      payload: { columnOrder: [longColumn] },
    });
    expect(ui.showErrorMessage).not.toHaveBeenCalled();
    expect(JSON.parse(await readFile(jsonPath, "utf8"))).toEqual([
      { long: { $numberLong: "9007199254740993" } },
    ]);
  });

  it("does not export a late BSON reply after results from another connection replace it", async () => {
    const { Long } = await import("mongodb");
    let finishOldReply: ((result: MongoDocument) => void) | undefined;
    const oldReply = new Promise<MongoDocument>((resolve) => {
      finishOldReply = resolve;
    });
    const command = vi.fn(({ ping }) =>
      ping === 1 ? oldReply : { value: Long.fromString("9007199254740995") },
    );
    const { controller } = await createHarness([], { command });
    const oldQuery = controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText: "db.runCommand({ping:1})",
        connectionId: "old",
      },
    });
    await vi.waitFor(() => expect(command).toHaveBeenCalledOnce());
    await controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText: "db.runCommand({ping:2})",
        connectionId: "new",
      },
    });
    finishOldReply?.({ value: Long.fromString("9007199254740993") });
    await oldQuery;

    const outputPath = join(tempDir, "latest.json");
    ui.showSaveDialog.mockResolvedValue({ fsPath: outputPath });
    await controller.handleMessage({ type: "exportResultsJSON" });
    expect(ui.showErrorMessage).not.toHaveBeenCalled();
    expect(JSON.parse(await readFile(outputPath, "utf8"))).toEqual([
      { value: { $numberLong: "9007199254740995" } },
    ]);
  });

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
    expect(formatted).toMatchObject({
      truncated: true,
      truncatedAt: retained,
    });

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
