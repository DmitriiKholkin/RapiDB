import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { isMutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import type {
  PreparedApplyPlan,
  PreparedInsertPlan,
} from "../../src/extension/table/tableDataContracts";
import {
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { TableDataService } from "../../src/extension/tableDataService";
import type { ApplyResultPayload } from "../../src/shared/webviewContracts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const column: ColumnTypeMeta = {
  name: "amount",
  type: "DECIMAL",
  nativeType: "DECIMAL(10,2)",
  category: "decimal",
  nullable: false,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};
const apply: PreparedApplyPlan = {
  connectionId: "preflight",
  database: "db",
  schema: "public",
  table: "updates",
  cols: [column],
  updates: [{ primaryKeys: { id: 1 }, changes: { amount: "12.34" } }],
  operations: [
    { sql: "UPDATE updates SET amount = ? WHERE id = ?", params: ["12.34", 1] },
  ],
  previewStatements: [],
  skippedRows: [],
  verificationTargets: [
    {
      rowIndex: 0,
      primaryKeys: { id: 1 },
      values: [{ column, expectedValue: "12.34" }],
    },
  ],
};
const insert: PreparedInsertPlan = {
  connectionId: "preflight",
  database: "db",
  schema: "public",
  table: "inserts",
  operation: {
    sql: "INSERT INTO inserts (amount) VALUES (?)",
    params: ["12.34"],
  },
  previewStatements: [],
  verificationCriteria: { id: 2 },
  verification: {
    mutation: "insert",
    rowIndex: 0,
    sql: "SELECT amount FROM inserts WHERE id = ?",
    params: [2],
    values: [{ column, expectedValue: "12.34" }],
  },
};
const paths = [
  "prepared-update",
  "atomic-update",
  "atomic-insert",
  "atomic-mixed",
  "prepared-insert",
] as const;
type Path = (typeof paths)[number];

function harness(path: Path) {
  const entered = deferred<void>();
  const risk = deferred<string | null>();
  const targets =
    path === "atomic-mixed"
      ? ["updates", "inserts"]
      : [path.endsWith("insert") ? "inserts" : "updates"];
  const driver = {
    qualifiedTableName: () => "public.updates",
    quoteIdentifier: (name: string) => `"${name}"`,
    coerceInputValue: (value: unknown) => value,
    buildInsertValueExpr: () => "?",
    getMutationAtomicityRisk: vi.fn(
      (_database: string, _schema: string, table: string) => {
        if (table !== targets.at(-1)) return Promise.resolve(null);
        entered.resolve();
        return risk.promise;
      },
    ),
    runTransaction: vi.fn(async () => {}),
    query: vi.fn(),
  };
  const replacement = {
    ...driver,
    runTransaction: vi.fn(async () => {}),
    query: vi.fn(),
  };
  let currentDriver: typeof driver | undefined = driver;
  const connection = {
    id: "preflight",
    name: "Preflight",
    type: "mysql",
    readOnly: false,
  };
  const manager = {
    getConnection: () => connection,
    getDriver: () => currentDriver,
    getQueryEditorPresentation: () => undefined,
  } as unknown as ConnectionManager;
  const service = new TableMutationService(manager, {
    getColumns: async () => [],
  });
  const controller = new TableMutationPreviewController({
    connectionId: connection.id,
    tableName: insert.table,
    connectionManager: manager,
    tableDataService: service,
    notifyWarning: vi.fn(),
  });
  async function execute(): Promise<ApplyResultPayload> {
    if (path === "prepared-update")
      return executePreparedApplyPlan(manager, apply);
    if (path === "prepared-insert") {
      // The single-insert consumer must retain the service's no-execution evidence.
      const preview = controller.createApplyChangesPreview("insert-one", {
        apply: null,
        applyResultWhenEmpty: null,
        inserts: [insert],
      });
      const result = await controller.confirm(
        preview.previewToken,
        "insert-one",
      );
      if (result?.type !== "applyResult")
        throw new Error("Expected applyResult");
      return result.payload;
    }
    return executeAtomicSqlApplyPlan(
      manager,
      path === "atomic-insert" ? null : apply,
      path === "atomic-update" ? [] : [insert],
    );
  }
  return {
    connection,
    driver,
    replacement,
    service,
    execute,
    entered,
    risk,
    targets,
    replaceDriver: () => {
      currentDriver = replacement;
    },
    disconnect: () => {
      currentDriver = undefined;
    },
  };
}

describe("SQL mutation service boundary after async rollback preflight", () => {
  for (const path of paths) {
    for (const transition of [
      "read-only",
      "replacement",
      "disconnect",
    ] as const) {
      it(`${path}: rejects pending ${transition} without reporting an unknown write`, async () => {
        const h = harness(path);
        const pending = h.execute();
        await h.entered.promise;
        expect(h.driver.runTransaction).not.toHaveBeenCalled();
        if (transition === "read-only") h.connection.readOnly = true;
        else if (transition === "replacement") h.replaceDriver();
        else h.disconnect();
        h.risk.resolve(null);
        const result = await pending;
        expect(result).toMatchObject({
          success: false,
          changesPossible: false,
          outcomeUnknown: false,
          error: expect.stringMatching(
            transition === "read-only" ? /read.only/i : /connection.*changed/i,
          ),
        });
        expect(result.insertApplied).not.toBe(true);
        expect(result.error).not.toMatch(/outcome may be unknown/i);
        if (
          path === "prepared-update" ||
          path === "atomic-update" ||
          path === "atomic-mixed"
        ) {
          expect(result.rowOutcomes).toEqual([
            expect.objectContaining({
              rowIndex: 0,
              success: false,
              status: "skipped",
            }),
          ]);
        }
        expect(
          h.driver.getMutationAtomicityRisk.mock.calls.map(
            ([, , table]) => table,
          ),
        ).toEqual(h.targets);
        expect(h.driver.runTransaction).not.toHaveBeenCalled();
        expect(h.replacement.runTransaction).not.toHaveBeenCalled();
        expect(h.driver.query).not.toHaveBeenCalled();
        expect(h.replacement.query).not.toHaveBeenCalled();
      });
    }

    it(`${path}: still executes after a safe deferred preflight`, async () => {
      const h = harness(path);
      const pending = h.execute();
      await h.entered.promise;
      expect(h.driver.runTransaction).not.toHaveBeenCalled();
      h.risk.resolve(null);
      expect(await pending).toMatchObject({ success: true });
      expect(h.driver.runTransaction).toHaveBeenCalledOnce();
      expect(
        h.driver.getMutationAtomicityRisk.mock.calls.map(
          ([, , table]) => table,
        ),
      ).toEqual(h.targets);
    });

    it(`${path}: preserves rollback-risk rejection without execution`, async () => {
      const h = harness(path);
      const pending = h.execute();
      await h.entered.promise;
      h.risk.resolve("Table does not support rollback.");
      expect(await pending).toMatchObject({
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
        error: expect.stringContaining("requires rollback support"),
      });
      expect(h.driver.runTransaction).not.toHaveBeenCalled();
    });
  }

  it("prepared INSERT service marks a post-preflight writable rejection as not executed", async () => {
    const h = harness("prepared-insert");
    const pending = h.service
      .executePreparedInsertPlan(insert)
      .catch((error: unknown) => error);
    await h.entered.promise;
    h.connection.readOnly = true;
    h.risk.resolve(null);
    const error = await pending;
    expect(error).toBeInstanceOf(Error);
    expect(isMutationNotExecutedError(error)).toBe(true);
    expect(h.driver.runTransaction).not.toHaveBeenCalled();
  });
});

function updateColumnsHarness(
  route: "driver" | "sql",
  entry: "mutation-service" | "public-service",
) {
  const entered = deferred<void>();
  const columns = deferred<ColumnTypeMeta[]>();
  const metadata: ColumnTypeMeta[] = [
    {
      ...column,
      name: "id",
      type: "INT",
      nativeType: "INT",
      category: "integer",
      isPrimaryKey: true,
    },
    column,
  ];
  const updateRows = vi.fn(async () => ({ affectedRows: 1 }));
  const query = vi.fn(async () => ({
    columns: [],
    rows: [],
    rowCount: 1,
    affectedRows: 1,
    executionTimeMs: 0,
  }));
  const driver = {
    describeColumns: vi.fn(() => {
      entered.resolve();
      return columns.promise;
    }),
    checkPersistedEdit: vi.fn(() => ({ ok: true, shouldVerify: false })),
    coerceInputValue: vi.fn((value: unknown) => value),
    qualifiedTableName: () => 'public."updates"',
    quoteIdentifier: (name: string) => `"${name}"`,
    buildSetExpr: (meta: ColumnTypeMeta) => `"${meta.name}" = ?`,
    buildInsertValueExpr: () => "?",
    runTransaction: vi.fn(),
    query,
    ...(route === "driver" ? { updateRows } : {}),
  };
  const replacementUpdateRows = vi.fn(async () => ({ affectedRows: 1 }));
  const replacement = {
    ...driver,
    describeColumns: vi.fn(async () => metadata),
    query: vi.fn(async () => ({
      columns: [],
      rows: [],
      rowCount: 1,
      affectedRows: 1,
      executionTimeMs: 0,
    })),
    runTransaction: vi.fn(),
    ...(route === "driver" ? { updateRows: replacementUpdateRows } : {}),
  };
  let currentDriver: typeof driver | undefined = driver;
  const connection = { id: "columns-preflight", readOnly: false };
  const manager = {
    getConnection: () => connection,
    getDriver: () => currentDriver,
  } as unknown as ConnectionManager;
  const service =
    entry === "public-service"
      ? new TableDataService(manager)
      : new TableMutationService(manager, {
          getColumns: driver.describeColumns,
        });
  return {
    entered,
    driver,
    updateRows,
    replacement,
    replacementUpdateRows,
    connection,
    resolveColumns: () => columns.resolve(metadata),
    replaceDriver: () => {
      currentDriver = replacement;
    },
    disconnect: () => {
      currentDriver = undefined;
    },
    execute: () =>
      service.updateRow(
        connection.id,
        "db",
        "public",
        "updates",
        { id: 1 },
        { amount: "12.34" },
      ),
  };
}

describe("UPDATE service boundary after deferred columns", () => {
  for (const entry of ["mutation-service", "public-service"] as const) {
    for (const route of ["driver", "sql"] as const) {
      for (const transition of [
        "read-only",
        "replacement",
        "disconnect",
      ] as const) {
        it(`${entry}/${route}: rejects ${transition} before writing`, async () => {
          const h = updateColumnsHarness(route, entry);
          const pending = h.execute().catch((error: unknown) => error);
          await h.entered.promise;
          expect(h.updateRows).not.toHaveBeenCalled();
          expect(h.driver.query).not.toHaveBeenCalled();
          if (transition === "read-only") h.connection.readOnly = true;
          else if (transition === "replacement") h.replaceDriver();
          else h.disconnect();
          h.resolveColumns();
          const error = await pending;
          expect(error).toBeInstanceOf(Error);
          expect(error).toHaveProperty(
            "message",
            expect.stringMatching(
              transition === "read-only"
                ? /read.only/i
                : /connection.*changed/i,
            ),
          );
          expect(isMutationNotExecutedError(error)).toBe(true);
          expect(h.updateRows).not.toHaveBeenCalled();
          expect(h.driver.query).not.toHaveBeenCalled();
          expect(h.driver.runTransaction).not.toHaveBeenCalled();
          expect(h.replacementUpdateRows).not.toHaveBeenCalled();
          expect(h.replacement.query).not.toHaveBeenCalled();
          expect(h.replacement.runTransaction).not.toHaveBeenCalled();
        });
      }

      it(`${entry}/${route}: preserves unchanged successful execution`, async () => {
        const h = updateColumnsHarness(route, entry);
        const pending = h.execute();
        await h.entered.promise;
        expect(h.updateRows).not.toHaveBeenCalled();
        expect(h.driver.query).not.toHaveBeenCalled();
        h.resolveColumns();
        await expect(pending).resolves.toBeUndefined();
        if (route === "driver") {
          expect(h.updateRows).toHaveBeenCalledExactlyOnceWith({
            database: "db",
            schema: "public",
            table: "updates",
            updates: [{ primaryKeys: { id: 1 }, changes: { amount: "12.34" } }],
          });
          expect(h.driver.query).not.toHaveBeenCalled();
        } else {
          expect(h.driver.query).toHaveBeenCalledExactlyOnceWith(
            'UPDATE public."updates" SET "amount" = ? WHERE "id" = ?',
            ["12.34", 1],
            { database: "db" },
          );
          expect(h.updateRows).not.toHaveBeenCalled();
        }
        expect(h.driver.checkPersistedEdit).toHaveBeenCalledOnce();
        expect(h.replacementUpdateRows).not.toHaveBeenCalled();
        expect(h.replacement.query).not.toHaveBeenCalled();
      });
    }
  }

  for (const route of ["driver", "sql"] as const) {
    it(`${route}: checks writability after value coercion too`, async () => {
      const h = updateColumnsHarness(route, "public-service");
      h.driver.coerceInputValue.mockImplementation((value) => {
        if (value === "12.34") h.connection.readOnly = true;
        return value;
      });
      const pending = h.execute().catch((error: unknown) => error);
      await h.entered.promise;
      h.resolveColumns();
      const error = await pending;
      expect(error).toHaveProperty(
        "message",
        expect.stringMatching(/read.only/i),
      );
      expect(isMutationNotExecutedError(error)).toBe(true);
      expect(h.updateRows).not.toHaveBeenCalled();
      expect(h.driver.query).not.toHaveBeenCalled();
    });

    it(`${route}: does not classify a write acknowledgement failure as not executed`, async () => {
      const h = updateColumnsHarness(route, "public-service");
      const writeError = new Error("Write acknowledgement lost");
      if (route === "driver") h.updateRows.mockRejectedValue(writeError);
      else h.driver.query.mockRejectedValue(writeError);
      const pending = h.execute().catch((error: unknown) => error);
      await h.entered.promise;
      h.resolveColumns();
      const error = await pending;
      expect(error).toBe(writeError);
      expect(isMutationNotExecutedError(error)).toBe(false);
      expect(
        route === "driver" ? h.updateRows : h.driver.query,
      ).toHaveBeenCalledOnce();
    });
  }
});
