import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import type {
  PreparedApplyPlan,
  PreparedDeletePlan,
} from "../../src/extension/table/tableDataContracts";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";

const drivers = new Set<SQLiteDriver>();

afterEach(async () => {
  await Promise.all(Array.from(drivers, (driver) => driver.disconnect()));
  drivers.clear();
  vi.unstubAllEnvs();
});

async function startDriver(initialTimeoutMs: number) {
  const directory = await createProjectTempDir("sqlite-queued-mutation");
  const workerPath = join(directory, "fakeSqliteWorker.cjs");
  const logPath = join(directory, "requests.jsonl");
  await writeFile(
    workerPath,
    `const fs = require("node:fs");
const logPath = process.env.RAPIDB_SQLITE_MUTATION_TEST_LOG;
process.on("message", (request) => {
  const command = request.args && request.args[0];
  fs.appendFileSync(logPath, JSON.stringify({ method: request.method, command }) + "\\n");
  if (request.method === "connect") {
    process.send({ id: request.id, value: "connected" });
    return;
  }
  if (request.method === "query" && command === "HOLD") return;
  if (request.method === "runTransaction" && command[0] && command[0].sql === "HOLD") return;
  process.send({ id: request.id, value: null });
});
process.on("disconnect", () => process.exit(0));
`,
    "utf8",
  );
  vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", workerPath);
  vi.stubEnv("RAPIDB_SQLITE_MUTATION_TEST_LOG", logPath);

  let timeoutMs = initialTimeoutMs;
  const config: ConnectionConfig = {
    id: "sqlite-queued-mutation-test",
    name: "SQLite queued mutation test",
    type: "sqlite",
    filePath: join(directory, "db.sqlite"),
  };
  const driver = new SQLiteDriver(config, () => ({
    connectionTimeoutSeconds: 1,
    connectionTimeoutMs: 1_000,
    dbOperationTimeoutSeconds: Math.ceil(timeoutMs / 1_000),
    dbOperationTimeoutMs: timeoutMs,
  }));
  drivers.add(driver);
  await driver.connect();

  return {
    config,
    driver,
    setTimeoutMs(value: number) {
      timeoutMs = value;
    },
    async requests() {
      const text = await readFile(logPath, "utf8").catch(() => "");
      return text
        .split("\n")
        .filter(Boolean)
        .map(
          (line) => JSON.parse(line) as { method: string; command: unknown },
        );
    },
  };
}

function applyPlan(connectionId: string): PreparedApplyPlan {
  return {
    connectionId,
    database: "main",
    schema: "",
    table: "items",
    cols: [],
    updates: [{ primaryKeys: { id: 1 }, changes: { name: "updated" } }],
    operations: [
      { sql: "UPDATE items SET name = ? WHERE id = ?", params: ["updated", 1] },
    ],
    previewStatements: ["UPDATE items SET name = 'updated' WHERE id = 1"],
    skippedRows: [],
    verificationTargets: [],
  };
}

function deletePlan(connectionId: string): PreparedDeletePlan {
  const identities = [{ id: 1 }];
  return {
    connectionId,
    database: "main",
    schema: "",
    table: "items",
    mode: "sql",
    executionMode: "transaction",
    rowIdentities: identities,
    operations: [{ sql: "DELETE FROM items WHERE id = ?", params: [1] }],
    previewStatements: ["DELETE FROM items WHERE id = 1"],
    verificationCriteriaList: identities,
  };
}

describe.each([
  ["delete", "timeout"],
  ["apply", "timeout"],
  ["delete", "cancel"],
  ["apply", "cancel"],
] as const)("SQLite queued %s after active %s", (kind, activeFailure) => {
  it("returns a skipped result with no possible changes or unknown outcome", async () => {
    const timeout = activeFailure === "timeout" ? 250 : 5_000;
    const { config, driver, requests, setTimeoutMs } =
      await startDriver(timeout);
    const active = driver.query("HOLD").then(
      () => null,
      (error: unknown) => error,
    );
    await vi.waitFor(async () =>
      expect(
        (await requests()).some((request) => request.command === "HOLD"),
      ).toBe(true),
    );

    setTimeoutMs(5_000);
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
      getQueryEditorPresentation: () => undefined,
    };
    const service = new TableMutationService(manager as never, {
      getColumns: async () => [],
    });
    const controller = new TableMutationPreviewController({
      connectionId: config.id,
      tableName: "items",
      connectionManager: manager as never,
      tableDataService: {
        executePreparedDeletePlan:
          service.executePreparedDeletePlan.bind(service),
        executePreparedInsertPlan: vi.fn(),
      },
      notifyWarning: vi.fn(),
    });
    const runTransaction = vi.spyOn(driver, "runTransaction");
    const operationId = `queued-${kind}-${activeFailure}`;
    const preview =
      kind === "delete"
        ? controller.createDeleteRowsPreview(operationId, deletePlan(config.id))
        : controller.createApplyChangesPreview(operationId, {
            apply: applyPlan(config.id),
            applyResultWhenEmpty: null,
            inserts: [],
          });
    const resultPromise = controller.confirm(preview.previewToken, operationId);
    await vi.waitFor(() => expect(runTransaction).toHaveBeenCalledOnce());

    if (activeFailure === "cancel") {
      await driver.cancelCurrentOperation({
        reason: "manual",
        operationName: "query",
      });
    }

    const [activeError, result] = await Promise.all([active, resultPromise]);
    expect(activeError).toMatchObject({
      code: "OUTCOME_UNKNOWN",
      executionState: "unknown",
    });
    expect((await requests()).map(({ method }) => method)).toEqual([
      "connect",
      "query",
    ]);

    if (kind === "delete") {
      expect(result).toMatchObject({
        type: "deleteResult",
        payload: {
          operationId,
          success: false,
          affectedRows: 0,
          changesPossible: false,
          outcomeUnknown: false,
          rowOutcomes: [
            {
              rowIndex: 0,
              primaryKeys: { id: 1 },
              status: "skipped",
              success: false,
            },
          ],
        },
      });
      expect(result?.payload.error).not.toMatch(
        /outcome.*unknown|may have changed/i,
      );
    } else {
      expect(result).toMatchObject({
        type: "applyResult",
        payload: {
          operationId,
          success: false,
          changesPossible: false,
          outcomeUnknown: false,
          rowOutcomes: [{ rowIndex: 0, status: "skipped", success: false }],
        },
      });
      expect(result?.payload.error).toMatch(
        /not executed; no changes were made/i,
      );
      expect(result?.payload.error).not.toMatch(/outcome may be unknown/i);
    }
  });
});

describe.each([
  "delete",
  "apply",
] as const)("active SQLite %s timeout outcome", (kind) => {
  it("keeps the outcome unknown and requests reconciliation", async () => {
    const { config, driver, requests } = await startDriver(250);
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
      getQueryEditorPresentation: () => undefined,
    };
    const service = new TableMutationService(manager as never, {
      getColumns: async () => [],
    });
    const controller = new TableMutationPreviewController({
      connectionId: config.id,
      tableName: "items",
      connectionManager: manager as never,
      tableDataService: {
        executePreparedDeletePlan:
          service.executePreparedDeletePlan.bind(service),
        executePreparedInsertPlan: vi.fn(),
      },
      notifyWarning: vi.fn(),
    });
    const operationId = `active-${kind}`;
    let preview: ReturnType<
      TableMutationPreviewController["createDeleteRowsPreview"]
    >;
    if (kind === "delete") {
      const plan = deletePlan(config.id);
      plan.operations[0].sql = "HOLD";
      preview = controller.createDeleteRowsPreview(operationId, plan);
    } else {
      const plan = applyPlan(config.id);
      plan.operations[0].sql = "HOLD";
      preview = controller.createApplyChangesPreview(operationId, {
        apply: plan,
        applyResultWhenEmpty: null,
        inserts: [],
      });
    }
    const resultPromise = controller.confirm(preview.previewToken, operationId);
    await vi.waitFor(async () =>
      expect(
        (await requests()).some(({ method }) => method === "runTransaction"),
      ).toBe(true),
    );
    const result = await resultPromise;

    expect((await requests()).map(({ method }) => method)).toEqual([
      "connect",
      "runTransaction",
    ]);
    if (kind === "delete") {
      expect(result).toMatchObject({
        type: "deleteResult",
        payload: {
          changesPossible: true,
          outcomeUnknown: true,
          rowOutcomes: [{ status: "unknown" }],
        },
      });
    } else {
      expect(result).toMatchObject({
        type: "applyResult",
        payload: {
          changesPossible: true,
          outcomeUnknown: true,
          rowOutcomes: [{ status: "skipped", success: false }],
        },
      });
      expect(result?.payload.error).toMatch(/outcome may be unknown/i);
    }
  });
});

describe.each([
  ["apply", "timeout"],
  ["atomic-apply", "timeout"],
  ["delete", "timeout"],
  ["apply", "cancel"],
  ["atomic-apply", "cancel"],
  ["delete", "cancel"],
] as const)("SQLite saved %s preview after worker %s", (kind, activeFailure) => {
  it("does not attribute an earlier request's unknown outcome to an unsent mutation", async () => {
    const { config, driver, requests } = await startDriver(
      activeFailure === "timeout" ? 250 : 5_000,
    );
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
      getQueryEditorPresentation: () => undefined,
    };
    const service = new TableMutationService(manager as never, {
      getColumns: async () => [],
    });
    const controller = new TableMutationPreviewController({
      connectionId: config.id,
      tableName: "items",
      connectionManager: manager as never,
      tableDataService: {
        executePreparedDeletePlan:
          service.executePreparedDeletePlan.bind(service),
        executePreparedInsertPlan: vi.fn(),
      },
      notifyWarning: vi.fn(),
    });
    const operationId = `saved-${kind}-${activeFailure}`;
    const plan = applyPlan(config.id);
    if (kind === "atomic-apply") {
      plan.updates.push({
        primaryKeys: { id: 2 },
        changes: { name: "updated" },
      });
      plan.operations.push({
        sql: "UPDATE items SET name = ? WHERE id = ?",
        params: ["updated", 2],
      });
    }
    const preview =
      kind === "delete"
        ? controller.createDeleteRowsPreview(operationId, deletePlan(config.id))
        : controller.createApplyChangesPreview(operationId, {
            apply: plan,
            applyResultWhenEmpty: null,
            inserts: [],
          });
    const active = driver.query("HOLD").catch((error: unknown) => error);
    await vi.waitFor(async () =>
      expect((await requests()).some(({ command }) => command === "HOLD")).toBe(
        true,
      ),
    );
    if (activeFailure === "cancel") {
      await driver.cancelCurrentOperation({
        reason: "manual",
        operationName: "query",
      });
    }
    expect(await active).toMatchObject({ code: "OUTCOME_UNKNOWN" });
    expect(driver.isConnected()).toBe(false);

    const result = await controller.confirm(preview.previewToken, operationId);
    expect(result).toMatchObject({
      type: kind === "delete" ? "deleteResult" : "applyResult",
      payload: {
        operationId,
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
        rowOutcomes: Array.from(
          { length: kind === "atomic-apply" ? 2 : 1 },
          (_, rowIndex) => ({ rowIndex, status: "skipped", success: false }),
        ),
      },
    });
    expect(result?.payload.error).toMatch(/NOT_EXECUTED/);
    expect(result?.payload.error).toMatch(/connect.*reconnect/i);
    expect(result?.payload.error).not.toMatch(
      /outcome.*unknown|may have changed/i,
    );
    expect((await requests()).map(({ method }) => method)).toEqual([
      "connect",
      "query",
    ]);
  });
});
