import { ObjectId } from "mongodb";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  DeleteExecutionError,
  getDeleteEvidence,
} from "../../src/extension/dbDrivers/deleteOutcomes";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import type {
  DriverDeleteRowsRequest,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import type { PreparedDeletePlan } from "../../src/extension/table/tableDataContracts";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const engines = ["redis", "dynamodb", "elasticsearch", "mongodb"] as const;
type Engine = (typeof engines)[number];
const config = { id: "delete", name: "delete" };
const timeoutSettings = () => ({
  connectionTimeoutMs: 1000,
  connectionTimeoutSeconds: 1,
  dbOperationTimeoutMs: 25,
  dbOperationTimeoutSeconds: 1,
});

function harness(engine: Engine, write: (index: number) => Promise<boolean>) {
  let index = 0;
  const call = vi.fn(async (..._args: unknown[]) => {
    const exists = await write(index++);
    switch (engine) {
      case "redis":
        return exists ? 1 : 0;
      case "dynamodb":
        if (!exists)
          throw Object.assign(new Error("missing"), {
            name: "ConditionalCheckFailedException",
          });
        return { Attributes: { id: { S: "old" } } };
      case "elasticsearch":
        return { result: exists ? "deleted" : "not_found" };
      case "mongodb":
        return { acknowledged: true, deletedCount: exists ? 1 : 0 };
    }
  });
  const driver =
    engine === "redis"
      ? new RedisDriver({ ...config, type: engine })
      : engine === "dynamodb"
        ? new DynamoDBDriver({ ...config, type: engine })
        : engine === "elasticsearch"
          ? new ElasticsearchDriver({ ...config, type: engine })
          : new MongoDBDriver({ ...config, type: engine });
  const client = {
    del: call,
    send: call,
    delete: call,
    deleteOne: call,
    withAbortSignal: vi.fn(() => client),
  };
  Object.assign(driver, {
    getDatabaseClient: async () => client,
    requireClient: () => client,
    getTableSchema: async () => ({ keys: ["id"] }),
    requireDb: () => ({ collection: () => client }),
  });
  const identities = ["a", "b", "c"].map((id) =>
    engine === "redis"
      ? { key: id }
      : engine === "dynamodb"
        ? { id }
        : { _id: id },
  );
  const request: DriverDeleteRowsRequest = {
    database: "db0",
    schema: "",
    table: "items",
    primaryKeyValuesList: identities,
  };
  return { driver, call, request, identities };
}

afterEach(() => vi.useRealTimers());

describe.each(engines)("B12 %s delete evidence", (engine) => {
  it("reports an already cancelled batch as skipped without sending writes", async () => {
    const { driver, request, call } = harness(engine, async () => true);
    const abort = new AbortController();
    abort.abort(new Error("cancel before delete"));
    const error = await driver
      .deleteRows(request, { signal: abort.signal, deadline: Infinity })
      .catch((error: unknown) => error);
    expect(getDeleteEvidence(error)?.affectedRows).toBe(0);
    expect(
      getDeleteEvidence(error)?.rowOutcomes?.map((row) => row.status),
    ).toEqual(["skipped", "skipped", "skipped"]);
    expect(call).not.toHaveBeenCalled();
  });

  it("identifies the missing row while retaining completed deletes", async () => {
    const { driver, request } = harness(engine, async (index) => index !== 1);
    const result = await driver.deleteRows(request);
    expect(result.affectedRows).toBe(2);
    expect(result.rowOutcomes?.map((row) => row.status)).toEqual([
      "deleted",
      "notfound",
      "deleted",
    ]);
    expect(result.rowOutcomes?.map((row) => row.primaryKeys)).toEqual(
      request.primaryKeyValuesList,
    );
  });

  it("retains the completed prefix on a network error and skips remaining requests", async () => {
    const { driver, request, call } = harness(engine, async (index) => {
      if (index === 1) throw new Error("connection lost after send");
      return true;
    });
    const error = await driver
      .deleteRows(request)
      .catch((error: unknown) => error);
    const result = getDeleteEvidence(error);
    expect(result?.affectedRows).toBe(1);
    expect(result?.rowOutcomes?.map((row) => row.status)).toEqual([
      "deleted",
      "unknown",
      "skipped",
    ]);
    expect(result?.rowOutcomes?.[1].message).toContain("connection lost");
    expect(call).toHaveBeenCalledTimes(2);
  });

  it("snapshots the completed prefix at timeout and fences writes after late settlement", async () => {
    vi.useFakeTimers();
    let finish!: () => void;
    const gate = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const {
      driver: raw,
      request,
      call,
    } = harness(engine, async (index) => {
      if (index === 1) await gate;
      return true;
    });
    const driver = createTimeoutAwareDriver(raw, timeoutSettings);
    const pending = driver.deleteRows(request).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(25);
    const error = await pending;
    expect(error).toMatchObject({ name: "DriverTimeoutError" });
    expect(getDeleteEvidence(error)?.affectedRows).toBe(1);
    expect(
      getDeleteEvidence(error)?.rowOutcomes?.map((row) => row.status),
    ).toEqual(["deleted", "unknown", "skipped"]);
    finish();
    await vi.advanceTimersByTimeAsync(0);
    expect(call).toHaveBeenCalledTimes(2);
    expect(getDeleteEvidence(error)?.affectedRows).toBe(1);
    expect(getDeleteEvidence(error)?.rowOutcomes?.[1].status).toBe("unknown");
  });

  it("honors caller cancellation between rows and reports unattempted rows as skipped", async () => {
    const abort = new AbortController();
    const { driver, request, call } = harness(engine, async () => {
      abort.abort(new Error("cancel delete"));
      return true;
    });
    const error = await driver
      .deleteRows(request, { signal: abort.signal, deadline: Infinity })
      .catch((error: unknown) => error);
    expect(getDeleteEvidence(error)?.affectedRows).toBe(1);
    expect(
      getDeleteEvidence(error)?.rowOutcomes?.map((row) => row.status),
    ).toEqual(["deleted", "skipped", "skipped"]);
    expect(call).toHaveBeenCalledOnce();
  });
});

function service(driver: Pick<IDBDriver, "deleteRows" | "runTransaction">) {
  return new TableMutationService(
    {
      getDriver: () => driver,
      getConnection: () => ({ ...config, type: "mongodb" }),
    } as never,
    { getColumns: async () => [] },
  );
}
function plan(
  identities: Record<string, unknown>[],
  mode: "driver" | "sql" = "driver",
): PreparedDeletePlan {
  return {
    connectionId: config.id,
    database: "db",
    schema: "",
    table: "items",
    mode,
    executionMode: mode === "driver" ? "sequential" : "transaction",
    primaryKeyValuesList: identities,
    rowIdentities: identities,
    verificationCriteriaList: identities,
    operations: [
      {
        sql: "DELETE FROM items WHERE id IN (?, ?, ?)",
        expectedAffectedRows: identities.length,
      },
    ],
    previewStatements: ["delete preview"],
  };
}

describe("B12 service and preview results", () => {
  it.each([
    "redis",
    "dynamodb",
    "elasticsearch",
  ] as const)("reports a local %s identity validation failure after a completed delete", async (engine) => {
    const { driver, request, call } = harness(engine, async () => true);
    request.primaryKeyValuesList[1] = {};
    const error = await driver
      .deleteRows(request)
      .catch((error: unknown) => error);
    expect(getDeleteEvidence(error)?.affectedRows).toBe(1);
    expect(
      getDeleteEvidence(error)?.rowOutcomes?.map((row) => row.status),
    ).toEqual(["deleted", "failed", "skipped"]);
    expect(call).toHaveBeenCalledOnce();
  });

  it("does not infer which rows were deleted from a legacy aggregate partial count", async () => {
    const prepared = plan([{ id: 1 }, { id: 2 }, { id: 3 }]);
    const error = await service({
      deleteRows: async () => ({ affectedRows: 1 }),
      runTransaction: vi.fn(),
    })
      .executePreparedDeletePlan(prepared)
      .catch((error: unknown) => error);
    expect(error).toMatchObject({
      deleteResult: {
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [
          { status: "unknown" },
          { status: "unknown" },
          { status: "unknown" },
        ],
      },
    });
  });

  it("keeps the confirmed delete count through the service after a wrapper timeout", async () => {
    vi.useFakeTimers();
    let finish!: () => void;
    const gate = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const {
      driver: raw,
      identities,
      call,
    } = harness("mongodb", async (index) => {
      if (index === 1) await gate;
      return true;
    });
    const wrapped = createTimeoutAwareDriver(raw, timeoutSettings);
    const pending = service(wrapped)
      .executePreparedDeletePlan(plan(identities))
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(25);
    const error = await pending;
    expect(error).toMatchObject({
      deleteResult: {
        affectedRows: 1,
        outcomeUnknown: true,
        changesPossible: true,
        rowOutcomes: [
          { status: "deleted" },
          { status: "unknown" },
          { status: "skipped" },
        ],
      },
    });
    expect(error).toHaveProperty(
      "message",
      expect.stringContaining("partially changed"),
    );
    finish();
    await vi.advanceTimersByTimeAsync(0);
    expect(call).toHaveBeenCalledTimes(2);
  });

  it("reports unacknowledged Mongo deletes as unknown rather than confirmed", async () => {
    const { driver, request, call } = harness("mongodb", async () => true);
    call.mockResolvedValue({ acknowledged: false, deletedCount: 1 });
    const error = await driver
      .deleteRows(request)
      .catch((error: unknown) => error);
    expect(getDeleteEvidence(error)?.affectedRows).toBe(0);
    expect(
      getDeleteEvidence(error)?.rowOutcomes?.map((row) => row.status),
    ).toEqual(["unknown", "skipped", "skipped"]);
    expect(call).toHaveBeenCalledOnce();
  });

  it("reports actual Mongo BSON matching without guessing string identities", async () => {
    const { driver, request, call } = harness(
      "mongodb",
      async (index) => index === 0,
    );
    const objectId = new ObjectId("507f1f77bcf86cd799439011");
    request.primaryKeyValuesList = [
      { _id: objectId },
      { _id: objectId.toHexString() },
    ];
    await driver.deleteRows(request);
    expect(call.mock.calls[0][0]).toEqual({ _id: { $eq: objectId } });
    expect(call.mock.calls[1][0]).toEqual({
      _id: { $eq: objectId.toHexString() },
    });
  });

  it("preserves original tagged row identities, count, and partial result through confirmation", async () => {
    const { driver, request } = harness(
      "mongodb",
      async (index) => index === 0,
    );
    const prepared = plan(request.primaryKeyValuesList);
    prepared.rowIdentities = request.primaryKeyValuesList.map((row) => ({
      _id: { $rapidbMongoId: { type: "string", value: row._id } },
    }));
    const svc = service(driver);
    const controller = new TableMutationPreviewController({
      connectionId: config.id,
      tableName: "items",
      connectionManager: {
        getQueryEditorPresentation: () => undefined,
      } as never,
      tableDataService: {
        executePreparedDeletePlan: svc.executePreparedDeletePlan.bind(svc),
        executePreparedInsertPlan: vi.fn(),
      },
      notifyWarning: vi.fn(),
    });
    const preview = controller.createDeleteRowsPreview("delete-1", prepared);
    const result = await controller.confirm(preview.previewToken, "delete-1");
    expect(result).toMatchObject({
      type: "deleteResult",
      payload: {
        operationId: "delete-1",
        success: false,
        affectedRows: 1,
        changesPossible: true,
        outcomeUnknown: false,
        rowOutcomes: [
          { status: "deleted", primaryKeys: prepared.rowIdentities[0] },
          { status: "notfound" },
          { status: "notfound" },
        ],
      },
    });
    expect(result?.payload.error).toContain("partially changed");
    expect(
      await controller.confirm(preview.previewToken, "delete-1"),
    ).toBeNull();
  });

  it("does not execute a second preview for an already executed operation ID", async () => {
    const execute = vi.fn(async () => undefined);
    const controller = new TableMutationPreviewController({
      connectionId: config.id,
      tableName: "items",
      connectionManager: {
        getQueryEditorPresentation: () => undefined,
      } as never,
      tableDataService: {
        executePreparedDeletePlan: execute,
        executePreparedInsertPlan: vi.fn(),
      },
      notifyWarning: vi.fn(),
    });
    const first = controller.createDeleteRowsPreview(
      "same-id",
      plan([{ id: 1 }]),
    );
    const duplicate = controller.createDeleteRowsPreview(
      "same-id",
      plan([{ id: 1 }]),
    );
    expect(await controller.confirm(first.previewToken, "wrong-id")).toBeNull();
    await controller.confirm(first.previewToken, "same-id");
    expect(
      await controller.confirm(duplicate.previewToken, "same-id"),
    ).toBeNull();
    expect(execute).toHaveBeenCalledOnce();
    const retry = controller.createDeleteRowsPreview(
      "new-id",
      plan([{ id: 1 }]),
    );
    expect(
      await controller.confirm(retry.previewToken, "new-id"),
    ).toMatchObject({ payload: { operationId: "new-id" } });
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("preserves SQL expected-count guards and treats a transaction error as unknown", async () => {
    const runTransaction = vi.fn(async () => {
      throw new Error("commit response lost");
    });
    const prepared = plan([{ id: 1 }, { id: 2 }], "sql");
    const error = await service({ runTransaction })
      .executePreparedDeletePlan(prepared)
      .catch((error: unknown) => error);
    expect(error).toBeInstanceOf(DeleteExecutionError);
    expect(error).toMatchObject({
      deleteResult: {
        success: false,
        affectedRows: 0,
        changesPossible: true,
        outcomeUnknown: true,
        rowOutcomes: [{ status: "unknown" }, { status: "unknown" }],
      },
    });
    expect(runTransaction).toHaveBeenCalledWith(
      prepared.operations,
      undefined,
      { database: "db" },
    );
    expect(prepared.operations[0].expectedAffectedRows).toBe(2);
  });
});
