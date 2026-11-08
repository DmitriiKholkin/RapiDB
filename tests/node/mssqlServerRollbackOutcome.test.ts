import { EventEmitter } from "node:events";
import { createRequire } from "node:module";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

interface NativeRequest {
  sqlTextOrProcedure: string;
  error: Error;
  callback(error: Error): void;
}

const { RequestTokenHandler } = createRequire(import.meta.url)(
  "tedious/lib/token/handler",
) as {
  RequestTokenHandler: new (
    connection: EventEmitter,
    request: NativeRequest,
  ) => {
    onRollbackTransaction(token: object): void;
    onErrorMessage(token: {
      message: string;
      number: number;
      state: number;
      class: number;
      lineNumber: number;
      procName: string;
      serverName: string;
    }): void;
    onDone(token: { more: boolean; sqlError: boolean }): void;
  };
};

const columns: ColumnTypeMeta[] = ["id", "v"].map((name) => ({
  name,
  type: "int",
  nativeType: "int",
  category: "integer",
  nullable: false,
  isPrimaryKey: name === "id",
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq"],
  valueSemantics: "plain",
}));

function harness() {
  const config = { id: "server-rollback", name: "QA", type: "mssql" as const };
  const driver = new MSSQLDriver(config);
  let durableTriggerWrites = 0;
  const connection = Object.assign(new EventEmitter(), {
    config: { options: {} },
    transactionDescriptors: [Buffer.alloc(8)],
    inTransaction: false,
    beginTransaction(callback: () => void) {
      this.inTransaction = true;
      callback();
    },
    rollbackTransaction: vi.fn((callback: () => void) => callback()),
    close: vi.fn(),
    execSql: vi.fn((request: NativeRequest) => {
      const handler = new RequestTokenHandler(connection, request);
      handler.onRollbackTransaction({});
      durableTriggerWrites++;
      handler.onErrorMessage({
        message:
          "The transaction ended in the trigger. The batch has been aborted.",
        number: 3609,
        state: 1,
        class: 16,
        lineNumber: 1,
        procName: "",
        serverName: "test-server",
      });
      handler.onDone({ more: false, sqlError: true });
      request.callback(request.error);
    }),
  });
  const pool = {
    config: connection.config,
    connected: true,
    acquire(
      _requester: unknown,
      callback: (
        error: null,
        physical: typeof connection,
        config: typeof connection.config,
      ) => void,
    ) {
      callback(null, connection, connection.config);
    },
    release: vi.fn(),
  };
  Object.assign(driver, { pool });
  const manager = {
    getConnection: () => config,
    getDriver: () => driver,
    getQueryEditorPresentation: () => undefined,
  } as unknown as ConnectionManager;
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  const controller = new TableMutationPreviewController({
    connectionId: config.id,
    tableName: "items",
    connectionManager: manager,
    tableDataService: service,
    notifyWarning: vi.fn(),
  });
  return {
    config,
    manager,
    service,
    controller,
    connection,
    pool,
    durableTriggerWrites: () => durableTriggerWrites,
  };
}

describe("MSSQL server rollback outcome", () => {
  it.each([
    "update",
    "insert",
    "apply-insert",
  ] as const)("keeps %s outcome unknown after a trigger rolls back and continues writing", async (mode) => {
    const h = harness();
    let result: unknown;
    if (mode === "update") {
      result = await applyChangesTransactional(
        h.manager,
        h.config.id,
        "qa",
        "dbo",
        "items",
        [
          {
            primaryKeys: { id: 1 },
            changes: { v: 9 },
            originalValues: { v: 0 },
          },
        ],
        columns,
      );
    } else {
      const plan = await h.service.prepareInsertRow(
        h.config.id,
        "qa",
        "dbo",
        "items",
        { id: "1", v: "9" },
      );
      const preview =
        mode === "insert"
          ? h.controller.createInsertPreview("insert-probe", plan)
          : h.controller.createApplyChangesPreview("insert-probe", {
              apply: null,
              applyResultWhenEmpty: null,
              inserts: [plan],
            });
      result = (await h.controller.confirm(preview.previewToken))?.payload;
    }
    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(result).toMatchObject({
      success: false,
      changesPossible: true,
      outcomeUnknown: true,
      error: expect.stringContaining("The transaction ended in the trigger"),
    });
    expect(result).not.toHaveProperty("insertApplied", true);
    expect((result as { error: string }).error).not.toContain(
      "no changes were made",
    );
    expect((result as { error: string }).error).not.toContain(
      "session was discarded",
    );
    expect(h.durableTriggerWrites()).toBe(1);
    expect(h.connection.execSql).toHaveBeenCalledOnce();
    expect(h.connection.inTransaction).toBe(false);
    expect(h.connection.rollbackTransaction).not.toHaveBeenCalled();
    expect(h.connection.close).not.toHaveBeenCalled();
    expect(h.pool.release).toHaveBeenCalledExactlyOnceWith(h.connection);
  });
});
