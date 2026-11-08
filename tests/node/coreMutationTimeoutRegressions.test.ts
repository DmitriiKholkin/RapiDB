import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import {
  createTimeoutAwareDriver,
  DriverTimeoutError,
} from "../../src/extension/dbDrivers/timeout";
import type {
  ColumnTypeMeta,
  DriverOperationContext,
  DriverTablePageRequest,
  IDBDriver,
  TransactionOperation,
} from "../../src/extension/dbDrivers/types";
import { DriverUpdateRowsError } from "../../src/extension/dbDrivers/updateRowOutcomes";
import type { PreparedDeletePlan } from "../../src/extension/table/tableDataContracts";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import {
  redactDiagnosticText,
  redactDiagnosticValue,
} from "../../src/extension/utils/diagnosticRedaction";
import { createSqlReadOnlyQueryGuard } from "../../src/extension/utils/readOnlyGuards";
import { applyHardCapToSqlQuery } from "../../src/extension/utils/sqlHardCap";
import {
  hasExecutableMysqlComment,
  isMysqlDashCommentStart,
  scanSqlForCap,
} from "../../src/extension/utils/sqlStatementScan";
import { parseTablePanelMessage } from "../../src/shared/webviewContracts";

const column = (
  name: string,
  overrides: Partial<ColumnTypeMeta> = {},
): ColumnTypeMeta => ({
  name,
  type: "integer",
  nativeType: "integer",
  category: "integer",
  nullable: false,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
  ...overrides,
});

const timeoutSettings = (dbOperationTimeoutMs: number) => () => ({
  connectionTimeoutMs: 1000,
  connectionTimeoutSeconds: 1,
  dbOperationTimeoutMs,
  dbOperationTimeoutSeconds: Math.ceil(dbOperationTimeoutMs / 1000),
});

describe("driver-mode UPDATE errors after applied rows", () => {
  it("surfaces a DriverUpdateRowsError even when every row was applied", async () => {
    const columns = [
      column("id", { isPrimaryKey: true }),
      column("value", { type: "text", nativeType: "text", category: "text" }),
    ];
    const driver = {
      updateRows: async () => {
        throw new DriverUpdateRowsError("post-write cache refresh failed", {
          affectedRows: 2,
          updateRowOutcomes: [
            { rowIndex: 0, status: "applied" },
            { rowIndex: 1, status: "applied" },
          ],
        });
      },
      checkPersistedEdit: () => undefined,
      coerceInputValue: (value: unknown) => value,
      buildMutationPreviewStatement: () => "UPDATE items",
      qualifiedTableName: () => "items",
    } as unknown as IDBDriver;
    const manager = {
      getConnection: () => ({ id: "conn", readOnly: false }),
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const prepared = prepareApplyChangesPlan(
      manager,
      "conn",
      "db",
      "",
      "items",
      [1, 2].map((id) => ({ primaryKeys: { id }, changes: { value: "x" } })),
      columns,
    );
    if (!prepared.executable) throw new Error("Expected an executable plan");

    const result = await executePreparedApplyPlan(manager, prepared.plan);

    expect(result).toMatchObject({
      success: false,
      changesPossible: true,
      outcomeUnknown: false,
      failedRows: [],
      rowOutcomes: [
        { rowIndex: 0, success: true, status: "applied" },
        { rowIndex: 1, success: true, status: "applied" },
      ],
    });
    expect(result.error).toContain("post-write cache refresh failed");
  });
});

describe("bulk delete primary-key deduplication", () => {
  function sqlService(runTransaction = vi.fn(async () => undefined)) {
    const driver = {
      qualifiedTableName: () => '"items"',
      quoteIdentifier: (name: string) => `"${name}"`,
      buildInsertValueExpr: (_column: ColumnTypeMeta, index: number) =>
        `$${index}`,
      coerceInputValue: (value: unknown) => value,
      materializePreviewSql: (sql: string) => sql,
      runTransaction,
      query: vi.fn(async () => ({ columns: [], rows: [], rowCount: 0 })),
    } as unknown as IDBDriver;
    const service = new TableMutationService(
      {
        getDriver: () => driver,
        getConnection: () => ({ id: "conn", type: "pg", readOnly: false }),
      } as never,
      { getColumns: async () => [column("id", { isPrimaryKey: true })] },
    );
    return { service, runTransaction };
  }

  it("deletes each duplicated primary key once with an exact expected count", async () => {
    const { service } = sqlService();
    const plan = await service.prepareDeleteRowsPlan(
      "conn",
      "db",
      "public",
      "items",
      [{ id: 1 }, { id: 2 }, { id: 1 }],
    );

    expect(plan?.operations).toHaveLength(1);
    expect(plan?.operations[0]).toMatchObject({
      params: [1, 2],
      expectedAffectedRows: 2,
    });
    expect(plan?.verificationCriteriaList).toEqual([{ id: 1 }, { id: 2 }]);
    expect(plan?.rowIdentities).toEqual([{ id: 1 }, { id: 2 }, { id: 1 }]);
  });

  it("deduplicates per-row DELETE statements for composite keys", async () => {
    const driver = {
      qualifiedTableName: () => "`items`",
      quoteIdentifier: (name: string) => `\`${name}\``,
      buildInsertValueExpr: () => "?",
      coerceInputValue: (value: unknown) => value,
      materializePreviewSql: (sql: string) => sql,
    } as unknown as IDBDriver;
    const service = new TableMutationService(
      {
        getDriver: () => driver,
        getConnection: () => ({ id: "conn", type: "mysql", readOnly: false }),
      } as never,
      {
        getColumns: async () => [
          column("a", { isPrimaryKey: true, primaryKeyOrdinal: 1 }),
          column("b", { isPrimaryKey: true, primaryKeyOrdinal: 2 }),
        ],
      },
    );

    const plan = await service.prepareDeleteRowsPlan(
      "conn",
      "db",
      "",
      "items",
      [
        { a: 1, b: 2 },
        { b: 2, a: 1 },
        { a: 1, b: 3 },
      ],
    );

    expect(
      plan?.operations.map(
        (operation: TransactionOperation) => operation.params,
      ),
    ).toEqual([
      [1, 2],
      [1, 3],
    ]);
    expect(
      plan?.operations.every(
        (operation: TransactionOperation) =>
          operation.expectedAffectedRows === 1,
      ),
    ).toBe(true);
  });

  it("sends unique keys to driver deletes and maps outcomes back to every selected row", async () => {
    const deleteRows = vi.fn(
      async (request: { primaryKeyValuesList: Record<string, unknown>[] }) => ({
        affectedRows: request.primaryKeyValuesList.length,
        rowOutcomes: request.primaryKeyValuesList.map(
          (primaryKeys, rowIndex) => ({
            rowIndex,
            primaryKeys,
            success: true,
            status: "deleted" as const,
          }),
        ),
      }),
    );
    const service = new TableMutationService(
      {
        getDriver: () => ({ deleteRows }),
        getConnection: () => ({ id: "conn", type: "mongodb", readOnly: false }),
      } as never,
      { getColumns: async () => [] },
    );
    const identities = [{ _id: "a" }, { _id: "b" }, { _id: "a" }];
    const plan: PreparedDeletePlan = {
      connectionId: "conn",
      database: "db",
      schema: "",
      table: "items",
      mode: "driver",
      executionMode: "sequential",
      primaryKeyValuesList: identities,
      rowIdentities: identities,
      verificationCriteriaList: identities,
      operations: [],
      previewStatements: [],
    };

    await expect(
      service.executePreparedDeletePlan(plan),
    ).resolves.toBeUndefined();
    expect(deleteRows).toHaveBeenCalledWith(
      expect.objectContaining({
        primaryKeyValuesList: [{ _id: "a" }, { _id: "b" }],
      }),
    );
  });

  it("reports duplicate driver outcomes at their original row indexes", async () => {
    const service = new TableMutationService(
      {
        getDriver: () => ({
          deleteRows: async () => ({
            affectedRows: 1,
            rowOutcomes: [
              {
                rowIndex: 0,
                primaryKeys: { _id: "a" },
                success: true,
                status: "deleted",
              },
              {
                rowIndex: 1,
                primaryKeys: { _id: "b" },
                success: false,
                status: "notfound",
              },
            ],
          }),
        }),
        getConnection: () => ({ id: "conn", type: "mongodb", readOnly: false }),
      } as never,
      { getColumns: async () => [] },
    );
    const identities = [{ _id: "a" }, { _id: "b" }, { _id: "a" }];
    const error = await service
      .executePreparedDeletePlan({
        connectionId: "conn",
        database: "db",
        schema: "",
        table: "items",
        mode: "driver",
        executionMode: "sequential",
        primaryKeyValuesList: identities,
        rowIdentities: identities,
        verificationCriteriaList: identities,
        operations: [],
        previewStatements: [],
      })
      .catch((caught: unknown) => caught);

    expect(error).toMatchObject({
      deleteResult: {
        rowOutcomes: [
          { rowIndex: 0, status: "deleted", primaryKeys: { _id: "a" } },
          { rowIndex: 1, status: "notfound", primaryKeys: { _id: "b" } },
          { rowIndex: 2, status: "deleted", primaryKeys: { _id: "a" } },
        ],
      },
    });
  });
});

describe("timeout wrapper honors earlier supplied deadlines", () => {
  it("times out a mutation at a supplied deadline earlier than the configured timeout", async () => {
    vi.useFakeTimers();
    try {
      let context: DriverOperationContext | undefined;
      const driver = createTimeoutAwareDriver(
        {
          runTransaction: (
            _operations: TransactionOperation[],
            supplied?: DriverOperationContext,
          ) => {
            context = supplied;
            return new Promise<void>(() => undefined);
          },
        },
        timeoutSettings(60_000),
      );
      const deadline = Date.now() + 50;
      const pending = driver
        .runTransaction([], {
          signal: new AbortController().signal,
          deadline,
        })
        .catch((error: unknown) => error);

      await vi.advanceTimersByTimeAsync(60);
      const error = await pending;

      expect(error).toBeInstanceOf(DriverTimeoutError);
      expect((error as Error).message).toContain(
        "Mutation outcome may be unknown",
      );
      expect(context?.deadline).toBe(deadline);
      expect(context?.signal.aborted).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps a mutation success that settles after the deadline but before the timer", async () => {
    vi.useFakeTimers();
    try {
      let finish!: () => void;
      const driver = createTimeoutAwareDriver(
        {
          runTransaction: (
            _operations: TransactionOperation[],
            _context?: DriverOperationContext,
          ) =>
            new Promise<void>((resolve) => {
              finish = resolve;
            }),
        },
        timeoutSettings(60_000),
      );
      const deadline = Date.now() + 50;
      const pending = driver
        .runTransaction([], { signal: new AbortController().signal, deadline })
        .then(
          () => "committed",
          (error: unknown) => error,
        );

      vi.setSystemTime(deadline + 10);
      finish();

      expect(await pending).toBe("committed");
    } finally {
      vi.useRealTimers();
    }
  });

  it("still times out a read that settles after its deadline before the timer", async () => {
    vi.useFakeTimers();
    try {
      let finish!: (value: unknown) => void;
      const driver = createTimeoutAwareDriver(
        {
          readTablePage: (_request: DriverTablePageRequest) =>
            new Promise((resolve) => {
              finish = resolve;
            }),
        },
        timeoutSettings(60_000),
      );
      const deadline = Date.now() + 50;
      const pending = driver
        .readTablePage({
          database: "db",
          schema: "public",
          table: "items",
          page: 1,
          pageSize: 10,
          skipCount: true,
          filters: [],
          sort: null,
          deadline,
        } as DriverTablePageRequest)
        .catch((error: unknown) => error);

      vi.setSystemTime(deadline + 10);
      finish({ columns: [], rows: [], totalCount: 0, executionTimeMs: 0 });

      expect(await pending).toBeInstanceOf(DriverTimeoutError);
    } finally {
      vi.useRealTimers();
    }
  });

  it("times out a table page read at an earlier request deadline", async () => {
    vi.useFakeTimers();
    try {
      const driver = createTimeoutAwareDriver(
        {
          readTablePage: (_request: DriverTablePageRequest) =>
            new Promise(() => undefined),
        },
        timeoutSettings(60_000),
      );
      const pending = driver
        .readTablePage({
          database: "db",
          schema: "public",
          table: "items",
          page: 1,
          pageSize: 10,
          skipCount: true,
          filters: [],
          sort: null,
          deadline: Date.now() + 50,
        } as DriverTablePageRequest)
        .catch((error: unknown) => error);

      await vi.advanceTimersByTimeAsync(60);

      expect(await pending).toBeInstanceOf(DriverTimeoutError);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("SQL lexer alignment", () => {
  it("does not treat Unicode spaces as token boundaries", () => {
    const words = scanSqlForCap("SELECT 1\u00a0LIMIT 5", "pg")
      .filter((token) => token.kind === "word")
      .map((token) => token.text);
    expect(words).toEqual(["SELECT", "\u00a0LIMIT"]);
    const rewrite = applyHardCapToSqlQuery("SELECT 1\u00a0LIMIT 5", "pg", 100);
    expect(rewrite.queryText).not.toBe("SELECT 1\u00a0LIMIT 5");
    expect(rewrite.queryText).toMatch(/LIMIT 100$/);
  });

  it("follows the MySQL '-- ' comment rule for control characters only", () => {
    expect(isMysqlDashCommentStart("--", 0)).toBe(true);
    expect(isMysqlDashCommentStart("-- x", 0)).toBe(true);
    expect(isMysqlDashCommentStart("--\tx", 0)).toBe(true);
    expect(isMysqlDashCommentStart("--\u0001x", 0)).toBe(true);
    expect(isMysqlDashCommentStart("--\u007fx", 0)).toBe(true);
    expect(isMysqlDashCommentStart("--\u00a0x", 0)).toBe(false);
    expect(isMysqlDashCommentStart("--x", 0)).toBe(false);
  });

  it("does not hide executable MySQL comments behind a NBSP dash sequence", () => {
    expect(
      hasExecutableMysqlComment("SELECT 1 --\u00a0/*!50000 DROP TABLE t */"),
    ).toBe(true);
    expect(
      hasExecutableMysqlComment("SELECT 1 -- /*!50000 DROP TABLE t */"),
    ).toBe(false);
  });

  it("treats a MySQL '--' followed by a control character as a comment in hard-cap scanning", () => {
    const rewrite = applyHardCapToSqlQuery(
      "SELECT 1 --\u0001 LIMIT 5",
      "mysql",
      100,
    );
    expect(rewrite.queryText).toContain("LIMIT 100");
  });

  it("applies the shared MySQL comment rule in read-only classification", () => {
    const guard = createSqlReadOnlyQueryGuard("mysql");
    expect(guard("SELECT 1 --\u007f; DELETE FROM t").allowed).toBe(true);
    expect(guard("SELECT 1 --\u00a0x; DELETE FROM t").allowed).toBe(false);
  });
});

describe("table panel sort contract", () => {
  it("accepts only an exact sort shape", () => {
    expect(
      parseTablePanelMessage({
        type: "fetchPage",
        payload: { sort: { column: "id", direction: "desc", extra: 1 } },
      }),
    ).toMatchObject({ payload: { sort: { column: "id", direction: "desc" } } });
    expect(
      (
        parseTablePanelMessage({
          type: "fetchPage",
          payload: { sort: { column: "id", direction: "desc", extra: 1 } },
        }) as { payload: { sort: object } }
      ).payload.sort,
    ).toEqual({ column: "id", direction: "desc" });
    expect(
      parseTablePanelMessage({ type: "fetchPage", payload: { sort: null } }),
    ).toMatchObject({ payload: { sort: null } });
    for (const sort of [
      { column: "id", direction: "DESC" },
      { column: 1, direction: "asc" },
      { column: "id" },
      "id",
    ]) {
      expect(
        parseTablePanelMessage({ type: "fetchPage", payload: { sort } }),
      ).toBeNull();
      expect(
        parseTablePanelMessage({ type: "exportCSV", payload: { sort } }),
      ).toBeNull();
      expect(
        parseTablePanelMessage({ type: "exportJSON", payload: { sort } }),
      ).toBeNull();
    }
  });
});

describe("diagnostic redaction details", () => {
  it("keeps the original error prototype and a redacted original stack", () => {
    class DriverFailure extends Error {
      code = "E_DRIVER";
    }
    const original = new DriverFailure("login failed for s3cr3t-pass");
    original.stack =
      "DriverFailure: login failed for s3cr3t-pass\n    at connect (driver.js:1:1)";

    const safe = redactDiagnosticValue(original, {
      password: "s3cr3t-pass",
    }) as DriverFailure;

    expect(safe).not.toBe(original);
    expect(safe).toBeInstanceOf(DriverFailure);
    expect(safe.message).toBe("login failed for [REDACTED]");
    expect(safe.stack).toBe(
      "DriverFailure: login failed for [REDACTED]\n    at connect (driver.js:1:1)",
    );
    for (const key of ["name", "message"]) {
      expect(Object.getOwnPropertyDescriptor(safe, key)?.enumerable).toBe(
        false,
      );
    }
    expect(Object.keys(safe)).toEqual(["code"]);
    expect(Object.getOwnPropertyDescriptor(safe, "stack")).toMatchObject({
      writable: true,
      enumerable: false,
    });
    expect(safe.code).toBe("E_DRIVER");
    expect(original.stack).toContain("s3cr3t-pass");
  });

  it("redacts the V8 stack accessor when it is not a data property", () => {
    const original = new Error("token abc-secret-123 rejected");
    Object.defineProperty(original, "stack", {
      get: () => "Error: token abc-secret-123 rejected\n    at run (x.js:1:1)",
      configurable: true,
    });
    const safe = redactDiagnosticValue(original, {
      token: "abc-secret-123",
    }) as Error;
    expect(safe.stack).toBe(
      "Error: token [REDACTED] rejected\n    at run (x.js:1:1)",
    );
  });

  it.each([
    "mysql://root:1234/abc@db:3306/app",
    "mysql://root:1234?abc@db:3306/app",
    "mysql://root:1234#abc@db:3306/app",
  ])("redacts numeric-prefix passwords followed by a delimiter: %s", (uri) => {
    const password = uri.slice("mysql://root:".length, uri.lastIndexOf("@"));
    expect(redactDiagnosticText(`connect ${uri} failed`)).toBe(
      "connect mysql://[REDACTED]@db:3306/app failed",
    );
    expect(
      redactDiagnosticText(`auth failed with ${password}`, {
        connectionUri: uri,
      }),
    ).toBe("auth failed with [REDACTED]");
  });

  it.each([
    ["sslkey", "/home/me/client.key"],
    ["sslpassword", "ssl-pass-1"],
    ["passphrase", "phrase-1"],
    ["x-api-key", "xkey-1"],
    ["xApiKey", "xkey-2"],
    ["refreshToken", "refresh-1"],
    ["refresh_token", "refresh-2"],
    ["cookie", "session=abc"],
  ])("treats %s as a secret key", (key, value) => {
    expect(
      redactDiagnosticText(`failure near ${value}`, { [key]: value }),
    ).toBe("failure near [REDACTED]");
    expect(redactDiagnosticValue({ [key]: value })).toEqual({
      [key]: "[REDACTED]",
    });
  });
});
