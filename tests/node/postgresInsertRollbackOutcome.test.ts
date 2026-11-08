import { DatabaseError } from "pg";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "integer",
    nativeType: "integer",
    category: "integer",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  },
  {
    name: "amount",
    type: "numeric(10,2)",
    nativeType: "numeric(10,2)",
    category: "decimal",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  },
];

async function confirm(
  mode: "insert" | "apply" | "batch",
  rollbackFails = false,
  sqlState = "23514",
) {
  const config = {
    id: "pg-rollback",
    name: "PG",
    type: "pg" as const,
    database: "app",
  };
  const rejection = new DatabaseError("insert rejected by server", 0, "error");
  rejection.code = sqlState;
  const query = vi.fn(async (sql: string) => {
    if (sql.startsWith("INSERT")) throw rejection;
    if (sql === "ROLLBACK" && rollbackFails)
      throw new Error("rollback connection lost");
    return { rows: [], rowCount: 0 };
  });
  const release = vi.fn();
  const driver = new PostgresDriver(config);
  Object.assign(driver, {
    pool: {
      connect: async () => ({ query, release }),
      totalCount: 0,
      idleCount: 1,
      waitingCount: 0,
    },
  });
  const manager = {
    getConnection: () => config,
    getDriver: () => driver,
    getQueryEditorPresentation: () => undefined,
  } as unknown as ConnectionManager;
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  const plan = await service.prepareInsertRow(
    config.id,
    "app",
    "public",
    "items",
    { id: 1, amount: "1.00" },
  );
  expect(plan.verification).toBeDefined();
  const controller = new TableMutationPreviewController({
    connectionId: config.id,
    tableName: "items",
    connectionManager: manager,
    tableDataService: service,
    notifyWarning: vi.fn(),
  });
  const preview =
    mode === "insert"
      ? controller.createInsertPreview("pg-insert", plan)
      : controller.createApplyChangesPreview("pg-insert", {
          apply: null,
          applyResultWhenEmpty: null,
          inserts: mode === "batch" ? [plan, plan] : [plan],
        });
  const result = await controller.confirm(preview.previewToken, "pg-insert");
  expect(
    query.mock.calls.map(([sql]) =>
      sql === "BEGIN" || sql === "ROLLBACK" ? sql : "INSERT",
    ),
  ).toEqual(["BEGIN", "INSERT", "ROLLBACK"]);
  expect(release).toHaveBeenCalledTimes(1);
  return { result, release };
}

describe("PostgreSQL verified insert rollback classification", () => {
  it.each([
    "insert",
    "apply",
    "batch",
  ] as const)("reports no changes after server rejection and confirmed rollback (%s)", async (mode) => {
    const { result, release } = await confirm(mode);
    expect(result).toMatchObject({
      payload: {
        success: false,
        changesPossible: false,
        outcomeUnknown: false,
        error: expect.stringContaining("insert rejected by server"),
      },
    });
    expect(result?.payload.error).not.toContain("outcome is unknown");
    expect(release).toHaveBeenCalledWith();
  });

  it.each([
    "insert",
    "apply",
  ] as const)("keeps a failed rollback unknown (%s)", async (mode) => {
    const { result, release } = await confirm(mode, true);
    expect(result).toMatchObject({
      payload: { success: false, changesPossible: true, outcomeUnknown: true },
    });
    expect(release).toHaveBeenCalledWith(true);
  });

  it("keeps a connection failure unknown even if ROLLBACK resolves", async () => {
    const { result } = await confirm("insert", false, "08006");
    expect(result).toMatchObject({
      payload: { success: false, changesPossible: true, outcomeUnknown: true },
    });
  });
});
