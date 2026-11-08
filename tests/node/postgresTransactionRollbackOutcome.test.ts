import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { DeleteExecutionError } from "../../src/extension/dbDrivers/deleteOutcomes";
import { isMutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";
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
    name: "label",
    type: "text",
    nativeType: "text",
    category: "text",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  },
];

function setup(affected: number[], rollbackFails = false) {
  const config = {
    id: "pg-conflict",
    name: "PG",
    type: "pg" as const,
    database: "app",
  };
  const query = vi.fn(async (sql: string) => {
    if (sql === "ROLLBACK" && rollbackFails)
      throw new Error("rollback connection lost");
    if (sql.startsWith("UPDATE") || sql.startsWith("DELETE"))
      return { rows: [], rowCount: affected.shift() ?? 0 };
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
  return { config, driver, manager, query, release };
}

describe("PostgreSQL rolled-back conflict classification", () => {
  it("marks an affected-row conflict as not executed after a confirmed rollback", async () => {
    const { driver, query, release } = setup([0]);
    const error = await driver
      .runTransaction([
        { sql: "UPDATE t SET a = 1", params: [], checkAffectedRows: true },
      ])
      .catch((caught: unknown) => caught);
    expect(isMutationNotExecutedError(error)).toBe(true);
    expect(query.mock.calls.map(([sql]) => sql)).toEqual([
      "BEGIN",
      "UPDATE t SET a = 1",
      "ROLLBACK",
    ]);
    expect(release).toHaveBeenCalledWith();
  });

  it("keeps the outcome unknown when ROLLBACK fails", async () => {
    const { driver, release } = setup([0], true);
    const error = await driver
      .runTransaction([
        { sql: "UPDATE t SET a = 1", params: [], checkAffectedRows: true },
      ])
      .catch((caught: unknown) => caught);
    expect(isMutationNotExecutedError(error)).toBe(false);
    expect(release).toHaveBeenCalledWith(true);
  });

  it("reports an UPDATE matching no rows as rolled back without changes", async () => {
    const { config, manager } = setup([0]);
    const result = await applyChangesTransactional(
      manager,
      config.id,
      "app",
      "public",
      "items",
      [
        {
          primaryKeys: { id: 1 },
          changes: { label: "new" },
          originalValues: { label: "old" },
        },
      ],
      columns,
    );
    expect(result).toMatchObject({
      success: false,
      changesPossible: false,
      outcomeUnknown: false,
    });
    expect(result.error).not.toContain("outcome may be unknown");
  });

  it("reports a multi-row DELETE with a missing row as rolled back without changes", async () => {
    const { config, manager } = setup([1, 0]);
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    const error = await service
      .deleteRows(config.id, "app", "public", "items", [{ id: 1 }, { id: 2 }])
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(DeleteExecutionError);
    expect((error as DeleteExecutionError).deleteResult).toMatchObject({
      success: false,
      affectedRows: 0,
      changesPossible: false,
      outcomeUnknown: false,
    });
  });
});
