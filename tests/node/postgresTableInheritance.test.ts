import { describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type {
  FilterExpression,
  QueryResult,
} from "../../src/extension/dbDrivers/types";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import {
  executePreparedApplyPlan,
  TableDataService,
} from "../../src/extension/tableDataService";

const config = { id: "pg", name: "PG", type: "pg" as const };
const target = ["data", "app", 'some"items'] as const;
const scope = [config.id, ...target] as const;
const qualified = '"app"."some""items"';
const columnRows = [
  { column_name: "id", data_type: "integer", is_pk: true, pk_ordinal: 1 },
  { column_name: "amount", data_type: "numeric(6,2)", is_nullable: true },
];

function setup(kind: string, partition: boolean | null) {
  const driver = new PostgresDriver(config);
  const catalog = vi.fn(
    async (_sql: string, _params: unknown[]): Promise<{ rows: unknown[] }> => ({
      rows: columnRows.map((column) => ({
        ...column,
        relation_kind: kind,
        is_partition: partition,
      })),
    }),
  );
  const requirePool = vi.fn((_database: string) => ({ query: catalog }));
  Object.assign(driver, { requirePool });
  const manager = {
    getDriver: () => driver,
    getConnection: () => config,
  } as never;
  const service = new TableDataService(manager);
  const query = vi.spyOn(driver, "query").mockResolvedValue(result([], []));
  const transaction = vi.spyOn(driver, "runTransaction").mockResolvedValue();
  return { driver, catalog, requirePool, service, manager, query, transaction };
}

function result(columns: string[], rows: unknown[][]): QueryResult {
  return {
    columns,
    rows: rows.map((row) =>
      Object.fromEntries(row.map((value, index) => [`__col_${index}`, value])),
    ),
    rowCount: rows.length,
    executionTimeMs: 0,
  };
}

const filters: FilterExpression[] = [
  { column: "id", operator: "gte", value: "1" },
];
const updates = [
  {
    primaryKeys: { id: 1 },
    changes: { amount: "12.50" },
    originalValues: { amount: "12.00" },
  },
];

describe("PostgreSQL Table Viewer relation scope", () => {
  it.each([
    { kind: "r", partition: false, only: true, label: "classical table" },
    { kind: "f", partition: false, only: true, label: "foreign table" },
    { kind: "p", partition: false, only: false, label: "partitioned root" },
    { kind: "p", partition: true, only: false, label: "partitioned subtree" },
    { kind: "r", partition: true, only: false, label: "partition leaf" },
    { kind: "f", partition: true, only: false, label: "foreign partition" },
    { kind: "v", partition: false, only: false, label: "view" },
    { kind: "m", partition: false, only: false, label: "materialized view" },
  ])("resolves $label by catalog relation kind, even with no columns", async ({
    kind,
    partition,
    only,
  }) => {
    const { driver, catalog, requirePool } = setup(kind, partition);
    catalog.mockResolvedValueOnce({
      rows: [
        { relation_kind: kind, is_partition: partition, column_name: null },
      ],
    });
    const columns = await driver.describeColumns(...target);
    expect(columns).toEqual([]);
    expect(driver.getTableDataReference(...target, columns)).toBe(
      only ? `ONLY ${qualified}` : qualified,
    );
    expect(requirePool).toHaveBeenCalledWith("data");
    expect(catalog).toHaveBeenCalledWith(
      expect.stringContaining("FROM pg_catalog.pg_class c"),
      ["app", 'some"items'],
    );
  });

  it.each([
    { kind: "r", only: "ONLY " },
    { kind: "p", only: "" },
  ])("shares the $kind scope across browsing, Apply, DELETE and both export orders", async ({
    kind,
    only,
  }) => {
    const { driver, service, manager, query, transaction } = setup(kind, false);
    const tableRef = `${only}${qualified}`;
    const names = ["id", "amount"];
    const observation = "101:42:(0,1)";
    query
      .mockResolvedValueOnce(result(["cnt"], [[2]]))
      .mockResolvedValueOnce(
        result([...names, "__rapidb_delete_0"], [[1, "12.00", observation]]),
      );
    const page = await service.getPage(
      ...scope,
      1,
      1,
      filters,
      null,
      false,
      undefined,
      true,
    );
    expect(page).toMatchObject({
      totalCount: 2,
      rows: [{ id: 1, amount: "12.00" }],
      sqlDeleteObservations: [{ values: [observation] }],
    });
    expect(query.mock.calls[0][0]).toContain(
      `COUNT(*) AS cnt FROM ${tableRef} WHERE`,
    );
    expect(query.mock.calls[1][0]).toContain(`FROM ${tableRef} WHERE`);
    expect(query.mock.calls[1][0]).toContain("xmin::pg_catalog.text");
    expect(query.mock.calls[0][1]).toEqual(query.mock.calls[1][1]?.slice(0, 1));
    expect(
      page.columns
        .filter((column) => column.isPrimaryKey)
        .map(({ name }) => name),
    ).toEqual(["id"]);
    await expect(
      service.getPage(...scope, 1, 1, [
        { column: "child_only", operator: "eq", value: "1" },
      ]),
    ).rejects.toThrow("Unknown filter column child_only");

    const prepared = prepareApplyChangesPlan(
      manager,
      ...scope,
      updates,
      page.columns,
    );
    if (!prepared.executable) throw new Error("Expected an UPDATE plan");
    const [operation] = prepared.plan.operations;
    expect(operation.sql).toContain(`UPDATE ${tableRef} SET`);
    expect(operation.sql).toContain('WHERE "id" = $2 AND "amount"');
    expect(prepared.plan.previewStatements[0]).toContain(
      `UPDATE ${tableRef} SET`,
    );
    expect(
      await executePreparedApplyPlan(manager, prepared.plan),
    ).toMatchObject({
      success: true,
    });
    expect(transaction.mock.calls[0][2]?.verifications?.[0].sql).toContain(
      `FROM ${tableRef} WHERE`,
    );

    const insert = await service.prepareInsertRow(...scope, {
      id: 3,
      amount: "14.00",
    });
    expect(insert.operation.sql).toContain(`INSERT INTO ${qualified}`);
    expect(insert.verification?.sql).toContain(`FROM ${tableRef} WHERE`);

    const deletion = await service.prepareDeleteRowsPlan(
      ...scope,
      [{ id: 1 }],
      page.sqlDeleteObservations,
    );
    if (!deletion) throw new Error("Expected a DELETE plan");
    expect(deletion.operations[0]).toMatchObject({
      sql: expect.stringContaining(`DELETE FROM ${tableRef} WHERE`),
      params: [1, observation],
    });
    expect(deletion.operations[0].sql).toContain("tableoid::pg_catalog.text");
    await service.executePreparedDeletePlan(deletion);
    expect(query.mock.lastCall?.[0]).toContain(
      `SELECT 1 FROM ${tableRef} WHERE`,
    );

    for (const sort of [
      null,
      { column: "amount", direction: "asc" },
    ] as const) {
      query.mockClear();
      query
        .mockResolvedValueOnce(result(names, [[1, "12.00"]]))
        .mockResolvedValueOnce(result(names, [[2, "13.00"]]));
      const exported = [];
      for await (const chunk of service.exportAll(...scope, 1, sort, filters)) {
        exported.push(...chunk.rows);
      }
      expect(exported.map(({ id }) => id)).toEqual([1, 2]);
      expect(query).toHaveBeenCalledTimes(3);
      for (const [sql] of query.mock.calls) {
        expect(sql).toContain(`FROM ${tableRef} WHERE`);
      }
      if (sort) {
        expect(query.mock.calls[1][0]).toContain(
          'ORDER BY "amount" ASC, "id" ASC',
        );
        expect(query.mock.calls[1][1]?.at(-1)).toBe(1);
      } else {
        expect(query.mock.calls[1][0]).toContain('"id" > $2');
        expect(query.mock.calls[1][1]?.at(-1)).toBe(0);
      }
    }
    expect(driver.qualifiedTableName(...target)).toBe(qualified);
  });

  it("fails closed for unknown scope instead of silently scanning an inheritance tree", async () => {
    const { service, manager, query, transaction } = setup("r", null);
    const columns = await service.getColumns(...scope);
    expect(() =>
      prepareApplyChangesPlan(manager, ...scope, updates, columns),
    ).toThrow("Could not determine PostgreSQL table inheritance scope");
    await expect(service.getPage(...scope, 1, 1, [])).rejects.toThrow(
      "Could not determine PostgreSQL table inheritance scope",
    );
    await expect(service.exportAll(...scope, 1).next()).rejects.toThrow(
      "Could not determine PostgreSQL table inheritance scope",
    );
    await expect(
      service.prepareDeleteRowsPlan(...scope, [{ id: 1 }]),
    ).rejects.toThrow("Could not determine PostgreSQL table inheritance scope");
    expect(query).not.toHaveBeenCalled();
    expect(transaction).not.toHaveBeenCalled();
  });

  it("does not use a recursive fallback if catalog access is denied", async () => {
    const { service, catalog, query, transaction } = setup("r", false);
    catalog.mockRejectedValue(
      new Error("permission denied for table pg_class"),
    );
    await expect(service.getPage(...scope, 1, 1, [])).rejects.toThrow(
      "permission denied",
    );
    await expect(
      service.prepareDeleteRowsPlan(...scope, [{ id: 1 }]),
    ).rejects.toThrow("permission denied");
    expect(query).not.toHaveBeenCalled();
    expect(transaction).not.toHaveBeenCalled();
  });
});
