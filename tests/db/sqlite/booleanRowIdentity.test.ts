import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import {
  applyChangesTransactional,
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import { buildUpdateRowSql } from "../../../src/extension/table/updateSql";

const drivers: SQLiteDriver[] = [];
afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

async function setup() {
  const driver = new SQLiteDriver({
    id: "boolean-pk",
    name: "Boolean PK",
    type: "sqlite",
    filePath: ":memory:",
  });
  drivers.push(driver);
  await driver.connect();
  await driver.query(
    "CREATE TABLE items (id BOOLEAN PRIMARY KEY NOT NULL, flag BOOLEAN, label TEXT); INSERT INTO items VALUES (0, 0, 'false row'), (1, 1, 'true row')",
  );
  const manager = {
    getConnection: () => ({ id: "boolean-pk", type: "sqlite" }),
    getDriver: () => driver,
  } as never;
  const read = new TableReadService(manager);
  const service = new TableMutationService(manager, read);
  const columns = await read.getColumns("boolean-pk", "", "main", "items");
  const page = () =>
    read.getPage("boolean-pk", "", "main", "items", 1, 25, [], {
      column: "id",
      direction: "asc",
    });
  expect((await page()).rows).toEqual([
    { id: false, flag: false, label: "false row" },
    { id: true, flag: true, label: "true row" },
  ]);
  return { driver, manager, service, columns, page };
}

describe("real SQLite worker boolean primary-key bindings", () => {
  it.each([
    "direct",
    "SQL builder",
    "transactional",
    "prepared",
    "atomic",
  ] as const)("mutates false and true identities via %s without changing the other row", async (mode) => {
    const { driver, manager, service, columns, page } = await setup();
    for (const id of [false, true]) {
      const original = (await page()).rows;
      const updates = [
        {
          primaryKeys: { id },
          changes: { flag: !id, label: "edited" },
          originalValues: { flag: id },
        },
      ];
      if (mode === "direct") {
        await service.updateRow(
          "boolean-pk",
          "",
          "main",
          "items",
          { id },
          updates[0].changes,
        );
      } else if (mode === "SQL builder") {
        const operation = buildUpdateRowSql(
          driver,
          "",
          "main",
          "items",
          { id },
          updates[0].changes,
          columns,
          { flag: id },
        );
        if (!operation) throw new Error("Expected an update");
        expect(operation.params).toEqual([
          id ? 0 : 1,
          "edited",
          id ? 1 : 0,
          id ? 1 : 0,
        ]);
        const result = await driver.query(operation.sql, operation.params);
        expect(result.affectedRows ?? result.rowCount).toBe(1);
      } else if (mode === "transactional") {
        expect(
          (
            await applyChangesTransactional(
              manager,
              "boolean-pk",
              "",
              "main",
              "items",
              updates,
              columns,
            )
          ).success,
        ).toBe(true);
      } else {
        const prepared = prepareApplyChangesPlan(
          manager,
          "boolean-pk",
          "",
          "main",
          "items",
          updates,
          columns,
        );
        if (!prepared.executable) throw new Error("Expected an apply plan");
        const result =
          mode === "atomic"
            ? await executeAtomicSqlApplyPlan(manager, prepared.plan, [])
            : await executePreparedApplyPlan(manager, prepared.plan);
        expect(result.success).toBe(true);
      }
      const next = (await page()).rows;
      expect(next.find((row) => row.id === id)).toEqual({
        id,
        flag: !id,
        label: "edited",
      });
      expect(next.find((row) => row.id === !id)).toEqual(
        original.find((row) => row.id === !id),
      );
    }
  });

  it.each([
    "direct",
    "prepared",
  ] as const)("deletes boolean keys through %s single-key IN and composite equality plans", async (mode) => {
    const { driver, service, page } = await setup();
    for (const id of [false, true]) {
      if (mode === "direct") {
        await service.deleteRows("boolean-pk", "", "main", "items", [{ id }]);
      } else {
        const plan = await service.prepareDeleteRowsPlan(
          "boolean-pk",
          "",
          "main",
          "items",
          [{ id }],
        );
        if (!plan) throw new Error("Expected a delete plan");
        expect(plan.operations[0].params).toEqual([id ? 1 : 0]);
        await service.executePreparedDeletePlan(plan);
      }
      expect((await page()).rows.some((row) => row.id === id)).toBe(false);
    }
    await driver.query(
      "CREATE TABLE composite (tenant TEXT, id BOOLEAN, PRIMARY KEY (tenant, id)); INSERT INTO composite VALUES ('tenant', 0), ('tenant', 1)",
    );
    await service.deleteRows("boolean-pk", "", "main", "composite", [
      { tenant: "tenant", id: false },
      { tenant: "tenant", id: true },
    ]);
    expect((await driver.query("SELECT * FROM composite")).rows).toEqual([]);
  });

  it("binds boolean inserts and atomic insert-plus-update values while preserving non-boolean coercion", async () => {
    const { driver, manager, service, columns, page } = await setup();
    await service.deleteRows("boolean-pk", "", "main", "items", [
      { id: false },
      { id: true },
    ]);
    await service.insertRow("boolean-pk", "", "main", "items", {
      id: false,
      flag: true,
      label: "inserted",
    });
    const insert = await service.prepareInsertRow(
      "boolean-pk",
      "",
      "main",
      "items",
      { id: true, flag: false, label: "atomic insert" },
    );
    const prepared = prepareApplyChangesPlan(
      manager,
      "boolean-pk",
      "",
      "main",
      "items",
      [
        {
          primaryKeys: { id: false },
          changes: { flag: false },
          originalValues: { flag: true },
        },
      ],
      columns,
    );
    if (!prepared.executable) throw new Error("Expected an apply plan");
    expect(
      (await executeAtomicSqlApplyPlan(manager, prepared.plan, [insert]))
        .success,
    ).toBe(true);
    expect((await page()).rows).toEqual([
      { id: false, flag: false, label: "inserted" },
      { id: true, flag: false, label: "atomic insert" },
    ]);
    const booleanColumn = columns.find((column) => column.name === "id");
    if (!booleanColumn) throw new Error("Expected a boolean primary key");
    for (const column of [
      booleanColumn,
      {
        ...booleanColumn,
        nativeType: "OTHER",
        category: "boolean" as const,
        valueSemantics: "plain" as const,
      },
      {
        ...booleanColumn,
        nativeType: "BOOLEAN",
        category: "other" as const,
        valueSemantics: "plain" as const,
      },
    ]) {
      expect(driver.coerceInputValue(false, column)).toBe(0);
      expect(driver.coerceInputValue(true, column)).toBe(1);
      expect(driver.coerceInputValue(null, column)).toBeNull();
      expect(driver.coerceInputValue(undefined, column)).toBeUndefined();
    }
    const text = columns.find((column) => column.name === "label");
    if (!text) throw new Error("Expected a text column");
    expect(driver.coerceInputValue(false, text)).toBe(false);
    expect(driver.coerceInputValue("false", text)).toBe("false");
    expect(driver.coerceInputValue(0, text)).toBe(0);
  });
});
