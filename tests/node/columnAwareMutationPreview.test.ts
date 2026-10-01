import { describe, expect, it } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationPreviewController } from "../../src/extension/panels/tableMutationPreviewController";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const config = {
  id: "preview-plan",
  name: "preview-plan",
  host: "localhost",
  database: "db",
  username: "user",
  password: "pass",
};

describe.each([
  "pg",
  "oracle",
] as const)("%s real mutation previews", (dialect) => {
  const marker = dialect === "pg" ? "$" : ":";
  const driver =
    dialect === "pg"
      ? new PostgresDriver({ ...config, type: "pg" })
      : new OracleDriver({ ...config, type: "oracle" });
  const key = `id"${marker}1`;
  const name = `x${marker}1`;
  const escapedName = `x"${marker}2`;
  const table = `t"${marker}1`;
  const schema = `s${marker}2`;
  const qualified = `"s${marker}2"."t""${marker}1"`;
  const columns: ColumnTypeMeta[] = [key, name, escapedName].map(
    (name, index) => ({
      name,
      type: index === 0 ? "INTEGER" : "TEXT",
      nativeType:
        index === 0 ? "INTEGER" : dialect === "pg" ? "text" : "VARCHAR2(100)",
      category: index === 0 ? "integer" : "text",
      nullable: true,
      isPrimaryKey: index === 0,
      primaryKeyOrdinal: index === 0 ? 1 : undefined,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    }),
  );
  const manager = {
    getConnection: () => ({ ...config, type: dialect }),
    getDriver: () => driver,
    getQueryEditorPresentation: () => ({
      editorLanguage: "sql",
      sqlDialect: dialect === "pg" ? "postgresql" : "plsql",
    }),
  };
  const service = new TableMutationService(manager as never, {
    getColumns: async () => columns,
  });
  const controller = new TableMutationPreviewController({
    connectionId: config.id,
    tableName: table,
    connectionManager: manager as never,
    tableDataService: service,
    notifyWarning: () => undefined,
  });

  it("materializes SET, primary key and optimistic predicates without modifying execution parameters", () => {
    const prepared = prepareApplyChangesPlan(
      manager as never,
      config.id,
      "db",
      schema,
      table,
      [
        {
          primaryKeys: { [key]: 7 },
          changes: { [name]: `new ${marker}2`, [escapedName]: null },
          originalValues: { [name]: `old ${marker}1`, [escapedName]: null },
        },
      ],
      columns,
    );
    if (!prepared.executable)
      throw new Error("Expected executable update plan");
    const executionSql = `UPDATE ${qualified} SET "x${marker}1" = ${marker}1, "x""${marker}2" = ${marker}2 WHERE "id""${marker}1" = ${marker}3 AND "x${marker}1" = ${marker}4 AND "x""${marker}2" IS NULL`;
    const previewSql = `UPDATE ${qualified} SET "x${marker}1" = 'new ${marker}2', "x""${marker}2" = NULL WHERE "id""${marker}1" = 7 AND "x${marker}1" = 'old ${marker}1' AND "x""${marker}2" IS NULL`;
    expect(prepared.plan.operations).toEqual([
      {
        sql: executionSql,
        params: [`new ${marker}2`, null, 7, `old ${marker}1`],
        checkAffectedRows: true,
      },
    ]);
    expect(prepared.plan.previewStatements).toEqual([previewSql]);
    const payload = controller.createApplyChangesPreview("update", {
      apply: prepared.plan,
      applyResultWhenEmpty: null,
      inserts: [],
    });
    expect(payload).toMatchObject({
      statementCount: 1,
      contentType: "application/sql",
    });
    expect(payload.text.replace(/\s+/g, " ")).toBe(`${previewSql};`);
    expect(prepared.plan.operations[0].sql).toBe(executionSql);
  });

  it("materializes the real insert plan and controller payload", async () => {
    const plan = await service.prepareInsertRow(
      config.id,
      "db",
      schema,
      table,
      { [key]: 7, [name]: `value ${marker}2`, [escapedName]: null },
    );
    const names = `"id""${marker}1", "x${marker}1", "x""${marker}2"`;
    expect(plan.operation).toEqual({
      sql: `INSERT INTO ${qualified} (${names}) VALUES (${marker}1, ${marker}2, NULL)`,
      params: [7, `value ${marker}2`],
    });
    const previewSql = `INSERT INTO ${qualified} (${names}) VALUES (7, 'value ${marker}2', NULL)`;
    expect(plan.previewStatements).toEqual([previewSql]);
    const payload = controller.createInsertPreview("insert", plan);
    expect(payload.statementCount).toBe(1);
    expect(payload.text.replace(/\s+/g, " ")).toBe(`${previewSql};`);
  });

  it("preserves quoted primary keys in the real delete plan and controller payload", async () => {
    const plan = await service.prepareDeleteRowsPlan(
      config.id,
      "db",
      schema,
      table,
      [{ [key]: 7 }, { [key]: 8 }],
    );
    if (!plan) throw new Error("Expected delete plan");
    expect(plan.operations[0].params).toEqual([7, 8]);
    expect(plan.operations[0].sql).toBe(
      `DELETE FROM ${qualified} WHERE "id""${marker}1" IN (${marker}1, ${marker}2)`,
    );
    const previewSql = `DELETE FROM ${qualified} WHERE "id""${marker}1" IN (7, 8)`;
    expect(plan.previewStatements).toEqual([previewSql]);
    const payload = controller.createDeleteRowsPreview("delete", plan);
    expect(payload.text.replace(/\s+/g, " ")).toBe(`${previewSql};`);
  });
});
