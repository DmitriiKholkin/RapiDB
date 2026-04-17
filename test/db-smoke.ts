import assert from "node:assert/strict";
import type {
  ConnectionConfig,
  ConnectionManager,
} from "../src/extension/connectionManager";
import { MSSQLDriver } from "../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../src/extension/dbDrivers/postgres";
import {
  type ColumnTypeMeta,
  type FilterExpression,
  type IDBDriver,
  NULL_SENTINEL,
} from "../src/extension/dbDrivers/types";
import {
  formatDatetimeForDisplay,
  TableDataService,
} from "../src/extension/tableDataService";
import {
  buildFixtures,
  type ComparisonMode,
  type DbFixture,
  type DbKind,
} from "./db-fixtures";

class SmokeConnectionManager {
  private readonly connections = new Map<string, ConnectionConfig>();
  private readonly drivers = new Map<string, IDBDriver>();

  register(id: string, connection: ConnectionConfig, driver: IDBDriver): void {
    this.connections.set(id, connection);
    this.drivers.set(id, driver);
  }

  clear(id: string): void {
    this.connections.delete(id);
    this.drivers.delete(id);
  }

  getConnection(id: string): ConnectionConfig | undefined {
    return this.connections.get(id);
  }

  getDriver(id: string): IDBDriver | undefined {
    return this.drivers.get(id);
  }
}

function isDbKind(value: string): value is DbKind {
  return (
    value === "pg" ||
    value === "mysql" ||
    value === "mssql" ||
    value === "oracle"
  );
}

function parseRequestedKinds(): Set<DbKind> {
  const tokens = [
    ...process.argv.slice(2),
    ...(process.env.RAPIDB_DBS ?? "").split(","),
  ];
  const requested = new Set<DbKind>();

  for (const token of tokens) {
    const normalized = token.trim().toLowerCase();
    if (!normalized) continue;
    if (isDbKind(normalized)) {
      requested.add(normalized);
      continue;
    }

    if (normalized.startsWith("--db=")) {
      const value = normalized.slice(5);
      if (isDbKind(value)) requested.add(value);
      continue;
    }

    if (normalized.startsWith("db=")) {
      const value = normalized.slice(3);
      if (isDbKind(value)) requested.add(value);
    }
  }

  return requested;
}

function createDriver(
  kind: DbKind,
  config: Omit<ConnectionConfig, "id">,
): IDBDriver {
  switch (kind) {
    case "pg":
      return new PostgresDriver({ ...config, id: "__smoke__" });
    case "mysql":
      return new MySQLDriver({ ...config, id: "__smoke__" });
    case "mssql":
      return new MSSQLDriver({ ...config, id: "__smoke__" });
    case "oracle":
      return new OracleDriver({ ...config, id: "__smoke__" });
  }
}

function withId(
  config: Omit<ConnectionConfig, "id">,
  id: string,
): ConnectionConfig {
  return { ...config, id };
}

function objectRef(fixture: DbFixture, name: string): string {
  switch (fixture.kind) {
    case "pg":
      return `"${fixture.schema}"."${name}"`;
    case "mysql":
      return `\`${name}\``;
    case "mssql":
      return `[${fixture.schema}].[${name}]`;
    case "oracle":
      return name.toUpperCase();
  }
}

function idColumnName(fixture: DbFixture): string {
  return fixture.kind === "oracle" ? "ID" : "id";
}

function normalizeValue(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  const formatted = formatDatetimeForDisplay(value);
  if (formatted !== null) {
    return formatted;
  }

  if (Buffer.isBuffer(value)) {
    return `\\x${value.toString("hex")}`;
  }

  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    if ("x" in record && "y" in record && Object.keys(record).length === 2) {
      return `(${String(record.x)}, ${String(record.y)})`;
    }
    return JSON.stringify(value);
  }

  return String(value);
}

function normalizePrefix(value: unknown): string {
  const normalized = normalizeValue(value) ?? "";
  return normalized
    .replace("T", " ")
    .replace(/\.\d+(?=([+-]\d{2}(:\d{2})?|Z)?$)/, "")
    .replace(/Z$/i, "")
    .replace(/[ ]?[+-]\d{2}:\d{2}$/, "")
    .replace(/[ ]?[+-]\d{2}$/, "")
    .trim();
}

function defaultFilterExpression(
  columns: ColumnTypeMeta[],
  columnName: string,
  value: string,
): FilterExpression {
  const column = columns.find((entry) => entry.name === columnName);
  if (
    column?.isBoolean ||
    column?.category === "integer" ||
    column?.category === "float" ||
    column?.category === "decimal" ||
    column?.category === "date"
  ) {
    return { column: columnName, operator: "eq", value };
  }
  return { column: columnName, operator: "like", value };
}

function assertCellValue(
  actual: unknown,
  expected: unknown,
  comparison: ComparisonMode,
  context: string,
): void {
  const actualText = normalizeValue(actual);
  assert.notEqual(
    actualText,
    null,
    `${context}: expected a value, got null/undefined`,
  );

  if (actualText === null) {
    throw new Error(`${context}: expected a value, got null/undefined`);
  }

  if (comparison === "presence") {
    return;
  }

  const expectedText = normalizeValue(expected);
  assert.notEqual(
    expectedText,
    null,
    `${context}: expected comparison value is null/undefined`,
  );

  if (expectedText === null) {
    throw new Error(`${context}: expected comparison value is null/undefined`);
  }

  if (comparison === "prefix") {
    const prefix = normalizePrefix(expected);
    assert.ok(
      actualText.startsWith(prefix),
      `${context}: expected prefix ${prefix}, got ${actualText}`,
    );
    return;
  }

  assert.equal(actualText, expectedText, `${context}: values differ`);
}

function assertOptionalExactValue(
  actual: unknown,
  expected: unknown,
  context: string,
): void {
  if (expected === null || expected === undefined) {
    assert.equal(actual ?? null, expected ?? null, `${context}: values differ`);
    return;
  }

  assertCellValue(actual, expected, "exact", context);
}

function assertRowMatchesColumns(
  row: Record<string, unknown>,
  columns: DbFixture["columns"],
  values: Record<string, unknown>,
  label: string,
): void {
  for (const column of columns) {
    const expectedValue = values[column.name];
    const context = `${label}.${column.name}`;
    assertCellValue(
      row[column.name],
      expectedValue,
      column.comparison ?? "exact",
      context,
    );
  }
}

function assertColumnNames(
  actual: string[],
  fixture: DbFixture,
  label: string,
): void {
  const expected = [
    idColumnName(fixture),
    ...fixture.columns.map((column) => column.name),
  ];
  assert.deepEqual(actual, expected, `${label}: column order mismatch`);
}

function assertColumnMetadata(
  columns: Awaited<ReturnType<TableDataService["getColumns"]>>,
  fixture: DbFixture,
): void {
  const columnMap = new Map(columns.map((column) => [column.name, column]));

  for (const expectation of fixture.metadataChecks) {
    const column = columnMap.get(expectation.column);
    assert.ok(
      column,
      `${fixture.displayName}: missing metadata for ${expectation.column}`,
    );
    if (!column) {
      continue;
    }

    assert.equal(
      column.category,
      expectation.category,
      `${fixture.displayName}: unexpected category for ${expectation.column}`,
    );
    if (expectation.filterable !== undefined) {
      assert.equal(
        column.filterable,
        expectation.filterable,
        `${fixture.displayName}: unexpected filterable for ${expectation.column}`,
      );
    }
    if (expectation.isAutoIncrement !== undefined) {
      assert.equal(
        column.isAutoIncrement ?? false,
        expectation.isAutoIncrement,
        `${fixture.displayName}: unexpected isAutoIncrement for ${expectation.column}`,
      );
    }
    if (expectation.isBoolean !== undefined) {
      assert.equal(
        column.isBoolean,
        expectation.isBoolean,
        `${fixture.displayName}: unexpected isBoolean for ${expectation.column}`,
      );
    }
  }
}

function execSql(driver: IDBDriver, sql: string): Promise<unknown> {
  const trimmed = sql.trim();
  if (!trimmed) {
    return Promise.resolve();
  }
  return driver.query(sql);
}

function isMeaningfulSql(sql: string): boolean {
  const trimmed = sql.trim();
  if (!trimmed) {
    return false;
  }
  return !trimmed.startsWith("--");
}

async function runStatements(
  driver: IDBDriver,
  statements: string[],
): Promise<void> {
  for (const statement of statements) {
    await execSql(driver, statement);
  }
}

async function bootstrapDatabaseIfNeeded(fixture: DbFixture): Promise<void> {
  if (
    !fixture.bootstrapConnection ||
    !fixture.bootstrapSql ||
    fixture.bootstrapSql.length === 0
  ) {
    return;
  }

  const bootstrapDriver = createDriver(
    fixture.kind,
    fixture.bootstrapConnection,
  );
  await bootstrapDriver.connect();
  try {
    await runStatements(bootstrapDriver, fixture.bootstrapSql);
  } finally {
    await bootstrapDriver.disconnect();
  }
}

async function runFixture(fixture: DbFixture, runId: string): Promise<void> {
  const connectionId = `${fixture.kind}-${runId}`;
  const harness = new SmokeConnectionManager();
  const service = new TableDataService(harness as unknown as ConnectionManager);

  console.log(`[${fixture.displayName}] bootstrapping`);
  await bootstrapDatabaseIfNeeded(fixture);

  const mainDriver = createDriver(fixture.kind, fixture.connection);
  const connection = withId(fixture.connection, connectionId);
  harness.register(connectionId, connection, mainDriver);
  let connected = false;

  try {
    console.log(`[${fixture.displayName}] connecting`);
    await mainDriver.connect();
    connected = true;

    console.log(`[${fixture.displayName}] resetting objects`);
    await runStatements(mainDriver, fixture.teardownSql);

    console.log(`[${fixture.displayName}] creating table/view/routines`);
    await execSql(mainDriver, fixture.createTableSql);
    await execSql(mainDriver, fixture.createViewSql);
    await execSql(mainDriver, fixture.createFunctionSql);
    if (isMeaningfulSql(fixture.createProcedureSql)) {
      await execSql(mainDriver, fixture.createProcedureSql);
    }

    const colMeta = await service.getColumns(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
    );
    assertColumnNames(
      colMeta.map((column) => column.name),
      fixture,
      `${fixture.displayName} describeTable`,
    );
    assertColumnMetadata(colMeta, fixture);

    console.log(`[${fixture.displayName}] inserting seed row`);
    await service.insertRow(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      fixture.seedValues,
    );

    let page = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      1,
      25,
      [],
      null,
    );
    assert.equal(
      page.rows.length,
      1,
      `${fixture.displayName}: expected 1 seed row`,
    );
    assertRowMatchesColumns(
      page.rows[0] as Record<string, unknown>,
      fixture.columns,
      fixture.seedValues,
      `${fixture.displayName} seed row`,
    );

    const pkName = idColumnName(fixture);
    const pkValue = (page.rows[0] as Record<string, unknown>)[pkName];

    console.log(`[${fixture.displayName}] verifying view and function`);
    const viewResult = await mainDriver.query(
      `SELECT * FROM ${objectRef(fixture, fixture.view)}`,
    );
    assert.ok(
      viewResult.rows.length >= 1,
      `${fixture.displayName}: view should return at least one row`,
    );

    const functionResult = await mainDriver.query(fixture.functionCallSql);
    assert.equal(
      normalizeValue(
        (functionResult.rows[0] as Record<string, unknown> | undefined)
          ?.__col_0,
      ),
      fixture.functionExpected,
      `${fixture.displayName}: function result mismatch`,
    );

    const routineName = fixture.functionName;
    const routineDef = await mainDriver.getRoutineDefinition(
      fixture.database,
      fixture.schema,
      routineName,
      "function",
    );
    assert.match(
      routineDef.toLowerCase(),
      new RegExp(routineName.toLowerCase()),
      `${fixture.displayName}: function definition should include the function name`,
    );

    const procedureDef = await mainDriver.getRoutineDefinition(
      fixture.database,
      fixture.schema,
      fixture.procedureName,
      "procedure",
    );
    assert.match(
      procedureDef.toLowerCase(),
      new RegExp(fixture.procedureName.toLowerCase()),
      `${fixture.displayName}: procedure definition should include the procedure name`,
    );

    const ddl = await mainDriver.getCreateTableDDL(
      fixture.database,
      fixture.schema,
      fixture.table,
    );
    assert.match(
      ddl.toLowerCase(),
      new RegExp(fixture.table.toLowerCase()),
      `${fixture.displayName}: table DDL should mention the table name`,
    );

    const objects = await mainDriver.listObjects(
      fixture.database,
      fixture.schema,
    );
    const hasTable = objects.some(
      (object) =>
        object.name.toLowerCase() === fixture.table.toLowerCase() &&
        object.type === "table",
    );
    const hasView = objects.some(
      (object) =>
        object.name.toLowerCase() === fixture.view.toLowerCase() &&
        object.type === "view",
    );
    const hasFunction = objects.some(
      (object) =>
        object.name.toLowerCase() === fixture.functionName.toLowerCase() &&
        object.type === "function",
    );
    const hasProcedure = objects.some(
      (object) =>
        object.name.toLowerCase() === fixture.procedureName.toLowerCase() &&
        object.type === "procedure",
    );
    assert.ok(
      hasTable,
      `${fixture.displayName}: listObjects should include the table`,
    );
    assert.ok(
      hasView,
      `${fixture.displayName}: listObjects should include the view`,
    );
    assert.ok(
      hasFunction,
      `${fixture.displayName}: listObjects should include the function`,
    );
    assert.ok(
      hasProcedure,
      `${fixture.displayName}: listObjects should include the procedure`,
    );

    console.log(
      `[${fixture.displayName}] updating rows through TableDataService`,
    );
    for (const column of fixture.columns) {
      if (column.updatable === false) {
        continue;
      }

      await service.updateRow(
        connectionId,
        fixture.database,
        fixture.schema,
        fixture.table,
        { [pkName]: pkValue },
        { [column.name]: fixture.updateValues[column.name] },
      );

      page = await service.getPage(
        connectionId,
        fixture.database,
        fixture.schema,
        fixture.table,
        1,
        25,
        [],
        null,
      );
      assert.equal(
        page.rows.length,
        1,
        `${fixture.displayName}: update should keep one row`,
      );
      assertCellValue(
        (page.rows[0] as Record<string, unknown>)[column.name],
        fixture.updateValues[column.name],
        column.comparison ?? "exact",
        `${fixture.displayName} update ${column.name}`,
      );
    }

    console.log(
      `[${fixture.displayName}] restoring seed row after update checks`,
    );
    for (const column of fixture.columns) {
      if (column.updatable === false) {
        continue;
      }

      await service.updateRow(
        connectionId,
        fixture.database,
        fixture.schema,
        fixture.table,
        { [pkName]: pkValue },
        { [column.name]: fixture.seedValues[column.name] },
      );
    }

    page = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      1,
      25,
      [],
      null,
    );
    assert.equal(
      page.rows.length,
      1,
      `${fixture.displayName}: restoring the seed row should keep one row`,
    );
    assertRowMatchesColumns(
      page.rows[0] as Record<string, unknown>,
      fixture.columns,
      fixture.seedValues,
      `${fixture.displayName} restored seed row`,
    );

    console.log(
      `[${fixture.displayName}] verifying explicit empty-string update`,
    );
    await service.updateRow(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      { [pkName]: pkValue },
      { [fixture.emptyStringColumn]: "" },
    );

    page = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      1,
      25,
      [],
      null,
    );
    assert.equal(
      page.rows.length,
      1,
      `${fixture.displayName}: empty-string update should keep one row`,
    );
    const updatedRow = page.rows[0] as Record<string, unknown>;
    assert.ok(
      Object.hasOwn(updatedRow, fixture.emptyStringColumn),
      `${fixture.displayName}: updated row should include ${fixture.emptyStringColumn}`,
    );
    assertOptionalExactValue(
      updatedRow[fixture.emptyStringColumn],
      fixture.emptyStringReadback,
      `${fixture.displayName} empty-string update ${fixture.emptyStringColumn}`,
    );

    await service.updateRow(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      { [pkName]: pkValue },
      {
        [fixture.emptyStringColumn]:
          fixture.seedValues[fixture.emptyStringColumn],
      },
    );

    console.log(
      `[${fixture.displayName}] verifying explicit empty-string insert`,
    );
    await service.insertRow(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      {
        [pkName]: 999999,
        [fixture.emptyStringLabelColumn]: fixture.emptyStringLabelValue,
        [fixture.emptyStringColumn]: "",
      },
    );

    const emptyStringPage = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      1,
      25,
      [
        defaultFilterExpression(
          colMeta,
          fixture.emptyStringLabelColumn,
          fixture.emptyStringLabelValue,
        ),
      ],
      null,
    );
    assert.equal(
      emptyStringPage.rows.length,
      1,
      `${fixture.displayName}: explicit empty-string insert should be queryable`,
    );
    const emptyStringRow = emptyStringPage.rows[0] as Record<string, unknown>;
    assert.ok(
      Object.hasOwn(emptyStringRow, fixture.emptyStringColumn),
      `${fixture.displayName}: explicit empty-string row should include ${fixture.emptyStringColumn}`,
    );
    assert.notEqual(
      emptyStringRow[pkName],
      999999,
      `${fixture.displayName}: auto-increment PK should ignore explicit insert values`,
    );
    assertOptionalExactValue(
      emptyStringRow[fixture.emptyStringColumn],
      fixture.emptyStringReadback,
      `${fixture.displayName} empty-string insert ${fixture.emptyStringColumn}`,
    );

    console.log(`[${fixture.displayName}] calling procedure`);
    await execSql(mainDriver, fixture.procedureCallSql);

    const procedureFilter = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      1,
      25,
      [
        defaultFilterExpression(
          colMeta,
          fixture.procedureColumn,
          String(fixture.procedureValue),
        ),
      ],
      null,
    );
    assert.equal(
      procedureFilter.rows.length,
      1,
      `${fixture.displayName}: procedure insert should be visible through filters`,
    );
    assertCellValue(
      (procedureFilter.rows[0] as Record<string, unknown>)[
        fixture.procedureColumn
      ],
      fixture.procedureValue,
      "exact",
      `${fixture.displayName} procedure insert`,
    );

    console.log(`[${fixture.displayName}] verifying NULL filters`);
    const nullFilterPage = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      1,
      25,
      [{ column: fixture.nullFilterColumn, operator: "is_null" }],
      null,
    );
    assert.equal(
      nullFilterPage.rows.length,
      1,
      `${fixture.displayName}: NULL filter should return exactly the procedure row`,
    );
    const nullFilterRow = nullFilterPage.rows[0] as Record<string, unknown>;
    assert.ok(
      Object.hasOwn(nullFilterRow, fixture.nullFilterColumn),
      `${fixture.displayName}: NULL filter row should include ${fixture.nullFilterColumn}`,
    );
    assert.equal(
      nullFilterRow[fixture.nullFilterColumn],
      null,
      `${fixture.displayName}: NULL filter row should contain NULL in ${fixture.nullFilterColumn}`,
    );
    assertCellValue(
      nullFilterRow[fixture.procedureColumn],
      fixture.procedureValue,
      "exact",
      `${fixture.displayName} NULL filter procedure row`,
    );

    console.log(`[${fixture.displayName}] verifying filters`);
    for (const filterCase of fixture.filterCases) {
      const filtered = await service.getPage(
        connectionId,
        fixture.database,
        fixture.schema,
        fixture.table,
        1,
        25,
        [defaultFilterExpression(colMeta, filterCase.column, filterCase.value)],
        null,
      );
      assert.equal(
        filtered.rows.length,
        1,
        `${fixture.displayName}: filter on ${filterCase.column} should return one row`,
      );
      const row = filtered.rows[0] as Record<string, unknown>;
      const column = fixture.columns.find(
        (entry) => entry.name === filterCase.column,
      );
      assert.notEqual(
        column,
        undefined,
        `${fixture.displayName}: missing column ${filterCase.column}`,
      );
      if (column) {
        assertCellValue(
          row[column.name],
          fixture.seedValues[column.name],
          column.comparison ?? "exact",
          `${fixture.displayName} filter ${column.name}`,
        );
      }
    }

    const allRows = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      1,
      25,
      [],
      null,
    );
    assert.equal(
      allRows.rows.length,
      3,
      `${fixture.displayName}: expected three rows before delete`,
    );

    const primaryKeys = allRows.rows.map((row) => ({
      [pkName]: (row as Record<string, unknown>)[pkName],
    }));
    await service.deleteRows(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      primaryKeys,
    );

    const afterDelete = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      fixture.table,
      1,
      25,
      [],
      null,
    );
    assert.equal(
      afterDelete.rows.length,
      0,
      `${fixture.displayName}: deleteRows should remove all rows`,
    );

    console.log(`[${fixture.displayName}] done`);
  } finally {
    if (connected) {
      try {
        await runStatements(mainDriver, [...fixture.teardownSql]);
      } catch (err) {
        console.warn(`[${fixture.displayName}] teardown warning:`, err);
      }
    }

    harness.clear(connectionId);
    try {
      await mainDriver.disconnect();
    } catch {}
  }
}

async function main(): Promise<void> {
  const runId =
    process.env.RAPIDB_SMOKE_RUN_ID ?? Date.now().toString(36).slice(-6);
  const requestedKinds = parseRequestedKinds();
  const fixtures = buildFixtures(runId).filter((fixture) =>
    requestedKinds.size === 0 ? true : requestedKinds.has(fixture.kind),
  );

  if (fixtures.length === 0) {
    console.log("No DB smoke test fixtures selected.");
    return;
  }

  console.log(
    `Running DB smoke tests: ${fixtures.map((fixture) => fixture.displayName).join(", ")}`,
  );

  for (const fixture of fixtures) {
    await runFixture(fixture, runId);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
