import assert from "node:assert/strict";
import { rmSync } from "node:fs";
import type {
  ConnectionConfig,
  ConnectionManager,
} from "../src/extension/connectionManager";
import { MSSQLDriver } from "../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../src/extension/dbDrivers/sqlite";
import type {
  ColumnTypeMeta,
  FilterExpression,
  IDBDriver,
} from "../src/extension/dbDrivers/types";
import {
  formatDatetimeForDisplay,
  TableDataService,
} from "../src/extension/tableDataService";
import { defaultFilterOperator } from "../src/shared/tableTypes";
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
    value === "oracle" ||
    value === "sqlite"
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
    case "sqlite":
      return new SQLiteDriver({ ...config, id: "__smoke__" });
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
    case "sqlite":
      return `"${name}"`;
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

  return {
    column: columnName,
    operator: column ? defaultFilterOperator(column) : "like",
    value,
  };
}

function assertCellValue(
  actual: unknown,
  expected: unknown,
  comparison: ComparisonMode,
  context: string,
): void {
  if (expected === null || expected === undefined) {
    assert.equal(actual ?? null, expected ?? null, `${context}: values differ`);
    return;
  }

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
    if (column.readbackOnly) {
      continue;
    }
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

function assertReadbackChecks(
  row: Record<string, unknown>,
  fixture: DbFixture,
  label: string,
): void {
  for (const check of fixture.readbackChecks) {
    assertCellValue(
      row[check.column],
      check.expected,
      check.comparison,
      `${label}.${check.column}`,
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
    if (expectation.editable !== undefined) {
      assert.equal(
        column.editable,
        expectation.editable,
        `${fixture.displayName}: unexpected editable for ${expectation.column}`,
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

function supportsFunctions(fixture: DbFixture): boolean {
  return fixture.supportsFunctions !== false;
}

function supportsProcedures(fixture: DbFixture): boolean {
  return fixture.supportsProcedures !== false;
}

async function runSqliteExtendedFlow(
  fixture: DbFixture,
  connectionId: string,
  service: TableDataService,
  driver: IDBDriver,
): Promise<void> {
  if (fixture.kind !== "sqlite") {
    return;
  }

  const docsTable = `${fixture.table}_docs`;
  console.log(`[${fixture.displayName}] running SQLite composite-key scenario`);

  try {
    await execSql(
      driver,
      `DROP TABLE IF EXISTS "${docsTable}";
       CREATE TABLE "${docsTable}" (
         tenant_id INTEGER NOT NULL,
         user_id INTEGER NOT NULL,
         external_id UUID NOT NULL,
         payload JSON NOT NULL,
         PRIMARY KEY (tenant_id, user_id)
       );
       INSERT INTO "${docsTable}" (user_id, tenant_id, external_id, payload)
       VALUES
         (2, 1, 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', '{"name":"second"}'),
         (1, 2, 'cccccccc-cccc-4ccc-8ccc-cccccccccccc', '{"name":"third"}'),
         (1, 1, 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', '{"name":"first"}');`,
    );

    const docsColumns = await service.getColumns(
      connectionId,
      fixture.database,
      fixture.schema,
      docsTable,
    );

    assert.deepEqual(
      docsColumns.map((column) => column.name),
      ["tenant_id", "user_id", "external_id", "payload"],
      `${fixture.displayName}: docs column order mismatch`,
    );
    const payloadColumn = docsColumns.find(
      (column) => column.name === "payload",
    );
    const externalIdColumn = docsColumns.find(
      (column) => column.name === "external_id",
    );
    const tenantIdColumn = docsColumns.find(
      (column) => column.name === "tenant_id",
    );
    const userIdColumn = docsColumns.find(
      (column) => column.name === "user_id",
    );

    assert.ok(
      payloadColumn,
      `${fixture.displayName}: docs payload metadata missing`,
    );
    assert.ok(
      externalIdColumn,
      `${fixture.displayName}: docs external_id metadata missing`,
    );
    assert.ok(
      tenantIdColumn,
      `${fixture.displayName}: docs tenant_id metadata missing`,
    );
    assert.ok(
      userIdColumn,
      `${fixture.displayName}: docs user_id metadata missing`,
    );

    assert.equal(payloadColumn?.category, "json");
    assert.deepEqual(payloadColumn?.filterOperators, [
      "like",
      "in",
      "is_null",
      "is_not_null",
    ]);
    assert.equal(externalIdColumn?.category, "uuid");
    assert.deepEqual(externalIdColumn?.filterOperators, [
      "like",
      "in",
      "is_null",
      "is_not_null",
    ]);
    assert.equal(tenantIdColumn?.isPrimaryKey, true);
    assert.equal(tenantIdColumn?.primaryKeyOrdinal, 1);
    assert.equal(userIdColumn?.isPrimaryKey, true);
    assert.equal(userIdColumn?.primaryKeyOrdinal, 2);

    await service.insertRow(
      connectionId,
      fixture.database,
      fixture.schema,
      docsTable,
      {
        user_id: 2,
        tenant_id: 2,
        external_id: "550e8400-e29b-41d4-a716-446655440000",
        payload: '{"name":"inserted"}',
      },
    );

    await service.updateRow(
      connectionId,
      fixture.database,
      fixture.schema,
      docsTable,
      { tenant_id: 2, user_id: 2 },
      { payload: '{"name":"updated"}' },
    );

    const orderedPage = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      docsTable,
      1,
      50,
      [],
      null,
    );
    assert.deepEqual(
      orderedPage.rows.map(
        (row) => `${String(row.tenant_id)}:${String(row.user_id)}`,
      ),
      ["1:1", "1:2", "2:1", "2:2"],
      `${fixture.displayName}: composite PK default ordering mismatch`,
    );

    const filteredPage = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      docsTable,
      1,
      50,
      [{ column: "external_id", operator: "like", value: "550e8400" }],
      null,
    );
    assert.deepEqual(filteredPage.rows, [
      {
        tenant_id: 2,
        user_id: 2,
        external_id: "550e8400-e29b-41d4-a716-446655440000",
        payload: '{"name":"updated"}',
      },
    ]);

    await service.deleteRows(
      connectionId,
      fixture.database,
      fixture.schema,
      docsTable,
      [
        { tenant_id: 1, user_id: 2 },
        { tenant_id: 2, user_id: 2 },
      ],
    );

    const afterDelete = await service.getPage(
      connectionId,
      fixture.database,
      fixture.schema,
      docsTable,
      1,
      50,
      [],
      null,
    );
    assert.deepEqual(
      afterDelete.rows.map(
        (row) => `${String(row.tenant_id)}:${String(row.user_id)}`,
      ),
      ["1:1", "2:1"],
      `${fixture.displayName}: composite PK deleteRows should use a transaction and keep the expected rows`,
    );
  } finally {
    await execSql(driver, `DROP TABLE IF EXISTS "${docsTable}";`);
  }
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
    if (
      supportsFunctions(fixture) &&
      isMeaningfulSql(fixture.createFunctionSql)
    ) {
      await execSql(mainDriver, fixture.createFunctionSql);
    }
    if (
      supportsProcedures(fixture) &&
      isMeaningfulSql(fixture.createProcedureSql)
    ) {
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
    assertReadbackChecks(
      page.rows[0] as Record<string, unknown>,
      fixture,
      `${fixture.displayName} seed readback`,
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

    if (supportsFunctions(fixture)) {
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
    }

    if (supportsProcedures(fixture)) {
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
    }

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
    assert.ok(
      hasTable,
      `${fixture.displayName}: listObjects should include the table`,
    );
    assert.ok(
      hasView,
      `${fixture.displayName}: listObjects should include the view`,
    );
    if (supportsFunctions(fixture)) {
      const hasFunction = objects.some(
        (object) =>
          object.name.toLowerCase() === fixture.functionName.toLowerCase() &&
          object.type === "function",
      );
      assert.ok(
        hasFunction,
        `${fixture.displayName}: listObjects should include the function`,
      );
    }
    if (supportsProcedures(fixture)) {
      const hasProcedure = objects.some(
        (object) =>
          object.name.toLowerCase() === fixture.procedureName.toLowerCase() &&
          object.type === "procedure",
      );
      assert.ok(
        hasProcedure,
        `${fixture.displayName}: listObjects should include the procedure`,
      );
    }

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
    assertReadbackChecks(
      page.rows[0] as Record<string, unknown>,
      fixture,
      `${fixture.displayName} restored readback`,
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
    if (supportsProcedures(fixture)) {
      await execSql(mainDriver, fixture.procedureCallSql);
    } else if (fixture.nullFilterRowValues) {
      await service.insertRow(
        connectionId,
        fixture.database,
        fixture.schema,
        fixture.table,
        fixture.nullFilterRowValues,
      );
    }

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
          column.seedValue,
          column.comparison ?? "exact",
          `${fixture.displayName} filter ${column.name}`,
        );
      }
    }

    await runSqliteExtendedFlow(fixture, connectionId, service, mainDriver);

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
    if (fixture.kind === "sqlite" && fixture.connection.filePath) {
      rmSync(fixture.connection.filePath, { force: true });
    }
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
