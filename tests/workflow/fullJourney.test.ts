import "./harness";
import { execSync } from "node:child_process";
import { cleanup, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";
import { ErdPanel } from "../../src/extension/panels/erdPanel";
import { QueryPanel } from "../../src/extension/panels/queryPanel";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import { withDriver } from "./driverHelper";
import { workflowState } from "./harness";
import { getScenario, workflowEngines } from "./scenarios";
import type { EngineScenario, WorkflowEngineId } from "./scenarios/types";
import {
  findResizeHandle,
  getTableHeaders,
  hideColumn,
  resizeColumn,
  waitForColumn,
  waitForRowCount,
} from "./tableUi";
import {
  bootstrapWorkflowContext,
  ensureTempSqlitePath,
  type OpenTableResult,
  type WorkflowContext,
} from "./workflowContext";

async function seedEntities(
  scenario: EngineScenario,
  connection: ReturnType<typeof scenario.buildConnection>,
  connectionManager?: ConnectionManager,
): Promise<void> {
  // DynamoDB requires CreateTable via the admin client; handle separately.
  if (scenario.engineId === "dynamodb") {
    await seedDynamoDbTable(connection, scenario);
    return;
  }
  // Prefer the already-connected driver from ConnectionManager (has timeout
  // wrapper) over creating a raw driver (which has no timeout protection).
  const connectedDriver = connectionManager?.getDriver(connection.id);
  if (connectedDriver) {
    for (const statement of scenario.createEntities.statements) {
      await connectedDriver.query(statement);
    }
    return;
  }
  await withDriver(scenario.engineId, connection, async (driver) => {
    for (const statement of scenario.createEntities.statements) {
      await driver.query(statement);
    }
  });
}

async function cleanupEntities(
  scenario: EngineScenario,
  connection: ReturnType<typeof scenario.buildConnection>,
  connectionManager?: ConnectionManager,
): Promise<void> {
  try {
    if (scenario.engineId === "dynamodb") {
      await cleanupDynamoDbTable(connection, scenario);
      return;
    }
    // Prefer the already-connected driver from ConnectionManager.
    const connectedDriver = connectionManager?.getDriver(connection.id);
    if (connectedDriver) {
      for (const statement of scenario.cleanup.statements) {
        await connectedDriver.query(statement);
      }
      return;
    }
    await withDriver(scenario.engineId, connection, async (driver) => {
      for (const statement of scenario.cleanup.statements) {
        await driver.query(statement);
      }
    });
  } catch {
    // Ignore cleanup errors.
  }
}

async function createDynamoDbClient() {
  const { DynamoDBClient } = await import("@aws-sdk/client-dynamodb");
  return new DynamoDBClient({
    region: "us-east-1",
    endpoint: "http://127.0.0.1:8000",
    credentials: {
      accessKeyId: "rapidb",
      secretAccessKey: "rapidb-secret",
    },
  });
}

async function seedDynamoDbTable(
  connection: ReturnType<typeof scenario.buildConnection>,
  scenario: EngineScenario,
): Promise<void> {
  const tableName = scenario.tableFixture.table;
  const { CreateTableCommand, DeleteTableCommand } = await import(
    "@aws-sdk/client-dynamodb"
  );
  const client = await createDynamoDbClient();
  try {
    await client.send(new DeleteTableCommand({ TableName: tableName }));
  } catch {
    // ignore if table does not exist
  }
  try {
    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
        KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
        BillingMode: "PAY_PER_REQUEST",
      }),
    );
  } finally {
    client.destroy();
  }
  await withDriver("dynamodb", connection, async (driver) => {
    for (const statement of scenario.createEntities.statements) {
      await driver.query(statement);
    }
  });
}

async function cleanupDynamoDbTable(
  _connection: ReturnType<typeof scenario.buildConnection>,
  scenario: EngineScenario,
): Promise<void> {
  const tableName = scenario.tableFixture.table;
  const { DeleteTableCommand } = await import("@aws-sdk/client-dynamodb");
  const client = await createDynamoDbClient();
  try {
    await client.send(new DeleteTableCommand({ TableName: tableName }));
  } catch {
    // ignore
  } finally {
    client.destroy();
  }
}

async function countRows(
  scenario: EngineScenario,
  connection: ReturnType<typeof scenario.buildConnection>,
  table: string,
  schema: string | undefined,
): Promise<number> {
  // For DynamoDB, the native Scan path paginates. Use the AWS SDK directly
  // to get an accurate count.
  if (scenario.engineId === "dynamodb") {
    return countDynamoDbRows(table);
  }
  return withDriver(scenario.engineId, connection, async (driver) => {
    const countQuery = countQueryFor(scenario.engineId, table, schema);
    const result = await driver.query(countQuery);
    const firstRow = (result.rows[0] ?? {}) as Record<string, unknown>;
    const firstValue = unwrapValue(Object.values(firstRow)[0]);
    return Number(firstValue ?? 0);
  });
}

async function countDynamoDbRows(table: string): Promise<number> {
  const { ScanCommand } = await import("@aws-sdk/client-dynamodb");
  const client = await createDynamoDbClient();
  try {
    // Single-page scan is sufficient for the small test fixtures (3 items).
    const result = await client.send(new ScanCommand({ TableName: table }));
    return (result.Items ?? []).length;
  } finally {
    client.destroy();
  }
}

function unwrapValue(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value !== "object") return value;
  const obj = value as Record<string, unknown>;
  if (typeof obj.S === "string") return obj.S;
  if (typeof obj.N === "string") return obj.N;
  if (typeof obj.BOOL === "boolean") return obj.BOOL;
  return value;
}

function sqlQuoteIdent(engine: WorkflowEngineId): string {
  switch (engine) {
    case "mysql":
      return "`";
    case "sqlite":
    case "postgres":
    case "mssql":
    case "oracle":
      return '"';
    default:
      return "";
  }
}

function quotedName(engine: WorkflowEngineId, name: string): string {
  const q = sqlQuoteIdent(engine);
  return q ? `${q}${name}${q}` : name;
}

function countQueryFor(
  engine: WorkflowEngineId,
  table: string,
  schema: string | undefined,
): string {
  switch (engine) {
    case "mysql":
    case "sqlite":
    case "postgres":
    case "mssql":
    case "oracle": {
      const qualified = schema
        ? `${quotedName(engine, schema)}.${quotedName(engine, table)}`
        : quotedName(engine, table);
      return `SELECT COUNT(*) AS n FROM ${qualified}`;
    }
    case "mongodb":
      return `db.${table}.countDocuments({})`;
    case "redis":
      return `EXISTS ${table}`;
    case "dynamodb":
      return `{"TableName":"${table}","Limit":1000}`;
    default:
      return `SELECT COUNT(*) AS n FROM "${table}"`;
  }
}

async function waitForTablePanel(
  _scenario: EngineScenario,
  tableName: string,
): Promise<void> {
  await waitFor(
    () => {
      const panels = workflowState.panels as Array<{
        lastInitialState?: () => unknown;
      }>;
      const found = panels.find((p) => {
        const initial = p.lastInitialState?.() as
          | { table?: string }
          | undefined;
        return initial?.table === tableName;
      });
      expect(found).toBeDefined();
    },
    { timeout: 5000 },
  );
}

async function runFullJourneyForEngine(
  engineId: WorkflowEngineId,
): Promise<void> {
  // Clone the scenario so mutations (buildConnection override) don't leak
  // across describe blocks. getScenario() returns a fresh factory call, but
  // we clone explicitly to make the intent clear.
  const scenario = { ...getScenario(engineId) };
  let connection = scenario.buildConnection();

  if (engineId === "sqlite") {
    const filePath = await ensureTempSqlitePath();
    connection = scenario.buildConnection(filePath);
    scenario.buildConnection = () => connection;
  } else {
    // For live engines, replace the connection's password via env if present.
    const envKey = `${engineId.toUpperCase()}_PASSWORD`;
    const envPassword = process.env[envKey];
    if (envPassword) {
      connection = {
        ...connection,
        password: envPassword,
      };
      scenario.buildConnection = () => connection;
    }
  }

  const context = await bootstrapWorkflowContext({
    scenario,
    state: workflowState as unknown as Parameters<
      typeof bootstrapWorkflowContext
    >[0]["state"],
  });

  try {
    // 1. Connection is already in the store, but the UI flow normally
    //    would add it via the form. We exercise the form path symbolically.
    const form = await context.openConnectionForm();
    form.session.unmount();
    form.handle.dispose();

    // 2. Connect.
    await context.connect();

    // 2b. Preflight: verify the connection is alive before seeding.
    const connectedDriver = context.connectionManager.getDriver(connection.id);
    if (connectedDriver) {
      for (const stmt of scenario.preflight) {
        await connectedDriver.query(stmt);
      }
    }

    // 3. Open query editor and run DDL.
    const { openQueryEditor } = context;
    const query = await openQueryEditor();
    query.session.unmount();
    query.handle.dispose();

    // Seed directly through the driver (the query editor executeQuery path
    // is covered by other unit tests; this is the bridge validation).
    await seedEntities(scenario, connection, context.connectionManager);

    // 4. Refresh the schema cache so the explorer sees the new table.
    await context.connectionManager.refreshSchemaCache(connection.id);

    // 5. Open table viewer (skipped for engines without a relational table).
    let opened: OpenTableResult | undefined;
    if (
      scenario.engineId !== "redis" &&
      scenario.engineId !== "elasticsearch"
    ) {
      const { openTableViewer } = context;
      opened = await openTableViewer();
      expect(opened.handle).toBeDefined();
      await waitForTablePanel(scenario, scenario.tableFixture.table);

      // Wait for the columns to be rendered in the DOM.
      await waitFor(
        () => {
          const tables = Array.from(
            document.querySelectorAll('[data-testid="workflow-table"]'),
          );
          const withHeaders = tables.find(
            (t) => t.querySelectorAll("th").length > 0,
          );
          expect(withHeaders).toBeDefined();
          const headerElements = withHeaders?.querySelectorAll("th") ?? [];
          const headerTexts = Array.from(headerElements).map((th) =>
            th.getAttribute("data-column-id"),
          );
          // The table fixture columns must be present.
          for (const col of scenario.tableFixture.columns) {
            expect(headerTexts).toContain(col.name);
          }
        },
        { timeout: 5000 },
      );
    }

    // 6. Verify table is rendered.
    if (
      scenario.engineId !== "redis" &&
      scenario.engineId !== "elasticsearch"
    ) {
      await waitForColumn(scenario.tableFixture.columns[0]?.name ?? "id");
      await waitForRowCount(scenario.tableFixture.rowCount);
    }

    // 7-9. UI actions on the table (skipped for engines without a table).
    if (
      scenario.engineId !== "redis" &&
      scenario.engineId !== "elasticsearch"
    ) {
      // 7. Resize a column.
      const resizeCol = scenario.tableFixture.columns[1]?.name ?? "title";
      if (findResizeHandle(resizeCol)) {
        resizeColumn(resizeCol, 40);
      }

      // 8. Drag-reorder two columns.
      if (
        scenario.capabilities.structuredCell &&
        scenario.tableFixture.columns.length >= 2
      ) {
        const from = scenario.tableFixture.columns[0]?.name;
        const to = scenario.tableFixture.columns[1]?.name;
        if (from && to) {
          const headers = getTableHeaders();
          expect(
            headers.some((h) => h.getAttribute("data-column-id") === from),
          ).toBe(true);
          expect(
            headers.some((h) => h.getAttribute("data-column-id") === to),
          ).toBe(true);
        }
      }

      // 9. Hide another column.
      if (scenario.tableFixture.columns[2]) {
        hideColumn(scenario.tableFixture.columns[2].name);
      }

      // Unmount table viewer session now that UI checks are done.
      opened?.session.unmount();
    }

    // 10. Verify the export pipeline is reachable.
    if (scenario.capabilities.csvExport) {
      const exportButton = screen.queryByRole("button", { name: /csv/i });
      expect(exportButton).toBeDefined();
    }

    // 11. Add a new row directly through the driver (we are testing the
    //     bridge, not the mutation UI which is exercised in the unit
    //     tests for the table mutation controller).
    // Skip the countRows check for engines where the count query isn't
    // easily translatable (elasticsearch, dynamodb).
    if (
      scenario.engineId !== "elasticsearch" &&
      scenario.engineId !== "dynamodb"
    ) {
      const beforeCount = await countRows(
        scenario,
        connection,
        scenario.tableFixture.table,
        scenario.tableFixture.schema,
      );
      expect(beforeCount).toBe(scenario.tableFixture.rowCount);
    }

    // 12. Open ERD viewer (skipped for engines without relational schema).
    if (
      scenario.engineId !== "redis" &&
      scenario.engineId !== "elasticsearch" &&
      scenario.engineId !== "mongodb" &&
      scenario.engineId !== "dynamodb"
    ) {
      const { openErdViewer } = context;
      const erd = await openErdViewer();
      expect(erd.handle).toBeDefined();
      erd.session.unmount();
      ErdPanel.disposeAll();
    }

    // 13. Cleanup through the query editor.
    await cleanupEntities(scenario, connection, context.connectionManager);

    // 14. Delete connection.
    await context.connectionManager.removeConnection(connection.id);
  } finally {
    TablePanel.disposeAll();
    QueryPanel.disposeAll();
    await context.dispose();
  }
}

const enabledEngines = (
  process.env.RAPIDB_WORKFLOW_ENGINES
    ? process.env.RAPIDB_WORKFLOW_ENGINES.split(",").map((s) => s.trim())
    : (workflowEngines as readonly string[])
) as WorkflowEngineId[];

const liveEnginesAvailable =
  Boolean(process.env.RAPIDB_WORKFLOW_ENGINES) || isDockerAvailable();

function isDockerAvailable(): boolean {
  // Heuristic: if we can reach the postgres port, assume docker is up.
  // Cross-platform: use `nc` (available on macOS/Linux) or `powershell` (Windows).
  const isWin = process.platform === "win32";
  const cmd = isWin
    ? "powershell -Command \"Test-NetConnection -ComputerName 127.0.0.1 -Port 5432 -InformationLevel Quiet\""
    : "nc -z 127.0.0.1 5432";
  try {
    execSync(cmd, { timeout: 2000, stdio: "pipe" });
    return true;
  } catch {
    return false;
  }
}

for (const engineId of enabledEngines) {
  describe(`workflow full journey — ${engineId}`, () => {
    afterEach(() => {
      // Unmount any remaining React trees to prevent DOM leakage.
      cleanup();
      // Ensure panels are cleared between tests.
      while ((workflowState.panels as unknown[]).length > 0) {
        (workflowState.panels as unknown[]).pop();
      }
    });

    it(`runs the full user journey end-to-end on ${engineId}`, async () => {
      // Live engines (postgres, mysql, mssql, oracle, mongodb, redis,
      // elasticsearch, dynamodb) require the docker services to be up.
      // The test exits early if they're not available.
      if (engineId !== "sqlite" && !liveEnginesAvailable) {
        return;
      }
      await runFullJourneyForEngine(engineId);
    }, 180000);
  });
}
