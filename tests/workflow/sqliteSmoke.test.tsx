import "./harness";
import { describe, expect, it } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import { withDriver } from "./driverHelper";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";
import {
  bootstrapWorkflowContext,
  ensureTempSqlitePath,
} from "./workflowContext";

describe("sqlite workflow smoke", () => {
  it("creates a temp sqlite db, runs DDL through the driver, opens the table viewer", async () => {
    TablePanel.disposeAll();
    const scenario = getScenario("sqlite");
    const filePath = await ensureTempSqlitePath();
    const connection = scenario.buildConnection(filePath);
    scenario.buildConnection = () => connection;
    const context = await bootstrapWorkflowContext({
      scenario,
      state: workflowState as unknown as Parameters<
        typeof bootstrapWorkflowContext
      >[0]["state"],
    });

    try {
      const { openTableViewer } = context;
      await withDriver("sqlite", connection, async (driver) => {
        for (const statement of scenario.createEntities.statements) {
          await driver.query(statement);
        }
      });

      const opened = await openTableViewer();
      expect(opened.handle).toBeDefined();
    } finally {
      await context.dispose();
    }
  });
});
