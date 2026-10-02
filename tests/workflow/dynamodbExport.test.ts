import "./harness";
import { randomUUID } from "node:crypto";
import { readdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  BatchWriteItemCommand,
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  ScanCommand,
  waitUntilTableExists,
} from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import { createProjectTempDir } from "../runtime/tempDirectories";
import { createWorkflowVscodeState } from "./bridge/workflowVscode";
import { workflowState } from "./harness";
import dynamoScenario from "./scenarios/dynamoScenario";
import {
  bootstrapWorkflowContext,
  type OpenTableResult,
} from "./workflowContext";

const enabled =
  !process.env.RAPIDB_WORKFLOW_ENGINES ||
  process.env.RAPIDB_WORKFLOW_ENGINES.split(",")
    .map((id) => id.trim())
    .includes("dynamodb");

describe.runIf(enabled)("dynamodb B04 public TablePanel CSV export", () => {
  it.each([
    "map",
    "list",
  ] as const)("exports actual strings from two Query chunks after sampling %s in another partition", async (sampleType) => {
    const scenario = dynamoScenario();
    const table = `RapiDBB04Type_${randomUUID().replaceAll("-", "")}`;
    scenario.tableFixture.table = table;
    scenario.tableFixture.primaryKey = ["pk", "sk"];
    scenario.tableFixture.database = "us-east-1";
    const connection = scenario.buildConnection();
    const admin = new DynamoDBClient({
      region: connection.awsRegion,
      endpoint: connection.endpoint,
      credentials: { accessKeyId: "rapidb", secretAccessKey: "rapidb-secret" },
    });
    const tempDir = await createProjectTempDir(
      "db-workflow",
      "dynamodb-type-change-",
    );
    const state = createWorkflowVscodeState();
    Object.assign(workflowState, state);
    delete window.__vscode;
    const context = await bootstrapWorkflowContext({
      scenario,
      state: workflowState as unknown as Parameters<
        typeof bootstrapWorkflowContext
      >[0]["state"],
    });
    let created = false;
    let opened: OpenTableResult | undefined;
    const putRows = async (rows: Record<string, unknown>[]) => {
      for (let start = 0; start < rows.length; start += 25) {
        const result = await admin.send(
          new BatchWriteItemCommand({
            RequestItems: {
              [table]: rows
                .slice(start, start + 25)
                .map((row) => ({ PutRequest: { Item: marshall(row) } })),
            },
          }),
        );
        expect(result.UnprocessedItems ?? {}).toEqual({});
      }
    };
    try {
      await admin.send(
        new CreateTableCommand({
          TableName: table,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
        }),
      );
      created = true;
      await waitUntilTableExists(
        { client: admin, maxWaitTime: 20, minDelay: 1, maxDelay: 2 },
        { TableName: table },
      );
      const rows = ["A", "B"].flatMap((pk) =>
        Array.from({ length: 501 }, (_, index) => ({
          pk,
          sk: String(index).padStart(4, "0"),
          value: "hello",
        })),
      );
      await putRows(rows);
      // Scan order is unspecified. Assign sampled/queried roles using the
      // actual SDK scan order, while keeping both partitions' keys unchanged.
      const scan = await admin.send(
        new ScanCommand({ TableName: table, Limit: 1, ConsistentRead: true }),
      );
      const sampledPartition = scan.Items?.[0]?.pk?.S;
      expect(["A", "B"]).toContain(sampledPartition);
      const queriedPartition = sampledPartition === "A" ? "B" : "A";
      await putRows(
        rows
          .filter((row) => row.pk === sampledPartition)
          .map((row) => ({
            ...row,
            value: sampleType === "map" ? { sampled: true } : ["sampled"],
          })),
      );
      const read = vi.spyOn(DynamoDBDriver.prototype, "readTablePage");
      await context.connect();
      const driver = context.connectionManager.getDriver(connection.id);
      if (!driver) throw new Error("Missing DynamoDB driver");
      const described = await driver.describeColumns("us-east-1", "", table);
      expect(described.find((column) => column.name === "value")).toMatchObject(
        {
          nativeType: sampleType,
          category: sampleType === "map" ? "json" : "array",
        },
      );
      opened = await context.openTableViewer();
      await vi.waitFor(() =>
        expect(
          opened?.handle
            .hostMessages()
            .some((message) => message.type === "tableInit"),
        ).toBe(true),
      );
      for (const format of ["csv", "json"] as const) {
        const outputPath = join(tempDir, `strings.${format}`);
        state.showSaveDialog.mockResolvedValue({ fsPath: outputPath });
        state.showInformationMessage.mockClear();
        state.showErrorMessage.mockClear();
        read.mockClear();
        await opened.handle.dispatchMessage({
          type: format === "csv" ? "exportCSV" : "exportJSON",
          payload: {
            filters: [
              { column: "pk", operator: "eq", value: queriedPartition },
            ],
            sort: { column: "sk", direction: "asc" },
            columnOrder: ["pk", "sk", "value"],
          },
        });
        await vi.waitFor(
          () =>
            expect(
              state.showInformationMessage.mock.calls.length +
                state.showErrorMessage.mock.calls.length,
            ).toBe(1),
          { timeout: 20000 },
        );
        expect(state.showErrorMessage).not.toHaveBeenCalled();
        const output = await readFile(outputPath, "utf8");
        if (format === "csv") {
          const lines = output.trimEnd().split("\n");
          expect(lines).toHaveLength(502);
          expect(lines[0]).toBe("pk,sk,value");
          for (let index = 0; index < 501; index++) {
            expect(lines[index + 1]).toBe(
              `${queriedPartition},${String(index).padStart(4, "0")},hello`,
            );
          }
        } else {
          expect(JSON.parse(output)).toEqual(
            rows.filter((row) => row.pk === queriedPartition),
          );
        }
        const exportCalls = read.mock.calls
          .map(([request], index) => ({
            request,
            result: read.mock.results[index],
          }))
          .filter(({ request }) => request.pageSize === 500);
        expect(exportCalls.map(({ request }) => request.page)).toEqual([1, 2]);
        for (const { result } of exportCalls) {
          const page = await result?.value;
          expect(
            page.columns.find(
              (column: { name: string }) => column.name === "value",
            ),
          ).toMatchObject({ nativeType: "string", category: "text" });
          expect(
            page.rows.every(
              (row: Record<string, unknown>) => row.value === "hello",
            ),
          ).toBe(true);
        }
      }
    } finally {
      opened?.session.unmount();
      TablePanel.disposeAll();
      try {
        await context.dispose();
      } finally {
        try {
          if (created)
            await admin.send(new DeleteTableCommand({ TableName: table }));
        } finally {
          admin.destroy();
          await rm(tempDir, { recursive: true, force: true });
        }
      }
    }
  }, 60000);

  it("exports all 1001 sparse items in three 500-row chunks with stable visible columns", async () => {
    const scenario = dynamoScenario();
    const table = `RapiDBB04_${randomUUID().replaceAll("-", "")}`;
    scenario.tableFixture.table = table;
    scenario.tableFixture.primaryKey = ["pk", "sk"];
    scenario.tableFixture.database = "us-east-1";
    const connection = scenario.buildConnection();
    const admin = new DynamoDBClient({
      region: connection.awsRegion,
      endpoint: connection.endpoint,
      credentials: { accessKeyId: "rapidb", secretAccessKey: "rapidb-secret" },
    });
    const tempDir = await createProjectTempDir(
      "db-workflow",
      "dynamodb-export-",
    );
    const outputPath = join(tempDir, "sparse.csv");
    const state = createWorkflowVscodeState();
    state.showSaveDialog.mockResolvedValue({ fsPath: outputPath });
    Object.assign(workflowState, state);
    delete window.__vscode;
    const context = await bootstrapWorkflowContext({
      scenario,
      state: workflowState as unknown as Parameters<
        typeof bootstrapWorkflowContext
      >[0]["state"],
    });
    let created = false;
    let opened: OpenTableResult | undefined;
    try {
      await admin.send(
        new CreateTableCommand({
          TableName: table,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
        }),
      );
      created = true;
      await waitUntilTableExists(
        { client: admin, maxWaitTime: 20, minDelay: 1, maxDelay: 2 },
        { TableName: table },
      );
      for (let start = 0; start < 1001; start += 25) {
        const result = await admin.send(
          new BatchWriteItemCommand({
            RequestItems: {
              [table]: Array.from(
                { length: Math.min(25, 1001 - start) },
                (_, offset) => {
                  const index = start + offset;
                  return {
                    PutRequest: {
                      Item: marshall({
                        pk: "audit",
                        sk: String(index).padStart(4, "0"),
                        ...(index < 500
                          ? { x: `X${index}` }
                          : { y: `Y${index}` }),
                      }),
                    },
                  };
                },
              ),
            },
          }),
        );
        expect(result.UnprocessedItems ?? {}).toEqual({});
      }
      const read = vi.spyOn(DynamoDBDriver.prototype, "readTablePage");
      await context.connect();
      const driver = context.connectionManager.getDriver(connection.id);
      if (!driver?.readTablePage) throw new Error("Missing DynamoDB reader");
      const described = await driver.describeColumns("us-east-1", "", table);
      expect(described.map((column) => column.name)).toEqual([
        "pk",
        "sk",
        "x",
        "y",
      ]);
      opened = await context.openTableViewer();
      await vi.waitFor(() =>
        expect(
          opened?.handle
            .hostMessages()
            .some((message) => message.type === "tableInit"),
        ).toBe(true),
      );
      read.mockClear();
      const payload = {
        filters: [{ column: "pk", operator: "eq", value: "audit" }],
        sort: { column: "sk", direction: "asc" },
        columnOrder: ["y", "sk", "x", "pk"],
      };
      await opened.handle.dispatchMessage({ type: "exportCSV", payload });
      await vi.waitFor(
        () =>
          expect(
            state.showInformationMessage.mock.calls.length +
              state.showErrorMessage.mock.calls.length,
          ).toBe(1),
        { timeout: 20000 },
      );
      expect(state.showErrorMessage).not.toHaveBeenCalled();
      const lines = (await readFile(outputPath, "utf8")).trimEnd().split("\n");
      expect(lines).toHaveLength(1002);
      expect(lines[0]).toBe("y,sk,x,pk");
      for (let index = 0; index < 1001; index++) {
        expect(lines[index + 1]).toBe(
          index < 500
            ? `,${String(index).padStart(4, "0")},X${index},audit`
            : `Y${index},${String(index).padStart(4, "0")},,audit`,
        );
      }
      expect(
        read.mock.calls
          .filter(([request]) => request.pageSize === 500)
          .map(([request]) => request.page),
      ).toEqual([1, 2, 3]);
      for (const [request] of read.mock.calls.filter(
        ([request]) => request.pageSize === 500,
      )) {
        expect(request.sort).toEqual(payload.sort);
        expect(request.filters).toEqual(payload.filters);
      }

      // An explicit visible-column selection intentionally excludes y.
      state.showInformationMessage.mockClear();
      await opened.handle.dispatchMessage({
        type: "exportCSV",
        payload: { ...payload, columnOrder: ["sk", "x"] },
      });
      await vi.waitFor(
        () => expect(state.showInformationMessage).toHaveBeenCalledOnce(),
        { timeout: 20000 },
      );
      const hiddenLines = (await readFile(outputPath, "utf8"))
        .trimEnd()
        .split("\n");
      expect(hiddenLines).toHaveLength(1002);
      expect(hiddenLines[0]).toBe("sk,x");
      expect(hiddenLines[501]).toBe("0500,");
      expect(hiddenLines[1001]).toBe("1000,");

      await writeFile(outputPath, "old export\n");
      state.showInformationMessage.mockClear();
      await opened.handle.dispatchMessage({
        type: "exportCSV",
        payload: { ...payload, columnOrder: ["unknown"] },
      });
      await vi.waitFor(
        () =>
          expect(state.showErrorMessage).toHaveBeenCalledWith(
            expect.stringContaining("unknown"),
          ),
        { timeout: 20000 },
      );
      expect(state.showInformationMessage).not.toHaveBeenCalled();
      expect(await readFile(outputPath, "utf8")).toBe("old export\n");
      expect(await readdir(tempDir)).toEqual(["sparse.csv"]);
    } finally {
      opened?.session.unmount();
      TablePanel.disposeAll();
      await context.dispose();
      try {
        if (created)
          await admin.send(new DeleteTableCommand({ TableName: table }));
      } finally {
        admin.destroy();
        await rm(tempDir, { recursive: true, force: true });
      }
    }
  }, 60000);
});
