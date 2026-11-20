import "./harness";
import { readFile, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { join } from "node:path";
import type {
  AttributeValue,
  QueryCommandInput,
} from "@aws-sdk/client-dynamodb";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import { createProjectTempDir } from "../runtime/tempDirectories";
import { createWorkflowVscodeState } from "./bridge/workflowVscode";
import { workflowState } from "./harness";
import dynamoScenario from "./scenarios/dynamoScenario";
import { getBodyCell, hideColumn } from "./tableUi";
import type { OpenTableResult } from "./workflowContext";

vi.doUnmock("../../src/webview/components/table/TableGrid");

type Item = Record<string, AttributeValue>;

function items(partition: string, size: number): Item[] {
  return Array.from({ length: size }, (_, index) => {
    const suffix = `${index}`.padStart(4, "0");
    return {
      pk: { S: partition },
      sk: { S: suffix },
      label: { S: `${partition}-${suffix}` },
    };
  });
}

function header(name: string) {
  return [...document.querySelectorAll("thead tr:first-child th")].find(
    (cell) => cell.getAttribute("data-column-id") === name,
  );
}

it("dynamodb shows attributes outside the schema sample on the page that contains them and exports the visible columns", async () => {
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const sampled = items("sample", 1000);
  const sparse = items("sparse", 27);
  Object.assign(sparse[25], {
    __sel: { S: "stored selection name" },
    __col_0: { S: "stored alias name" },
    constructor: { S: "stored constructor name" },
    "profile.name": { N: "9007199254740993" },
    detail: { M: { ranks: { NS: ["9007199254740993"] } } },
    empty: { NULL: true },
  });
  const state = createWorkflowVscodeState();
  state.showWarningMessage.mockResolvedValue("Export Anyway");
  Object.assign(workflowState, state);
  delete window.__vscode;
  const scenario = dynamoScenario();
  const table = scenario.tableFixture.table;
  const respond = (operation: string | undefined, input: QueryCommandInput) => {
    if (operation === "DynamoDB_20120810.ListTables")
      return { TableNames: [table] };
    if (operation === "DynamoDB_20120810.DescribeTable") {
      return {
        Table: {
          TableName: table,
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "sk", KeyType: "RANGE" },
          ],
          AttributeDefinitions: [
            { AttributeName: "pk", AttributeType: "S" },
            { AttributeName: "sk", AttributeType: "S" },
          ],
        },
      };
    }
    let rows: Item[];
    if (operation === "DynamoDB_20120810.Scan") {
      rows = [...sampled, ...sparse];
    } else if (operation === "DynamoDB_20120810.Query") {
      const partition = Object.values(input.ExpressionAttributeValues ?? {})[0]
        ?.S;
      rows = partition === "sparse" ? sparse : sampled;
      if (input.ScanIndexForward === false) rows = [...rows].reverse();
    } else {
      throw new Error(`Unexpected request: ${operation}`);
    }
    if (input.Select === "COUNT") return { Count: rows.length };
    const cursor = input.ExclusiveStartKey;
    const offset = cursor
      ? rows.findIndex(
          (row) => row.pk.S === cursor.pk.S && row.sk.S === cursor.sk.S,
        ) + 1
      : 0;
    const end = offset + (input.Limit ?? rows.length);
    const last = rows[end - 1];
    return {
      Items: rows.slice(offset, end),
      ...(last && end < rows.length
        ? { LastEvaluatedKey: { pk: last.pk, sk: last.sk } }
        : {}),
    };
  };
  const unexpectedRequests: unknown[] = [];
  const server = createServer(async (request, response) => {
    try {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(chunk);
      const result = respond(
        request.headers["x-amz-target"] as string | undefined,
        JSON.parse(Buffer.concat(chunks).toString("utf8")),
      );
      response.writeHead(200, { "content-type": "application/x-amz-json-1.0" });
      response.end(JSON.stringify(result));
    } catch (error) {
      unexpectedRequests.push(error);
      response.writeHead(400).end("{}");
    }
  });
  const tempDir = await createProjectTempDir("db-workflow", "dynamo-sparse-");
  const read = vi.spyOn(DynamoDBDriver.prototype, "readTablePage");
  let context: Awaited<ReturnType<typeof bootstrapWorkflowContext>> | undefined;
  let opened: OpenTableResult | undefined;
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject).listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (!address || typeof address === "string")
      throw new Error("No fixture port");
    const connection = {
      ...scenario.buildConnection(),
      endpoint: `http://127.0.0.1:${address.port}`,
    };
    scenario.buildConnection = () => connection;
    context = await bootstrapWorkflowContext({
      scenario,
      state: workflowState as unknown as Parameters<
        typeof bootstrapWorkflowContext
      >[0]["state"],
    });
    await context.connect();
    opened = await context.openTableViewer();
    const { session } = opened;
    await screen.findByText("sample-0000");
    const init = session
      .hostMessages()
      .find(({ type }) => type === "tableInit");
    expect(init?.payload).toMatchObject({
      columns: ["pk", "sk", "label"].map((name) =>
        expect.objectContaining({ name }),
      ),
      primaryKeyColumns: ["pk", "sk"],
    });
    const filter = (value: string) => {
      const input = screen.getByLabelText("pk filter value");
      fireEvent.change(input, { target: { value } });
      fireEvent.keyDown(input, { key: "Enter" });
    };
    await session.user.click(
      screen.getByRole("button", { name: "pk filter operator" }),
    );
    await session.user.click(
      screen.getByRole("menuitemradio", { name: /equals/i }),
    );
    filter("sparse");
    await screen.findByText("sparse-0000");
    expect(header("profile.name")).toBeUndefined();
    const next = () =>
      session.user.click(screen.getByRole("button", { name: "Next →" }));
    await next();
    await screen.findByText("sparse-0025");
    expect(getBodyCell("__sel").textContent).toBe("stored selection name");
    expect(getBodyCell("__col_0").textContent).toBe("stored alias name");
    expect(getBodyCell("constructor").textContent).toBe(
      "stored constructor name",
    );
    expect(getBodyCell("profile.name").textContent).toBe("9007199254740993");
    expect(screen.getAllByRole("checkbox")).toHaveLength(3);
    expect(screen.getByLabelText("profile.name filter value")).toHaveProperty(
      "disabled",
      true,
    );
    expect(
      session
        .hostMessages()
        .filter(({ type }) => type === "tableData")
        .at(-1)?.payload,
    ).toMatchObject({
      pageScopedExtraColumns: true,
      extraColumns: expect.arrayContaining([
        expect.objectContaining({
          name: "profile.name",
          isReadOnly: true,
          isPrimaryKey: false,
          filterable: false,
        }),
      ]),
    });
    const calls = read.mock.calls.length;
    await session.user.click(screen.getByText("profile.name"));
    fireEvent.doubleClick(getBodyCell("profile.name"));
    expect(read).toHaveBeenCalledTimes(calls);
    expect(screen.queryByLabelText("Cell value")).toBeNull();
    fireEvent.doubleClick(getBodyCell("detail"));
    await screen.findByRole("textbox", { name: "Cell data" });
    expect(
      screen.getByText(/closes this dialog without sending a mutation/),
    ).toBeTruthy();
    await session.user.click(
      screen.getByRole("button", { name: "Close cell data" }),
    );
    await session.sendFromWebview({
      type: "applyChanges",
      payload: {
        operationId: "page-attribute-update",
        updates: [
          {
            primaryKeys: { pk: "sparse", sk: "0025" },
            changes: { "profile.name": "1" },
          },
        ],
      },
    });
    expect(session.hostMessages()).toContainEqual(
      expect.objectContaining({
        type: "applyResult",
        payload: expect.objectContaining({
          operationId: "page-attribute-update",
          success: false,
          error: expect.stringContaining('unknown column "profile.name"'),
        }),
      }),
    );

    hideColumn("__col_0");
    const expected = sparse.slice(25).map(({ __col_0: _, ...row }) => row);
    for (const format of ["csv", "json"] as const) {
      const path = join(tempDir, `sparse.${format}`);
      state.showSaveDialog.mockResolvedValue({ fsPath: path });
      state.showInformationMessage.mockClear();
      await session.user.click(
        screen.getByRole("button", { name: `Export ${format.toUpperCase()}` }),
      );
      await session.user.click(
        screen.getByRole("button", { name: "Export visible (2 rows)" }),
      );
      await waitFor(() =>
        expect(state.showInformationMessage).toHaveBeenCalledOnce(),
      );
      const content = await readFile(path, "utf8");
      if (format === "json") {
        expect(JSON.parse(content)).toEqual(expected);
      } else {
        const [names, ...lines] = content.trimEnd().split(/\r?\n/);
        expect(names).toBe(
          "pk,sk,label,__sel,constructor,detail,empty,profile.name",
        );
        const cell = (value: unknown) =>
          value === undefined
            ? "MISSING"
            : `"${JSON.stringify(value).replaceAll('"', '""')}"`;
        expect(lines).toEqual(
          expected.map((row) =>
            names
              .split(",")
              .map((name) =>
                cell(Object.hasOwn(row, name) ? row[name] : undefined),
              )
              .join(","),
          ),
        );
      }
      expect(read.mock.calls.at(-1)?.[0]).toMatchObject({
        page: 2,
        pageSize: 25,
        skipCount: true,
        filters: [{ column: "pk", operator: "eq", value: "sparse" }],
      });
    }

    // A non-key sort materializes the whole matching partition, but should
    // still only discover attributes from the visible page.
    await session.user.click(screen.getByText("label"));
    await screen.findByText("sparse-0000");
    expect(header("profile.name")).toBeUndefined();
    expect(read.mock.calls.at(-1)?.[0].sort).toEqual({
      column: "label",
      direction: "asc",
    });
    await session.user.click(screen.getByText("label"));
    await screen.findByText("sparse-0026");
    expect(header("profile.name")).toBeDefined();
    await session.user.click(screen.getByText("label"));
    await screen.findByText("sparse-0000");
    expect(read.mock.calls.at(-1)?.[0].sort).toBeNull();
    expect(header("profile.name")).toBeUndefined();

    await next();
    await screen.findByText("sparse-0025");
    expect(header("profile.name")).toBeDefined();
    filter("sample");
    await screen.findByText("sample-0000");
    expect(header("profile.name")).toBeUndefined();
    expect(state.showErrorMessage).not.toHaveBeenCalled();
    expect(unexpectedRequests).toEqual([]);
  } finally {
    opened?.session.unmount();
    TablePanel.disposeAll();
    await context?.dispose();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(tempDir, { recursive: true, force: true });
  }
});
