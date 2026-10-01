import "./harness";
import net from "node:net";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { logger } from "../../src/extension/utils/logger";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";
import type {
  ApplyResultPayload,
  TableMutationPreviewPayload,
} from "../../src/shared/webviewContracts";
import type { WorkflowMessageEnvelope } from "./bridge/fakeWebviewPanel";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import redisScenario from "./scenarios/redisScenario";
import { getBodyCell } from "./tableUi";
import type { WorkflowContext } from "./workflowContext";

vi.doUnmock("../../src/webview/components/table/TableGrid");
vi.doMock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: ({
    initialValue,
    ariaLabel,
    readOnly,
    onChange,
  }: {
    initialValue?: string;
    ariaLabel?: string;
    readOnly?: boolean;
    onChange?: (value: string) => void;
  }) => (
    <textarea
      aria-label={ariaLabel}
      defaultValue={initialValue}
      readOnly={readOnly}
      onChange={(event) => onChange?.(event.target.value)}
    />
  ),
  connTypeToDialect: () => "javascript",
}));

const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`;
const array = (values: string[]) =>
  `*${values.length}\r\n${values.map(bulk).join("")}`;

// A small RESP endpoint: node-redis itself is never mocked. Record decoded
// wire arguments, serve reads for the real host refresh, and inject SET errors.
class InsertRespServer {
  readonly commands: string[][] = [];
  readonly sockets = new Set<net.Socket>();
  readonly rows = new Map<string, { value: string | string[]; ttl: number }>();
  rejectNextSet = false;
  readonly server = net.createServer((socket) => {
    this.sockets.add(socket);
    socket.on("close", () => this.sockets.delete(socket));
    socket.on("error", () => undefined);
    let input = Buffer.alloc(0);
    socket.on("data", (data) => {
      input = Buffer.concat([input, Buffer.from(data)]);
      while (input.length) {
        const parsed = this.parse(input);
        if (!parsed) break;
        input = input.subarray(parsed.consumed);
        this.commands.push(parsed.args);
        socket.write(this.reply(parsed.args));
      }
    });
  });

  private parse(input: Buffer) {
    const firstEnd = input.indexOf("\r\n");
    if (firstEnd === -1) return undefined;
    const count = Number(input.subarray(1, firstEnd).toString());
    let offset = firstEnd + 2;
    const args: string[] = [];
    for (let index = 0; index < count; index++) {
      const end = input.indexOf("\r\n", offset);
      if (end === -1) return undefined;
      const length = Number(input.subarray(offset + 1, end).toString());
      offset = end + 2;
      if (input.length < offset + length + 2) return undefined;
      args.push(input.subarray(offset, offset + length).toString());
      offset += length + 2;
    }
    return { args, consumed: offset };
  }

  private reply(args: string[]) {
    const row = this.rows.get(args[1]);
    switch (args[0]) {
      case "HELLO":
        return "%1\r\n+server\r\n+redis\r\n";
      case "SCAN":
        return `*2\r\n${bulk("0")}${array([...this.rows.keys()])}`;
      case "TYPE":
        return `+${Array.isArray(row?.value) ? "list" : "string"}\r\n`;
      case "GET":
        return row ? bulk(String(row.value)) : "$-1\r\n";
      case "LRANGE":
        return array(row?.value as string[]);
      case "TTL":
        return `:${row?.ttl ?? -2}\r\n`;
      case "EVAL": {
        // Model the string-key compare-and-write using the actual RESP
        // arguments emitted by node-redis, including embedded NUL bytes.
        const source = args[3];
        const target = args[4];
        const stored = this.rows.get(source);
        if (!stored) return ":0\r\n";
        if (source !== target && this.rows.has(target)) return ":-1\r\n";
        if (Array.isArray(stored.value) || args[5] !== "string")
          return ":-2\r\n";
        if (args[11] === "1" && stored.value !== args[6]) return ":-3\r\n";
        if (args[7] === "1") stored.value = JSON.parse(args[8]);
        if (args[9] === "persist") stored.ttl = -1;
        if (args[9] === "set") stored.ttl = Number(args[10]);
        this.rows.delete(source);
        this.rows.set(target, stored);
        return ":1\r\n";
      }
      case "SET": {
        if (this.rejectNextSet) {
          this.rejectNextSet = false;
          return "-ERR injected insert failure\r\n";
        }
        if (args.includes("NX") && row) return "$-1\r\n";
        const ex = args.indexOf("EX");
        this.rows.set(args[1], {
          value: args[2],
          ttl: ex < 0 ? -1 : Number(args[ex + 1]),
        });
        return "+OK\r\n";
      }
      default:
        return "+OK\r\n";
    }
  }

  async listen() {
    await new Promise<void>((resolve) =>
      this.server.listen(0, "127.0.0.1", resolve),
    );
    return (this.server.address() as net.AddressInfo).port;
  }

  async close() {
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

async function withTable(
  structured: boolean,
  run: (
    server: InsertRespServer,
    context: WorkflowContext,
    session: BridgeSession,
  ) => Promise<void>,
) {
  vi.spyOn(logger, "error").mockReturnValue(new Error("logged"));
  const server = new InsertRespServer();
  server.rows.set("drafts:seed", {
    value: structured ? ["seed"] : "seed",
    ttl: -1,
  });
  const port = await server.listen();
  const scenario = redisScenario();
  scenario.buildConnection = () => ({
    id: "redis-null-draft",
    name: "Redis NULL draft",
    type: "redis",
    host: "127.0.0.1",
    port,
  });
  scenario.tableFixture.table = "drafts";
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  delete window.__vscode;
  TablePanel.disposeAll();
  const context = await bootstrapWorkflowContext({
    scenario,
    state: workflowState as unknown as Parameters<
      typeof bootstrapWorkflowContext
    >[0]["state"],
  });
  let session: BridgeSession | undefined;
  try {
    await context.connect();
    context.store.setSkipTableMutationPreview(false);
    session = (await context.openTableViewer()).session;
    await waitFor(() =>
      expect(getBodyCell("key").textContent).toBe("drafts:seed"),
    );
    await waitFor(() =>
      expect(
        (screen.getByRole("button", { name: "Add Row" }) as HTMLButtonElement)
          .disabled,
      ).toBe(false),
    );
    await session.user.click(screen.getByRole("button", { name: "Add Row" }));
    await waitFor(() => expect(getBodyCell("key").textContent).toBe("DEFAULT"));
    await editCell(session, "key", "drafts:new");
    await run(server, context, session);
  } finally {
    session?.unmount();
    TablePanel.disposeAll();
    await context.dispose();
    await server.close();
  }
}

async function editCell(
  session: BridgeSession,
  column: string,
  value: string | null,
) {
  fireEvent.doubleClick(getBodyCell(column));
  if (value === null) {
    await session.user.click(screen.getByRole("button", { name: "NULL" }));
    return;
  }
  const inline = screen.queryByLabelText("Cell value");
  const editor = inline ?? screen.getByLabelText("Cell data");
  fireEvent.change(editor, { target: { value } });
  if (inline) fireEvent.keyDown(editor, { key: "Enter" });
  else await session.user.click(screen.getByRole("button", { name: "Apply" }));
}

async function apply(session: BridgeSession, expectedValues: object) {
  const postMessage = vi.spyOn(
    window.__vscode as NonNullable<Window["__vscode"]>,
    "postMessage",
  );
  const previousResults = session
    .hostMessages()
    .filter((message) => message.type === "applyResult").length;
  await session.user.click(
    screen.getByRole("button", { name: "Apply Changes" }),
  );
  await screen.findByRole("dialog");
  const request = postMessage.mock.calls
    .map(([message]) => message as WorkflowMessageEnvelope)
    .find((message) => message.type === "applyChanges");
  const requestPayload = request?.payload as {
    updates: unknown[];
    insertValues: unknown[];
  };
  expect(requestPayload.updates).toEqual([]);
  expect(requestPayload.insertValues).toEqual([expectedValues]);
  expect(JSON.stringify(request?.payload)).not.toContain("__NULL__");
  const preview = session
    .hostMessages()
    .filter((message) => message.type === "tableMutationPreview")
    .at(-1);
  expect(preview?.payload).toMatchObject({ statementCount: 1 });
  expect(JSON.stringify(preview?.payload)).not.toContain("__NULL__");
  await session.user.click(
    screen.getByRole("button", { name: "Apply Changes" }),
  );
  await waitFor(() =>
    expect(
      session
        .hostMessages()
        .filter((message) => message.type === "applyResult"),
    ).toHaveLength(previousResults + 1),
  );
  postMessage.mockRestore();
  return session
    .hostMessages()
    .filter((message) => message.type === "applyResult")
    .at(-1)?.payload as ApplyResultPayload;
}

describe("B07 Redis draft NULL UI → host → real node-redis RESP wire", () => {
  it.each([
    { name: "DEFAULT", value: undefined, ttl: undefined, stored: "", ex: [] },
    { name: "explicit NULL", value: null, ttl: null, stored: "", ex: [] },
    { name: "empty", value: "", ttl: "", stored: "", ex: [] },
    { name: "-1", value: null, ttl: "-1", stored: "", ex: [] },
    {
      name: "positive TTL",
      value: null,
      ttl: "60",
      stored: "",
      ex: ["EX", "60"],
    },
    {
      name: "literal NULL text",
      value: "NULL",
      ttl: null,
      stored: "NULL",
      ex: [],
    },
  ])("previews and inserts $name without leaking markers", async ({
    value,
    ttl,
    stored,
    ex,
  }) => {
    await withTable(false, async (server, context, session) => {
      if (value !== undefined) await editCell(session, "value", value);
      if (ttl !== undefined) await editCell(session, "ttl", ttl);
      const values = {
        key: "drafts:new",
        ...(value === undefined ? {} : { value }),
        ...(ttl === undefined ? {} : { ttl }),
      };
      const result = await apply(session, values);
      expect(result).toMatchObject({ success: true, insertApplied: true });
      const expected = ["SET", "drafts:new", stored, ...ex, "NX"];
      expect(server.commands.filter((args) => args[0] === "SET")).toEqual([
        expected,
      ]);
      expect(server.rows.get("drafts:new")).toEqual({
        value: stored,
        ttl: ex.length ? 60 : -1,
      });
      await waitFor(() =>
        expect(document.querySelector('td[data-row="-1"]')).toBeNull(),
      );
      // Replay the host's actual B06 preview through the query parser and
      // compare wire args, including the empty value and optional EX/NX.
      const preview = session
        .hostMessages()
        .find((message) => message.type === "tableMutationPreview");
      const payload = preview?.payload as TableMutationPreviewPayload;
      const driver = context.connectionManager.getDriver("redis-null-draft");
      await driver?.query(payload.sql, [], { database: "db0" });
      expect(server.commands.filter((args) => args[0] === "SET")).toEqual([
        expected,
        expected,
      ]);
      expect(server.commands.flat()).not.toContain(NULL_SENTINEL);
    });
  });

  it("serializes structured-dialog NULL and retains it after a host error for retry", async () => {
    await withTable(true, async (server, _context, session) => {
      fireEvent.doubleClick(getBodyCell("value"));
      expect(screen.getByLabelText("Cell data")).toBeTruthy();
      await session.user.click(screen.getByRole("button", { name: "NULL" }));
      await editCell(session, "ttl", null);
      server.rejectNextSet = true;
      const values = { key: "drafts:new", value: null, ttl: null };
      const failed = await apply(session, values);
      expect(failed).toMatchObject({ success: false });
      expect(failed.error).toContain("injected insert failure");
      expect(server.rows.has("drafts:new")).toBe(false);
      expect(getBodyCell("value").textContent).toBe("NULL");
      expect(getBodyCell("ttl").textContent).toBe("NULL");
      fireEvent.doubleClick(getBodyCell("value"));
      expect(
        (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
      ).toBe("");
      await session.user.click(screen.getByRole("button", { name: "NULL" }));
      expect(await apply(session, values)).toMatchObject({
        success: true,
        insertApplied: true,
      });
      expect(server.commands.filter((args) => args[0] === "SET")).toEqual([
        ["SET", "drafts:new", "", "NX"],
        ["SET", "drafts:new", "", "NX"],
      ]);
      expect(server.commands.flat()).not.toContain(NULL_SENTINEL);
      await waitFor(() =>
        expect(document.querySelector('td[data-row="-1"]')).toBeNull(),
      );
    });
  });

  it("defensively normalizes direct host sentinel inputs before preview and execution", async () => {
    await withTable(false, async (server, context) => {
      const driver = context.connectionManager.getDriver("redis-null-draft");
      if (!driver) throw new Error("Missing Redis driver");
      const service = new TableMutationService(context.connectionManager, {
        getColumns: (_id, database, schema, table) =>
          driver.describeColumns(database, schema, table),
      });
      const plan = await service.prepareInsertRow(
        "redis-null-draft",
        "db0",
        "",
        "drafts",
        { key: "drafts:direct", value: NULL_SENTINEL, ttl: NULL_SENTINEL },
      );
      expect(plan.values).toEqual({
        key: "drafts:direct",
        value: null,
        ttl: null,
      });
      expect(plan.previewStatements).toEqual(['SET "drafts:direct" "" "NX"']);
      await service.executePreparedInsertPlan(plan);
      expect(server.commands.filter((args) => args[0] === "SET")).toEqual([
        ["SET", "drafts:direct", "", "NX"],
      ]);
      expect(server.commands.flat()).not.toContain(NULL_SENTINEL);
    });
  });

  it.each([
    { change: "value", concurrent: false },
    { change: "value", concurrent: true },
    { change: "ttl", concurrent: false },
    { change: "ttl", concurrent: true },
  ] as const)("preserves literal sentinel originals for a $change NULL edit (concurrent empty: $concurrent)", async ({
    change,
    concurrent,
  }) => {
    await withTable(false, async (server, context) => {
      const connectionId = "redis-null-draft";
      const driver = context.connectionManager.getDriver(connectionId);
      if (!driver) throw new Error("Missing Redis driver");
      const key = "drafts:seed";
      server.rows.set(key, { value: NULL_SENTINEL, ttl: 60 });
      const page = await driver.readTablePage?.({
        database: "db0",
        schema: "",
        table: "drafts",
        page: 1,
        pageSize: 25,
        filters: [],
        sort: null,
        skipCount: false,
      });
      const original = page?.rows.find((row) => row.key === key);
      expect(original?.value).toBe(NULL_SENTINEL);
      const columns = await driver.describeColumns("db0", "", "drafts");
      const prepared = prepareApplyChangesPlan(
        context.connectionManager,
        connectionId,
        "db0",
        "",
        "drafts",
        [
          {
            primaryKeys: { key },
            changes: { [change]: NULL_SENTINEL },
            originalValues: { value: original?.value },
          },
        ],
        columns,
      );
      if (!prepared.executable) throw new Error("Missing executable edit plan");
      expect(prepared.plan.updates).toEqual([
        {
          primaryKeys: { key },
          changes: { [change]: null },
          originalValues: { value: NULL_SENTINEL },
        },
      ]);
      // Change the actual store after preparation, before the atomic edit.
      if (concurrent) server.rows.set(key, { value: "", ttl: 60 });
      const result = await executePreparedApplyPlan(
        context.connectionManager,
        prepared.plan,
      );
      const evals = server.commands.filter((args) => args[0] === "EVAL");
      expect(evals).toHaveLength(1);
      expect(evals[0][1]).toContain("redis.call('GET', source) ~= ARGV[2]");
      expect(evals[0].slice(2)).toEqual([
        "2",
        key,
        key,
        "string",
        NULL_SENTINEL,
        change === "value" ? "1" : "0",
        change === "value" ? '""' : "null",
        change === "ttl" ? "persist" : "keep",
        "0",
        "1",
      ]);
      if (concurrent) {
        expect(result.success).toBe(false);
        expect(result.error).toContain(
          "Redis value changed after the row was loaded",
        );
        expect(server.rows.get(key)).toEqual({ value: "", ttl: 60 });
      } else {
        expect(result.success).toBe(true);
        expect(server.rows.get(key)).toEqual({
          value: change === "value" ? "" : NULL_SENTINEL,
          ttl: change === "ttl" ? -1 : 60,
        });
      }
    });
  });
});
