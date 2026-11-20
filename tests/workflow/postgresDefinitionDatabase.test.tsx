import "./harness";
import { act, fireEvent, screen, waitFor } from "@testing-library/react";
import { Client } from "pg";
import type { Ref } from "react";
import { expect, it, vi } from "vitest";
import { registerExplorerCommands } from "../../src/extension/commands/explorerCommands";
import { registerSavedEntryCommands } from "../../src/extension/commands/savedEntryCommands";
import { colKey } from "../../src/extension/dbDrivers/types";
import { QueryPanel } from "../../src/extension/panels/queryPanel";
import {
  ConnectionProvider,
  type RapiDBNode,
} from "../../src/extension/providers/connectionProvider";
import type { MonacoEditorHandle } from "../../src/webview/components/MonacoEditor";
import { type QueryResult, useSchemaStore } from "../../src/webview/store";
import { createExtensionContextStub } from "../support/fakeConnectionManagerStore";
import type { BridgeSession } from "./bridge/hostWebviewBridge";
import { workflowState } from "./harness";
import { getScenario } from "./scenarios";

vi.doMock("../../src/webview/components/MonacoEditor", async () => {
  const { useImperativeHandle, useState } = await import("react");
  return {
    MonacoEditor({
      initialValue = "",
      onChange,
      ref,
    }: {
      initialValue?: string;
      onChange?: (value: string) => void;
      ref?: Ref<Partial<MonacoEditorHandle>>;
    }) {
      const [text, setText] = useState(initialValue);
      useImperativeHandle(
        ref,
        () => ({
          getSelectionOrValue: () => text,
          getValue: () => text,
          format: () => null,
          placeCursor: () => undefined,
        }),
        [text],
      );
      return (
        <textarea
          aria-label="SQL fixture editor"
          value={text}
          onChange={(event) => {
            setText(event.target.value);
            onChange?.(event.target.value);
          }}
        />
      );
    },
  };
});

it("postgres Explorer definitions, regular queries and saved entries execute only in their displayed database", async () => {
  const { startBridge } = await import("./bridge/hostWebviewBridge");
  const { bootstrapWorkflowContext } = await import("./workflowContext");
  const scenario = getScenario("postgres");
  const seed = scenario.buildConnection();
  const uri = process.env.RAPIDB_DATA_SAFETY_POSTGRES_URI;
  const url = uri ? new URL(uri) : undefined;
  const connection = {
    ...seed,
    folder: undefined,
    ...(url
      ? {
          host: url.hostname,
          port: Number(url.port),
          username: decodeURIComponent(url.username),
          password: decodeURIComponent(url.password),
          database: decodeURIComponent(url.pathname.slice(1)),
        }
      : {}),
  };
  scenario.buildConnection = () => connection;
  const context = await bootstrapWorkflowContext({
    scenario,
    state: workflowState as unknown as Parameters<
      typeof bootstrapWorkflowContext
    >[0]["state"],
  });
  const { connectionManager } = context;
  const extensionContext =
    createExtensionContextStub() as unknown as Parameters<
      typeof registerExplorerCommands
    >[0]["context"];
  const suffix = `${process.pid}_${Date.now().toString(36)}`;
  const target = `  H10:"Δ"/?%${suffix}  `;
  const quotedTarget = `"${target.replaceAll('"', '""')}"`;
  const table = `h10_definition_${suffix}`;
  const routine = `h10_routine_${suffix}`;
  const admin = new Client({
    host: connection.host,
    port: connection.port,
    user: connection.username,
    password: connection.password,
    database: connection.database,
  });
  const provider = new ConnectionProvider(connectionManager);
  const commands = new Map<string, (...args: unknown[]) => unknown>();
  const register = <T extends unknown[]>(
    command: string,
    callback: (...args: T) => unknown,
  ) => {
    commands.set(command, callback as (...args: unknown[]) => unknown);
    return { dispose() {} };
  };
  registerExplorerCommands(
    {
      context: extensionContext,
      connectionManager,
      refresh: () => provider.refresh(),
    },
    register,
  );
  registerSavedEntryCommands(
    { context: extensionContext, connectionManager },
    register,
  );
  const runCommand = async (command: string, argument: unknown) => {
    const handler = commands.get(command);
    if (!handler) throw new Error(`Missing command ${command}`);
    await handler(argument);
  };
  let session: BridgeSession | undefined;
  const loadPanel = async (command: string, argument: unknown) => {
    session?.unmount();
    session?.panel.dispose();
    session = undefined;
    useSchemaStore.setState({ schemaByConnection: {} });
    const previous = context.state.panels.length;
    await runCommand(command, argument);
    expect(context.state.panels).toHaveLength(previous + 1);
    const panel = context.state.panels.at(-1);
    if (!panel) throw new Error("No query panel opened");
    session = await startBridge({
      state: context.state,
      initialState: panel.lastInitialState(),
    });
    return session;
  };
  const findChild = async (
    parent: RapiDBNode | undefined,
    predicate: (node: RapiDBNode) => boolean,
  ) => {
    return waitFor(
      async () => {
        const node = (await provider.getChildren(parent)).find(predicate);
        if (!node)
          throw new Error(
            `Explorer did not load children of ${parent?.kind ?? "root"}`,
          );
        return node;
      },
      { timeout: 5000 },
    );
  };
  const runInUi = async (panel: BridgeSession) => {
    const before = panel
      .hostMessages()
      .filter(({ type }) => type === "queryResult").length;
    await panel.user.click(screen.getByRole("button", { name: "Run" }));
    return waitFor(
      () => {
        const results = panel
          .hostMessages()
          .filter(({ type }) => type === "queryResult");
        expect(results).toHaveLength(before + 1);
        return results.at(-1)?.payload as QueryResult;
      },
      { timeout: 5000 },
    );
  };

  let adminConnected = false;
  let createdDatabase = false;
  try {
    await admin.connect();
    adminConnected = true;
    await admin.query(`CREATE DATABASE ${quotedTarget}`);
    createdDatabase = true;
    await admin.query(`CREATE TABLE public.${table} (default_only int)`);
    await admin.query(
      `CREATE FUNCTION public.${routine}() RETURNS int LANGUAGE sql AS 'SELECT 1'`,
    );
    await context.connect();
    const driver = connectionManager.getDriver(connection.id);
    if (!driver) throw new Error("No PostgreSQL driver");
    await driver.query(
      `CREATE TABLE public.${table} (scoped_only int);
       CREATE FUNCTION public.${routine}() RETURNS int LANGUAGE sql AS 'SELECT 2';
       CREATE SCHEMA h10_aux;
       CREATE TABLE h10_aux.completion_only (id int);`,
      undefined,
      { database: target },
    );

    const profileNode = await findChild(
      undefined,
      (node) => node.connectionId === connection.id,
    );
    const databaseNode = await findChild(
      profileNode,
      (node) => node.kind === "database" && node.database === target,
    );
    const schemaNode = await findChild(
      databaseNode,
      (node) => node.kind === "schema" && node.schema === "public",
    );
    const tableCategory = await findChild(
      schemaNode,
      (node) => node.kind === "category_tables",
    );
    const tableNode = await findChild(
      tableCategory,
      (node) => node.objectName === table,
    );
    const routineCategory = await findChild(
      schemaNode,
      (node) => node.kind === "category_functions",
    );
    const routineNode = await findChild(
      routineCategory,
      (node) => node.objectName === routine,
    );

    const ddlPanel = await loadPanel("rapidb.showDDL", tableNode);
    expect(ddlPanel.panel.title).toContain(JSON.stringify(target));
    expect(screen.getByText(/Pinned target/).textContent).toContain(
      JSON.stringify(target),
    );
    expect(
      screen.queryByRole("combobox", { name: "Active connection" }),
    ).toBeNull();
    expect(
      screen.getByRole("textbox", { name: "SQL fixture editor" }),
    ).toHaveProperty("value", expect.stringContaining("scoped_only"));
    await waitFor(
      () => {
        const schema =
          useSchemaStore.getState().schemaByConnection[connection.id];
        expect(schema?.map((entry) => entry.database)).toEqual(
          schema?.map(() => target),
        );
        expect(schema).toContainEqual(
          expect.objectContaining({
            object: "completion_only",
            schema: "h10_aux",
          }),
        );
      },
      { timeout: 5000 },
    );
    await driver.query(`DROP TABLE public.${table}`, undefined, {
      database: target,
    });
    expect((await runInUi(ddlPanel)).error).toBeUndefined();
    const observed = await driver.query(
      `SELECT scoped_only FROM public.${table}`,
      undefined,
      { database: target },
    );
    expect(observed.columns).toEqual(["scoped_only"]);
    expect(
      (await admin.query(`SELECT default_only FROM public.${table}`)).fields[0]
        ?.name,
    ).toBe("default_only");

    const routinePanel = await loadPanel("rapidb.openRoutine", routineNode);
    expect(
      screen.getByRole("textbox", { name: "SQL fixture editor" }),
    ).toHaveProperty("value", expect.stringContaining("SELECT 2"));
    await driver.query(
      `CREATE OR REPLACE FUNCTION public.${routine}() RETURNS int LANGUAGE sql AS 'SELECT 3'`,
      undefined,
      { database: target },
    );
    expect((await runInUi(routinePanel)).error).toBeUndefined();
    const sql = `SELECT current_database() AS db, public.${routine}() AS value`;
    fireEvent.change(
      screen.getByRole("textbox", { name: "SQL fixture editor" }),
      { target: { value: sql } },
    );
    expect((await runInUi(routinePanel)).rows).toEqual([
      { [colKey(0)]: target, [colKey(1)]: 2 },
    ]);
    await routinePanel.user.click(
      screen.getByRole("button", { name: "Bookmark" }),
    );
    const bookmark = await waitFor(() => {
      const entry = connectionManager.getBookmarks()[0];
      expect(entry?.postgresDatabase).toBe(target);
      if (!entry) throw new Error("Bookmark not saved");
      return entry;
    });
    const history = connectionManager.getHistory()[0];
    expect(history).toMatchObject({ sql, postgresDatabase: target });
    for (const [command, entry] of [
      ["rapidb.openHistoryEntry", history],
      ["rapidb.openBookmarkEntry", bookmark],
    ] as const) {
      const reopened = await loadPanel(command, entry);
      expect((await runInUi(reopened)).rows).toEqual([
        { [colKey(0)]: target, [colKey(1)]: 2 },
      ]);
    }

    const regular = await loadPanel("rapidb.newQuery", profileNode);
    expect(
      screen.getByRole("combobox", { name: "Active connection" }),
    ).toBeTruthy();
    fireEvent.change(
      screen.getByRole("textbox", { name: "SQL fixture editor" }),
      { target: { value: sql } },
    );
    expect((await runInUi(regular)).rows).toEqual([
      { [colKey(0)]: connection.database, [colKey(1)]: 1 },
    ]);
    expect(connectionManager.getHistory().slice(0, 2)).toEqual([
      expect.objectContaining({ sql }),
      expect.objectContaining({ sql, postgresDatabase: target }),
    ]);
    expect(connectionManager.getHistory()[0]?.postgresDatabase).toBeUndefined();

    const unavailable = await loadPanel("rapidb.openBookmarkEntry", bookmark);
    const slowSql = `SELECT pg_sleep(30) /* ${suffix} */`;
    fireEvent.change(
      screen.getByRole("textbox", { name: "SQL fixture editor" }),
      { target: { value: slowSql } },
    );
    await unavailable.user.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(async () => {
      const active = await admin.query(
        "SELECT datname FROM pg_stat_activity WHERE query = $1 AND state = 'active'",
        [slowSql],
      );
      expect(active.rows).toEqual([{ datname: target }]);
    });
    await unavailable.user.click(screen.getByRole("button", { name: "Stop" }));
    await waitFor(() => {
      expect(unavailable.hostMessages()).toContainEqual({
        type: "queryResult",
        payload: expect.objectContaining({
          error: expect.stringMatching(/cancel/i),
        }),
      });
      expect(screen.queryByRole("button", { name: "Stop" })).toBeNull();
    });
    fireEvent.change(
      screen.getByRole("textbox", { name: "SQL fixture editor" }),
      { target: { value: sql } },
    );
    await act(async () => {
      await connectionManager.saveConnection({
        ...connection,
        database: connection.database === "postgres" ? "template1" : "postgres",
      });
    });
    expect(
      screen.getByRole<HTMLButtonElement>("button", { name: "Run" }).disabled,
    ).toBe(true);
    expect(
      screen.getByRole("textbox", { name: "SQL fixture editor" }),
    ).toHaveProperty("value", sql);
    for (const [command, entry] of [
      ["rapidb.openHistoryEntry", history],
      ["rapidb.openBookmarkEntry", bookmark],
    ] as const) {
      await loadPanel(command, entry);
      expect(
        screen.getByRole<HTMLButtonElement>("button", { name: "Run" }).disabled,
      ).toBe(true);
      expect(
        screen.getByRole("textbox", { name: "SQL fixture editor" }),
      ).toHaveProperty("value", sql);
    }
    const reviewed = await loadPanel("rapidb.openBookmarkEntry", bookmark);
    await reviewed.user.click(
      screen.getByRole("button", { name: "Use current target" }),
    );
    expect(screen.getByText(/Pinned target/).textContent).toContain(
      JSON.stringify(target),
    );
    expect((await runInUi(reviewed)).rows).toEqual([
      { [colKey(0)]: target, [colKey(1)]: 2 },
    ]);
    expect(connectionManager.getHistory()[0]).toMatchObject({
      sql,
      postgresDatabase: target,
    });
    await act(async () => {
      await connectionManager.disconnectFrom(connection.id);
      await admin.query(`DROP DATABASE ${quotedTarget}`);
    });
    createdDatabase = false;
    const result = await runInUi(reviewed);
    expect(result.error).toBeTruthy();
    expect(result.rows).toEqual([]);
    expect(
      (await admin.query(`SELECT public.${routine}() AS value`)).rows,
    ).toEqual([{ value: 1 }]);
  } finally {
    try {
      session?.unmount();
      QueryPanel.disposeAll();
      provider.disposable.dispose();
      await context.dispose();
      if (adminConnected) {
        if (createdDatabase) await admin.query(`DROP DATABASE ${quotedTarget}`);
        await admin.query(
          `DROP TABLE IF EXISTS public.${table}; DROP FUNCTION IF EXISTS public.${routine}()`,
        );
      }
    } finally {
      await admin.end();
    }
  }
});
