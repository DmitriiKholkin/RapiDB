import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ColumnTypeMeta,
  MongoValueTypeHint,
} from "../../src/shared/tableTypes";
import { parseTablePanelMessage } from "../../src/shared/webviewContracts";
import { TableView } from "../../src/webview/components/TableView";
import { rowPrimaryKeySignature } from "../../src/webview/components/table/tableViewHelpers";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: ({ count }: { count: number }) => ({
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: index,
        start: index * 26,
        end: (index + 1) * 26,
      })),
    getTotalSize: () => count * 26,
    scrollToIndex: () => undefined,
  }),
}));
vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: () => null,
}));

let state: Record<string, unknown>;
beforeEach(() => {
  state = {};
  if (!window.__vscode) throw new Error("Missing VS Code API");
  vi.mocked(window.__vscode.getState).mockImplementation(() => state);
  vi.mocked(window.__vscode.setState).mockImplementation((next) => {
    state = JSON.parse(JSON.stringify(next));
    return next;
  });
  clearPostedMessages();
});
const messages = (type: string) =>
  getPostedMessages().filter((message) => message.type === type);

function mount(
  name: string,
  row: Record<string, unknown>,
  types: Record<string, MongoValueTypeHint> | null = {},
) {
  const columns: ColumnTypeMeta[] = ["_id", name].map((column) => ({
    name: column,
    type: "text",
    nativeType: "text",
    category: "text",
    nullable: true,
    isPrimaryKey: column === "_id",
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  }));
  const view = render(
    <TableView
      connectionId="c"
      database="db"
      schema="public"
      table="proto"
      mongoRowIdentity
    />,
  );
  act(() =>
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["_id"],
    }),
  );
  const commit = (next: Record<string, unknown>) => {
    const request = messages("fetchPage").at(-1)?.payload as {
      fetchId: number;
    };
    act(() =>
      dispatchIncomingMessage(
        "tableData",
        JSON.parse(
          JSON.stringify({
            fetchId: request.fetchId,
            rows: [next],
            totalCount: 1,
            mongoIdTypes: ["string"],
            ...(types ? { rowValueTypes: [types] } : {}),
          }),
        ),
      ),
    );
  };
  commit(row);
  const cell = () => {
    const result = view.container
      .querySelectorAll<HTMLElement>('td[data-row="0"][data-col]')
      .item(1);
    if (!result) throw new Error("Missing cell");
    return result;
  };
  const paste = (text: string) => {
    fireEvent.mouseDown(cell(), { button: 0 });
    fireEvent.mouseUp(document);
    clearPostedMessages();
    fireEvent.paste(document.activeElement as HTMLElement);
    const request = messages("readClipboard").at(-1);
    expect(request).toBeDefined();
    act(() =>
      dispatchIncomingMessage("clipboardText", {
        ...(request?.payload as object),
        text,
      }),
    );
  };
  return { ...view, cell, paste, commit };
}

describe.each(["__proto__", "constructor"])("prototype field %s", (name) => {
  it("does not attach a real legacy JSON undefined baseline to a concurrent NULL", () => {
    const signature = rowPrimaryKeySignature(
      { _id: "1" },
      ["_id"],
      "string",
      true,
    );
    state.tableDraft = JSON.parse(
      JSON.stringify({
        tableKey: JSON.stringify(["c", "db", "public", "proto"]),
        newRows: [],
        restoreState: {
          entries: [
            {
              originalSignature: signature,
              changes: [[name, "pending"]],
              originalValues: [[name, undefined]],
            },
          ],
        },
      }),
    );
    const row = Object.fromEntries([
      ["_id", "1"],
      [name, null],
    ]);
    const restored = mount(
      name,
      row,
      Object.fromEntries([[name, { category: "other", nativeType: "null" }]]),
    );
    expect(restored.cell().textContent).toContain("NULL");
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(messages("applyChanges")).toHaveLength(0);
  });
  it("distinguishes BSON undefined from a concurrently deleted field after JSON transport", () => {
    const row = Object.fromEntries([
      ["_id", "1"],
      [name, undefined],
    ]);
    const types = Object.fromEntries([
      [name, { category: "other" as const, nativeType: "undefined" }],
    ]);
    const first = mount(name, row, types);
    expect(first.cell().textContent).toBe("NULL");
    first.paste("pending");
    first.unmount();
    clearPostedMessages();
    const restored = mount(name, { _id: "1" }, {});
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(restored.cell().textContent).toContain("MISSING");
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(messages("applyChanges")).toHaveLength(0);
  });

  it("reattaches an unchanged BSON undefined baseline and retains its type guard", () => {
    const row = Object.fromEntries([
      ["_id", "1"],
      [name, undefined],
    ]);
    const types = Object.fromEntries([
      [name, { category: "other" as const, nativeType: "undefined" }],
    ]);
    const first = mount(name, row, types);
    first.paste("pending");
    first.unmount();
    clearPostedMessages();
    const restored = mount(name, row, types);
    expect(restored.cell().textContent).toContain("pending");
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    const message = messages("applyChanges").at(-1);
    expect(
      parseTablePanelMessage(JSON.parse(JSON.stringify(message))),
    ).not.toBeNull();
    const update = (
      message?.payload as {
        updates: Array<{ originalTypes: Record<string, unknown> }>;
      }
    ).updates[0];
    expect(update.originalTypes[name]).toEqual({
      category: "other",
      nativeType: "undefined",
    });
  });

  it("keeps legacy ambiguous missing baselines unresolved", () => {
    const first = mount(name, { _id: "1" });
    first.paste("pending");
    first.unmount();
    const draft = state.tableDraft as {
      restoreState: { entries: Array<{ baselinePresenceVersion?: number }> };
    };
    delete draft.restoreState.entries[0].baselinePresenceVersion;
    clearPostedMessages();
    mount(name, { _id: "1" });
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(messages("applyChanges")).toHaveLength(0);
  });

  it.each([
    "initial",
    "reload",
  ])("fails closed when %s type hints are unavailable", (missingAt) => {
    const types = Object.fromEntries([
      [name, { category: "other" as const, nativeType: "undefined" }],
    ]);
    const row = Object.fromEntries([
      ["_id", "1"],
      [name, undefined],
    ]);
    const first = mount(name, row, missingAt === "initial" ? null : types);
    first.paste("pending");
    first.unmount();
    clearPostedMessages();
    mount(name, row, missingAt === "reload" ? null : types);
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(messages("applyChanges")).toHaveLength(0);
  });
  it("restores a missing-field edit after remount and metadata refresh, sending valid missing type metadata", () => {
    const first = mount(name, { _id: "1" });
    first.paste("{}");
    expect(first.cell().textContent).toContain("{}");
    first.unmount();
    clearPostedMessages();
    const restored = mount(name, { _id: "1" });
    expect(restored.cell().textContent).toContain("{}");
    expect(screen.queryByRole("alert")).toBeNull();
    act(() =>
      dispatchIncomingMessage("tableInit", {
        intent: "metadataRefresh",
        columns: [
          {
            name: "_id",
            type: "text",
            nativeType: "text",
            category: "text",
            nullable: true,
            isPrimaryKey: true,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["eq"],
            valueSemantics: "plain",
          },
          {
            name,
            type: "text",
            nativeType: "text",
            category: "text",
            nullable: true,
            isPrimaryKey: false,
            isForeignKey: false,
            filterable: true,
            filterOperators: ["eq"],
            valueSemantics: "plain",
          },
        ],
        primaryKeyColumns: ["_id"],
      }),
    );
    restored.commit({ _id: "1" });
    expect(restored.cell().textContent).toContain("{}");
    clearPostedMessages();
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    const message = messages("applyChanges").at(-1);
    expect(message).toBeDefined();
    expect(
      parseTablePanelMessage(JSON.parse(JSON.stringify(message))),
    ).not.toBeNull();
    const update = (
      message?.payload as {
        updates: Array<{
          changes: Record<string, unknown>;
          originalTypes: Record<string, unknown>;
        }>;
      }
    ).updates[0];
    expect(Object.hasOwn(update.changes, name)).toBe(true);
    expect(update.changes[name]).toBe("{}");
    expect(Object.hasOwn(update.originalTypes, name)).toBe(true);
    expect(update.originalTypes[name]).toEqual({
      category: "other",
      nativeType: "missing",
    });
    expect(Object.getPrototypeOf(update.originalTypes)).toBe(Object.prototype);
  });

  it("keeps the edit unresolved if the absent field appears, including across refresh", () => {
    const first = mount(name, { _id: "1" });
    first.paste("pending");
    first.unmount();
    clearPostedMessages();
    const row = Object.fromEntries([
      ["_id", "1"],
      [name, "concurrent"],
    ]);
    const restored = mount(name, row);
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(restored.cell().textContent).toContain("concurrent");
    expect(
      (
        screen.getByRole("button", {
          name: "Apply Changes",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    clearPostedMessages();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    restored.commit(row);
    expect(screen.getByRole("alert").textContent).toMatch(/not attached/i);
    expect(messages("applyChanges")).toHaveLength(0);
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
  });

  it("preserves an own type hint for an existing field", () => {
    const row = Object.fromEntries([
      ["_id", "1"],
      [name, "old"],
    ]);
    const types = Object.fromEntries([
      [name, { category: "text" as const, nativeType: "string" }],
    ]);
    const grid = mount(name, row, types);
    grid.paste("new");
    clearPostedMessages();
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    const message = messages("applyChanges").at(-1);
    expect(parseTablePanelMessage(message)).not.toBeNull();
    const update = (
      message?.payload as {
        updates: Array<{ originalTypes: Record<string, unknown> }>;
      }
    ).updates[0];
    expect(update.originalTypes[name]).toEqual({
      category: "text",
      nativeType: "string",
    });
  });
});

it("pastes into a recovered empty draft and a persisted row into literal __proto__ without changing prototypes", () => {
  state.tableDraft = {
    tableKey: JSON.stringify(["c", "db", "public", "proto"]),
    restoreState: { entries: [] },
    newRows: [{}],
  };
  const grid = mount("__proto__", { _id: "1", ["__proto__"]: "old" });
  const draft = grid.container.querySelector<HTMLElement>(
    'td[data-row="-1"][data-col="2"]',
  );
  if (!draft) throw new Error("Missing draft cell");
  expect(draft.textContent).toContain("DEFAULT");
  fireEvent.mouseDown(draft, { button: 0 });
  fireEvent.mouseUp(document);
  clearPostedMessages();
  fireEvent.paste(document.activeElement as HTMLElement);
  const request = messages("readClipboard").at(-1);
  expect(request).toBeDefined();
  act(() =>
    dispatchIncomingMessage("clipboardText", {
      ...(request?.payload as object),
      text: "draft value",
    }),
  );
  grid.paste("persisted value");
  expect(draft.textContent).toContain("draft value");
  expect(grid.cell().textContent).toContain("persisted value");
  const saved = (
    state.tableDraft as { newRows: Array<Record<string, { value: string }>> }
  ).newRows[0];
  expect(Object.hasOwn(saved, "__proto__")).toBe(true);
  expect(saved.__proto__.value).toBe("draft value");
  expect(Object.getPrototypeOf(saved)).toBe(Object.prototype);
  expect(({} as Record<string, unknown>).value).toBeUndefined();
  fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
  const message = messages("applyChanges").at(-1);
  expect(
    parseTablePanelMessage(JSON.parse(JSON.stringify(message))),
  ).not.toBeNull();
  const payload = message?.payload as {
    insertValues: Record<string, unknown>[];
    updates: Array<{ changes: Record<string, unknown> }>;
  };
  expect(Object.hasOwn(payload.insertValues[0], "__proto__")).toBe(true);
  expect(payload.insertValues[0].__proto__).toBe("draft value");
  expect(payload.updates[0].changes.__proto__).toBe("persisted value");
  expect(Object.getPrototypeOf(payload.insertValues[0])).toBe(Object.prototype);
});
