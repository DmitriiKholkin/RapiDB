import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableView } from "../../src/webview/components/TableView";
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

const columns: ColumnTypeMeta[] = ["id", "__proto__"].map((name) => ({
  name,
  type: "text",
  nativeType: "text",
  category: "text",
  nullable: true,
  isPrimaryKey: name === "id",
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq"],
  valueSemantics: "plain",
}));
const messages = (type: string) =>
  getPostedMessages().filter((message) => message.type === type);

function setup(row: Record<string, unknown> = { id: "1" }) {
  clearPostedMessages();
  const view = render(
    <TableView
      connectionId="c"
      database="db"
      schema="public"
      table="proto_paste"
    />,
  );
  act(() =>
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    }),
  );
  const request = messages("fetchPage").at(-1)?.payload as { fetchId: number };
  act(() =>
    dispatchIncomingMessage("tableData", {
      fetchId: request.fetchId,
      rows: [row],
      totalCount: 1,
    }),
  );
  const cell = (index: number) => {
    const element = view.container.querySelector<HTMLElement>(
      `td[data-row="${index}"][data-col="2"]`,
    );
    if (!element) throw new Error(`Missing __proto__ cell ${index}`);
    return element;
  };
  const paste = (index: number, text: string) => {
    fireEvent.mouseDown(cell(index), { button: 0 });
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
  return { cell, paste, row };
}

describe("TableView paste into missing __proto__", () => {
  it.each([
    false,
    true,
  ])("stages an absent persisted field, including alongside a draft paste (withDraft=%s)", (withDraft) => {
    const grid = setup();
    expect(grid.cell(0).textContent).toContain("MISSING");
    if (withDraft) {
      fireEvent.click(screen.getByRole("button", { name: "Add Row" }));
      grid.paste(-1, "draft");
    }
    grid.paste(0, "{}");
    if (withDraft) expect(grid.cell(-1).textContent).toContain("draft");
    expect(grid.cell(0).textContent).toContain("{}");
    expect(Object.hasOwn(grid.row, "__proto__")).toBe(false);
    expect(Object.getPrototypeOf(grid.row)).toBe(Object.prototype);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    grid.paste(0, "{}");
    expect(grid.cell(0).textContent).toContain("{}");
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    const payload = messages("applyChanges").at(-1)?.payload as {
      updates: Array<{
        changes: Record<string, unknown>;
        originalValues: Record<string, unknown>;
      }>;
    };
    expect(payload.updates).toHaveLength(1);
    const update = payload.updates[0];
    expect(Object.hasOwn(update.changes, "__proto__")).toBe(true);
    expect(update.changes.__proto__).toBe("{}");
    expect(update.originalValues.__proto__).toBeUndefined();
    expect(Object.getPrototypeOf(update.changes)).toBe(Object.prototype);
  });

  it("preserves an own __proto__ baseline and clears a pending edit when pasted back", () => {
    const row = { id: "1", ["__proto__"]: "original" };
    const grid = setup(row);
    grid.paste(0, "{}");
    expect(grid.cell(0).textContent).toContain("{}");
    grid.paste(0, "original");
    expect(grid.cell(0).textContent).toContain("original");
    expect(screen.queryByRole("button", { name: "Apply Changes" })).toBeNull();
    expect(Object.hasOwn(row, "__proto__")).toBe(true);
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
  });
});
