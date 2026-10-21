import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

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
  }),
}));
vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: (props: {
    initialValue: string;
    ariaLabel: string;
    onChange?: (value: string) => void;
  }) => (
    <textarea
      aria-label={props.ariaLabel}
      defaultValue={props.initialValue}
      onChange={(event) => props.onChange?.(event.target.value)}
    />
  ),
}));

import { TableView } from "../../src/webview/components/TableView";
import { dispatchIncomingMessage, getPostedMessages } from "./testUtils";

describe("literal table column names", () => {
  it.each([
    "profile.name",
    "__proto__",
    "constructor",
    "toString",
    "__sel",
  ])("displays the stored value of %s", (name) => {
    const columns: ColumnTypeMeta[] = ["id", name].map((column) => ({
      name: column,
      type: "text",
      nativeType: "text",
      category: "text",
      nullable: false,
      isPrimaryKey: column === "id",
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    }));
    render(
      <TableView
        connectionId="c"
        database="db"
        schema="public"
        table="records"
      />,
    );
    act(() =>
      dispatchIncomingMessage("tableInit", {
        columns,
        primaryKeyColumns: ["id"],
      }),
    );
    const request = getPostedMessages()
      .filter((message) => message.type === "fetchPage")
      .at(-1)?.payload as { fetchId: number };
    act(() =>
      dispatchIncomingMessage("tableData", {
        fetchId: request.fetchId,
        rows: [
          Object.fromEntries([
            ["id", "1"],
            [name, "literal stored value"],
          ]),
        ],
        totalCount: 1,
      }),
    );
    const value = screen.getByText("literal stored value");
    const cell = value.closest("td");
    if (!cell) throw new Error("Expected data cell");
    expect(Number.parseFloat(cell.style.width)).toBeGreaterThan(0);
    expect(screen.getAllByRole("checkbox")).toHaveLength(2);
    const ids = [...document.querySelectorAll("thead tr:first-child th")].map(
      (header) => header.getAttribute("data-column-id"),
    );
    expect(new Set(ids).size).toBe(3);
    fireEvent.click(screen.getByRole("button", { name: "Export CSV" }));
    fireEvent.click(screen.getByRole("button", { name: "Export JSON" }));
    expect(
      getPostedMessages().filter((message) =>
        message.type.startsWith("export"),
      ),
    ).toEqual([
      {
        type: "exportCSV",
        payload: { sort: null, filters: [], columnOrder: ["id", name] },
      },
      {
        type: "exportJSON",
        payload: { sort: null, filters: [], columnOrder: ["id", name] },
      },
    ]);
    fireEvent.doubleClick(cell);
    const editor = screen.getByRole("textbox", {
      name: "Cell data",
    }) as HTMLTextAreaElement;
    expect(editor.value).toBe("literal stored value");
    fireEvent.change(editor, { target: { value: "updated literal" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
    const apply = getPostedMessages()
      .filter((message) => message.type === "applyChanges")
      .at(-1);
    expect(apply?.payload).toMatchObject({
      updates: [
        {
          primaryKeys: { id: "1" },
          changes: Object.fromEntries([[name, "updated literal"]]),
          originalValues: Object.fromEntries([[name, "literal stored value"]]),
        },
      ],
    });
  });
});
