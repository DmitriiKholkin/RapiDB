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

import { TableView } from "../../src/webview/components/TableView";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    type: "integer",
    nativeType: "integer",
    category: "integer",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
  {
    name: "qty",
    type: "integer",
    nativeType: "integer",
    category: "integer",
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  },
];

const rows = [
  { id: 1, qty: 5 },
  { id: 2, qty: 7 },
];

function lastFetch() {
  return getPostedMessages()
    .filter((message) => message.type === "fetchPage")
    .at(-1)?.payload as {
    fetchId: number;
    filters: unknown[];
    page: number;
  };
}

function exportMessages() {
  return getPostedMessages().filter((message) =>
    message.type.startsWith("export"),
  );
}

function renderCommitted(totalCount = rows.length) {
  render(
    <TableView connectionId="c" database="db" schema="public" table="items" />,
  );
  act(() =>
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["id"],
    }),
  );
  act(() =>
    dispatchIncomingMessage("tableData", {
      fetchId: lastFetch().fetchId,
      rows,
      totalCount,
    }),
  );
  expect(screen.getByText("7")).toBeTruthy();
}

function requestQtyFilter(value: string) {
  const input = screen.getByLabelText("qty filter value");
  fireEvent.change(input, { target: { value } });
  fireEvent.keyDown(input, { key: "Enter" });
  const request = lastFetch();
  expect(request.filters).toEqual([{ column: "qty", operator: "eq", value }]);
  return request;
}

describe("table export uses the committed view snapshot", () => {
  it("exports the displayed rows' filters after a non-filter read error", () => {
    renderCommitted();
    const request = requestQtyFilter("9999999999");
    act(() =>
      dispatchIncomingMessage("tableError", {
        fetchId: request.fetchId,
        error: 'value "9999999999" is out of range for type integer',
        isFilterError: false,
      }),
    );
    expect(screen.getByText("7")).toBeTruthy();
    clearPostedMessages();

    fireEvent.click(screen.getByRole("button", { name: "Export CSV" }));

    expect(exportMessages()).toEqual([
      {
        type: "exportCSV",
        payload: { sort: null, filters: [], columnOrder: ["id", "qty"] },
      },
    ]);
  });

  it("keeps filters and page coherent when exporting the visible page after a read error", () => {
    renderCommitted(60);
    const request = requestQtyFilter("9999999999");
    act(() =>
      dispatchIncomingMessage("tableError", {
        fetchId: request.fetchId,
        error: "connection reset",
      }),
    );
    clearPostedMessages();

    fireEvent.click(screen.getByRole("button", { name: "Export JSON" }));
    fireEvent.click(
      screen.getByRole("button", { name: "Export visible (2 rows)" }),
    );

    expect(exportMessages()).toEqual([
      {
        type: "exportJSON",
        payload: {
          sort: null,
          filters: [],
          limitToPage: { page: 1, pageSize: 25 },
          columnOrder: ["id", "qty"],
        },
      },
    ]);
  });

  it("exports the filters of rows committed by a successful filtered read", () => {
    renderCommitted();
    const request = requestQtyFilter("7");
    act(() =>
      dispatchIncomingMessage("tableData", {
        fetchId: request.fetchId,
        rows: [rows[1]],
        totalCount: 1,
      }),
    );
    clearPostedMessages();

    fireEvent.click(screen.getByRole("button", { name: "Export CSV" }));

    expect(exportMessages()).toEqual([
      {
        type: "exportCSV",
        payload: {
          sort: null,
          filters: [{ column: "qty", operator: "eq", value: "7" }],
          columnOrder: ["id", "qty"],
        },
      },
    ]);
  });
});
