import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { TableView } from "../../src/webview/components/TableView";
import { validatePasteValue } from "../../src/webview/utils/pasteUtils";
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

function makeColumn(
  name: string,
  category: ColumnTypeMeta["category"],
  nativeType: string,
  isPrimaryKey = false,
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    nullable: true,
    isPrimaryKey,
    isForeignKey: false,
    category,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
}

const SQL_INTS = [
  ["MySQL 8 int", makeColumn("qty", "integer", "int")],
  ["MSSQL int", makeColumn("qty", "integer", "int")],
  ["SQLite int", makeColumn("qty", "integer", "int")],
  ["MSSQL bigint", makeColumn("qty", "integer", "bigint")],
] as const;
const MONGO_INT = makeColumn("qty", "integer", "int");
const MONGO_LONG = makeColumn("qty", "integer", "long");
const DYNAMO_NUMBER = makeColumn("qty", "integer", "number");
const TIMESTAMP = makeColumn("ts", "datetime", "timestamp");
const DATE = makeColumn("ts", "datetime", "date");
const MONGO = { mongoTypes: true };

describe("integer comma grouping depends on the column source", () => {
  it.each(SQL_INTS)("accepts US-grouped 1,234 for %s", (_label, column) => {
    expect(validatePasteValue("1,234", column)).toEqual({
      valid: true,
      coercedValue: "1234",
    });
    expect(validatePasteValue("1,234", column, false, {})).toEqual({
      valid: true,
      coercedValue: "1234",
    });
  });

  it.each([
    MONGO_INT,
    MONGO_LONG,
  ])("rejects 1,234 as ambiguous for Mongo $nativeType", (column) => {
    const result = validatePasteValue("1,234", column, false, MONGO);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("Ambiguous");
  });

  it("rejects 1,234 for Dynamo number with or without the Mongo flag", () => {
    for (const options of [{}, MONGO]) {
      const result = validatePasteValue("1,234", DYNAMO_NUMBER, false, options);
      expect(result.valid).toBe(false);
      expect(result.error).toContain("Ambiguous");
    }
  });
});

describe("ambiguous comma hint", () => {
  it.each([
    MONGO_INT,
    DYNAMO_NUMBER,
  ])("suggests only the plain integer for $nativeType", (column) => {
    const result = validatePasteValue("1,234", column, false, MONGO);
    expect(result.error).toContain("(write 1234)");
    expect(result.error).not.toContain("1,234.00");
    expect(validatePasteValue("1,234.00", column, false, MONGO).valid).toBe(
      false,
    );
  });

  it("keeps the decimal spelling for decimal columns", () => {
    const result = validatePasteValue(
      "1,234",
      makeColumn("amount", "decimal", "numeric(10,2)"),
    );
    expect(result.error).toContain("(write 1234 or 1,234.00)");
  });
});

describe("quoted text in datetime columns depends on the column source", () => {
  it.each([
    TIMESTAMP,
    DATE,
  ])("rejects quoted text in a SQL $nativeType column", (column) => {
    for (const value of ['"pending"', "'N/A'"]) {
      expect(validatePasteValue(value, column).valid).toBe(false);
    }
  });

  it.each([
    TIMESTAMP,
    DATE,
  ])("accepts quoted text in a Mongo $nativeType field", (column) => {
    expect(validatePasteValue('"pending"', column, false, MONGO)).toEqual({
      valid: true,
      coercedValue: '"pending"',
    });
  });
});

const messages = (type: string) =>
  getPostedMessages().filter((message) => message.type === type);

function setup(mongoRowIdentity: boolean) {
  clearPostedMessages();
  const columns = [
    makeColumn("_id", "text", "text", true),
    makeColumn("qty", "integer", "int"),
  ];
  const view = render(
    <TableView
      connectionId="c"
      database="db"
      schema="public"
      table="paste_gating"
      mongoRowIdentity={mongoRowIdentity}
    />,
  );
  act(() =>
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["_id"],
    }),
  );
  const request = messages("fetchPage").at(-1)?.payload as { fetchId: number };
  act(() =>
    dispatchIncomingMessage("tableData", {
      fetchId: request.fetchId,
      rows: [{ _id: "1", qty: 5 }],
      totalCount: 1,
    }),
  );
  const cell = (index: number) => {
    const element = view.container.querySelector<HTMLElement>(
      `td[data-row="${index}"][data-col="2"]`,
    );
    if (!element) throw new Error(`Missing qty cell ${index}`);
    return element;
  };
  const paste = (index: number, text: string) => {
    fireEvent.mouseDown(cell(index), { button: 0 });
    fireEvent.mouseUp(document);
    clearPostedMessages();
    fireEvent.paste(document.activeElement as HTMLElement);
    const pasteRequest = messages("readClipboard").at(-1);
    expect(pasteRequest).toBeDefined();
    act(() =>
      dispatchIncomingMessage("clipboardText", {
        ...(pasteRequest?.payload as object),
        text,
      }),
    );
  };
  return { cell, paste };
}

describe("TableView passes the Mongo flag into paste validation", () => {
  it.each([
    false,
    true,
  ])("accepts 1,234 into a SQL int column (draft=%s)", (draft) => {
    const grid = setup(false);
    const index = draft ? -1 : 0;
    if (draft) fireEvent.click(screen.getByRole("button", { name: "Add Row" }));
    grid.paste(index, "1,234");
    expect(screen.queryByText(/Paste failed/)).toBeNull();
    expect(grid.cell(index).textContent).toContain("1234");
  });

  it.each([
    false,
    true,
  ])("rejects 1,234 into a Mongo int field (draft=%s)", (draft) => {
    const grid = setup(true);
    const index = draft ? -1 : 0;
    if (draft) fireEvent.click(screen.getByRole("button", { name: "Add Row" }));
    grid.paste(index, "1,234");
    expect(screen.getByText(/Paste failed/)).toBeTruthy();
    expect(screen.getByText(/Ambiguous number value/)).toBeTruthy();
    expect(grid.cell(index).textContent).not.toContain("1234");
  });
});
