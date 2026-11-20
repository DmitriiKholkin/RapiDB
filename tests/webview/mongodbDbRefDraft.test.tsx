import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import type {
  ColumnTypeMeta,
  MongoValueTypeHint,
} from "../../src/shared/tableTypes";
import { parseTablePanelMessage } from "../../src/shared/webviewContracts";
import { TableView } from "../../src/webview/components/TableView";
import {
  parsePersistedTableDraft,
  rowPrimaryKeySignature,
  serializeTableDraft,
} from "../../src/webview/components/table/tableViewHelpers";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: () => ({
    getVirtualItems: () => [{ key: 0, index: 0, start: 0, end: 26 }],
    getTotalSize: () => 26,
  }),
}));
vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: ({
    initialValue,
    ariaLabel,
    onChange,
  }: {
    initialValue?: string;
    ariaLabel?: string;
    onChange?: (value: string) => void;
  }) => (
    <textarea
      defaultValue={initialValue}
      aria-label={ariaLabel}
      onChange={(event) => onChange?.(event.target.value)}
    />
  ),
}));

const hex = "507f1f77bcf86cd799439011";
const legacyText = `{"$ref":"other","$id":"${hex}"}`;
const oidText = `{"$ref":"other","$id":{"$oid":"${hex}"}}`;
const stringText = `{"$ref":"other","$id":{"$string":"${hex}"}}`;
const tableKey = JSON.stringify(["conn", "db", "db", "items"]);
const typedRef = {
  category: "other",
  nativeType: "dbRef",
  dbRefTextVersion: 1,
} satisfies MongoValueTypeHint;
const columns: ColumnTypeMeta[] = [
  {
    name: "_id",
    type: "string",
    nativeType: "string",
    category: "text",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  },
  {
    ...typedRef,
    name: "ref",
    type: "dbRef",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["like"],
    valueSemantics: "plain",
  },
];
let state: Record<string, unknown>;

beforeEach(() => {
  state = {};
  const vscode = window.__vscode;
  if (!vscode) throw new Error("Missing VS Code API");
  vi.mocked(vscode.getState).mockImplementation(() => state);
  vi.mocked(vscode.setState).mockImplementation((next) => {
    state = JSON.parse(JSON.stringify(next));
    return next;
  });
  clearPostedMessages();
});

function mount(ref: string) {
  const view = render(
    <TableView
      connectionId="conn"
      database="db"
      schema="db"
      table="items"
      mongoRowIdentity
    />,
  );
  act(() =>
    dispatchIncomingMessage("tableInit", {
      columns,
      primaryKeyColumns: ["_id"],
    }),
  );
  const request = getPostedMessages().find(({ type }) => type === "fetchPage")
    ?.payload as { fetchId: number };
  act(() =>
    dispatchIncomingMessage(
      "tableData",
      JSON.parse(
        JSON.stringify({
          fetchId: request.fetchId,
          rows: [{ _id: "row", ref }],
          totalCount: 1,
          mongoIdTypes: ["string"],
          rowValueTypes: [{ ref: typedRef }],
        }),
      ),
    ),
  );
  return view;
}

it("round-trips an explicit same-spelling string edit through the grid and a saved draft", () => {
  const view = mount(oidText);
  const cell = screen.getByText(oidText);
  fireEvent.doubleClick(cell);
  const editor = screen.getByLabelText<HTMLInputElement>("Cell data");
  expect(JSON.parse(editor.value)).toEqual(JSON.parse(oidText));
  fireEvent.change(editor, { target: { value: stringText } });
  fireEvent.click(screen.getByRole("button", { name: "Apply" }));

  expect(
    parsePersistedTableDraft(state.tableDraft, tableKey)?.restoreState
      .entries[0],
  ).toMatchObject({
    changes: new Map([["ref", stringText]]),
    originalValues: new Map([["ref", oidText]]),
    originalTypes: new Map([["ref", typedRef]]),
  });
  view.unmount();
  clearPostedMessages();
  mount(oidText);
  fireEvent.click(screen.getByRole("button", { name: "Apply Changes" }));
  const message = getPostedMessages().find(
    ({ type }) => type === "applyChanges",
  );
  expect(parseTablePanelMessage(message)).toMatchObject({
    type: "applyChanges",
    payload: {
      updates: [
        {
          primaryKeys: {
            _id: { $rapidbMongoId: { type: "string", value: "row" } },
          },
          changes: { ref: stringText },
          originalValues: { ref: oidText },
          originalTypes: { ref: typedRef },
        },
      ],
    },
  });
});

it.each([
  [legacyText, oidText],
  [legacyText, stringText],
  [oidText, oidText], // Legacy text could also come from a literal BSON document ID.
])("keeps a pre-fix DBRef draft unresolved: %s against %s", (baseline, ref) => {
  const signature = rowPrimaryKeySignature({ _id: "row" }, ["_id"], "string");
  expect.assert(signature);
  state.tableDraft = serializeTableDraft(
    tableKey,
    {
      entries: [
        {
          originalSignature: signature,
          changes: new Map([["ref", legacyText]]),
          originalValues: new Map([["ref", baseline]]),
          originalTypes: new Map([
            ["ref", { category: "other", nativeType: "dbRef" }],
          ]),
          originalMissingFields: new Set(),
          baselinePresenceVersion: 1,
        },
      ],
    },
    [],
  );

  mount(ref);
  expect(
    screen.getByText(/edits are preserved but not attached/i),
  ).toBeTruthy();
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Apply Changes" })
      .disabled,
  ).toBe(true);
  expect(
    parsePersistedTableDraft(
      state.tableDraft,
      tableKey,
    )?.restoreState.entries[0]?.changes.get("ref"),
  ).toBe(legacyText);
});
