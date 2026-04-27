import { act, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { SchemaView } from "../../src/webview/components/SchemaView";
import type { ColumnMeta, ForeignKeyMeta } from "../../src/webview/types";
import {
  dispatchIncomingMessage,
  expectNoAxeViolations,
  getPostedMessages,
} from "./testUtils";

function makeColumn(
  column: Omit<
    ColumnMeta,
    | "nativeType"
    | "category"
    | "filterable"
    | "filterOperators"
    | "valueSemantics"
  > & {
    nativeType?: string;
    category?: ColumnMeta["category"];
  },
): ColumnMeta {
  return {
    nativeType: column.nativeType ?? column.type,
    category: column.category ?? "text",
    filterable: true,
    filterOperators: ["eq", "like", "is_null", "is_not_null"],
    valueSemantics: "plain",
    ...column,
  };
}

function getRowForColumn(columnName: string): HTMLElement {
  const cell = screen.getAllByText(columnName)[0];
  const row = cell.closest("tr");

  if (!row) {
    throw new Error(`Expected a table row for column ${columnName}`);
  }

  return row;
}

describe("SchemaView", () => {
  it("renders schema metadata in the redesigned layout", async () => {
    const columns: ColumnMeta[] = [
      makeColumn({
        name: "id",
        type: "bigint",
        category: "integer",
        nullable: false,
        defaultValue: "nextval('users_id_seq'::regclass)",
        defaultKind: "expression",
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
        isForeignKey: false,
        isAutoIncrement: true,
      }),
      makeColumn({
        name: "external_id",
        type: "binary(16)",
        category: "uuid",
        nullable: false,
        defaultValue: "uuid()",
        defaultKind: "expression",
        isPrimaryKey: false,
        isForeignKey: false,
        isAutoIncrement: false,
      }),
      makeColumn({
        name: "updated_at",
        type: "timestamp(6)",
        category: "datetime",
        nullable: false,
        defaultValue: "CURRENT_TIMESTAMP(6)",
        defaultKind: "expression",
        onUpdateExpression: "CURRENT_TIMESTAMP(6)",
        isPrimaryKey: false,
        isForeignKey: false,
        isAutoIncrement: false,
      }),
      makeColumn({
        name: "display_name",
        type: "varchar(255)",
        category: "text",
        nullable: false,
        isComputed: true,
        computedExpression: "concat(first_name, ' ', last_name)",
        generatedKind: "stored",
        isPersisted: true,
        isPrimaryKey: false,
        isForeignKey: false,
        isAutoIncrement: false,
      }),
      makeColumn({
        name: "search_name",
        type: "varchar(255)",
        category: "text",
        nullable: false,
        isComputed: true,
        computedExpression: "lower(display_name)",
        generatedKind: "virtual",
        isPersisted: false,
        isPrimaryKey: false,
        isForeignKey: false,
        isAutoIncrement: false,
      }),
      makeColumn({
        name: "owner_id",
        type: "bigint",
        category: "integer",
        nullable: false,
        isPrimaryKey: false,
        isForeignKey: true,
        isAutoIncrement: false,
      }),
    ];

    const foreignKeys: ForeignKeyMeta[] = [
      {
        column: "owner_id",
        referencedSchema: "public",
        referencedTable: "accounts",
        referencedColumn: "id",
        constraintName: "users_owner_id_fkey",
      },
    ];

    const { container } = render(
      <SchemaView
        connectionId="conn-1"
        database="app"
        schema="public"
        table="users"
      />,
    );

    expect(getPostedMessages()).toEqual([{ type: "ready" }]);

    await act(async () => {
      dispatchIncomingMessage("schemaData", {
        columns,
        indexes: [],
        foreignKeys,
      });
    });

    await waitFor(() => {
      expect(screen.getByRole("columnheader", { name: "Null" })).toBeTruthy();
      expect(
        screen.getByRole("columnheader", { name: "Default / Generated" }),
      ).toBeTruthy();
    });

    // id: PK + AI badges, default shown as a SQL chip
    const idRow = within(getRowForColumn("id"));
    expect(idRow.getByText("PK")).toBeTruthy();
    expect(idRow.getByText("AI")).toBeTruthy();
    expect(
      idRow.getByText("DEFAULT nextval('users_id_seq'::regclass)"),
    ).toBeTruthy();

    // external_id: expression default chip
    const externalIdRow = within(getRowForColumn("external_id"));
    expect(externalIdRow.getByText("DEFAULT uuid()")).toBeTruthy();

    // updated_at: two separate chips — default and ON UPDATE
    const updatedAtRow = within(getRowForColumn("updated_at"));
    expect(updatedAtRow.getByText("DEFAULT CURRENT_TIMESTAMP(6)")).toBeTruthy();
    expect(
      updatedAtRow.getByText("ON UPDATE CURRENT_TIMESTAMP(6)"),
    ).toBeTruthy();

    // display_name: GEN badge + STORED generated expression chip
    const displayNameRow = within(getRowForColumn("display_name"));
    expect(displayNameRow.getByText("GEN")).toBeTruthy();
    expect(
      displayNameRow.getByText(
        "GENERATED ALWAYS AS (concat(first_name, ' ', last_name)) STORED",
      ),
    ).toBeTruthy();

    // search_name: GEN badge + VIRTUAL generated expression chip
    const searchNameRow = within(getRowForColumn("search_name"));
    expect(searchNameRow.getByText("GEN")).toBeTruthy();
    expect(
      searchNameRow.getByText(
        "GENERATED ALWAYS AS (lower(display_name)) VIRTUAL",
      ),
    ).toBeTruthy();

    // owner_id: FK badge, no default → dash placeholder
    const ownerIdRow = within(getRowForColumn("owner_id"));
    expect(ownerIdRow.getByText("FK")).toBeTruthy();
    expect(ownerIdRow.getByText("—")).toBeTruthy();

    await expectNoAxeViolations(container);
  });
});
