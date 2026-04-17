/**
 * @vitest-environment jsdom
 */

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NewRowForm } from "../../src/webview/components/table/NewRowForm";
import { type ColumnMeta, NULL_SENTINEL } from "../../src/webview/types";

afterEach(cleanup);

function makeColumn(
  overrides: Partial<ColumnMeta> & { name: string; type: string },
): ColumnMeta {
  return {
    name: overrides.name,
    type: overrides.type,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    nativeType: overrides.type,
    filterable: true,
    editable: true,
    filterOperators: ["like"],
    isBoolean: false,
    ...overrides,
  };
}

describe("NewRowForm", () => {
  it("renders non-insertable columns as disabled", () => {
    render(
      <NewRowForm
        columns={[
          makeColumn({
            name: "id",
            type: "integer",
            category: "integer",
            editable: true,
            isAutoIncrement: true,
          }),
          makeColumn({ name: "readonly_col", type: "text", editable: false }),
          makeColumn({ name: "name", type: "text" }),
        ]}
        newRow={{}}
        setNewRow={vi.fn()}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    expect(screen.getByText("id")).toBeDefined();
    expect(screen.getByText("readonly_col")).toBeDefined();
    expect(screen.getByText("name")).toBeDefined();

    const inputs = screen.getAllByRole("textbox");
    expect((inputs[0] as HTMLInputElement).disabled).toBe(true);
    expect((inputs[1] as HTMLInputElement).disabled).toBe(true);
    expect((inputs[2] as HTMLInputElement).disabled).toBe(false);
  });

  it("preserves an explicit empty string after the user clears a text field", () => {
    const setNewRow = vi.fn();
    const column = makeColumn({ name: "name", type: "text" });
    const { rerender } = render(
      <NewRowForm
        columns={[column]}
        newRow={{}}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("filter");
    fireEvent.change(input, { target: { value: "alpha" } });
    expect(setNewRow).toHaveBeenLastCalledWith({ name: "alpha" });

    rerender(
      <NewRowForm
        columns={[column]}
        newRow={{ name: "alpha" }}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByDisplayValue("alpha"), {
      target: { value: "" },
    });
    expect(setNewRow).toHaveBeenLastCalledWith({ name: "" });
  });

  it("preserves an explicit empty string for SET-backed fields", () => {
    const setNewRow = vi.fn();
    const column = makeColumn({
      name: "tags",
      type: "set",
      nativeType: "set('a','b')",
      category: "enum",
    });
    const { rerender } = render(
      <NewRowForm
        columns={[column]}
        newRow={{}}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText("filter"), {
      target: { value: "a,b" },
    });
    expect(setNewRow).toHaveBeenLastCalledWith({ tags: "a,b" });

    rerender(
      <NewRowForm
        columns={[column]}
        newRow={{ tags: "a,b" }}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByDisplayValue("a,b"), {
      target: { value: "" },
    });

    expect(setNewRow).toHaveBeenLastCalledWith({ tags: "" });
  });

  it("omits plain enum fields when the user clears them", () => {
    const setNewRow = vi.fn();
    const column = makeColumn({
      name: "status",
      type: "enum",
      nativeType: "enum('a','b','c')",
      category: "enum",
    });

    render(
      <NewRowForm
        columns={[column]}
        newRow={{ status: "b" }}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByDisplayValue("b"), {
      target: { value: "" },
    });

    expect(setNewRow).toHaveBeenLastCalledWith({});
  });

  it("removes typed draft keys when the user clears a typed input", () => {
    const setNewRow = vi.fn();
    const column = makeColumn({
      name: "created_on",
      type: "date",
      category: "date",
    });

    render(
      <NewRowForm
        columns={[column]}
        newRow={{ created_on: "2026-04-15" }}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByDisplayValue("2026-04-15"), {
      target: { value: "" },
    });

    expect(setNewRow).toHaveBeenLastCalledWith({});
  });

  it("restores nullable text fields to an omitted draft when NULL is cleared", () => {
    const setNewRow = vi.fn();
    render(
      <NewRowForm
        columns={[makeColumn({ name: "name", type: "text" })]}
        newRow={{ name: NULL_SENTINEL }}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByTitle("Remove NULL"));
    expect(setNewRow).toHaveBeenCalledWith({});
  });

  it("restores nullable typed fields to an omitted draft when NULL is cleared", () => {
    const setNewRow = vi.fn();
    render(
      <NewRowForm
        columns={[
          makeColumn({ name: "id", type: "integer", category: "integer" }),
        ]}
        newRow={{ id: NULL_SENTINEL }}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByTitle("Remove NULL"));
    expect(setNewRow).toHaveBeenCalledWith({});
  });

  it("keeps boolean drafts distinct for untouched, explicit values, and key removal", () => {
    const setNewRow = vi.fn();
    const boolColumn = makeColumn({
      name: "active",
      type: "boolean",
      category: "boolean",
      isBoolean: true,
    });
    const { rerender } = render(
      <NewRowForm
        columns={[boolColumn]}
        newRow={{}}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByRole("combobox"), {
      target: { value: "true" },
    });
    expect(setNewRow).toHaveBeenLastCalledWith({ active: "true" });

    rerender(
      <NewRowForm
        columns={[boolColumn]}
        newRow={{ active: "true" }}
        setNewRow={setNewRow}
        inserting={false}
        onInsert={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByRole("combobox"), { target: { value: "" } });
    expect(setNewRow).toHaveBeenLastCalledWith({});
  });

  it("removes nullable boolean draft key when NULL is cleared", async () => {
    const setNewRow = vi.fn();

    render(
      <NewRowForm
        columns={[
          makeColumn({
            name: "active",
            type: "boolean",
            category: "boolean",
            isBoolean: true,
          }),
        ]}
        newRow={{ active: NULL_SENTINEL }}
        setNewRow={setNewRow}
        onInsert={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByTitle("Remove NULL"));

    expect(setNewRow).toHaveBeenCalledWith({});
  });
});
