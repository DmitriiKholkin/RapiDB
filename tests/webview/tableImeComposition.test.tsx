import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { ColumnFilterControl } from "../../src/webview/components/table/ColumnFilterControl";
import { LargeMonacoDialog } from "../../src/webview/components/table/TableDialogs";

vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: ({
    initialValue,
    ariaLabel,
  }: {
    initialValue: string;
    ariaLabel: string;
  }) => <textarea aria-label={ariaLabel} defaultValue={initialValue} />,
}));

const textColumn = {
  name: "name",
  type: "TEXT",
  nativeType: "TEXT",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  category: "text",
  filterable: true,
  filterOperators: ["eq", "like", "between", "is_null", "is_not_null"],
  valueSemantics: "plain",
} as ColumnTypeMeta;

describe("IME composition in table dialogs and filters", () => {
  it("does not cancel the structured cell dialog on Escape while composing", () => {
    const onCancel = vi.fn();
    render(
      <LargeMonacoDialog
        title="Edit"
        description="JSON"
        value="{}"
        language="json"
        ariaLabel="Structured value"
        confirmLabel="Save"
        onCancel={onCancel}
        onConfirm={vi.fn()}
      />,
    );
    const editor = screen.getByLabelText("Structured value");

    fireEvent.keyDown(editor, { key: "Escape", isComposing: true });
    fireEvent.keyDown(editor, { key: "Escape", keyCode: 229 });
    expect(onCancel).not.toHaveBeenCalled();

    fireEvent.keyDown(editor, { key: "Escape" });
    expect(onCancel).toHaveBeenCalledTimes(1);
  });

  it("does not apply a scalar filter on the Enter that confirms a conversion", () => {
    const onChange = vi.fn();
    render(
      <ColumnFilterControl
        column={textColumn}
        draft={{ operator: "eq", value: "にほん" }}
        onChange={onChange}
      />,
    );
    const input = screen.getByLabelText("name filter value");

    fireEvent.keyDown(input, { key: "Enter", isComposing: true });
    fireEvent.keyDown(input, { key: "Enter", keyCode: 229 });
    expect(onChange).not.toHaveBeenCalled();

    fireEvent.keyDown(input, { key: "Enter" });
    expect(onChange).toHaveBeenCalledWith(
      { operator: "eq", value: "にほん" },
      { applyImmediately: true },
    );
  });

  it("does not apply a between filter on the Enter that confirms a conversion", () => {
    const onChange = vi.fn();
    render(
      <ColumnFilterControl
        column={textColumn}
        draft={{ operator: "between", value: ["あ", "ん"] }}
        onChange={onChange}
      />,
    );
    const start = screen.getByLabelText("name filter start");
    const end = screen.getByLabelText("name filter end");

    fireEvent.keyDown(start, { key: "Enter", keyCode: 229 });
    fireEvent.keyDown(end, { key: "Enter", keyCode: 229 });
    expect(onChange).not.toHaveBeenCalled();

    fireEvent.keyDown(end, { key: "Enter" });
    expect(onChange).toHaveBeenCalledWith(
      { operator: "between", value: ["あ", "ん"] },
      { applyImmediately: true },
    );
  });
});
