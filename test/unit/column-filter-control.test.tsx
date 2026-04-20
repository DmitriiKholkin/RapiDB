/**
 * @vitest-environment jsdom
 */

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { ColumnFilterControl } from "../../src/webview/components/table/ColumnFilterControl";

afterEach(cleanup);

function makeColumn(
  overrides: Partial<ColumnTypeMeta> & { name: string; type: string },
): ColumnTypeMeta {
  const { name, type, ...rest } = overrides;

  return {
    ...rest,
    name,
    type,
    nativeType: rest.nativeType ?? type,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    filterable: true,
    editable: true,
    filterOperators: ["like", "between", "is_null"],
    isBoolean: false,
  };
}

describe("ColumnFilterControl", () => {
  it("preserves the scalar draft value when switching to between", () => {
    const onChange = vi.fn();
    render(
      <ColumnFilterControl
        column={makeColumn({
          name: "created_on",
          type: "date",
          category: "date",
        })}
        draft={{ operator: "like", value: "2026-04-15" }}
        onChange={onChange}
      />,
    );

    fireEvent.click(screen.getByLabelText("created_on filter operator"));
    fireEvent.click(screen.getByRole("menuitemradio", { name: /Between$/ }));

    expect(onChange).toHaveBeenCalledWith({
      operator: "between",
      value: ["2026-04-15", ""],
    });
  });

  it("reuses the lower bound when switching from between to a scalar operator", () => {
    const onChange = vi.fn();
    render(
      <ColumnFilterControl
        column={makeColumn({
          name: "created_on",
          type: "date",
          category: "date",
        })}
        draft={{ operator: "between", value: ["2026-04-01", "2026-04-15"] }}
        onChange={onChange}
      />,
    );

    fireEvent.click(screen.getByLabelText("created_on filter operator"));
    fireEvent.click(screen.getByRole("menuitemradio", { name: /Contains$/ }));

    expect(onChange).toHaveBeenCalledWith({
      operator: "like",
      value: "2026-04-01",
    });
  });
});
