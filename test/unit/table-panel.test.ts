import { describe, expect, it, vi } from "vitest";
import { NULL_SENTINEL } from "../../src/extension/dbDrivers/types";

vi.mock("vscode", () => ({
  ProgressLocation: { Notification: 1 },
  Uri: {
    file: vi.fn(),
    joinPath: vi.fn(),
  },
  ViewColumn: { One: 1 },
  window: {},
  workspace: {},
}));

import { __testOnly } from "../../src/extension/panels/tablePanel";

describe("tablePanel legacy filter normalization", () => {
  it("keeps structured filter payloads unchanged", () => {
    const filters = __testOnly.normalizeFilters(
      [{ column: "name", operator: "like", value: "alice" }],
      [
        {
          name: "name",
          type: "text",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "text",
          nativeType: "text",
          filterable: true,
          editable: true,
          filterOperators: ["like"],
          isBoolean: false,
        },
      ],
    );

    expect(filters).toEqual([
      { column: "name", operator: "like", value: "alice" },
    ]);
  });

  it("falls back to eq for legacy date filters", () => {
    const filters = __testOnly.normalizeFilters(
      [{ column: "created_on", value: "2026-04-15" }],
      [
        {
          name: "created_on",
          type: "date",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "date",
          nativeType: "date",
          filterable: true,
          editable: true,
          filterOperators: ["eq", "like"],
          isBoolean: false,
        },
      ],
    );

    expect(filters).toEqual([
      { column: "created_on", operator: "eq", value: "2026-04-15" },
    ]);
  });

  it("keeps the legacy NULL sentinel fallback", () => {
    const filters = __testOnly.normalizeFilters(
      [{ column: "name", value: NULL_SENTINEL }],
      [
        {
          name: "name",
          type: "text",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "text",
          nativeType: "text",
          filterable: true,
          editable: true,
          filterOperators: ["like", "is_null"],
          isBoolean: false,
        },
      ],
    );

    expect(filters).toEqual([{ column: "name", operator: "is_null" }]);
  });

  it("falls back to eq for legacy boolean filters", () => {
    const filters = __testOnly.normalizeFilters(
      [{ column: "active", value: "true" }],
      [
        {
          name: "active",
          type: "boolean",
          nullable: false,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "boolean",
          nativeType: "boolean",
          filterable: true,
          editable: true,
          filterOperators: ["eq", "neq"],
          isBoolean: true,
        },
      ],
    );

    expect(filters).toEqual([
      { column: "active", operator: "eq", value: "true" },
    ]);
  });

  it("drops malformed between payloads", () => {
    const filters = __testOnly.normalizeFilters(
      [{ column: "created_on", operator: "between", value: ["2026-04-15"] }],
      [
        {
          name: "created_on",
          type: "date",
          nullable: true,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "date",
          nativeType: "date",
          filterable: true,
          editable: true,
          filterOperators: ["between"],
          isBoolean: false,
        },
      ],
    );

    expect(filters).toEqual([]);
  });
});
