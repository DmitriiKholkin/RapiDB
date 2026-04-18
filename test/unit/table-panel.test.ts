import { describe, expect, it, vi } from "vitest";
import {
  coerceFilterExpressions,
  NULL_SENTINEL,
} from "../../src/shared/tableTypes";

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

describe("tablePanel structured filter coercion", () => {
  it("keeps structured filter payloads unchanged", () => {
    const filters = coerceFilterExpressions([
      { column: "name", operator: "like", value: "alice" },
    ]);

    expect(filters).toEqual([
      { column: "name", operator: "like", value: "alice" },
    ]);
  });

  it("accepts structured NULL filters", () => {
    const filters = coerceFilterExpressions([
      { column: "name", operator: "is_null" },
    ]);

    expect(filters).toEqual([{ column: "name", operator: "is_null" }]);
  });

  it("drops legacy value-only payloads instead of inferring operators", () => {
    const filters = coerceFilterExpressions([
      { column: "created_on", value: "2026-04-15" },
      { column: "name", value: NULL_SENTINEL },
    ]);

    expect(filters).toEqual([]);
  });

  it("drops malformed between payloads", () => {
    const filters = coerceFilterExpressions([
      { column: "created_on", operator: "between", value: ["2026-04-15"] },
    ]);

    expect(filters).toEqual([]);
  });
});
