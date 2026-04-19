import { describe, expect, it } from "vitest";
import { NULL_SENTINEL as extensionNullSentinel } from "../../src/extension/dbDrivers/types";
import {
  buildFilterExpression,
  coerceFilterExpressions,
  defaultFilterOperator,
  type FilterOperator,
  isNumericCategory,
  NULL_SENTINEL as sharedNullSentinel,
  valueFilterOperator,
} from "../../src/shared/tableTypes";
import { NULL_SENTINEL as webviewNullSentinel } from "../../src/webview/types";

describe("shared contract parity", () => {
  it("keeps the NULL sentinel aligned across shared, extension, and webview layers", () => {
    expect(extensionNullSentinel).toBe(sharedNullSentinel);
    expect(webviewNullSentinel).toBe(sharedNullSentinel);
  });

  it("centralizes the default value-filter operator policy in shared helpers", () => {
    expect(
      defaultFilterOperator({ category: "boolean", isBoolean: true }),
    ).toBe("eq");
    expect(
      defaultFilterOperator({ category: "integer", isBoolean: false }),
    ).toBe("eq");
    expect(defaultFilterOperator({ category: "date", isBoolean: false })).toBe(
      "eq",
    );
    expect(defaultFilterOperator({ category: "text", isBoolean: false })).toBe(
      "like",
    );
  });

  it("uses shared operator policy only when the column supports value filters", () => {
    expect(
      valueFilterOperator({
        category: "text",
        filterable: true,
        filterOperators: ["like", "is_null"],
        isBoolean: false,
      }),
    ).toBe("like");

    expect(
      valueFilterOperator({
        category: "text",
        filterable: true,
        filterOperators: ["eq", "is_null"],
        isBoolean: false,
      }),
    ).toBeNull();
  });

  it("keeps numeric category detection in shared helpers", () => {
    expect(isNumericCategory("integer")).toBe(true);
    expect(isNumericCategory("float")).toBe(true);
    expect(isNumericCategory("decimal")).toBe(true);
    expect(isNumericCategory("text")).toBe(false);
    expect(isNumericCategory("datetime")).toBe(false);
  });

  it("keeps structured filter coercion in shared helpers", () => {
    expect(
      coerceFilterExpressions([
        { column: "created_on", operator: "eq", value: "2026-04-15" },
        { column: "name", operator: "is_null" },
      ]),
    ).toEqual([
      { column: "created_on", operator: "eq", value: "2026-04-15" },
      { column: "name", operator: "is_null" },
    ]);

    expect(
      coerceFilterExpressions([
        { column: "created_on", value: "2026-04-15" },
        { column: "name", value: sharedNullSentinel },
      ]),
    ).toEqual([]);
  });

  describe("buildFilterExpression", () => {
    const column = {
      name: "name",
      type: "text",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "text" as const,
      nativeType: "text",
      filterable: true,
      editable: true,
      filterOperators: ["like"] as FilterOperator[],
      isBoolean: false,
    };

    it("builds a structured value filter", () => {
      expect(buildFilterExpression(column, " alice ")).toEqual({
        column: "name",
        operator: "like",
        value: "alice",
      });
    });

    it("builds a NULL filter only when supported", () => {
      expect(
        buildFilterExpression(
          { ...column, filterOperators: ["like", "is_null"] },
          sharedNullSentinel,
        ),
      ).toEqual({
        column: "name",
        operator: "is_null",
      });
      expect(buildFilterExpression(column, sharedNullSentinel)).toBeNull();
    });

    it("skips value filters when the column cannot accept the shared default operator", () => {
      expect(
        buildFilterExpression({ ...column, filterOperators: ["eq"] }, "alice"),
      ).toBeNull();

      expect(
        buildFilterExpression(
          {
            ...column,
            filterable: false,
            filterOperators: ["like", "is_null"],
          },
          "alice",
        ),
      ).toBeNull();
    });

    it("skips empty values", () => {
      expect(buildFilterExpression(column, "   ")).toBeNull();
    });
  });
});
