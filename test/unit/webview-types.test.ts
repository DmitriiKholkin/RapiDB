import { describe, expect, it } from "vitest";
import {
  buildFilterExpression,
  categoryColor,
  categoryLabel,
  defaultFilterOperator,
  isNumericCategory,
  NULL_SENTINEL,
  placeholderForCategory,
  type TypeCategory,
  valueFilterOperator,
} from "../../src/webview/types";

describe("NULL_SENTINEL", () => {
  it("matches the backend value", () => {
    expect(NULL_SENTINEL).toBe("\x00__NULL__\x00");
  });
});

describe("placeholderForCategory", () => {
  it("returns 'true / false' for boolean columns", () => {
    expect(placeholderForCategory("boolean", true)).toBe("true / false");
    expect(placeholderForCategory("text", true)).toBe("true / false");
  });

  it("returns 'number' for numeric categories", () => {
    expect(placeholderForCategory("integer", false)).toBe("number");
    expect(placeholderForCategory("float", false)).toBe("number");
    expect(placeholderForCategory("decimal", false)).toBe("number");
  });

  it("returns date/time format strings", () => {
    expect(placeholderForCategory("date", false)).toBe("YYYY-MM-DD");
    expect(placeholderForCategory("time", false)).toBe("HH:MM:SS");
    expect(placeholderForCategory("datetime", false)).toBe(
      "YYYY-MM-DD HH:MM:SS",
    );
  });

  it("returns appropriate placeholders for other categories", () => {
    expect(placeholderForCategory("uuid", false)).toBe("UUID");
    expect(placeholderForCategory("json", false)).toBe('{"key": "value"}');
    expect(placeholderForCategory("binary", false)).toBe("\\xHEX");
    expect(placeholderForCategory("spatial", false)).toBe("POINT(x y)");
    expect(placeholderForCategory("interval", false)).toBe("interval");
    expect(placeholderForCategory("array", false)).toBe("[1, 2, 3]");
    expect(placeholderForCategory("enum", false)).toBe("value");
  });

  it("returns 'filter' for text and other categories", () => {
    expect(placeholderForCategory("text", false)).toBe("filter");
    expect(placeholderForCategory("other", false)).toBe("filter");
    expect(placeholderForCategory("lob", false)).toBe("filter");
  });
});

describe("isNumericCategory", () => {
  it("returns true for numeric categories", () => {
    expect(isNumericCategory("integer")).toBe(true);
    expect(isNumericCategory("float")).toBe(true);
    expect(isNumericCategory("decimal")).toBe(true);
  });

  it("returns false for non-numeric categories", () => {
    const nonNumeric: TypeCategory[] = [
      "text",
      "boolean",
      "date",
      "time",
      "datetime",
      "binary",
      "json",
      "uuid",
      "spatial",
      "interval",
      "array",
      "enum",
      "lob",
      "other",
    ];
    for (const cat of nonNumeric) {
      expect(isNumericCategory(cat)).toBe(false);
    }
  });
});

describe("defaultFilterOperator", () => {
  it("uses equality for typed boolean/numeric/date filters", () => {
    expect(
      defaultFilterOperator({ category: "boolean", isBoolean: true }),
    ).toBe("eq");
    expect(
      defaultFilterOperator({ category: "integer", isBoolean: false }),
    ).toBe("eq");
    expect(defaultFilterOperator({ category: "date", isBoolean: false })).toBe(
      "eq",
    );
  });

  it("uses like for text-like filters", () => {
    expect(defaultFilterOperator({ category: "text", isBoolean: false })).toBe(
      "like",
    );
    expect(
      defaultFilterOperator({ category: "datetime", isBoolean: false }),
    ).toBe("like");
  });
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
    filterOperators: ["like"],
    isBoolean: false,
  };

  it("builds a structured value filter", () => {
    expect(buildFilterExpression(column, " alice ")).toEqual({
      column: "name",
      operator: "like",
      value: "alice",
    });
  });

  it("builds a NULL filter", () => {
    expect(
      buildFilterExpression(
        { ...column, filterOperators: ["like", "is_null"] },
        NULL_SENTINEL,
      ),
    ).toEqual({
      column: "name",
      operator: "is_null",
    });
  });

  it("skips NULL filters when is_null is not supported", () => {
    expect(buildFilterExpression(column, NULL_SENTINEL)).toBeNull();
  });

  it("skips value filters when the default operator is not supported", () => {
    expect(
      buildFilterExpression({ ...column, filterOperators: ["eq"] }, "alice"),
    ).toBeNull();
  });

  it("skips value filters for non-filterable columns", () => {
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

describe("valueFilterOperator", () => {
  it("returns the default operator when value filtering is supported", () => {
    expect(
      valueFilterOperator({
        category: "text",
        filterable: true,
        filterOperators: ["like", "is_null"],
        isBoolean: false,
      }),
    ).toBe("like");
  });

  it("returns null when the default operator is unavailable", () => {
    expect(
      valueFilterOperator({
        category: "text",
        filterable: true,
        filterOperators: ["eq", "is_null"],
        isBoolean: false,
      }),
    ).toBeNull();
  });
});

describe("categoryLabel", () => {
  it("returns short labels for each category", () => {
    expect(categoryLabel("integer")).toBe("NUM");
    expect(categoryLabel("float")).toBe("NUM");
    expect(categoryLabel("decimal")).toBe("NUM");
    expect(categoryLabel("boolean")).toBe("BOOL");
    expect(categoryLabel("date")).toBe("DATE");
    expect(categoryLabel("time")).toBe("DATE");
    expect(categoryLabel("datetime")).toBe("DATE");
    expect(categoryLabel("binary")).toBe("BIN");
    expect(categoryLabel("json")).toBe("JSON");
    expect(categoryLabel("uuid")).toBe("UUID");
    expect(categoryLabel("spatial")).toBe("GEO");
    expect(categoryLabel("interval")).toBe("INTV");
    expect(categoryLabel("array")).toBe("ARR");
    expect(categoryLabel("enum")).toBe("ENUM");
    expect(categoryLabel("lob")).toBe("LOB");
    expect(categoryLabel("text")).toBe("TEXT");
    expect(categoryLabel("other")).toBe("—");
  });
});

describe("categoryColor", () => {
  it("returns a CSS string for each category", () => {
    const allCategories: TypeCategory[] = [
      "text",
      "integer",
      "float",
      "decimal",
      "boolean",
      "date",
      "time",
      "datetime",
      "binary",
      "json",
      "uuid",
      "spatial",
      "interval",
      "array",
      "enum",
      "lob",
      "other",
    ];
    for (const cat of allCategories) {
      const color = categoryColor(cat);
      expect(typeof color).toBe("string");
      expect(color.length).toBeGreaterThan(0);
    }
  });

  it("groups related categories to the same color", () => {
    expect(categoryColor("integer")).toBe(categoryColor("float"));
    expect(categoryColor("float")).toBe(categoryColor("decimal"));
    expect(categoryColor("date")).toBe(categoryColor("datetime"));
    expect(categoryColor("date")).toBe(categoryColor("time"));
  });
});
