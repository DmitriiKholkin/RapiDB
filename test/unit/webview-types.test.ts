import { describe, expect, it } from "vitest";
import {
  categoryColor,
  categoryLabel,
  getCategoryPresentation,
  getStructuralBadgePresentation,
  inferQueryColumnCategory,
  inferValueCategory,
  placeholderForCategory,
  type TypeCategory,
} from "../../src/webview/types";

describe("placeholderForCategory", () => {
  it("returns 'true / false' for boolean columns", () => {
    expect(placeholderForCategory("boolean")).toBe("true / false");
  });

  it("returns 'number' for numeric categories", () => {
    expect(placeholderForCategory("integer")).toBe("number");
    expect(placeholderForCategory("float")).toBe("number");
    expect(placeholderForCategory("decimal")).toBe("number");
  });

  it("returns date/time format strings", () => {
    expect(placeholderForCategory("date")).toBe("YYYY-MM-DD");
    expect(placeholderForCategory("time")).toBe("HH:MM:SS");
    expect(placeholderForCategory("datetime")).toBe("YYYY-MM-DD HH:MM:SS");
  });

  it("returns appropriate placeholders for other categories", () => {
    expect(placeholderForCategory("uuid")).toBe("UUID");
    expect(placeholderForCategory("json")).toBe('{"key": "value"}');
    expect(placeholderForCategory("binary")).toBe("\\xHEX");
    expect(placeholderForCategory("spatial")).toBe("POINT(x y)");
    expect(placeholderForCategory("interval")).toBe("interval");
    expect(placeholderForCategory("array")).toBe("[1, 2, 3]");
    expect(placeholderForCategory("enum")).toBe("value");
  });

  it("returns 'filter' for text and other categories", () => {
    expect(placeholderForCategory("text")).toBe("filter");
    expect(placeholderForCategory("other")).toBe("filter");
    expect(placeholderForCategory("lob")).toBe("filter");
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

describe("getCategoryPresentation", () => {
  it("returns borderless badge presentation for every category", () => {
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
      const presentation = getCategoryPresentation(cat);
      expect(presentation.label.length).toBeGreaterThan(0);
      expect(presentation.foreground.length).toBeGreaterThan(0);
      expect(presentation.badgeBackground.length).toBeGreaterThan(0);
      expect(presentation.badgeBorder).toBe("none");
    }
  });
});

describe("getStructuralBadgePresentation", () => {
  it("returns borderless shared presentation for structural badges", () => {
    for (const kind of [
      "pk",
      "fk",
      "ai",
      "primary",
      "unique",
      "index",
    ] as const) {
      const presentation = getStructuralBadgePresentation(kind);
      expect(presentation.label.length).toBeGreaterThan(0);
      expect(presentation.foreground.length).toBeGreaterThan(0);
      expect(presentation.badgeBackground.length).toBeGreaterThan(0);
      expect(presentation.badgeBorder).toBe("none");
    }
  });
});

describe("query result type inference", () => {
  it("infers common categories from individual values", () => {
    expect(inferValueCategory(true)).toBe("boolean");
    expect(inferValueCategory(42)).toBe("integer");
    expect(inferValueCategory("2024-01-15")).toBe("date");
    expect(inferValueCategory("POINT(1 2)")).toBe("spatial");
    expect(inferValueCategory("\\xDEADBEEF")).toBe("binary");
    expect(inferValueCategory('{"ok":true}')).toBe("json");
  });

  it("infers a column category from sampled query values", () => {
    expect(inferQueryColumnCategory([null, undefined, "POINT(1 2)"])).toBe(
      "spatial",
    );
    expect(inferQueryColumnCategory([null, "\\xAA"])).toBe("binary");
    expect(inferQueryColumnCategory([null, null, null])).toBeNull();
  });
});
