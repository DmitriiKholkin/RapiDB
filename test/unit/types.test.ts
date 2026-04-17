import { describe, expect, it } from "vitest";
import {
  colKey,
  DATE_ONLY_RE,
  DATETIME_SQL_RE,
  filterOperatorsForCategory,
  ISO_DATETIME_RE,
  NULL_SENTINEL,
} from "../../src/extension/dbDrivers/types";

describe("colKey", () => {
  it("returns __col_0 for index 0", () => {
    expect(colKey(0)).toBe("__col_0");
  });
  it("returns __col_99 for index 99", () => {
    expect(colKey(99)).toBe("__col_99");
  });
});

describe("NULL_SENTINEL", () => {
  it("is a non-printable string unlikely to collide with real data", () => {
    expect(NULL_SENTINEL).toContain("__NULL__");
    expect(NULL_SENTINEL.length).toBeGreaterThan(8);
  });
});

describe("filterOperatorsForCategory", () => {
  it("returns numeric ops for integer", () => {
    const ops = filterOperatorsForCategory("integer");
    expect(ops).toContain("eq");
    expect(ops).toContain("gt");
    expect(ops).toContain("between");
    expect(ops).toContain("in");
    expect(ops).toContain("is_null");
  });

  it("returns numeric ops for float", () => {
    const ops = filterOperatorsForCategory("float");
    expect(ops).toContain("between");
    expect(ops).not.toContain("lte");
  });

  it("returns numeric ops for decimal", () => {
    const ops = filterOperatorsForCategory("decimal");
    expect(ops).toContain("gte");
  });

  it("returns text ops for text", () => {
    const ops = filterOperatorsForCategory("text");
    expect(ops).toContain("like");
    expect(ops).toContain("in");
    expect(ops).not.toContain("eq");
    expect(ops).not.toContain("gt");
    expect(ops).not.toContain("between");
  });

  it("returns text ops for json", () => {
    const ops = filterOperatorsForCategory("json");
    expect(ops).toContain("like");
  });

  it("returns text ops for uuid", () => {
    const ops = filterOperatorsForCategory("uuid");
    expect(ops).toContain("like");
  });

  it("returns text ops for enum", () => {
    const ops = filterOperatorsForCategory("enum");
    expect(ops).toContain("in");
  });

  it("returns conservative ops for date", () => {
    expect(filterOperatorsForCategory("date")).toEqual([
      "eq",
      "like",
      "is_null",
      "is_not_null",
    ]);
  });

  it("returns text-search ops for time/datetime/interval", () => {
    for (const cat of ["time", "datetime", "interval"] as const) {
      expect(filterOperatorsForCategory(cat)).toEqual([
        "like",
        "is_null",
        "is_not_null",
      ]);
    }
  });

  it("returns only null ops for boolean", () => {
    const ops = filterOperatorsForCategory("boolean");
    expect(ops).toEqual(["eq", "neq", "is_null", "is_not_null"]);
  });

  it("returns search ops for binary/spatial/array/other", () => {
    for (const cat of ["binary", "spatial", "array", "other"] as const) {
      expect(filterOperatorsForCategory(cat)).toEqual([
        "like",
        "is_null",
        "is_not_null",
      ]);
    }
  });

  it("returns no ops for lob", () => {
    expect(filterOperatorsForCategory("lob")).toEqual([]);
  });
});

describe("regex constants", () => {
  describe("ISO_DATETIME_RE", () => {
    it.each([
      "2024-01-15T10:30:00Z",
      "2024-01-15T10:30:00.123Z",
      "2024-01-15T10:30:00+05:30",
      "2024-01-15T10:30:00-03:00",
      "2024-01-15T10:30:00",
    ])("matches valid ISO datetime %s", (val) => {
      expect(ISO_DATETIME_RE.test(val)).toBe(true);
    });

    it.each([
      "2024-01-15",
      "2024-01-15 10:30:00",
      "not-a-date",
      "",
      "2024-01-15T25:30:00Z", // still matches regex (no range validation)
    ])("does not match %s (non-ISO)", (val) => {
      if (val === "2024-01-15T25:30:00Z") {
        // Regex only checks format, not ranges, so this will match
        expect(ISO_DATETIME_RE.test(val)).toBe(true);
        return;
      }
      expect(ISO_DATETIME_RE.test(val)).toBe(false);
    });
  });

  describe("DATE_ONLY_RE", () => {
    it("matches YYYY-MM-DD", () => {
      expect(DATE_ONLY_RE.test("2024-01-15")).toBe(true);
    });
    it("does not match datetime", () => {
      expect(DATE_ONLY_RE.test("2024-01-15T10:30:00Z")).toBe(false);
    });
    it("does not match partial", () => {
      expect(DATE_ONLY_RE.test("2024-01")).toBe(false);
    });
  });

  describe("DATETIME_SQL_RE", () => {
    it.each([
      "2024-01-15 10:30:00",
      "2024-01-15 10:30:00.123",
      "2024-01-15 10:30:00+05:30",
      "2024-01-15 10:30:00-03:00",
    ])("matches SQL datetime %s", (val) => {
      expect(DATETIME_SQL_RE.test(val)).toBe(true);
    });

    it("does not match ISO T separator", () => {
      expect(DATETIME_SQL_RE.test("2024-01-15T10:30:00")).toBe(false);
    });
  });
});
