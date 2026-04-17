import { describe, expect, it } from "vitest";
import {
  formatDatetimeForDisplay,
  hexFromBuffer,
  isHexLike,
  isoToLocalDateStr,
  parseHexToBuffer,
} from "../../src/extension/dbDrivers/BaseDBDriver";
import { NULL_SENTINEL } from "../../src/extension/dbDrivers/types";
import { col, StubDriver } from "./helpers";

// ─── Standalone utility functions ───

describe("formatDatetimeForDisplay", () => {
  it("formats Date to UTC string without ms when ms=0", () => {
    const d = new Date("2024-06-15T08:30:45.000Z");
    expect(formatDatetimeForDisplay(d)).toBe("2024-06-15 08:30:45");
  });

  it("includes ms when non-zero", () => {
    const d = new Date("2024-06-15T08:30:45.123Z");
    expect(formatDatetimeForDisplay(d)).toBe("2024-06-15 08:30:45.123");
  });

  it("returns null for invalid Date", () => {
    expect(formatDatetimeForDisplay(new Date("invalid"))).toBeNull();
  });

  it("formats SQL datetime string without fractional seconds", () => {
    expect(formatDatetimeForDisplay("2024-06-15 08:30:45")).toBe(
      "2024-06-15 08:30:45",
    );
  });

  it("preserves timezone in SQL datetime string", () => {
    expect(formatDatetimeForDisplay("2024-06-15 08:30:45+05:30")).toBe(
      "2024-06-15 08:30:45+05:30",
    );
    expect(formatDatetimeForDisplay("2024-06-15 08:30:45Z")).toBe(
      "2024-06-15 08:30:45Z",
    );
  });

  it("strips trailing zeros from fractional seconds", () => {
    expect(formatDatetimeForDisplay("2024-06-15 08:30:45.100")).toBe(
      "2024-06-15 08:30:45.1",
    );
    expect(formatDatetimeForDisplay("2024-06-15 08:30:45.120")).toBe(
      "2024-06-15 08:30:45.12",
    );
  });

  it("omits .000 fractional seconds", () => {
    expect(formatDatetimeForDisplay("2024-06-15 08:30:45.000")).toBe(
      "2024-06-15 08:30:45",
    );
  });

  it("returns null for non-datetime strings", () => {
    expect(formatDatetimeForDisplay("hello")).toBeNull();
    expect(formatDatetimeForDisplay("2024-06-15")).toBeNull();
  });

  it("returns null for non-string non-Date values", () => {
    expect(formatDatetimeForDisplay(42)).toBeNull();
    expect(formatDatetimeForDisplay(null)).toBeNull();
    expect(formatDatetimeForDisplay(undefined)).toBeNull();
  });

  it("handles midnight correctly", () => {
    const d = new Date("2024-01-01T00:00:00.000Z");
    expect(formatDatetimeForDisplay(d)).toBe("2024-01-01 00:00:00");
  });

  it("handles end of year", () => {
    const d = new Date("2024-12-31T23:59:59.999Z");
    expect(formatDatetimeForDisplay(d)).toBe("2024-12-31 23:59:59.999");
  });
});

describe("isoToLocalDateStr", () => {
  it("extracts date part from ISO string", () => {
    expect(isoToLocalDateStr("2024-06-15T08:30:45.000Z")).toBe("2024-06-15");
  });

  it("returns null for invalid ISO", () => {
    expect(isoToLocalDateStr("not-a-date")).toBeNull();
  });

  it("handles timezone offsets correctly (UTC extraction)", () => {
    // Midnight UTC should yield 2024-06-15
    expect(isoToLocalDateStr("2024-06-15T00:00:00Z")).toBe("2024-06-15");
  });
});

describe("hexFromBuffer", () => {
  it("returns \\x-prefixed hex for non-empty buffer", () => {
    expect(hexFromBuffer(Buffer.from([0xde, 0xad, 0xbe, 0xef]))).toBe(
      "\\xdeadbeef",
    );
  });

  it("returns empty string for empty buffer", () => {
    expect(hexFromBuffer(Buffer.alloc(0))).toBe("");
  });
});

describe("parseHexToBuffer", () => {
  it("parses \\x prefix", () => {
    const buf = parseHexToBuffer("\\xdeadbeef");
    expect(buf.toString("hex")).toBe("deadbeef");
  });

  it("parses 0x prefix", () => {
    const buf = parseHexToBuffer("0xDEAD");
    expect(buf.toString("hex")).toBe("dead");
  });

  it("parses bare hex string", () => {
    const buf = parseHexToBuffer("ff00ff");
    expect(buf.toString("hex")).toBe("ff00ff");
  });

  it("throws for odd hex digits", () => {
    expect(() => parseHexToBuffer("\\xabc")).toThrow("odd number");
  });

  it("throws for non-hex characters", () => {
    expect(() => parseHexToBuffer("xyz")).toThrow("Invalid hex");
  });

  it("handles empty after prefix", () => {
    const buf = parseHexToBuffer("0x");
    expect(buf.length).toBe(0);
  });
});

describe("isHexLike", () => {
  it("returns true for \\x-prefixed hex", () => {
    expect(isHexLike("\\xdeadbeef")).toBe(true);
  });

  it("returns true for 0x-prefixed hex", () => {
    expect(isHexLike("0xFF00")).toBe(true);
  });

  it("returns true for bare even-length hex", () => {
    expect(isHexLike("aabb")).toBe(true);
  });

  it("returns false for bare odd-length hex", () => {
    expect(isHexLike("abc")).toBe(false);
  });

  it("returns false for non-hex", () => {
    expect(isHexLike("hello")).toBe(false);
  });

  it("returns true for \\x with non-hex characters returns false", () => {
    expect(isHexLike("\\xhello")).toBe(false);
  });
});

// ─── BaseDBDriver default methods (via StubDriver) ───

describe("BaseDBDriver defaults", () => {
  const drv = new StubDriver();

  describe("quoteIdentifier", () => {
    it("wraps in double quotes", () => {
      expect(drv.quoteIdentifier("users")).toBe('"users"');
    });
    it("escapes embedded double quotes", () => {
      expect(drv.quoteIdentifier('my"col')).toBe('"my""col"');
    });
    it("handles empty string", () => {
      expect(drv.quoteIdentifier("")).toBe('""');
    });
  });

  describe("qualifiedTableName", () => {
    it("returns schema.table when schema is present", () => {
      expect(drv.qualifiedTableName("db", "public", "users")).toBe(
        '"public"."users"',
      );
    });
    it("returns only table when schema is empty", () => {
      expect(drv.qualifiedTableName("db", "", "users")).toBe('"users"');
    });
  });

  describe("buildPagination", () => {
    it("returns LIMIT ? OFFSET ? with [limit, offset] params", () => {
      const result = drv.buildPagination(10, 25, 1);
      expect(result.sql).toBe("LIMIT ? OFFSET ?");
      expect(result.params).toEqual([25, 10]);
    });
    it("works with zero offset", () => {
      const result = drv.buildPagination(0, 50, 1);
      expect(result.params).toEqual([50, 0]);
    });
  });

  describe("buildOrderByDefault", () => {
    it("returns ORDER BY for PK columns", () => {
      const cols = [
        col({ name: "id", type: "integer", isPrimaryKey: true }),
        col({ name: "name", type: "text", isPrimaryKey: false }),
      ];
      expect(drv.buildOrderByDefault(cols)).toBe('ORDER BY "id"');
    });

    it("returns empty string when no PK", () => {
      const cols = [col({ name: "a", type: "text", isPrimaryKey: false })];
      expect(drv.buildOrderByDefault(cols)).toBe("");
    });

    it("includes all PK columns", () => {
      const cols = [
        col({ name: "a", type: "integer", isPrimaryKey: true }),
        col({ name: "b", type: "integer", isPrimaryKey: true }),
      ];
      expect(drv.buildOrderByDefault(cols)).toBe('ORDER BY "a", "b"');
    });
  });

  describe("buildInsertValueExpr", () => {
    it("returns ? placeholder", () => {
      expect(
        drv.buildInsertValueExpr(col({ name: "a", type: "text" }), 1),
      ).toBe("?");
    });
  });

  describe("buildSetExpr", () => {
    it("returns quoted_col = ?", () => {
      expect(drv.buildSetExpr(col({ name: "age", type: "int" }), 1)).toBe(
        '"age" = ?',
      );
    });
  });

  describe("coerceInputValue", () => {
    it("returns null/undefined/empty string unchanged", () => {
      const c = col({ name: "a", type: "text" });
      expect(drv.coerceInputValue(null, c)).toBeNull();
      expect(drv.coerceInputValue(undefined, c)).toBeUndefined();
      expect(drv.coerceInputValue("", c)).toBe("");
    });

    it("converts NULL_SENTINEL to null", () => {
      const c = col({ name: "a", type: "text" });
      expect(drv.coerceInputValue(NULL_SENTINEL, c)).toBeNull();
    });

    it("passes non-string values through", () => {
      const c = col({ name: "a", type: "integer", category: "integer" });
      expect(drv.coerceInputValue(42, c)).toBe(42);
      expect(drv.coerceInputValue(true, c)).toBe(true);
    });

    it("coerces 'true'/'1' to true for boolean columns", () => {
      const c = col({
        name: "b",
        type: "boolean",
        category: "boolean",
        isBoolean: true,
      });
      expect(drv.coerceInputValue("true", c)).toBe(true);
      expect(drv.coerceInputValue("1", c)).toBe(true);
      expect(drv.coerceInputValue("TRUE", c)).toBe(true);
    });

    it("coerces 'false'/'0' to false for boolean columns", () => {
      const c = col({
        name: "b",
        type: "boolean",
        category: "boolean",
        isBoolean: true,
      });
      expect(drv.coerceInputValue("false", c)).toBe(false);
      expect(drv.coerceInputValue("0", c)).toBe(false);
    });

    it("converts hex string to Buffer for binary columns", () => {
      const c = col({ name: "data", type: "bytea", category: "binary" });
      const result = drv.coerceInputValue("\\xdeadbeef", c);
      expect(Buffer.isBuffer(result)).toBe(true);
      expect((result as Buffer).toString("hex")).toBe("deadbeef");
    });

    it("passes regular strings through for text columns", () => {
      const c = col({ name: "name", type: "text" });
      expect(drv.coerceInputValue("hello world", c)).toBe("hello world");
    });
  });

  describe("formatOutputValue", () => {
    it("passes null/undefined through", () => {
      const c = col({ name: "a", type: "text" });
      expect(drv.formatOutputValue(null, c)).toBeNull();
      expect(drv.formatOutputValue(undefined, c)).toBeUndefined();
    });

    it("converts Buffer to hex string", () => {
      const c = col({ name: "data", type: "binary", category: "binary" });
      expect(drv.formatOutputValue(Buffer.from([0xab, 0xcd]), c)).toBe(
        "\\xabcd",
      );
    });

    it("converts bigint to string", () => {
      const c = col({ name: "n", type: "bigint", category: "integer" });
      expect(drv.formatOutputValue(BigInt("9007199254740991"), c)).toBe(
        "9007199254740991",
      );
    });

    it("formats Date with date category as YYYY-MM-DD", () => {
      const c = col({ name: "d", type: "date", category: "date" });
      expect(drv.formatOutputValue(new Date("2024-06-15T10:30:00Z"), c)).toBe(
        "2024-06-15",
      );
    });

    it("formats Date with datetime category as datetime string", () => {
      const c = col({ name: "dt", type: "datetime", category: "datetime" });
      expect(drv.formatOutputValue(new Date("2024-06-15T10:30:00Z"), c)).toBe(
        "2024-06-15 10:30:00",
      );
    });

    it("JSON.stringify for objects", () => {
      const c = col({ name: "j", type: "json", category: "json" });
      expect(drv.formatOutputValue({ a: 1 }, c)).toBe('{"a":1}');
    });

    it("formats SQL datetime strings via formatDatetimeForDisplay", () => {
      const c = col({
        name: "ts",
        type: "timestamp",
        category: "datetime",
        nativeType: "timestamp",
      });
      expect(drv.formatOutputValue("2024-06-15 10:30:00.100", c)).toBe(
        "2024-06-15 10:30:00.1",
      );
    });

    it("passes regular strings through for text columns", () => {
      const c = col({
        name: "s",
        type: "text",
        category: "text",
        nativeType: "text",
      });
      expect(drv.formatOutputValue("hello", c)).toBe("hello");
    });

    it("passes numbers through", () => {
      const c = col({ name: "n", type: "integer", category: "integer" });
      expect(drv.formatOutputValue(42, c)).toBe(42);
    });
  });

  describe("enrichColumn", () => {
    it("enriches a basic text column", () => {
      const result = (drv as any).enrichColumn({
        name: "title",
        type: "text",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.category).toBe("text");
      expect(result.nativeType).toBe("text");
      expect(result.filterable).toBe(true);
      expect(result.editable).toBe(true);
      expect(result.isBoolean).toBe(false);
      expect(result.filterOperators).toContain("like");
    });

    it("enriches a boolean column", () => {
      const result = (drv as any).enrichColumn({
        name: "active",
        type: "boolean",
        nullable: false,
        isPrimaryKey: false,
        isForeignKey: false,
      });
      expect(result.category).toBe("boolean");
      expect(result.isBoolean).toBe(true);
      expect(result.filterOperators).toEqual([
        "eq",
        "neq",
        "is_null",
        "is_not_null",
      ]);
    });
  });

  describe("buildFilterCondition", () => {
    it("returns null for non-filterable columns", () => {
      const c = col({ name: "a", type: "text", filterable: false });
      expect(drv.buildFilterCondition(c, "eq", "x", 1)).toBeNull();
    });

    it("generates IS NULL", () => {
      const c = col({ name: "a", type: "text" });
      const r = drv.buildFilterCondition(c, "is_null", "", 1);
      expect(r?.sql).toBe('"a" IS NULL');
      expect(r?.params).toEqual([]);
    });

    it("generates IS NOT NULL", () => {
      const c = col({ name: "a", type: "text" });
      const r = drv.buildFilterCondition(c, "is_not_null", "", 1);
      expect(r?.sql).toBe('"a" IS NOT NULL');
    });

    it("generates boolean eq filter with 1/0", () => {
      const c = col({
        name: "active",
        type: "boolean",
        category: "boolean",
        isBoolean: true,
      });
      const r = drv.buildFilterCondition(c, "eq", "true", 1);
      expect(r?.sql).toBe('"active" = ?');
      expect(r?.params).toEqual([1]);
    });

    it("generates boolean neq filter", () => {
      const c = col({
        name: "active",
        type: "boolean",
        category: "boolean",
        isBoolean: true,
      });
      const r = drv.buildFilterCondition(c, "neq", "true", 1);
      expect(r?.sql).toBe('"active" != ?');
      expect(r?.params).toEqual([1]);
    });

    it("generates numeric eq filter", () => {
      const c = col({ name: "age", type: "integer", category: "integer" });
      const r = drv.buildFilterCondition(c, "eq", "25", 1);
      expect(r?.sql).toBe('"age" = ?');
      expect(r?.params).toEqual([25]);
    });

    it("generates numeric gt filter", () => {
      const c = col({ name: "age", type: "integer", category: "integer" });
      const r = drv.buildFilterCondition(c, "gt", "25", 1);
      expect(r?.sql).toBe('"age" > ?');
      expect(r?.params).toEqual([25]);
    });

    it("generates numeric between filter", () => {
      const c = col({ name: "age", type: "integer", category: "integer" });
      const r = drv.buildFilterCondition(c, "between", ["10", "20"], 1);
      expect(r?.sql).toBe('"age" BETWEEN ? AND ?');
      expect(r?.params).toEqual(["10", "20"]);
    });

    it("generates text LIKE filter", () => {
      const c = col({ name: "name", type: "text" });
      const r = drv.buildFilterCondition(c, "like", "john", 1);
      expect(r?.sql).toBe('CAST("name" AS CHAR) LIKE ?');
      expect(r?.params).toEqual(["%john%"]);
    });

    it("generates text NOT LIKE filter for neq", () => {
      const c = col({ name: "name", type: "text" });
      const r = drv.buildFilterCondition(c, "neq", "john", 1);
      expect(r?.sql).toBe('CAST("name" AS CHAR) NOT LIKE ?');
      expect(r?.params).toEqual(["%john%"]);
    });

    it("generates IN filter with comma-separated values", () => {
      const c = col({ name: "status", type: "text" });
      const r = drv.buildFilterCondition(c, "in", "a, b, c", 1);
      expect(r?.sql).toBe('"status" IN (?, ?, ?)');
      expect(r?.params).toEqual(["a", "b", "c"]);
    });

    it("generates text eq as LIKE (contains search)", () => {
      const c = col({ name: "name", type: "text" });
      const r = drv.buildFilterCondition(c, "eq", "john", 1);
      expect(r?.sql).toContain("LIKE");
      expect(r?.params).toEqual(["%john%"]);
    });
  });
});
