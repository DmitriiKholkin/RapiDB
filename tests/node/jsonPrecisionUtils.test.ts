import { describe, expect, it } from "vitest";
import { serializeArrayPreservingRawTokens } from "../../src/extension/utils/arraySerialization";
import {
  canonicalizeJsonPreservingRawNumbers,
  parseJsonPreservingRawNumbers,
  serializeCanonicalJson,
} from "../../src/extension/utils/jsonCanonical";
import { jsonArrayLiteralToPgArrayLiteral } from "../../src/extension/utils/postgresArrayLiteral";

describe("serializeArrayPreservingRawTokens", () => {
  it("emits string elements that look like numeric tokens without quotes", () => {
    expect(serializeArrayPreservingRawTokens(["13000.0", "42.5"])).toBe(
      "[13000.0,42.5]",
    );
  });

  it("emits null and undefined as JSON null", () => {
    expect(serializeArrayPreservingRawTokens([null, undefined, "1"])).toBe(
      "[null,null,1]",
    );
  });

  it("quotes non-numeric strings", () => {
    expect(serializeArrayPreservingRawTokens(["abc", "with space"])).toBe(
      '["abc","with space"]',
    );
  });

  it("handles nested arrays", () => {
    expect(serializeArrayPreservingRawTokens([["1.0", "2"], ["3.5"]])).toBe(
      "[[1.0,2],[3.5]]",
    );
  });

  it("passes through native numbers and booleans via JSON.stringify", () => {
    expect(serializeArrayPreservingRawTokens([1, true, "1.0"])).toBe(
      "[1,true,1.0]",
    );
  });
});

describe("parseJsonPreservingRawNumbers", () => {
  it("parses objects keeping number tokens as raw strings", () => {
    const parsed = parseJsonPreservingRawNumbers('{"x":13000.0,"y":42}');
    expect(parsed).toEqual({
      x: { __rapidbRawNumber: true, raw: "13000.0" },
      y: { __rapidbRawNumber: true, raw: "42" },
    });
  });

  it("parses arrays keeping number tokens as raw strings", () => {
    const parsed = parseJsonPreservingRawNumbers("[13000.0, 42.5]");
    expect(parsed).toEqual([
      { __rapidbRawNumber: true, raw: "13000.0" },
      { __rapidbRawNumber: true, raw: "42.5" },
    ]);
  });

  it("returns undefined for invalid JSON", () => {
    expect(parseJsonPreservingRawNumbers("{not json")).toBeUndefined();
  });

  it("handles null, true, false", () => {
    expect(parseJsonPreservingRawNumbers("null")).toBeNull();
    expect(parseJsonPreservingRawNumbers("true")).toBe(true);
    expect(parseJsonPreservingRawNumbers("false")).toBe(false);
  });

  it("preserves numbers with exponent notation", () => {
    const parsed = parseJsonPreservingRawNumbers("1.5e3");
    expect(parsed).toEqual({ __rapidbRawNumber: true, raw: "1.5e3" });
  });

  it("rejects numbers with a bare decimal point (e.g. '5.')", () => {
    expect(parseJsonPreservingRawNumbers("5.")).toBeUndefined();
  });
});

describe("serializeCanonicalJson", () => {
  it("sorts object keys for stable comparison", () => {
    const parsed = parseJsonPreservingRawNumbers('{"b":2,"a":13000.0}');
    expect(parsed).toBeDefined();
    if (!parsed) return;
    expect(serializeCanonicalJson(parsed)).toBe('{"a":13000.0,"b":2}');
  });

  it("emits raw number tokens verbatim", () => {
    const parsed = parseJsonPreservingRawNumbers('{"x":13000.0}');
    expect(parsed).toBeDefined();
    if (!parsed) return;
    expect(serializeCanonicalJson(parsed)).toBe('{"x":13000.0}');
  });
});

describe("canonicalizeJsonPreservingRawNumbers", () => {
  it("canonicalizes whitespace and key order while preserving number tokens", () => {
    const canonical = canonicalizeJsonPreservingRawNumbers(
      '  { "b" :  2  ,  "a" : 13000.0 }  ',
    );
    expect(canonical).toBe('{"a":13000.0,"b":2}');
  });

  it("returns null for invalid JSON", () => {
    expect(canonicalizeJsonPreservingRawNumbers("{broken")).toBeNull();
  });

  it("preserves trailing zeros in nested arrays", () => {
    const canonical = canonicalizeJsonPreservingRawNumbers(
      '{"arr":[13000.0,42.5]}',
    );
    expect(canonical).toBe('{"arr":[13000.0,42.5]}');
  });
});

describe("jsonArrayLiteralToPgArrayLiteral", () => {
  it("converts a JSON array of numbers to a PG array literal", () => {
    expect(jsonArrayLiteralToPgArrayLiteral("[1, 2, 3]")).toBe("{1,2,3}");
  });

  it("preserves decimal precision when converting numeric arrays", () => {
    expect(jsonArrayLiteralToPgArrayLiteral("[13000.0, 42.5]")).toBe(
      "{13000.0,42.5}",
    );
  });

  it("converts null elements to NULL", () => {
    expect(jsonArrayLiteralToPgArrayLiteral("[1, null, 3]")).toBe("{1,NULL,3}");
  });

  it("quotes and escapes strings", () => {
    expect(
      jsonArrayLiteralToPgArrayLiteral('["hello","with \\"quote\\""]'),
    ).toBe('{"hello","with \\"quote\\""}');
  });

  it("escapes control characters inside JSON string elements", () => {
    expect(
      jsonArrayLiteralToPgArrayLiteral('["line1\\nline2","col1\\tcol2"]'),
    ).toBe('{"line1\\nline2","col1\\tcol2"}');
  });

  it("returns the empty literal for the empty array", () => {
    expect(jsonArrayLiteralToPgArrayLiteral("[]")).toBe("{}");
  });

  it("returns the input unchanged when it is already a PG array literal", () => {
    expect(jsonArrayLiteralToPgArrayLiteral("{1,2,3}")).toBe("{1,2,3}");
  });

  it("returns the empty string unchanged", () => {
    expect(jsonArrayLiteralToPgArrayLiteral("")).toBe("{}");
  });
});
