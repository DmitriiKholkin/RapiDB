import { describe, expect, it } from "vitest";
import { serializeJsonExportRecord } from "../../src/extension/utils/exportValueFormat";

function exportArray(value: unknown, nativeType: string): string {
  return serializeJsonExportRecord([
    { key: "a", sourceKey: "a", category: "array", nativeType, value },
  ]);
}

describe("PostgreSQL json[] export keeps element source text", () => {
  it.each([
    "json[]",
    "_json",
    "pg_catalog.json[]",
  ])("keeps duplicate keys and key order for %s", (nativeType) => {
    expect(exportArray(['{"x":1,"x":2}', ' {"b":1,"2":2} '], nativeType)).toBe(
      '{"a":[{"x":1,"x":2},{"b":1,"2":2}]}',
    );
  });

  it("keeps source text in nested arrays and array text values", () => {
    expect(exportArray([['{"b":1,"a":2}'], [null]], "json[][]")).toBe(
      '{"a":[[{"b":1,"a":2}],[null]]}',
    );
    expect(exportArray(String.raw`["{\"x\":1,\"x\":2}"]`, "json[]")).toBe(
      '{"a":[{"x":1,"x":2}]}',
    );
  });

  it("exports invalid element text as a string", () => {
    expect(exportArray(["{bad", '{"a":1} /* x */'], "json[]")).toBe(
      '{"a":["{bad","{\\"a\\":1} /* x */"]}',
    );
  });

  it("keeps normalizing jsonb[] elements", () => {
    expect(exportArray(['{"b": 1, "a": 2}'], "jsonb[]")).toBe(
      '{"a":[{"b":1,"a":2}]}',
    );
  });
});
