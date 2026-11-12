import { describe, expect, it } from "vitest";
import { formatJsonPreservingTokens } from "../../src/webview/utils/jsonFormatting";

describe("JSON formatter resource safety", () => {
  it("returns an error instead of throwing or expanding tiny deeply nested input excessively", () => {
    const input = "[".repeat(20_000) + "]".repeat(20_000);
    expect(() => JSON.parse(input)).not.toThrow();
    expect(formatJsonPreservingTokens(input)).toEqual({
      error: expect.any(String),
    });
  });

  it("preserves a valid ten-million-character string without a regexp stack overflow", () => {
    const input = `"${"a".repeat(10_000_000)}"`;
    expect(JSON.parse(input).length).toBe(10_000_000);
    const formatted = formatJsonPreservingTokens(input);
    expect("result" in formatted).toBe(true);
    if ("result" in formatted) expect(formatted.result === input).toBe(true);
  });

  it.each([
    '"unterminated',
    '"raw\nnewline"',
    '"bad\\x41"',
    '"bad\\u123"',
    '"bad\\u12x4"',
    '"trailing\\',
  ])("still rejects invalid string syntax: %j", (input) => {
    expect(formatJsonPreservingTokens(input)).toEqual({
      error: expect.any(String),
    });
  });

  it("preserves escapes, lone surrogates and duplicate keys after a long string", () => {
    const text = `"${"a".repeat(100_000)}\\u0041\\/\\\\\\""`;
    const input = `{"s":${text},"s":"\\ud800","n":9007199254740993}`;
    const formatted = formatJsonPreservingTokens(input);
    expect(formatted).toEqual({
      result: `{\n  "s": ${text},\n  "s": "\\ud800",\n  "n": 9007199254740993\n}`,
    });
  });

  it("reports a malformed escape after a long prefix rather than a regexp stack failure", () => {
    expect(
      formatJsonPreservingTokens(`"${"a".repeat(10_000_000)}\\u12x4"`),
    ).toEqual({
      error: expect.stringContaining("Invalid Unicode escape"),
    });
  });
});
