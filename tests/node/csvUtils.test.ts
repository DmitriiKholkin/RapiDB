import { describe, expect, it } from "vitest";
import { csvCell } from "../../src/extension/utils/csvUtils";

describe("csvCell", () => {
  it("neutralizes spreadsheet formulas without changing numeric values", () => {
    expect(csvCell('=HYPERLINK("https://example.com")')).toBe(
      '"\'=HYPERLINK(""https://example.com"")"',
    );
    expect(csvCell("@SUM(A1:A2)")).toBe("'@SUM(A1:A2)");
    expect(csvCell(-42)).toBe("-42");
    expect(csvCell("-42", true)).toBe("-42");
  });

  it("quotes tab-containing cells", () => {
    expect(csvCell("left\tright")).toBe('"left\tright"');
  });

  it.each([
    "\t",
    "\r",
  ])("retains neutralization for leading control %s", (value) => {
    expect(csvCell(value)).toBe(`"'${value}"`);
  });

  it.each([
    "=",
    "+",
    "-",
    "@",
  ])("neutralizes formula marker %s at the start of an untrusted string", (marker) => {
    expect(csvCell(`${marker}SUM(A1:A2)`)).toBe(`'${marker}SUM(A1:A2)`);
  });

  it.each([
    ["ASCII space", " ", "' =1"],
    ["tab", "\t", '"\'\t=1"'],
    ["carriage return", "\r", '"\'\r=1"'],
    ["line feed", "\n", '"\'\n=1"'],
    ["NUL control", "\u0000", "'\u0000=1"],
    ["Unicode next-line whitespace", "\u0085", "'\u0085=1"],
    ["non-breaking space", "\u00a0", "'\u00a0=1"],
    ["em space", "\u2003", "'\u2003=1"],
    ["zero-width space", "\u200b", "'\u200b=1"],
    ["word joiner", "\u2060", "'\u2060=1"],
    ["BOM", "\ufeff", "'\ufeff=1"],
    ["combining grapheme joiner", "\u034f", "'\u034f=1"],
  ])("preserves a leading %s while neutralizing formulas", (_name, prefix, expected) => {
    expect(csvCell(`${prefix}=1`)).toBe(expected);
  });

  it("sanitizes before quoting and escaping formula cells", () => {
    expect(csvCell(' \t=HYPERLINK("https://example.com")')).toBe(
      '"\' \t=HYPERLINK(""https://example.com"")"',
    );
  });

  it("keeps trusted negative numeric strings unchanged", () => {
    expect(csvCell("-42", true)).toBe("-42");
  });
});
