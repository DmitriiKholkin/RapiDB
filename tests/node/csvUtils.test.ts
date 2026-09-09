import { describe, expect, it } from "vitest";
import { csvCell } from "../../src/extension/utils/csvUtils";

describe("csvCell", () => {
  it("neutralizes spreadsheet formulas without changing numeric values", () => {
    expect(csvCell('=HYPERLINK("https://example.com")')).toBe(
      '"\'=HYPERLINK(""https://example.com"")"',
    );
    expect(csvCell("@SUM(A1:A2)")).toBe("'@SUM(A1:A2)");
    expect(csvCell(-42)).toBe("-42");
  });

  it("quotes tab-containing cells", () => {
    expect(csvCell("left\tright")).toBe('"left\tright"');
  });
});
