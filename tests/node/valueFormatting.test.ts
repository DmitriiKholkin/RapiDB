import { describe, expect, it } from "vitest";
import { formatBinaryValueForViewer } from "../../src/webview/utils/valueFormatting";

describe("valueFormatting", () => {
  it("preserves quoted BinData payloads in viewer output", () => {
    expect(formatBinaryValueForViewer('BinData(0, "")')).toBe('BinData(0, "")');
    expect(formatBinaryValueForViewer('BinData(4, "YWJj")')).toBe(
      'BinData(4, "YWJj")',
    );
    expect(formatBinaryValueForViewer("new BinData(4, 'YWJj')")).toBe(
      "BinData(4, 'YWJj')",
    );
  });
});
