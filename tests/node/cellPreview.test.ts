import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import {
  formatTableCsvExportValue,
  serializeJsonExportRecord,
} from "../../src/extension/utils/exportValueFormat";
import {
  CELL_PREVIEW_LIMIT,
  CELL_PREVIEW_NOTICE,
  getCellPreview,
  getCellPreviewTitle,
} from "../../src/webview/utils/cellPreview";
import { formatScalarValueForDisplay } from "../../src/webview/utils/valueFormatting";

describe("grid-only cell previews", () => {
  it.each([
    "ordinary text",
    "",
    "a\r\nb\nc",
    "😀",
    42,
    false,
    123n,
    new Date("2026-01-02T03:04:05Z"),
    { nested: [1, "two", null, true], omitted: undefined },
    { timestamp: new Date("2026-01-02T03:04:05Z") },
    new Uint8Array([1, 2, 3]),
    Buffer.from([1, 2, 3]),
  ])("preserves ordinary value formatting: %s", (value) => {
    expect(getCellPreview(value)).toEqual({
      text: formatScalarValueForDisplay(value),
      truncated: false,
    });
  });

  it("caps multi-million-character CLOBs and titles without changing the data", () => {
    const value = "x\n".repeat(2_500_000);
    const preview = getCellPreview(value);
    expect(preview.text.length).toBe(CELL_PREVIEW_LIMIT);
    expect(preview.text.endsWith("…")).toBe(true);
    expect(preview.truncated).toBe(true);
    expect(getCellPreviewTitle(value).length).toBeLessThanOrEqual(
      CELL_PREVIEW_LIMIT + CELL_PREVIEW_NOTICE.length + 1,
    );
    expect(value.length).toBe(5_000_000);
  });

  it("does not split a surrogate pair at either cutoff", () => {
    for (const padding of [CELL_PREVIEW_LIMIT - 2, CELL_PREVIEW_LIMIT - 1]) {
      const value = `${"x".repeat(padding)}😀tail`;
      const preview = getCellPreview(value).text;
      expect(preview.length).toBeLessThanOrEqual(CELL_PREVIEW_LIMIT);
      expect(preview).not.toMatch(/[\ud800-\udbff]…$/);
      expect(preview.endsWith("…")).toBe(true);
    }
    expect(getCellPreview("x".repeat(CELL_PREVIEW_LIMIT)).truncated).toBe(
      false,
    );
  });

  it("bounds JSON escaping and stops visiting arrays and object values", () => {
    const reads = vi.fn(() => "unvisited");
    const array = new Array(1_000_000).fill(1);
    Object.defineProperty(array, 5000, { get: reads });
    const object = {
      large: "\n".repeat(5_000_000),
      get later() {
        return reads();
      },
    };
    for (const value of [array, object, { type: "Buffer", data: array }]) {
      const preview = getCellPreview(value);
      expect(preview.truncated).toBe(true);
      expect(preview.text.length).toBeLessThanOrEqual(CELL_PREVIEW_LIMIT);
    }
    expect(reads).not.toHaveBeenCalled();
  });

  it("does not materialize a full Buffer through toJSON", () => {
    const buffer = Buffer.alloc(5_000_000, 255);
    const toJSON = vi.spyOn(buffer, "toJSON");
    expect(getCellPreview(buffer).text.length).toBeLessThanOrEqual(
      CELL_PREVIEW_LIMIT,
    );
    expect(toJSON).not.toHaveBeenCalled();
  });

  it("bounds deep and circular objects", () => {
    let deep: unknown = "end";
    for (let i = 0; i < 10_000; i++) deep = [deep];
    expect(getCellPreview(deep).truncated).toBe(true);
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(getCellPreview(cyclic).text.length).toBeLessThanOrEqual(
      CELL_PREVIEW_LIMIT,
    );
  });

  it("preserves scalar fallback formatting for actual circular objects and arrays", () => {
    const object: Record<string, unknown> = { label: "small" };
    object.self = object;
    const array: unknown[] = ["first", object];
    array.push(array, "last");
    for (const value of [object, array, { nested: array }]) {
      const expected = formatScalarValueForDisplay(value);
      expect(getCellPreview(value)).toEqual({
        text: expected,
        truncated: false,
      });
      expect(getCellPreviewTitle(value)).toBe(expected);
    }
    expect(getCellPreview(object).text).toBe("[object Object]");
  });

  it("bounds circular fallback without calling custom coercion or joining large arrays", () => {
    const customToString = vi.fn(() => "x".repeat(5_000_000));
    const object: Record<string, unknown> = {};
    object.self = object;
    Object.defineProperty(object, "toString", { value: customToString });
    expect(getCellPreview(object)).toEqual({
      text: "[object Object]",
      truncated: false,
    });
    expect(customToString).not.toHaveBeenCalled();

    const array: unknown[] = [];
    array.push(array, "x".repeat(5_000_000));
    const join = vi.spyOn(array, "join");
    const preview = getCellPreview(array);
    expect(preview.truncated).toBe(true);
    expect(preview.text.length).toBeLessThanOrEqual(CELL_PREVIEW_LIMIT);
    expect(join).not.toHaveBeenCalled();
  });

  it("stops at inherited keys, preserving small own values on a million-key prototype", () => {
    const prototype = new Array(1_000_000).fill("inherited");
    const inheritedRead = vi.fn(() => "ignored");
    Object.defineProperty(prototype, "inheritedGetter", {
      enumerable: true,
      get: inheritedRead,
    });
    const object = Object.create(prototype) as Record<string, unknown>;
    object.own = "small";
    const hasOwn = vi.spyOn(Object, "hasOwn");
    const ownKeys = vi.spyOn(Object, "keys");
    let preview: ReturnType<typeof getCellPreview>;
    let checks: number;
    let keyLists: number;
    try {
      preview = getCellPreview(object);
      checks = hasOwn.mock.calls.length;
      keyLists = ownKeys.mock.calls.length;
    } finally {
      hasOwn.mockRestore();
      ownKeys.mockRestore();
    }
    expect(checks).toBe(2);
    expect(checks).toBeLessThanOrEqual(CELL_PREVIEW_LIMIT);
    expect(keyLists).toBe(0);
    expect(inheritedRead).not.toHaveBeenCalled();
    expect(preview).toEqual({ text: '{"own":"small"}', truncated: false });
    expect(preview.text).toBe(formatScalarValueForDisplay(object));
  });

  it("keeps full original text for scalar formatting, CSV, and JSON exports", () => {
    const value = `${"x".repeat(5_000_000)}tail`;
    const row = Object.freeze({ __col_0: value });
    getCellPreview(row.__col_0);
    expect(formatScalarValueForDisplay(row.__col_0)).toBe(value);
    expect(formatTableCsvExportValue(row.__col_0, "lob")).toBe(value);
    const exported = serializeJsonExportRecord([
      {
        key: "clob",
        sourceKey: "__col_0",
        category: "lob",
        value: row.__col_0,
      },
    ]);
    expect(JSON.parse(exported).clob).toBe(value);
    expect(row.__col_0).toBe(value);
  });
});
