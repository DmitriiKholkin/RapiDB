import { describe, expect, it } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import {
  getStructuredCellDialogValue,
  serializeStructuredCellDialogDraft,
} from "../../src/webview/components/table/structuredCellDialog";

const textColumn: Pick<ColumnTypeMeta, "category" | "nativeType"> = {
  category: "text",
  nativeType: "TEXT",
};

const xmlColumn: Pick<ColumnTypeMeta, "category" | "nativeType"> = {
  category: "text",
  nativeType: "XML",
};

describe("getStructuredCellDialogValue", () => {
  it("returns empty editors for nullable structured null values", () => {
    expect(
      getStructuredCellDialogValue(null, {
        category: "json",
        nativeType: "JSON",
      }),
    ).toEqual({
      kind: "json",
      language: "json",
      formattedText: "",
    });

    expect(
      getStructuredCellDialogValue(undefined, {
        category: "array",
        nativeType: "TEXT[]",
      }),
    ).toEqual({
      kind: "array",
      language: "json",
      formattedText: "",
    });

    expect(
      getStructuredCellDialogValue(null, {
        category: "text",
        nativeType: "XML",
      }),
    ).toEqual({
      kind: "xml",
      language: "xml",
      formattedText: "",
    });

    expect(getStructuredCellDialogValue(null, textColumn)).toBeNull();
  });

  it("formats array and object runtime values only for structured column types", () => {
    expect(
      getStructuredCellDialogValue(["alpha", "beta"], {
        category: "array",
        nativeType: "TEXT[]",
      }),
    ).toEqual({
      kind: "array",
      language: "json",
      formattedText: '[\n  "alpha",\n  "beta"\n]',
    });

    expect(
      getStructuredCellDialogValue(
        { nested: { ok: true } },
        {
          category: "json",
          nativeType: "JSONB",
        },
      ),
    ).toEqual({
      kind: "json",
      language: "json",
      formattedText: '{\n  "nested": {\n    "ok": true\n  }\n}',
    });

    expect(
      getStructuredCellDialogValue(
        { nested: [1, 2] },
        {
          category: "array",
          nativeType: "JSONB",
        },
      ),
    ).toEqual({
      kind: "array",
      language: "json",
      formattedText: '{\n  "nested": [\n    1,\n    2\n  ]\n}',
    });

    expect(
      getStructuredCellDialogValue({ nested: { ok: true } }, textColumn),
    ).toBeNull();
  });

  it("uses the real column type instead of string content when choosing the dialog", () => {
    expect(
      getStructuredCellDialogValue('{"nested":{"ok":true}}', {
        category: "json",
        nativeType: "JSON",
      }),
    ).toEqual({
      kind: "json",
      language: "json",
      formattedText: '{\n  "nested": {\n    "ok": true\n  }\n}',
    });

    expect(
      getStructuredCellDialogValue('["alpha","beta"]', {
        category: "array",
        nativeType: "TEXT[]",
      }),
    ).toEqual({
      kind: "array",
      language: "json",
      formattedText: '[\n  "alpha",\n  "beta"\n]',
    });

    expect(
      getStructuredCellDialogValue("42", {
        category: "json",
        nativeType: "JSON",
      }),
    ).toEqual({
      kind: "json",
      language: "json",
      formattedText: "42",
    });

    expect(
      getStructuredCellDialogValue('{"nested":{"ok":true}}', textColumn),
    ).toBeNull();
    expect(
      getStructuredCellDialogValue('["alpha","beta"]', textColumn),
    ).toBeNull();
  });

  it("falls back to raw text for invalid json and array categories", () => {
    expect(
      getStructuredCellDialogValue('{"broken":', {
        category: "json",
        nativeType: "JSON",
      }),
    ).toEqual({
      kind: "json",
      language: "json",
      formattedText: '{"broken":',
    });

    expect(
      getStructuredCellDialogValue("alpha,beta", {
        category: "array",
        nativeType: "TEXT[]",
      }),
    ).toEqual({
      kind: "array",
      language: "plaintext",
      formattedText: "alpha,beta",
    });
  });

  it("uses xml native types for xml columns instead of xml-looking content", () => {
    expect(
      getStructuredCellDialogValue(
        '<root><item id="1">Alice</item><item id="2"/></root>',
        xmlColumn,
      ),
    ).toEqual({
      kind: "xml",
      language: "xml",
      formattedText:
        '<root>\n  <item id="1">Alice</item>\n  <item id="2"/>\n</root>',
    });

    expect(
      getStructuredCellDialogValue("   ", {
        category: "text",
        nativeType: "XML",
      }),
    ).toBeNull();

    expect(
      getStructuredCellDialogValue("not-xml-but-typed-xml", xmlColumn),
    ).toEqual({
      kind: "xml",
      language: "xml",
      formattedText: "not-xml-but-typed-xml",
    });

    expect(
      getStructuredCellDialogValue(
        '<root><item id="1">Alice</item></root>',
        textColumn,
      ),
    ).toBeNull();

    expect(getStructuredCellDialogValue("hello world", textColumn)).toBeNull();
  });

  it("treats Oracle IS JSON and IS XML constrained text columns as structured", () => {
    expect(
      getStructuredCellDialogValue(null, {
        category: "text",
        nativeType: "VARCHAR2(4000) IS JSON",
      }),
    ).toEqual({
      kind: "json",
      language: "json",
      formattedText: "",
    });

    expect(
      getStructuredCellDialogValue("<root><item/></root>", {
        category: "text",
        nativeType: "VARCHAR2(4000) IS XML",
      }),
    ).toEqual({
      kind: "xml",
      language: "xml",
      formattedText: "<root>\n  <item/>\n</root>",
    });
  });

  it("serializes structured drafts back to compact commit text", () => {
    expect(
      serializeStructuredCellDialogDraft(
        '{\n  "name": "Alice",\n  "meta": {\n    "active": false\n  }\n}',
        {
          category: "json",
          nativeType: "JSON",
        },
      ),
    ).toBe('{"name":"Alice","meta":{"active":false}}');

    expect(
      serializeStructuredCellDialogDraft('[\n  "one",\n  "two"\n]', {
        category: "array",
        nativeType: "TEXT[]",
      }),
    ).toBe('["one","two"]');

    expect(
      serializeStructuredCellDialogDraft(
        '<root>\n  <item id="1">Alice</item>\n  <item id="2"/>\n</root>',
        xmlColumn,
      ),
    ).toBe('<root><item id="1">Alice</item><item id="2"/></root>');
  });
});
