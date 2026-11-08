import { describe, expect, it } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import {
  getStructuredCellDialogValue,
  getStructuredCellDraftValidationError,
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

describe("DynamoDB native structured drafts", () => {
  it.each([
    "[]",
    "[42]",
    '["not-base64!"]',
    '["0xGG"]',
    '["abcd12"]',
    "{}",
    "null",
  ])("rejects invalid Binary Set JSON %s", (text) => {
    expect(
      getStructuredCellDraftValidationError(text, {
        category: "array",
        nativeType: "binary set",
      }),
    ).toMatch(/non-empty DynamoDB binary set/);
  });
  it.each([
    '["0x01", "Ag=="]',
    '["0x"]',
    '[" 0xAB "]',
    '["AAAA"]',
  ])("accepts Binary Set JSON %s", (text) => {
    expect(
      getStructuredCellDraftValidationError(text, {
        category: "array",
        nativeType: "binary set",
      }),
    ).toBeNull();
  });
  it.each([
    "{}",
    "null",
    '"text"',
    "1",
  ])("rejects a non-array DynamoDB List draft: %s", (draft) => {
    expect(
      getStructuredCellDraftValidationError(draft, {
        category: "array",
        nativeType: "list",
      }),
    ).toMatch(/JSON array/);
  });
  it("accepts empty and populated DynamoDB Lists", () => {
    for (const draft of ["[]", '[1,{"x":true}]']) {
      expect(
        getStructuredCellDraftValidationError(draft, {
          category: "array",
          nativeType: "list",
        }),
      ).toBeNull();
    }
  });
  it.each([
    ["string set", "array", "<<'a', 'O''Brien', 'a,b'>>"],
    ["number set", "array", "<<1.25, 9007199254740993, 7.50>>"],
    ["binary set", "array", "<<0x01, 0x02>>"],
    [
      "map",
      "json",
      'DynamoDB native value (nested sets/binary): {"M":{"bytes":{"B":"Ag=="},"n":{"N":"9007199254740993"}}}',
    ],
    [
      "list",
      "array",
      'DynamoDB native value (nested sets/binary): {"L":[{"SS":["a","b"]}]}',
    ],
  ] as const)("accepts native %s text without rewriting it", (nativeType, category, text) => {
    const column = { nativeType, category };
    expect(getStructuredCellDraftValidationError(text, column)).toBeNull();
    expect(serializeStructuredCellDialogDraft(text, column)).toBe(text);
  });

  it.each([
    ["string set", "array", "<<'unclosed>>"],
    ["number set", "array", "<<1, invalid>>"],
    ["binary set", "array", "<<0xGG>>"],
    ["number set", "array", "<<>>"],
    [
      "map",
      "json",
      'DynamoDB native value (nested sets/binary): {"M":{"bytes":{"B":"AR=="}}}',
    ],
    [
      "map",
      "json",
      'DynamoDB native value (nested sets/binary): {"M":{"bad":{"N":"oops"}}}',
    ],
    ["map", "json", 'DynamoDB native value (nested sets/binary): {"L":[]}'],
    [
      "list",
      "array",
      'DynamoDB native value (nested sets/binary): {"L":[{"S":"x","N":"1"}]}',
    ],
  ] as const)("blocks malformed native %s input %s", (nativeType, category, text) => {
    expect(
      getStructuredCellDraftValidationError(text, { nativeType, category }),
    ).toBeTruthy();
  });
});

describe("getStructuredCellDialogValue", () => {
  it("keeps binary BLOB IS JSON values on the hex editor path", () => {
    const column = { category: "binary", nativeType: "BLOB IS JSON" } as const;
    const hex = "0x7b226e223a393030373139393235343734303939337d";
    expect(getStructuredCellDialogValue(hex, column)).toBeNull();
    expect(getStructuredCellDialogValue(null, column)).toBeNull();
    expect(getStructuredCellDraftValidationError(hex, column)).toBeNull();
    expect(serializeStructuredCellDialogDraft(hex, column)).toBe(hex);
  });

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
    ).toEqual({
      kind: "text",
      language: "plaintext",
      formattedText: '{"nested":{"ok":true}}',
    });
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
    ).toEqual({
      kind: "text",
      language: "plaintext",
      formattedText: '{"nested":{"ok":true}}',
    });
    expect(
      getStructuredCellDialogValue('["alpha","beta"]', textColumn),
    ).toEqual({
      kind: "text",
      language: "plaintext",
      formattedText: '["alpha","beta"]',
    });
  });

  it("opens multiline plain text without changing its contents", () => {
    const multilineText = "\nfirst line\r\nsecond line\n";

    expect(getStructuredCellDialogValue(multilineText, textColumn)).toEqual({
      kind: "text",
      language: "plaintext",
      formattedText: multilineText,
    });
    expect(serializeStructuredCellDialogDraft(multilineText, textColumn)).toBe(
      multilineText,
    );
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
      formattedText: '<root><item id="1">Alice</item><item id="2"/></root>',
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
    ).toEqual({
      kind: "text",
      language: "plaintext",
      formattedText: '<root><item id="1">Alice</item></root>',
    });

    expect(getStructuredCellDialogValue("hello world", textColumn)).toEqual({
      kind: "text",
      language: "plaintext",
      formattedText: "hello world",
    });
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
      formattedText: "<root><item/></root>",
    });
  });

  it("returns the JSON/array draft text verbatim so trailing zeros survive", () => {
    expect(
      serializeStructuredCellDialogDraft(
        '{"amt_estimated_loan":13000.0,"rate_interest_annual":0.95}',
        {
          category: "json",
          nativeType: "JSON",
        },
      ),
    ).toBe('{"amt_estimated_loan":13000.0,"rate_interest_annual":0.95}');

    expect(
      serializeStructuredCellDialogDraft("[13000.0,42.5]", {
        category: "array",
        nativeType: "NUMERIC[]",
      }),
    ).toBe("[13000.0,42.5]");

    expect(
      serializeStructuredCellDialogDraft("[1, 2, 3]", {
        category: "array",
        nativeType: "TEXT[]",
      }),
    ).toBe("[1, 2, 3]");
  });

  it("preserves whitespace between XML elements when opening and saving", () => {
    const xml = "<p><b>Hello</b> <i>world</i>\n  <pre>  a\n  b </pre></p>";
    const dialogValue = getStructuredCellDialogValue(xml, xmlColumn);
    expect(dialogValue?.formattedText).toBe(xml);
    expect(
      serializeStructuredCellDialogDraft(
        dialogValue?.formattedText ?? "",
        xmlColumn,
      ),
    ).toBe(xml);
    expect(
      serializeStructuredCellDialogDraft(
        '<root>\n  <item id="1">Alice</item> <item id="2"/></root>\n',
        xmlColumn,
      ),
    ).toBe('<root>\n  <item id="1">Alice</item> <item id="2"/></root>');
  });

  it("pretty-prints JSON text without losing trailing zeros", () => {
    const result = getStructuredCellDialogValue(
      '{"amt_estimated_loan":13000.0,"rate_interest_annual":0.95}',
      {
        category: "json",
        nativeType: "JSONB",
      },
    );
    expect(result).toEqual({
      kind: "json",
      language: "json",
      formattedText:
        '{\n  "amt_estimated_loan": 13000.0,\n  "rate_interest_annual": 0.95\n}',
    });
  });

  it("pretty-prints array text with raw numeric tokens", () => {
    const result = getStructuredCellDialogValue("[13000.0, 42.5]", {
      category: "array",
      nativeType: "NUMERIC[]",
    });
    expect(result).toEqual({
      kind: "array",
      language: "json",
      formattedText: "[\n  13000.0,\n  42.5\n]",
    });
  });
});

describe("getStructuredCellDraftValidationError", () => {
  it.each([
    "{1,2}",
    "[0:1]={1,2}",
    "[-2:-1][0:1]={{1,2},{3,4}}",
    '{"a,b","NULL",NULL,"a\\\\b"}',
    "{}",
    " {9007199254740993,9007199254740992} ",
    "[1,2]",
  ])("accepts supported PostgreSQL array text without rewriting %s", (input) => {
    const column = { category: "array", nativeType: "integer[]" } as const;
    expect(getStructuredCellDraftValidationError(input, column)).toBeNull();
    expect(serializeStructuredCellDialogDraft(input, column)).toBe(
      input.trim(),
    );
  });

  it.each([
    "{1,}",
    "[0:2]={1,2}",
    "{{1},{2,3}}",
    '{"unterminated}',
    "{1,2} trailing",
    "{1,\0}",
  ])("rejects malformed PostgreSQL native literals %s", (input) => {
    expect(
      getStructuredCellDraftValidationError(input, {
        category: "array",
        nativeType: "integer[]",
      }),
    ).toBe(
      "Enter a valid PostgreSQL array literal before applying this value.",
    );
  });

  it("keeps native PostgreSQL grammar out of JSON/BSON/DynamoDB array validation", () => {
    for (const nativeType of ["array", "list", "L", "VARRAY"]) {
      expect(
        getStructuredCellDraftValidationError("{1,2}", {
          category: "array",
          nativeType,
        }),
      ).toBe(
        nativeType === "list"
          ? "Enter a JSON array before applying this DynamoDB List value."
          : "Enter valid JSON before applying this value.",
      );
    }
    expect(
      getStructuredCellDraftValidationError("{1,2}", {
        category: "json",
        nativeType: "jsonb",
      }),
    ).toBe("Enter valid JSON before applying this value.");
    expect(
      getStructuredCellDraftValidationError(
        "{bad",
        { category: "array", nativeType: "integer[]" },
        true,
      ),
    ).toBeNull();
    expect(
      getStructuredCellDialogValue("[0:1]={1,2}", {
        category: "array",
        nativeType: "integer[]",
      }),
    ).toEqual({
      kind: "array",
      language: "plaintext",
      formattedText: "[0:1]={1,2}",
    });
  });

  it("blocks invalid JSON drafts while preserving valid raw JSON tokens", () => {
    const mongoObject = { category: "json", nativeType: "object" } as const;

    expect(getStructuredCellDraftValidationError('{"k":}', mongoObject)).toBe(
      "Enter valid JSON before applying this value.",
    );
    expect(
      getStructuredCellDraftValidationError('{"amount":13000.0}', mongoObject),
    ).toBeNull();
    expect(
      getStructuredCellDraftValidationError("", mongoObject, true),
    ).toBeNull();
  });

  it("requires DynamoDB Map edits to remain JSON objects", () => {
    const dynamoMap = { category: "json", nativeType: "map" } as const;

    expect(getStructuredCellDraftValidationError('{"k":}', dynamoMap)).toBe(
      "Enter a valid JSON object before applying this DynamoDB Map value.",
    );
    expect(getStructuredCellDraftValidationError('"text"', dynamoMap)).toBe(
      "Enter a JSON object before applying this DynamoDB Map value.",
    );
    expect(getStructuredCellDraftValidationError("[1,2]", dynamoMap)).toBe(
      "Enter a JSON object before applying this DynamoDB Map value.",
    );
    expect(getStructuredCellDraftValidationError("null", dynamoMap)).toBe(
      "Enter a JSON object before applying this DynamoDB Map value.",
    );
    expect(
      getStructuredCellDraftValidationError(
        '{"precise":9007199254740993,"amount":13000.0}',
        dynamoMap,
      ),
    ).toBeNull();
  });

  it("does not impose JSON validation on plain text and XML columns", () => {
    expect(
      getStructuredCellDraftValidationError("not JSON", textColumn),
    ).toBeNull();
    expect(
      getStructuredCellDraftValidationError("<root>", xmlColumn),
    ).toBeNull();
  });
});
