import { describe, expect, it, vi } from "vitest";
import * as baseHex from "../../src/extension/dbDrivers/BaseDBDriver";
import * as extractedHex from "../../src/extension/dbDrivers/hexUtils";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { canonicalizeBinaryPersistedEditValue } from "../../src/extension/dbDrivers/persistedEditCanonicalizers";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";

const binaryColumn: ColumnTypeMeta = {
  name: "payload",
  type: "blob",
  nativeType: "blob",
  category: "binary",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "neq", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

const oddHexValues = [
  "0xA",
  "0xABC",
  "0XABC",
  "\\xABC",
  "\\XABC",
  "ABC",
  "123",
];
const nonHexValues = ["0xGG", "\\xGG", "plain text", "0xAB CD", " 0xAB"];

describe.each([
  ["BaseDBDriver", baseHex],
  ["hexUtils", extractedHex],
] as const)("%s hex recognizer/parser contract", (_name, hex) => {
  it.each([
    ...oddHexValues,
    ...nonHexValues,
  ])("rejects %j without decoding it", (value) => {
    expect(hex.isHexLike(value)).toBe(false);
    expect(() => hex.parseHexToBuffer(value)).toThrow(/Invalid hex/);
  });

  it.each([
    ["0xABCD", "abcd"],
    ["0XABCD", "abcd"],
    ["\\xABCD", "abcd"],
    ["\\XABCD", "abcd"],
    ["ABCD", "abcd"],
    ["1234", "1234"],
    ["0x", ""],
    ["0X", ""],
    ["\\x", ""],
    ["\\X", ""],
  ])("only recognizes parseable bytes in %j", (value, expected) => {
    expect(hex.isHexLike(value)).toBe(true);
    expect(hex.parseHexToBuffer(value)).toEqual(Buffer.from(expected, "hex"));
  });

  it("keeps empty text distinct from an empty buffer", () => {
    expect(hex.isHexLike("")).toBe(false);
    expect(hex.parseHexToBuffer("")).toEqual(Buffer.alloc(0));
    expect(hex.isHexLike("0x")).toBe(true);
    expect(hex.hexFromBuffer(Buffer.alloc(0))).toBe("0x");
  });
});

describe("binary result formatting", () => {
  it.each([
    ...oddHexValues,
    ...nonHexValues,
  ])("preserves one non-hex query cell %j without failing the result", (value) => {
    const formatted = formatQueryResult(
      {
        columns: ["payload", "valid_payload", "plain_text"],
        columnMeta: [
          { category: "binary" },
          { category: "binary" },
          { category: "text" },
        ],
        rows: [
          { __col_0: value, __col_1: "\\xABCD", __col_2: "0xABC" },
          { __col_0: Buffer.from([0xef]), __col_1: "1234", __col_2: "1234" },
        ],
        rowCount: 2,
        executionTimeMs: 1,
      },
      100,
    );

    expect(formatted.rows).toEqual([
      { __col_0: value, __col_1: "0xabcd", __col_2: "0xABC" },
      { __col_0: "0xef", __col_1: "0x1234", __col_2: "1234" },
    ]);
    expect(formatted.rowCount).toBe(2);
  });

  it.each([
    ...oddHexValues,
    ...nonHexValues,
  ])("preserves SQLite binary-column text %j in a whole table page", async (value) => {
    // Non-STRICT SQLite can legitimately return TEXT from a BLOB column.
    const driver = new SQLiteDriver({
      id: "hex-table",
      name: "Hex Table",
      type: "sqlite",
      filePath: ":memory:",
    });
    vi.spyOn(driver, "describeColumns").mockResolvedValue([binaryColumn]);
    vi.spyOn(driver, "query").mockResolvedValue({
      columns: ["payload"],
      rows: [{ __col_0: value }, { __col_0: "\\xABCD" }],
      rowCount: 2,
      executionTimeMs: 1,
    });
    const service = new TableReadService({
      getConnection: () => ({ id: "hex-table" }),
      getDriver: () => driver,
    } as never);

    const page = await service.getPage(
      "hex-table",
      "main",
      "",
      "t",
      1,
      25,
      [],
      null,
      true,
    );
    expect(page.rows).toEqual([{ payload: value }, { payload: "0xabcd" }]);
  });

  it.each([
    ...oddHexValues,
    ...nonHexValues,
  ])("keeps MongoDB binary-column output tolerant for %j", (value) => {
    const driver = new MongoDBDriver({
      id: "hex-mongo",
      name: "Hex Mongo",
      type: "mongodb",
    });
    const column = { ...binaryColumn, type: "binData", nativeType: "binData" };
    const expected = value === " 0xAB" ? "0xab" : value;
    expect(driver.formatOutputValue(value, column)).toBe(expected);
    expect(
      driver.formatOutputValue(
        driver.coerceOriginalValue(value, column),
        column,
      ),
    ).toBe(expected);
    expect(driver.formatOutputValue("0xABCD", column)).toBe("0xabcd");
  });

  it.each(
    oddHexValues,
  )("rejects MySQL binary edits %j without a parser crash", (value) => {
    const driver = new MySQLDriver({
      id: "hex-mysql",
      name: "Hex MySQL",
      type: "mysql",
    });
    expect(driver.coerceInputValue(value, binaryColumn)).toBe(value);
    expect(driver.formatOutputValue(value, binaryColumn)).toBe(value);
    expect(driver.checkPersistedEdit(binaryColumn, value)).toMatchObject({
      ok: false,
      shouldVerify: false,
    });
    expect(driver.coerceInputValue("1234", binaryColumn)).toEqual(
      Buffer.from("1234", "hex"),
    );
  });

  it.each(
    oddHexValues,
  )("rejects %j in the extracted binary edit canonicalizer", (value) => {
    expect(canonicalizeBinaryPersistedEditValue(value)).toBeNull();
  });

  it("keeps empty text distinct from empty query-result bytes", () => {
    const formatted = formatQueryResult(
      {
        columns: ["payload"],
        columnMeta: [{ category: "binary" }],
        rows: [
          { __col_0: Buffer.alloc(0) },
          { __col_0: "0x" },
          { __col_0: "" },
          { __col_0: null },
        ],
        rowCount: 4,
        executionTimeMs: 1,
      },
      100,
    );
    expect(formatted.rows).toEqual([
      { __col_0: "0x" },
      { __col_0: "0x" },
      { __col_0: "" },
      { __col_0: null },
    ]);
  });
});
