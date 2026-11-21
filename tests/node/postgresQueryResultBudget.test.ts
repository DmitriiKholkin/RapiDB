import { describe, expect, it, vi } from "vitest";
import { BoundedPostgresQuery } from "../../src/extension/dbDrivers/postgresBoundedQuery";
import { PostgresQueryResultBudget } from "../../src/extension/dbDrivers/postgresQueryResultBudget";
import {
  jsonStringByteSize,
  queryResultJsonFits,
} from "../../src/extension/utils/queryResultSize";

function startQuery(cellBytes: number, totalBytes: number) {
  const done = vi.fn();
  const query = new BoundedPostgresQuery(
    "mock query",
    [],
    10,
    done,
    new PostgresQueryResultBudget({ cellBytes, totalBytes }),
  );
  return {
    query,
    done,
    fields: (...names: string[]) =>
      query.handleRowDescription({
        fields: names.map((name) => ({ name, dataTypeID: 25, format: "text" })),
      }),
    row: (...fields: string[]) => query.handleDataRow({ fields }),
    complete: (text: string) => query.handleCommandComplete({ text }, {}),
  };
}

describe("PostgreSQL Query Editor byte limits", () => {
  it("keeps a prefix, skips type decoding after a large wire cell, and drains later statements", () => {
    const { query, done, fields, row, complete } = startQuery(160, 2000);
    fields("text");
    const parse = vi.spyOn(query.currentResult, "parseRow");
    row("first");
    // 40 UTF-16 units, 120 UTF-8 bytes: exceeds the remaining cell budget.
    row("文".repeat(40));
    row("not part of the prefix");
    complete("SELECT 3");
    fields("returning");
    row("second set");
    complete("UPDATE 1");
    expect(done).not.toHaveBeenCalled();
    query.handleReadyForQuery({});
    expect(done).toHaveBeenCalledWith(null, expect.anything());
    expect(parse).toHaveBeenCalledExactlyOnceWith(["first"]);
    expect(query.affectedRows).toBe(1);
    expect(query.resultSets).toMatchObject([
      {
        rows: [["first"]],
        rowCount: 3,
        completed: true,
        truncatedBySize: true,
      },
      {
        rows: [["second set"]],
        rowCount: 1,
        completed: true,
        truncated: false,
      },
    ]);
    expect(query.resultSets[0].partial).toBeUndefined();
  });

  it.each([
    { decoded: "\u0001".repeat(24) },
    { decoded: Buffer.alloc(60) },
    { decoded: [[`mixed"\\${"\u0001".repeat(18)}`]] },
    { decoded: Array.from({ length: 4 }, () => null) },
  ])("checks decoded cell representation without formatting it", ({
    decoded,
  }) => {
    const { query, fields, row, complete } = startQuery(160, 2000);
    fields("value");
    const parse = vi
      .spyOn(query.currentResult, "parseRow")
      .mockReturnValue([decoded]);
    row("fits as wire text");
    complete("SELECT 1");
    expect(parse).toHaveBeenCalledOnce();
    expect(query.resultSets[0]).toMatchObject({
      rows: [],
      rowCount: 1,
      completed: true,
      truncated: true,
      truncatedBySize: true,
    });
  });

  it("charges metadata and complete rows against the shared budget across statements", () => {
    const { query, fields, row, complete } = startQuery(160, 1000);
    fields("one");
    row("a");
    row("b");
    complete("SELECT 2");
    fields("two");
    row("c");
    const parse = vi.spyOn(query.currentResult, "parseRow");
    row("d");
    complete("SELECT 2");
    fields("three");
    row("e");
    complete("SELECT 1");
    expect(parse).not.toHaveBeenCalled();
    expect(query.resultSetsTruncated).toBe(true);
    expect(query.resultSets).toMatchObject([
      { rows: [["a"], ["b"]], rowCount: 2, truncated: false },
      { rows: [], rowCount: 2, truncatedBySize: true },
    ]);
  });

  it("does not install pg parsers or retain metadata for an omitted set", () => {
    const { query, fields, row, complete } = startQuery(160, 1000);
    fields("文".repeat(60));
    expect(query.currentResult.fields).toEqual([]);
    row("ignored");
    complete("SELECT 1");
    fields("smaller");
    row("retained");
    complete("SELECT 1");
    expect(query.resultSetsTruncated).toBe(true);
    expect(query.resultSets[0].rows).toEqual([["retained"]]);
  });

  it.each([
    "",
    `"\\`,
    "\u0000\b\t\n\f\r",
    "😀я文",
    "\ud800",
    "\udfff",
  ])("counts UTF-8 JSON string bytes, including nested escaping", (value) => {
    const json = JSON.stringify(value);
    for (const [embedded, text] of [
      [false, json],
      [true, JSON.stringify(json)],
    ] as const) {
      const bytes = Buffer.byteLength(text);
      expect(jsonStringByteSize(value, bytes, embedded)).toBe(bytes);
      expect(jsonStringByteSize(value, bytes - 1, embedded)).toBe(Infinity);
    }
  });

  it("does not fit a complete message if escaped content alone exceeds its budget", () => {
    const envelope = {
      type: "queryResult",
      payload: { rows: [{ text: "\u0000".repeat(20) }] },
    };
    expect(queryResultJsonFits(envelope, 100)).toBe(false);
    expect(queryResultJsonFits(envelope, 256)).toBe(true);
  });
});
