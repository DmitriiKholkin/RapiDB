import { afterEach, describe, expect, it } from "vitest";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";
import { type QueryResult, useQueryStore } from "../../src/webview/store";

afterEach(() => useQueryStore.getState().reset());

function multiResult(firstCount: number, secondCount: number): QueryResult {
  const first = Array.from({ length: firstCount }, (_, i) => ({ __col_0: i }));
  const second = Array.from({ length: secondCount }, (_, i) => ({
    __col_0: i,
  }));
  return {
    columns: ["first"],
    columnMeta: [],
    rows: first,
    rowCount: firstCount + secondCount,
    executionTimeMs: 1,
    resultSets: [
      {
        id: "one",
        columns: ["first"],
        rows: first,
        rowCount: firstCount,
        truncatedAt: firstCount,
      },
      {
        id: "two",
        columns: ["second"],
        rows: second,
        rowCount: secondCount,
        truncatedAt: secondCount,
        partial: true,
      },
    ],
  };
}

describe("query result aggregate limits", () => {
  it.each([
    [10_000, 1, 0],
    [6_000, 4_001, 4_000],
  ])("keeps formatter/export cache and store aligned for %i + %i rows", (first, second, retained) => {
    const formatted = formatQueryResult(multiResult(first, second), 10_000);
    useQueryStore.getState().setResult(formatted);
    const displayed = useQueryStore.getState().result;
    expect(displayed?.resultSets).toEqual(formatted.resultSets);
    expect(displayed?.resultSets?.[1]).toMatchObject({
      rows: expect.any(Array),
      rowCount: second,
      truncated: true,
      truncatedAt: retained,
      partial: true,
    });
    expect(displayed?.resultSets?.[1].rows.length).toBe(retained);
  });

  it.each([
    [10_000, 1, 0],
    [6_000, 4_001, 4_000],
  ])("corrects stale truncation metadata in oversized fallback %i + %i", (first, second, retained) => {
    const raw = multiResult(first, second);
    useQueryStore.getState().setResult(raw);
    const last = useQueryStore.getState().result?.resultSets?.[1];
    expect(last?.rows.length).toBe(retained);
    expect(last).toMatchObject({
      rowCount: second,
      truncated: true,
      truncatedAt: retained,
      partial: true,
    });
    expect(raw.resultSets?.[1].truncatedAt).toBe(second);
  });

  it("corrects stale top-level truncatedAt when applying the defensive hard cap", () => {
    const result = multiResult(10_002, 0);
    delete result.resultSets;
    result.truncatedAt = 10_002;
    useQueryStore.getState().setResult(result);
    expect(useQueryStore.getState().result).toMatchObject({
      truncated: true,
      truncatedAt: 10_000,
      rowCount: 10_002,
    });
    expect(useQueryStore.getState().result?.rows.length).toBe(10_000);
  });
});
