import { describe, expect, it, vi } from "vitest";
import { TableReadService } from "../../src/extension/table/tableReadService";

describe("offset export under concurrent writes", () => {
  it.each([
    { operation: "delete", expected: [1, 2, 4] },
    { operation: "insert", expected: [1, 2, 2, 3, 4] },
  ])("does not imply a snapshot after an $operation between pages", async ({
    operation,
    expected,
  }) => {
    const rows = [1, 2, 3, 4];
    const readTablePage = vi.fn(
      async ({ page, pageSize }: { page: number; pageSize: number }) => ({
        columns: [],
        rows: rows
          .slice((page - 1) * pageSize, page * pageSize)
          .map((id) => ({ id })),
        totalCount: 0,
        executionTimeMs: 0,
      }),
    );
    const driver = { readTablePage };
    const service = new TableReadService({
      getConnection: () => ({ id: "c" }),
      getDriver: () => driver,
    } as never);
    const iterator = service.exportAll("c", "db", "", "items", 2);
    const first = await iterator.next();
    if (operation === "delete") rows.shift();
    else rows.unshift(0);
    const result = [...(first.value?.rows ?? [])];
    for await (const chunk of iterator) result.push(...chunk.rows);

    expect(result.map((row) => row.id)).toEqual(expected);
    expect(readTablePage.mock.calls.map(([request]) => request.page)).toEqual(
      operation === "delete" ? [1, 2] : [1, 2, 3],
    );
  });
});
