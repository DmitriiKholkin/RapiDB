import { describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";

const column: ColumnTypeMeta = {
  name: "name",
  type: "text",
  nativeType: "text",
  category: "text",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq"],
  valueSemantics: "plain",
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("export connection session", () => {
  it.each([
    "fallback",
    "page",
    "chunks",
    "native",
  ])("stops %s export after reconnect during column lookup, before any data read", async (mode) => {
    const pending = deferred<ColumnTypeMeta[]>();
    const read = vi.fn();
    const original = {
      describeColumns: vi.fn(() => pending.promise),
      query: read,
      ...(mode === "page" ? { readTablePage: read } : {}),
      ...(mode === "chunks"
        ? { readTablePageChunks: read, readTablePage: read }
        : {}),
      ...(mode === "native" ? { exportTableChunks: read } : {}),
    };
    const replacementRead = vi.fn();
    const replacement = {
      ...original,
      query: replacementRead,
      readTablePage: replacementRead,
    };
    let current: object = original;
    const dispose = vi.fn();
    const reader = new TableReadService({
      getConnection: () => ({ id: "c" }),
      getDriver: () => current,
      onDidInvalidateConnectionMetadata: () => ({ dispose }),
    } as never);
    const next = reader
      .exportAll("c", "db", "", "t", 2, null, [
        { column: "name", operator: "eq", value: "a" },
      ])
      .next();
    expect(original.describeColumns).toHaveBeenCalledOnce();
    current = replacement;
    pending.resolve([column]);
    await expect(next).rejects.toThrow(/export connection changed/i);
    expect(read).not.toHaveBeenCalled();
    expect(replacementRead).not.toHaveBeenCalled();
    expect(dispose).toHaveBeenCalledOnce();
  });

  it("stops a fallback after invalidation during row identity lookup, even if the driver is retained", async () => {
    const pending = deferred<string[]>();
    const query = vi.fn();
    const driver = {
      describeColumns: vi.fn(async () => [column]),
      qualifiedTableName: () => '"t"',
      getTableRowIdentityOrderExpressions: vi.fn(() => pending.promise),
      buildOrderByDefault: () => "",
      query,
    };
    let invalidate!: (id: string) => void;
    const reader = new TableReadService({
      getConnection: () => ({ id: "c" }),
      getDriver: () => driver,
      onDidInvalidateConnectionMetadata: (listener: (id: string) => void) => {
        invalidate = listener;
        return { dispose: vi.fn() };
      },
    } as never);
    const next = reader.exportAll("c", "db", "", "t", 2).next();
    await vi.waitFor(() =>
      expect(driver.getTableRowIdentityOrderExpressions).toHaveBeenCalledOnce(),
    );
    invalidate("c");
    pending.resolve([]);
    await expect(next).rejects.toThrow(/export connection changed/i);
    expect(query).not.toHaveBeenCalled();
  });

  it.each([
    false,
    true,
  ])("stops a page export after cancellation during metadata (filtered=%s)", async (filtered) => {
    const pending = deferred<ColumnTypeMeta[]>();
    const query = vi.fn();
    const driver = {
      describeColumns: vi.fn(() => pending.promise),
      qualifiedTableName: () => '"t"',
      query,
    };
    const reader = new TableReadService({
      getConnection: () => ({ id: "c" }),
      getDriver: () => driver,
    } as never);
    const controller = new AbortController();
    const next = reader
      .getPageChunks(
        "c",
        "db",
        "",
        "t",
        1,
        2,
        filtered ? [{ column: "name", operator: "eq", value: "a" }] : [],
        null,
        controller.signal,
      )
      .next();
    expect(driver.describeColumns).toHaveBeenCalledOnce();
    controller.abort();
    pending.resolve([column]);
    await expect(next).rejects.toMatchObject({ name: "AbortError" });
    expect(query).not.toHaveBeenCalled();
  });

  it("honours the page export session guard after delayed filter metadata", async () => {
    const pending = deferred<ColumnTypeMeta[]>();
    const read = vi.fn();
    const driver = {
      describeColumns: vi.fn(() => pending.promise),
      readTablePage: read,
    };
    let current: object = driver;
    const reader = new TableReadService({
      getConnection: () => ({ id: "c" }),
      getDriver: () => current,
    } as never);
    const next = reader
      .getPageChunks(
        "c",
        "db",
        "",
        "t",
        1,
        2,
        [{ column: "name", operator: "eq", value: "a" }],
        null,
        undefined,
        () => {
          if (current !== driver) throw new Error("session changed");
        },
      )
      .next();
    current = { readTablePage: read };
    pending.resolve([column]);
    await expect(next).rejects.toThrow("session changed");
    expect(read).not.toHaveBeenCalled();
  });

  it("forwards cancellation to fallback count and data queries", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ columns: ["cnt"], rows: [{ cnt: 1 }] })
      .mockResolvedValueOnce({ columns: ["name"], rows: [{ __col_0: "a" }] });
    const driver = {
      describeColumns: vi.fn(async () => [column]),
      qualifiedTableName: () => '"t"',
      buildOrderByDefault: () => "",
      buildPagination: () => ({ sql: "LIMIT 2", params: [] }),
      formatOutputValue: (value: unknown) => value,
      query,
    };
    const reader = new TableReadService({
      getConnection: () => ({ id: "c" }),
      getDriver: () => driver,
    } as never);
    const controller = new AbortController();
    await reader.getPage(
      "c",
      "db",
      "",
      "t",
      1,
      2,
      [],
      null,
      false,
      controller.signal,
    );
    expect(query).toHaveBeenCalledTimes(2);
    for (const call of query.mock.calls)
      expect(call[2]).toEqual({ database: "db", signal: controller.signal });
  });

  it("does not retry a projection after cancellation during its query", async () => {
    const pending = deferred<never>();
    const query = vi.fn(() => pending.promise);
    const driver = {
      describeColumns: vi.fn(async () => [column]),
      qualifiedTableName: () => '"t"',
      buildOrderByDefault: () => "",
      buildPagination: () => ({ sql: "LIMIT 2", params: [] }),
      buildTableDataProjection: () => "name",
      shouldFallbackTableDataProjection: () => true,
      query,
    };
    const reader = new TableReadService({
      getConnection: () => ({ id: "c" }),
      getDriver: () => driver,
    } as never);
    const controller = new AbortController();
    const next = reader
      .getPageChunks("c", "db", "", "t", 1, 2, [], null, controller.signal)
      .next();
    await vi.waitFor(() => expect(query).toHaveBeenCalledOnce());
    controller.abort();
    pending.resolve(undefined as never);
    await expect(next).rejects.toMatchObject({ name: "AbortError" });
    expect(query).toHaveBeenCalledOnce();
  });
});
