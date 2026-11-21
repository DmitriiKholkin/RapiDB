import { Binary, BSON, Long, ObjectId } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { iterateMongoTableCursor } from "../../src/extension/dbDrivers/mongodbExportBson";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";

const MiB = 1024 * 1024;
const request = {
  database: "db",
  schema: "",
  table: "items",
  page: 1,
  pageSize: 1,
  filters: [],
  sort: null,
  skipCount: false,
  omitRowIdentity: true,
} satisfies DriverTablePageRequest;

const sample = BSON.serialize({ _id: "sample", "literal.field": "value" });

function harness(replies: readonly (readonly Uint8Array[])[]) {
  const config = {
    id: "mongo-budget",
    name: "Mongo",
    type: "mongodb",
  } as const;
  const driver = new MongoDBDriver(config);
  const stats: {
    pulled: number;
    closed: boolean;
    limit: number;
    skip: number;
  }[] = [];
  const find = vi.fn((_criteria: unknown, _options: unknown) => {
    const documents = replies[stats.length] ?? [];
    const state = { pulled: 0, closed: false, limit: Infinity, skip: 0 };
    stats.push(state);
    const cursor = {
      sort: vi.fn(() => cursor),
      limit: vi.fn((limit: number) => {
        state.limit = limit;
        return cursor;
      }),
      skip: vi.fn((skip: number) => {
        state.skip = skip;
        return cursor;
      }),
      toArray: vi.fn(() => {
        throw new Error("A table cursor must not call toArray");
      }),
      async *[Symbol.asyncIterator]() {
        try {
          for (
            let i = state.skip;
            i < documents.length && i - state.skip < state.limit;
            i++
          ) {
            state.pulled++;
            yield documents[i];
          }
        } finally {
          state.closed = true;
        }
      },
    };
    return cursor;
  });
  const countDocuments = vi.fn(async () => 12);
  Object.assign(driver, {
    requireDb: () => ({ collection: () => ({ find, countDocuments }) }),
    isView: async () => false,
  });
  const reads = new TableReadService({
    getConnection: () => config,
    getDriver: () => driver,
  } as unknown as ConnectionManager);
  return { driver, reads, find, stats, countDocuments };
}

// A valid BSON document with the requested length, without a large source fixture.
function documentOfSize(size: number, binary = false) {
  if (!binary) {
    const base = { _id: "id", data: "" };
    return BSON.serialize({
      ...base,
      data: "x".repeat(size - BSON.serialize(base).byteLength),
    });
  }
  const base = { _id: "id", data: new Binary() };
  const overhead = BSON.serialize(base).byteLength;
  return BSON.serialize({
    ...base,
    data: new Binary(Buffer.alloc(size - overhead)),
  });
}

describe("MongoDB Table Viewer read budgets", () => {
  it("checks raw size before decoding, closes the cursor and detaches accepted bytes", async () => {
    const document = {
      _id: new ObjectId(),
      data: new Binary(Buffer.from([1, 2])),
      long: Long.fromString("9007199254740993"),
    };
    const raw = BSON.serialize(document);
    const response = Buffer.alloc(raw.byteLength + 100);
    Buffer.from(raw).copy(response, 10);
    const view = response.subarray(10, 10 + raw.byteLength);
    const close = vi.fn();
    const cursor = {
      async *[Symbol.asyncIterator]() {
        try {
          yield view;
          // A sentinel, not BSON: if the iterator decodes it before charging
          // the budget, it will raise a decoder error instead of the limit.
          yield Buffer.alloc(raw.byteLength + 1);
        } finally {
          close();
        }
      },
    };
    const rows = iterateMongoTableCursor(cursor, (bytes) => {
      if (bytes > raw.byteLength) throw new Error("test byte limit");
    });
    const accepted = await rows.next();
    expect(accepted.value).toMatchObject({
      document,
      bsonBytes: raw.byteLength,
    });
    if (accepted.done) throw new Error("expected a BSON document");
    expect(accepted.value.wire?.buffer).not.toBe(response.buffer);
    expect(accepted.value.document.data.buffer.buffer).not.toBe(
      response.buffer,
    );
    await expect(rows.next()).rejects.toThrow("test byte limit");
    expect(close).toHaveBeenCalledOnce();
  });

  it("stops schema sampling on the first document over 2 MiB even for a one-row page", async () => {
    const h = harness([[documentOfSize(2 * MiB + 1), sample]]);
    await expect(h.driver.readTablePage(request)).rejects.toThrow(
      /schema sample.*2 MiB BSON.*No partial result/,
    );
    expect(h.find).toHaveBeenCalledTimes(1);
    expect(h.find.mock.calls[0][1]).toMatchObject({ raw: true, batchSize: 8 });
    expect(h.stats[0]).toMatchObject({ limit: 100, pulled: 1, closed: true });
    expect(h.countDocuments).not.toHaveBeenCalled();
  });

  it("rejects oversized metadata instead of caching a partial schema", async () => {
    const item = documentOfSize(1 * MiB);
    const h = harness([Array(50).fill(item), [sample]]);
    const readColumns = () =>
      h.reads.getColumns("mongo-budget", "db", "", "items");
    await expect(readColumns()).rejects.toThrow(/schema sample.*4 MiB BSON/);
    expect(h.stats[0]).toMatchObject({ limit: 50, pulled: 5, closed: true });
    expect(await readColumns()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "literal.field" }),
      ]),
    );
    expect(h.find).toHaveBeenCalledTimes(2);
  });

  it.each([
    false,
    true,
  ])("does not return partial sorted pages (local sort=%s)", async (localSort) => {
    const item = documentOfSize(2 * MiB);
    const h = harness([[sample], Array(12).fill(item)]);
    await expect(
      h.driver.readTablePage({
        ...request,
        pageSize: 12,
        sort: { column: localSort ? "literal.field" : "_id", direction: "asc" },
      }),
    ).rejects.toThrow(
      localSort
        ? /client-side filter or sort.*16 MiB BSON.*No partial result/
        : /page.*8 MiB BSON.*No partial result/,
    );
    expect(h.stats[1]).toMatchObject({
      pulled: localSort ? 9 : 5,
      closed: true,
    });
    expect(h.find.mock.calls[1][1]).toMatchObject({ raw: true, batchSize: 8 });
    expect(h.countDocuments).not.toHaveBeenCalled();
  });

  it("bounds the client-side filtered page even when its complete scan fits", async () => {
    const item = BSON.serialize({
      _id: "id",
      "literal.field": "yes",
      data: "x".repeat(MiB),
    });
    const h = harness([[sample], Array(8).fill(item)]);
    await expect(
      h.driver.readTablePage({
        ...request,
        pageSize: 8,
        filters: [{ column: "literal.field", operator: "eq", value: "yes" }],
      }),
    ).rejects.toThrow(/page.*8 MiB BSON.*No partial result/);
    expect(h.stats[1]).toMatchObject({ pulled: 8, closed: true });
  });

  it("checks expanded display text before requesting another document", async () => {
    const item = BSON.serialize({ data: { text: "\n".repeat(MiB + 1024) } });
    // Nested JSON materializes escapes; some control characters take six bytes.
    const expanded = BSON.serialize({ data: { text: "\x01".repeat(MiB) } });
    const h = harness([[sample], [item, expanded, sample]]);
    await expect(
      h.driver.readTablePage({ ...request, pageSize: 3 }),
    ).rejects.toThrow(/page.*4 MiB display text.*No partial result/);
    expect(h.stats[1]).toMatchObject({ pulled: 2, closed: true });
  });

  it("bounds total display bytes independently of the BSON scan limit", async () => {
    const item = documentOfSize(2 * MiB, true);
    const h = harness([[sample], Array(10).fill(item)]);
    await expect(
      h.driver.readTablePage({
        ...request,
        sort: { column: "literal.field", direction: "asc" },
      }),
    ).rejects.toThrow(/client-side filter or sort.*24 MiB display text/);
    expect(h.stats[1]).toMatchObject({ pulled: 7, closed: true });
  });

  it("propagates a chunk failure past the export service's page iterator", async () => {
    const chunksPerPage = 5;
    const item = documentOfSize(2 * MiB);
    const h = harness([
      [sample],
      [...Array(chunksPerPage).fill(sample)],
      [sample],
      [
        ...Array(chunksPerPage).fill(sample),
        ...Array(chunksPerPage).fill(item),
      ],
    ]);
    const result = h.reads.exportAll(
      "mongo-budget",
      "db",
      "",
      "items",
      chunksPerPage,
    );
    expect((await result.next()).value).toMatchObject({
      rows: Array(chunksPerPage).fill({
        _id: "sample",
        "literal.field": "value",
      }),
    });
    await expect(result.next()).rejects.toThrow(/page.*8 MiB BSON/);
    expect(h.stats[3]).toMatchObject({ pulled: 5, closed: true });
  });
});
