import { Client } from "@elastic/elasticsearch";
import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import {
  createTimeoutAwareDriver,
  getDefaultDriverTimeoutSettings,
} from "../../src/extension/dbDrivers/timeout";
import type { DriverTableExportChunk } from "../../src/extension/dbDrivers/types";
import {
  createElasticsearchPitMock,
  deferred,
  required,
} from "../support/elasticsearchPitMock";

const request = {
  database: "default",
  schema: "indices",
  table: "records",
  chunkSize: 500,
};

async function collect(chunks: AsyncIterable<DriverTableExportChunk>) {
  const rows: Record<string, unknown>[] = [];
  for await (const chunk of chunks) rows.push(...chunk.rows);
  return rows;
}

describe("Elasticsearch PIT paging and full export (B11)", () => {
  it("uses SDK 9 request serialization and transport options, closing the response's latest PIT ID", async () => {
    const client = new Client({ node: "http://localhost:9200" });
    const transport = vi
      .spyOn(client.transport, "request")
      .mockResolvedValueOnce({
        records: { defaults: { index: { max_result_window: "3" } } },
      })
      .mockResolvedValueOnce({ id: "initial-pit" })
      .mockResolvedValueOnce({
        pit_id: "latest-pit",
        hits: { hits: [{ _id: "one", _source: {}, sort: [2 ** 32] }] },
      })
      .mockResolvedValueOnce({ succeeded: true, num_freed: 1 });
    const driver = new ElasticsearchDriver({
      id: "sdk9",
      name: "SDK 9",
      type: "elasticsearch",
    });
    Object.assign(driver, { client, connected: true });
    try {
      expect(await collect(driver.exportTableChunks(request))).toEqual([
        { _id: "one", _source: "{}" },
      ]);
      expect(transport.mock.calls[0]).toEqual([
        expect.objectContaining({
          method: "GET",
          path: "/records/_settings/index.max_result_window",
          querystring: { include_defaults: true },
        }),
        { signal: expect.any(AbortSignal), requestTimeout: expect.any(Number) },
      ]);
      expect(transport.mock.calls[1]).toEqual([
        expect.objectContaining({
          method: "POST",
          path: "/records/_pit",
          querystring: {
            keep_alive: "1m",
            allow_partial_search_results: false,
          },
        }),
        { signal: expect.any(AbortSignal), requestTimeout: expect.any(Number) },
      ]);
      expect(transport.mock.calls[2]).toEqual([
        expect.objectContaining({
          method: "POST",
          path: "/_search",
          querystring: { allow_partial_search_results: false },
          body: expect.objectContaining({
            pit: { id: "initial-pit", keep_alive: "1m" },
            sort: [{ _shard_doc: "asc" }],
            size: 3,
            track_total_hits: false,
          }),
        }),
        { signal: expect.any(AbortSignal), requestTimeout: expect.any(Number) },
      ]);
      expect(transport.mock.calls[3]).toEqual([
        expect.objectContaining({
          method: "DELETE",
          path: "/_pit",
          body: { id: "latest-pit" },
        }),
        { signal: expect.any(AbortSignal), requestTimeout: 1000 },
      ]);
    } finally {
      await driver.disconnect();
    }
  });
  it.each([
    9999, 10000, 10001,
  ])("exports all %i rows in unique snapshot order through the service", async (count) => {
    const { service, client, documents, snapshots } =
      createElasticsearchPitMock(count);
    const rows = await collect(
      service.exportAll("es-pit", "default", "indices", "records", 500),
    );
    expect(rows.map((row) => row._id)).toEqual(documents.map((hit) => hit._id));
    expect(new Set(rows.map((row) => row._id)).size).toBe(count);
    expect(client.search).toHaveBeenCalledTimes(Math.floor(count / 500) + 1);
    expect(client.search.mock.calls.at(-1)?.[0].search_after).toHaveLength(1);
    expect(client.openPointInTime).toHaveBeenCalledWith(
      {
        index: "records",
        keep_alive: "1m",
        allow_partial_search_results: false,
      },
      { signal: expect.any(AbortSignal), requestTimeout: expect.any(Number) },
    );
    expect(client.closePointInTime).toHaveBeenCalledTimes(1);
    expect(snapshots.size).toBe(0);
    for (const [search, options] of client.search.mock.calls) {
      expect(search).toMatchObject({
        sort: [{ _shard_doc: "asc" }],
        track_total_hits: false,
      });
      expect(search).not.toHaveProperty("index");
      expect(search).not.toHaveProperty("from");
      expect(options?.signal).toBeInstanceOf(AbortSignal);
    }
  });

  it.each([
    1, 3, 7,
  ])("honors a custom result window of %i even for larger requested chunks", async (window) => {
    const { service, client, documents } = createElasticsearchPitMock(
      17,
      window,
    );
    const rows = await collect(
      service.exportAll("es-pit", "default", "indices", "records", 100),
    );
    expect(rows.map((row) => row._id)).toEqual(documents.map((hit) => hit._id));
    expect(
      client.search.mock.calls.every(([args]) => args.size === window),
    ).toBe(true);
    expect(client.indices.getSettings).toHaveBeenCalledWith(
      {
        index: "records",
        name: "index.max_result_window",
        include_defaults: true,
      },
      { signal: expect.any(AbortSignal), requestTimeout: expect.any(Number) },
    );
  });

  it.each([
    9999, 10000, 10001,
  ])("reads deep pages of a %i-row index including pages past its end", async (count) => {
    const { driver, documents, snapshots } = createElasticsearchPitMock(count);
    for (const page of [100, 101, 102]) {
      const result = await driver.readTablePage({
        ...request,
        page,
        pageSize: 100,
        filters: [],
        sort: null,
        skipCount: false,
      });
      expect(result.rows.map((row) => row._id)).toEqual(
        documents.slice((page - 1) * 100, page * 100).map((hit) => hit._id),
      );
      expect(result.totalCount).toBe(count);
      expect(snapshots.size).toBe(0);
    }
  });

  it("reads deep pages with a small window and keeps counts optional", async () => {
    const { driver, client, documents } = createElasticsearchPitMock(21, 3);
    const result = await driver.readTablePage({
      ...request,
      page: 3,
      pageSize: 7,
      filters: [],
      sort: null,
      skipCount: true,
    });
    expect(result.rows.map((row) => row._id)).toEqual(
      documents.slice(14, 21).map((hit) => hit._id),
    );
    expect(result.totalCount).toBe(0);
    expect(
      client.search.mock.calls.every(
        ([args]) => args.track_total_hits === false,
      ),
    ).toBe(true);
  });

  it("holds a PIT snapshot despite writes and keeps same-ID documents from different indices", async () => {
    const { driver, client, documents } = createElasticsearchPitMock(8, 3);
    documents[4]._id = documents[0]._id;
    documents[4]._index = "another-index";
    const expected = structuredClone(documents);
    const chunks = driver.exportTableChunks(request);
    const first = await chunks.next();
    documents.splice(0, 3);
    documents[0]._source.ordinal = 999;
    const rows = [...(first.value?.rows ?? []), ...(await collect(chunks))];
    expect(rows).toEqual(
      expected.map((hit) => ({
        _id: hit._id,
        _source: JSON.stringify(hit._source),
      })),
    );
    expect(client.search.mock.calls[1][0].pit?.id).not.toBe(
      client.search.mock.calls[0][0].pit?.id,
    );
  });

  it("yields columns for an empty index and closes its PIT", async () => {
    const { driver, snapshots } = createElasticsearchPitMock(0);
    const chunks = driver.exportTableChunks(request);
    const first = await chunks.next();
    expect(first.value).toMatchObject({
      columns: [{ name: "_id" }, { name: "_source" }],
      rows: [],
    });
    expect((await chunks.next()).done).toBe(true);
    expect(snapshots.size).toBe(0);
  });

  it("preserves the optional hook through the real timeout proxy", async () => {
    const { driver } = createElasticsearchPitMock(3);
    const wrapped = createTimeoutAwareDriver(
      driver,
      getDefaultDriverTimeoutSettings,
    );
    expect(await collect(wrapped.exportTableChunks(request))).toHaveLength(3);
  });

  it.each([
    "filter",
    "sort",
  ])("keeps %s materialization fail-closed above 10000, including narrow filters", async (mode) => {
    const { driver, service, client } = createElasticsearchPitMock(10001);
    const exportHook = vi.spyOn(driver, "exportTableChunks");
    const sort =
      mode === "sort" ? { column: "_id", direction: "asc" as const } : null;
    const filters =
      mode === "filter"
        ? [{ column: "_id", operator: "like" as const, value: "%only-one%" }]
        : [];
    await expect(
      collect(
        service.exportAll(
          "es-pit",
          "default",
          "indices",
          "records",
          500,
          sort,
          filters,
        ),
      ),
    ).rejects.toThrow("requires reading the entire index");
    expect(exportHook).not.toHaveBeenCalled();
    expect(client.search).toHaveBeenCalledWith(
      expect.objectContaining({ size: 10000, track_total_hits: true }),
      {
        signal: expect.any(AbortSignal),
        requestTimeout: expect.any(Number),
        maxRetries: 0,
      },
    );
    expect(client.openPointInTime).not.toHaveBeenCalled();
  });

  it.each(
    [undefined, [], ["bad"], [NaN], [1, 2], [-1]].map((sort) => ({ sort })),
  )("rejects malformed cursor $sort and closes PIT", async ({ sort }) => {
    const { driver, client, snapshots } = createElasticsearchPitMock(1);
    const realSearch = required(client.search.getMockImplementation());
    client.search.mockImplementation(async (...args) => {
      const response = await realSearch(...args);
      Object.assign(response.hits.hits[0], { sort });
      return response;
    });
    await expect(collect(driver.exportTableChunks(request))).rejects.toThrow(
      "cursor is malformed",
    );
    expect(snapshots.size).toBe(0);
  });

  it("rejects a repeated/backwards cursor before yielding duplicate rows", async () => {
    const { driver, client, snapshots } = createElasticsearchPitMock(9, 3);
    const chunks = driver.exportTableChunks(request);
    await chunks.next();
    const realSearch = required(client.search.getMockImplementation());
    client.search.mockImplementationOnce(async (...args) => {
      const response = await realSearch(...args);
      response.hits.hits[0].sort = required(args[0].search_after);
      return response;
    });
    await expect(chunks.next()).rejects.toThrow("made no progress");
    expect(snapshots.size).toBe(0);
  });

  it.each([
    "timed_out",
    "shard failure",
  ])("rejects incomplete results: %s", async (kind) => {
    const { driver, client, snapshots } = createElasticsearchPitMock(3);
    const realSearch = required(client.search.getMockImplementation());
    client.search.mockImplementationOnce(async (...args) => {
      const response = await realSearch(...args);
      response.timed_out = kind === "timed_out";
      response._shards.failed = kind === "shard failure" ? 1 : 0;
      return response;
    });
    await expect(collect(driver.exportTableChunks(request))).rejects.toThrow(
      "incomplete results",
    );
    expect(snapshots.size).toBe(0);
  });

  it.each([
    1, 9,
  ])("closes immediately on cancellation at yield, including a final short batch (%i rows)", async (count) => {
    const { driver, snapshots } = createElasticsearchPitMock(count, 3);
    const controller = new AbortController();
    const chunks = driver.exportTableChunks(request, controller.signal);
    await chunks.next();
    controller.abort();
    await vi.waitFor(() => expect(snapshots.size).toBe(0));
    await expect(chunks.next()).rejects.toMatchObject({ name: "AbortError" });
  });

  it.each([
    false,
    true,
  ])("closes the latest ID on mid-fetch cancellation (cleanup rejects: %s)", async (closeFails) => {
    const { driver, client, snapshots } = createElasticsearchPitMock(9, 3);
    const controller = new AbortController();
    const chunks = driver.exportTableChunks(request, controller.signal);
    await chunks.next();
    const response = deferred<Awaited<ReturnType<typeof client.search>>>();
    const realSearch = required(client.search.getMockImplementation());
    let rotatedId: string | undefined;
    client.search.mockImplementationOnce(async (...args) => {
      const result = await realSearch(...args);
      rotatedId = result.pit_id;
      return response.promise;
    });
    const next = chunks.next();
    await vi.waitFor(() => expect(rotatedId).toBeDefined());
    const snapshot = required(snapshots.get(required(rotatedId)));
    if (closeFails)
      client.closePointInTime.mockRejectedValueOnce(new Error("close failed"));
    controller.abort();
    expect(client.search.mock.calls.at(-1)?.[1]?.signal?.aborted).toBe(true);
    const rejection = expect(next).rejects.toMatchObject({
      name: "AbortError",
    });
    response.resolve({
      pit_id: rotatedId,
      timed_out: false,
      _shards: { total: 2, successful: 2, failed: 0 },
      hits: { total: { value: 9, relation: "eq" }, hits: snapshot.slice(3, 6) },
    });
    await rejection;
    expect(client.closePointInTime).toHaveBeenCalledWith(
      { id: rotatedId },
      { signal: expect.any(AbortSignal), requestTimeout: 1000 },
    );
  });

  it.each([
    "abort",
    "disconnect",
  ])("cleans an initial PIT that resolves after %s before closing the client", async (kind) => {
    const { driver, client, snapshots } = createElasticsearchPitMock(9);
    const opened = deferred<{ id: string }>();
    const realOpen = required(client.openPointInTime.getMockImplementation());
    let id = "";
    client.openPointInTime.mockImplementationOnce(async (args) => {
      id = (await realOpen(args)).id;
      return opened.promise;
    });
    const controller = new AbortController();
    const chunks = driver.exportTableChunks(request, controller.signal);
    const next = chunks.next();
    const rejection = expect(next).rejects.toMatchObject({
      name: "AbortError",
    });
    await vi.waitFor(() => expect(id).not.toBe(""));
    const disconnect = kind === "disconnect" ? driver.disconnect() : undefined;
    if (kind === "abort") controller.abort();
    expect(client.close).not.toHaveBeenCalled();
    opened.resolve({ id });
    await rejection;
    await disconnect;
    expect(client.closePointInTime).toHaveBeenCalledWith(
      { id },
      { signal: expect.any(AbortSignal), requestTimeout: 1000 },
    );
    expect(client.search).not.toHaveBeenCalled();
    expect(snapshots.size).toBe(0);
    if (kind === "disconnect") expect(client.close).toHaveBeenCalledTimes(1);
  });

  it("disconnects a paused reader and closes PIT before the transport", async () => {
    const { driver, client, snapshots } = createElasticsearchPitMock(9, 3);
    const chunks = driver.exportTableChunks(request);
    await chunks.next();
    client.close.mockImplementationOnce(async () => {
      expect(snapshots.size).toBe(0);
    });
    await driver.disconnect();
    expect(snapshots.size).toBe(0);
    await expect(chunks.next()).rejects.toMatchObject({ name: "AbortError" });
  });

  it("closes on consumer return and propagates close failures on success", async () => {
    const { driver, client } = createElasticsearchPitMock(9, 3);
    const chunks = driver.exportTableChunks(request);
    await chunks.next();
    await chunks.return(undefined);
    expect(client.closePointInTime).toHaveBeenCalledTimes(1);
    client.closePointInTime.mockRejectedValueOnce(new Error("close failed"));
    await expect(collect(driver.exportTableChunks(request))).rejects.toThrow(
      "close failed",
    );
  });

  it("preserves search errors when PIT cleanup also fails", async () => {
    const { driver, client } = createElasticsearchPitMock(9, 3);
    client.search.mockRejectedValueOnce(new Error("search failed"));
    client.closePointInTime.mockRejectedValueOnce(new Error("close failed"));
    await expect(collect(driver.exportTableChunks(request))).rejects.toThrow(
      "search failed",
    );
    expect(client.closePointInTime).toHaveBeenCalledTimes(1);
  });

  it("fails the export when PIT close reports succeeded=false", async () => {
    const { driver, client } = createElasticsearchPitMock(1);
    client.closePointInTime.mockResolvedValueOnce({
      succeeded: false,
      num_freed: 0,
    });
    await expect(collect(driver.exportTableChunks(request))).rejects.toThrow(
      "PIT cleanup failed",
    );
  });

  it("closes every active PIT on disconnect even if one cleanup fails", async () => {
    const { driver, client } = createElasticsearchPitMock(9, 3);
    const first = driver.exportTableChunks(request);
    const second = driver.exportTableChunks(request);
    await first.next();
    await second.next();
    client.closePointInTime.mockRejectedValueOnce(new Error("close failed"));
    await driver.disconnect();
    expect(client.closePointInTime).toHaveBeenCalledTimes(2);
    expect(client.close).toHaveBeenCalledTimes(1);
    await expect(first.next()).rejects.toMatchObject({ name: "AbortError" });
    await expect(second.next()).rejects.toMatchObject({ name: "AbortError" });
  });

  it("does not open a PIT for an already-cancelled export", async () => {
    const { driver, client } = createElasticsearchPitMock(9);
    const controller = new AbortController();
    controller.abort();
    await expect(
      collect(driver.exportTableChunks(request, controller.signal)),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(client.openPointInTime).not.toHaveBeenCalled();
  });
});
