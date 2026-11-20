import { describe, expect, it, vi } from "vitest";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import {
  createElasticsearchPitMock,
  deferred,
  elasticsearchResponseError,
  required,
} from "../support/elasticsearchPitMock";

const request = {
  database: "default",
  schema: "indices",
  table: "records",
  page: 1,
  pageSize: 2,
  filters: [],
  sort: null,
  skipCount: false,
} satisfies DriverTablePageRequest;

function fixture() {
  const mock = createElasticsearchPitMock(6);
  const ids = mock.documents.map((hit) => hit._id);
  const changeShardOrder = () => {
    // Another PIT can see the same documents with different Lucene doc IDs.
    mock.documents.unshift(...mock.documents.splice(-2));
    mock.documents.forEach((hit, index) => {
      hit.sort = [index + 100];
    });
  };
  return { ...mock, ids, changeShardOrder };
}

describe("Elasticsearch browsing snapshot", () => {
  it.each([
    false,
    true,
  ])("keeps adjacent and arbitrary pages in one PIT (materialized ties: %s)", async (materialized) => {
    const { driver, service, client, ids, snapshots, changeShardOrder } =
      fixture();
    const scope = new AbortController();
    const read = async (page: number) =>
      (
        await service.getPage(
          "es-pit",
          request.database,
          request.schema,
          request.table,
          page,
          request.pageSize,
          materialized
            ? [{ column: "_id", operator: "like", value: "doc-" }]
            : [],
          materialized ? { column: "_identity", direction: "asc" } : null,
          false,
          undefined,
          undefined,
          false,
          false,
          scope.signal,
        )
      ).rows.map((row) => row._id);
    try {
      expect(await read(1)).toEqual(ids.slice(0, 2));
      changeShardOrder();
      expect(await read(2)).toEqual(ids.slice(2, 4));
      expect(await read(4)).toEqual([]);
      expect(await read(3)).toEqual(ids.slice(4, 6));
      expect(await read(1)).toEqual(ids.slice(0, 2));
      expect(client.openPointInTime).toHaveBeenCalledTimes(1);
      expect(client.closePointInTime).not.toHaveBeenCalled();
      expect(snapshots.size).toBe(1);
    } finally {
      scope.abort();
      await driver.disconnect();
    }
    expect(client.closePointInTime).toHaveBeenCalledTimes(1);
    expect(snapshots.size).toBe(0);
  });

  it("opens independent snapshots for refresh and export, and closes the latest IDs", async () => {
    const { driver, client, ids, snapshots, changeShardOrder, documents } =
      fixture();
    const scope = new AbortController();
    const refreshed = new AbortController();
    try {
      await driver.readTablePage({
        ...request,
        browsingSnapshot: scope.signal,
      });
      const firstPit = required(client.search.mock.calls[0][0].pit).id;
      const latestId = required([...snapshots.keys()][0]);
      expect(latestId).not.toBe(firstPit);
      changeShardOrder();
      documents[0]._source.ordinal = 999;

      const exported = [];
      for await (const batch of driver.exportTableChunks({
        ...request,
        chunkSize: 3,
      })) {
        exported.push(...batch.rows);
      }
      expect(exported.map((row) => row._id)).toEqual(
        documents.map((hit) => hit._id),
      );
      expect(exported[0]._source).toBe('{"ordinal":999}');
      expect([...snapshots.keys()]).toEqual([latestId]);
      const secondPage = await driver.readTablePage({
        ...request,
        page: 2,
        browsingSnapshot: scope.signal,
      });
      expect(secondPage.rows.map((row) => row._id)).toEqual(ids.slice(2, 4));
      const closeId = required([...snapshots.keys()][0]);
      scope.abort();
      await vi.waitFor(() => expect(snapshots.size).toBe(0));
      expect(client.closePointInTime).toHaveBeenLastCalledWith(
        { id: closeId },
        expect.anything(),
      );
      const freshPage = await driver.readTablePage({
        ...request,
        browsingSnapshot: refreshed.signal,
      });
      expect(freshPage.rows.map((row) => row._id)).toEqual(ids.slice(4));
      expect(freshPage.rows[0]._source).toBe('{"ordinal":999}');
      expect(client.openPointInTime).toHaveBeenCalledTimes(3);
    } finally {
      await driver.disconnect();
    }
    expect(snapshots.size).toBe(0);
  });

  it("serializes overlapping pages, and cancelling a queued read does not close the active PIT", async () => {
    const { driver, client, ids, snapshots, changeShardOrder } = fixture();
    const scope = new AbortController();
    const queued = new AbortController();
    const gate = deferred<void>();
    const started = deferred<void>();
    const search = required(client.search.getMockImplementation());
    client.search.mockImplementationOnce(async (...args) => {
      started.resolve();
      await gate.promise;
      return search(...args);
    });
    const read = (page: number, signal?: AbortSignal) =>
      driver.readTablePage({
        ...request,
        page,
        browsingSnapshot: scope.signal,
        signal,
      });
    try {
      const first = read(1);
      await started.promise;
      changeShardOrder();
      const second = read(2);
      const cancelled = expect(read(3, queued.signal)).rejects.toMatchObject({
        name: "AbortError",
      });
      queued.abort();
      await cancelled;
      expect(client.search).toHaveBeenCalledTimes(1);
      gate.resolve();
      expect((await first).rows.map((row) => row._id)).toEqual(ids.slice(0, 2));
      expect((await second).rows.map((row) => row._id)).toEqual(
        ids.slice(2, 4),
      );
      expect(client.openPointInTime).toHaveBeenCalledTimes(1);
      expect(snapshots.size).toBe(1);
    } finally {
      gate.resolve();
      await driver.disconnect();
    }
    expect(snapshots.size).toBe(0);
  });

  it.each([
    "cancel",
    "server expiry",
    "bad reply",
  ] as const)("closes a failed browsing PIT on %s and never silently reopens it", async (kind) => {
    const { driver, client, snapshots } = fixture();
    const scope = new AbortController();
    const requestCancel = new AbortController();
    const read = (page: number) =>
      driver.readTablePage({
        ...request,
        page,
        browsingSnapshot: scope.signal,
        signal: requestCancel.signal,
      });
    try {
      await read(1);
      const started = deferred<void>();
      if (kind === "cancel") {
        client.search.mockImplementationOnce(() => {
          started.resolve();
          return new Promise(() => undefined);
        });
      } else if (kind === "server expiry") {
        client.search.mockRejectedValueOnce(
          elasticsearchResponseError(404, "Missing search context"),
        );
      } else {
        const search = required(client.search.getMockImplementation());
        client.search.mockImplementationOnce(async (...args) => ({
          ...(await search(...args)),
          timed_out: true,
        }));
      }
      const next = read(2);
      const rejected =
        kind === "cancel"
          ? expect(next).rejects.toMatchObject({ name: "AbortError" })
          : expect(next).rejects.toThrow(
              kind === "server expiry" ? /Refresh the table/ : /incomplete/,
            );
      if (kind === "cancel") {
        await started.promise;
        requestCancel.abort();
      }
      await rejected;
      await vi.waitFor(() => expect(snapshots.size).toBe(0));
      await expect(
        driver.readTablePage({
          ...request,
          page: 2,
          browsingSnapshot: scope.signal,
        }),
      ).rejects.toThrow(/Refresh the table/);
      expect(client.openPointInTime).toHaveBeenCalledTimes(1);
    } finally {
      await driver.disconnect();
    }
  });

  it.each([
    { reads: 0, wait: 45_000 },
    { reads: 7, wait: 20_000 },
  ])("bounds PIT idle and absolute lifetime ($reads intervening reads)", async ({
    reads,
    wait,
  }) => {
    vi.useFakeTimers();
    const { driver, client, snapshots } = fixture();
    const scope = new AbortController();
    const read = () =>
      driver.readTablePage({ ...request, browsingSnapshot: scope.signal });
    try {
      await read();
      for (let i = 0; i < reads; i++) {
        await vi.advanceTimersByTimeAsync(40_000);
        await read();
      }
      await vi.advanceTimersByTimeAsync(wait - 1);
      expect(snapshots.size).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(snapshots.size).toBe(0);
      await expect(read()).rejects.toThrow(/Refresh the table/);
      expect(client.openPointInTime).toHaveBeenCalledTimes(1);
    } finally {
      await driver.disconnect();
      vi.useRealTimers();
    }
  });

  it("evicts old viewing scopes rather than retaining unbounded PITs", async () => {
    const { driver, client, snapshots } = fixture();
    const scopes = Array.from({ length: 9 }, () => new AbortController());
    try {
      for (const scope of scopes)
        await driver.readTablePage({
          ...request,
          browsingSnapshot: scope.signal,
        });
      expect(snapshots.size).toBe(8);
      await expect(
        driver.readTablePage({
          ...request,
          browsingSnapshot: scopes[0].signal,
        }),
      ).rejects.toThrow(/Refresh the table/);
      expect(client.openPointInTime).toHaveBeenCalledTimes(9);
    } finally {
      await driver.disconnect();
    }
    expect(snapshots.size).toBe(0);
  });
});
