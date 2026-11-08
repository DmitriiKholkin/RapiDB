import { afterEach, describe, expect, it, vi } from "vitest";
import { createDriverTimeoutSettingsSnapshot } from "../../src/extension/dbDrivers/timeout";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import { ELASTICSEARCH_READ_BUDGET } from "../../src/shared/safetyContracts";
import {
  createElasticsearchPitMock,
  deferred,
  elasticsearchResponseError,
  required,
} from "../support/elasticsearchPitMock";

const request: DriverTablePageRequest = {
  database: "default",
  schema: "indices",
  table: "records",
  page: 2,
  pageSize: 2,
  filters: [],
  sort: { column: "_id", direction: "desc" },
  skipCount: false,
};

describe("Elasticsearch filtered/sorted full-index PIT materialization", () => {
  afterEach(() => vi.useRealTimers());

  it.each([
    1, 3, 7,
  ])("applies exact filters, sort and pagination only after all window-%i batches", async (window) => {
    const { driver, client, documents, snapshots } = createElasticsearchPitMock(
      17,
      window,
    );
    const expectedIds = ["doc-1", "doc-9", "doc-16"];
    const result = await driver.readTablePage({
      ...request,
      filters: [
        { column: "_id", operator: "in", value: expectedIds.join(",") },
      ],
    });
    expect(result.rows.map((row) => row._id)).toEqual(["doc-1"]);
    expect(result.rows[0]._source).toBe(
      JSON.stringify(documents.find((hit) => hit._id === "doc-1")?._source),
    );
    expect(result.totalCount).toBe(3);
    expect(client.search).toHaveBeenCalledTimes(Math.floor(17 / window) + 1);
    for (const [args] of client.search.mock.calls) {
      expect(args).toMatchObject({
        size: window,
        sort: [{ _shard_doc: "asc" }],
      });
      expect(args).not.toHaveProperty("index");
      expect(args).not.toHaveProperty("from");
    }
    expect(client.search.mock.calls[0][0].track_total_hits).toBe(true);
    expect(
      client.search.mock.calls
        .slice(1)
        .every(([args]) => args.track_total_hits === false),
    ).toBe(true);
    expect(snapshots.size).toBe(0);
    expect(client.closePointInTime).toHaveBeenCalledOnce();
  });

  it("keeps filter-only snapshot order and skipCount without skipping the full-index safety check", async () => {
    const { driver, documents } = createElasticsearchPitMock(17, 3);
    const result = await driver.readTablePage({
      ...request,
      page: 1,
      pageSize: 20,
      sort: null,
      skipCount: true,
      filters: [{ column: "_id", operator: "in", value: "doc-1,doc-8,doc-16" }],
    });
    expect(result.rows.map((row) => row._id)).toEqual(
      documents
        .filter((hit) => ["doc-1", "doc-8", "doc-16"].includes(hit._id))
        .map((hit) => hit._id),
    );
    expect(result.totalCount).toBe(0);
  });

  it.each([
    9999, 10000,
  ])("sorts the entire %i-row snapshot below/at the cap with a small window", async (count) => {
    const { driver, documents, snapshots } = createElasticsearchPitMock(
      count,
      125,
    );
    const result = await driver.readTablePage({ ...request, page: 1 });
    expect(result.rows.map((row) => row._id)).toEqual(
      documents
        .map((hit) => hit._id)
        .sort((a, b) => b.localeCompare(a))
        .slice(0, 2),
    );
    expect(result.totalCount).toBe(count);
    expect(snapshots.size).toBe(0);
  });

  it.each([
    false,
    true,
  ])("rejects an over-cap full index before applying a narrow filter (skipCount: %s)", async (skipCount) => {
    const { driver, client, snapshots } = createElasticsearchPitMock(
      ELASTICSEARCH_READ_BUDGET.hardCap + 1,
      3,
    );
    await expect(
      driver.readTablePage({
        ...request,
        sort: null,
        skipCount,
        filters: [{ column: "_id", operator: "eq", value: "doc-1" }],
      }),
    ).rejects.toThrow("10000-row safety limit");
    expect(client.search).toHaveBeenCalledOnce();
    expect(client.closePointInTime).toHaveBeenCalledOnce();
    expect(snapshots.size).toBe(0);
  });

  it("rejects a truncated later batch rather than returning a partial filtered page", async () => {
    const { driver, client, snapshots } = createElasticsearchPitMock(17, 3);
    const search = required(client.search.getMockImplementation());
    client.search.mockImplementation(async (...args) => {
      const result = await search(...args);
      if (args[0].search_after) result.hits.hits.length = 1;
      return result;
    });
    await expect(driver.readTablePage(request)).rejects.toThrow(
      "hit count mismatch",
    );
    expect(client.search).toHaveBeenCalledTimes(2);
    expect(snapshots.size).toBe(0);
  });

  it("also enforces the accumulated row cap if a server underreports the total", async () => {
    const { driver, client, snapshots } = createElasticsearchPitMock(
      10001,
      125,
    );
    const search = required(client.search.getMockImplementation());
    client.search.mockImplementationOnce(async (...args) => {
      const result = await search(...args);
      result.hits.total.value = 10000;
      return result;
    });
    await expect(driver.readTablePage(request)).rejects.toThrow(
      "10000-row safety limit",
    );
    expect(client.search).toHaveBeenCalledTimes(81);
    expect(snapshots.size).toBe(0);
  });

  it("preserves a later search error when closing the PIT also fails", async () => {
    const { driver, client } = createElasticsearchPitMock(17, 3);
    const search = required(client.search.getMockImplementation());
    client.search.mockImplementation(async (...args) => {
      if (args[0].search_after) throw new Error("search failed");
      return search(...args);
    });
    client.closePointInTime.mockRejectedValueOnce(new Error("cleanup failed"));
    await expect(driver.readTablePage(request)).rejects.toThrow(
      "search failed",
    );
    expect(client.closePointInTime).toHaveBeenCalledOnce();
  });

  it("rejects a missing or lower-bound total instead of accepting an unverified full index", async () => {
    for (const total of [undefined, { value: 17, relation: "gte" }]) {
      const { driver, client, snapshots } = createElasticsearchPitMock(17, 3);
      const search = required(client.search.getMockImplementation());
      client.search.mockImplementationOnce(async (...args) => {
        const result = await search(...args);
        Object.assign(result.hits, { total });
        return result;
      });
      await expect(driver.readTablePage(request)).rejects.toThrow(
        "exact total hit count",
      );
      expect(snapshots.size).toBe(0);
    }
  });

  it.each([
    "timeout",
    "shards",
    "cursor",
    "search",
  ])("rejects a later %s error and closes the rotated PIT", async (failure) => {
    const { driver, client, snapshots } = createElasticsearchPitMock(17, 3);
    const search = required(client.search.getMockImplementation());
    client.search.mockImplementation(async (...args) => {
      if (failure === "search" && args[0].search_after)
        throw new Error("search failed");
      const result = await search(...args);
      if (args[0].search_after) {
        if (failure === "timeout") result.timed_out = true;
        if (failure === "shards") result._shards.failed = 1;
        if (failure === "cursor")
          result.hits.hits[0].sort = required(args[0].search_after);
      }
      return result;
    });
    await expect(driver.readTablePage(request)).rejects.toThrow(
      failure === "cursor"
        ? "made no progress"
        : failure === "search"
          ? "search failed"
          : "incomplete results",
    );
    expect(client.search).toHaveBeenCalledTimes(2);
    expect(snapshots.size).toBe(0);
  });

  it("does not swallow non-window search errors when metadata is forbidden", async () => {
    const { driver, client, snapshots } = createElasticsearchPitMock(17, 3);
    client.indices.getSettings.mockRejectedValueOnce(
      elasticsearchResponseError(403, "forbidden"),
    );
    client.search.mockRejectedValueOnce(
      elasticsearchResponseError(400, "invalid query"),
    );
    await expect(driver.readTablePage(request)).rejects.toThrow(
      "invalid query",
    );
    expect(client.search).toHaveBeenCalledOnce();
    expect(snapshots.size).toBe(0);
  });

  it("propagates cleanup failure instead of publishing a successful materialization", async () => {
    const { driver, client } = createElasticsearchPitMock(17, 3);
    client.closePointInTime.mockRejectedValueOnce(new Error("cleanup failed"));
    await expect(driver.readTablePage(request)).rejects.toThrow(
      "cleanup failed",
    );
  });

  it.each([
    undefined,
    Infinity,
  ])("uses a single operation timeout across all chunks (request deadline: %s)", async (deadline) => {
    vi.useFakeTimers();
    const { driver, client, snapshots } = createElasticsearchPitMock(
      17,
      3,
      () =>
        createDriverTimeoutSettingsSnapshot({ dbOperationTimeoutSeconds: 1 }),
    );
    const search = required(client.search.getMockImplementation());
    client.search.mockImplementation(async (...args) => {
      await new Promise((resolve) => setTimeout(resolve, 600));
      return search(...args);
    });
    const rejected = expect(
      driver.readTablePage({ ...request, deadline }),
    ).rejects.toMatchObject({ name: "DriverTimeoutError" });
    await vi.advanceTimersByTimeAsync(1300);
    await rejected;
    expect(client.search).toHaveBeenCalledTimes(2);
    expect(snapshots.size).toBe(0);
  });

  it("cancels a later chunk and cleans a PIT ID returned after cancellation", async () => {
    vi.useFakeTimers();
    const { driver, client, snapshots } = createElasticsearchPitMock(17, 3);
    const search = required(client.search.getMockImplementation());
    const late = deferred<Awaited<ReturnType<typeof client.search>>>();
    let response: Awaited<ReturnType<typeof client.search>> | undefined;
    client.search.mockImplementation(async (...args) => {
      const result = await search(...args);
      if (!args[0].search_after) return result;
      response = result;
      return late.promise;
    });
    const controller = new AbortController();
    const rejected = expect(
      driver.readTablePage({ ...request, signal: controller.signal }),
    ).rejects.toMatchObject({ name: "AbortError" });
    await vi.advanceTimersByTimeAsync(0);
    expect(client.search).toHaveBeenCalledTimes(2);
    controller.abort();
    late.resolve(required(response));
    await vi.advanceTimersByTimeAsync(100);
    await rejected;
    expect(snapshots.size).toBe(0);
    expect(client.search).toHaveBeenCalledTimes(2);
  });
});
