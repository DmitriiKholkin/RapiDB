import { describe, expect, it, vi } from "vitest";
import { createElasticsearchHttpFixture } from "../support/elasticsearchHttpFixture";

const request = {
  database: "default",
  schema: "indices",
  table: "records",
  chunkSize: 500,
};

describe("B11 SDK 9 HTTP regression fixture", () => {
  it("observes the real SDK RequestAbortedError from a cancelled HTTP request", async () => {
    const fixture = await createElasticsearchHttpFixture({ stallSearch: true });
    try {
      const controller = new AbortController();
      const search = fixture.client.search(
        { query: { match_all: {} } },
        { signal: controller.signal, requestTimeout: 1000 },
      );
      const rejected = expect(search).rejects.toMatchObject({
        name: "RequestAbortedError",
      });
      await fixture.searchStarted.promise;
      controller.abort();
      await rejected;
    } finally {
      await fixture.close();
    }
  });

  it("normalizes real SDK HTTP cancellation and closes the export PIT", async () => {
    const fixture = await createElasticsearchHttpFixture({ stallSearch: true });
    try {
      const controller = new AbortController();
      const chunks = fixture.driver.exportTableChunks(
        request,
        controller.signal,
      );
      const rejected = expect(chunks.next()).rejects.toMatchObject({
        name: "AbortError",
      });
      await fixture.searchStarted.promise;
      controller.abort();
      await rejected;
      await vi.waitFor(() => expect(fixture.closedIds).toEqual(["pit-1"]));
      expect(fixture.pits.size).toBe(0);
    } finally {
      await fixture.close();
    }
  });

  it("exports with a real HTTP metadata 403 and an unknown custom window", async () => {
    const fixture = await createElasticsearchHttpFixture({
      denySettings: true,
      window: 3,
    });
    try {
      const ids: unknown[] = [];
      for await (const chunk of fixture.driver.exportTableChunks(request))
        ids.push(...chunk.rows.map((row) => row._id));
      expect(ids).toEqual(
        Array.from({ length: 17 }, (_, index) => `doc-${index}`),
      );
      expect(fixture.searches[0].size).toBe(500);
      expect(fixture.searches.at(-1)?.size).toBeLessThanOrEqual(3);
      expect(fixture.pits.size).toBe(0);
    } finally {
      await fixture.close();
    }
  });
});

const materializedCases = (["sorted", "filtered"] as const).flatMap((mode) =>
  (["table", "page-export", "full-export"] as const).flatMap((scope) =>
    (["abort", "deadline"] as const).map((reason) => ({ mode, scope, reason })),
  ),
);

function materializedOptions(mode: "sorted" | "filtered") {
  return {
    sort:
      mode === "sorted" ? { column: "_id", direction: "asc" as const } : null,
    filters:
      mode === "filtered"
        ? [{ column: "_id", operator: "like" as const, value: "%doc%" }]
        : [],
  };
}

describe("B11 materialized reads over real SDK HTTP", () => {
  it.each(
    materializedCases,
  )("$mode $scope stops logical and HTTP work on $reason", async ({
    mode,
    scope,
    reason,
  }) => {
    const fixture = await createElasticsearchHttpFixture({
      stallSearch: true,
      window: 10000,
    });
    try {
      const controller = new AbortController();
      const search = vi.spyOn(fixture.client, "search");
      const { sort, filters } = materializedOptions(mode);
      const operation =
        scope === "full-export"
          ? fixture.service
              .exportAll(
                "es-http",
                "default",
                "indices",
                "records",
                500,
                sort,
                filters,
                controller.signal,
              )
              .next()
          : scope === "page-export"
            ? fixture.service.getPage(
                "es-http",
                "default",
                "indices",
                "records",
                1,
                25,
                filters,
                sort,
                true,
                controller.signal,
              )
            : fixture.wrappedDriver.readTablePage({
                ...request,
                page: 1,
                pageSize: 25,
                sort,
                filters,
                skipCount: false,
                signal: controller.signal,
              });
      const rejected = expect(operation).rejects.toMatchObject({
        name: reason === "abort" ? "AbortError" : "DriverTimeoutError",
      });
      await fixture.searchStarted.promise;
      expect(fixture.pendingSearches.size).toBe(1);
      expect(fixture.searches[0]).toMatchObject({
        query: { match_all: {} },
        sort: ["_doc"],
        size: 10000,
        track_total_hits: true,
      });
      if (reason === "abort") controller.abort();
      await rejected;
      await vi.waitFor(() => {
        expect(fixture.pendingSearches.size).toBe(0);
        expect(fixture.activeReaders).toBe(0);
      });
      const options = search.mock.calls[0][1];
      expect(options?.requestTimeout).toBeGreaterThan(0);
      expect(options?.requestTimeout).toBeLessThanOrEqual(1000);
      expect(options?.signal?.aborted).toBe(true);
      expect(options?.maxRetries).toBe(0);
      fixture.releaseSearches();
      expect(search).toHaveBeenCalledTimes(1);
      expect(fixture.requests).toHaveLength(1);
      expect(fixture.pits.size).toBe(0);
      expect(fixture.closedIds).toEqual([]);
    } finally {
      await fixture.close();
    }
  });

  it.each([
    "sorted",
    "filtered",
  ] as const)("makes no HTTP request for a pre-aborted or expired %s read", async (mode) => {
    const fixture = await createElasticsearchHttpFixture({ stallSearch: true });
    try {
      const controller = new AbortController();
      controller.abort();
      const page = {
        ...request,
        ...materializedOptions(mode),
        page: 1,
        pageSize: 25,
        skipCount: false,
      };
      await expect(
        fixture.driver.readTablePage({ ...page, signal: controller.signal }),
      ).rejects.toMatchObject({ name: "AbortError" });
      await expect(
        fixture.driver.readTablePage({ ...page, deadline: Date.now() - 1 }),
      ).rejects.toMatchObject({ name: "DriverTimeoutError" });
      await vi.waitFor(() => expect(fixture.activeReaders).toBe(0));
      expect(fixture.requests).toEqual([]);
    } finally {
      await fixture.close();
    }
  });

  it.each([
    "abort-first",
    "response-first",
  ])("discards a materialized HTTP response racing cancellation (%s)", async (order) => {
    const fixture = await createElasticsearchHttpFixture({
      stallSearch: true,
      window: 10000,
    });
    try {
      const controller = new AbortController();
      const operation = fixture.driver.readTablePage({
        ...request,
        ...materializedOptions("sorted"),
        page: 1,
        pageSize: 25,
        skipCount: false,
        signal: controller.signal,
      });
      const rejected = expect(operation).rejects.toMatchObject({
        name: "AbortError",
      });
      await fixture.searchStarted.promise;
      if (order === "response-first") fixture.releaseSearches();
      controller.abort();
      if (order === "abort-first") fixture.releaseSearches();
      await rejected;
      await vi.waitFor(() => {
        expect(fixture.pendingSearches.size).toBe(0);
        expect(fixture.activeReaders).toBe(0);
      });
      expect(fixture.searches).toHaveLength(1);
      expect(fixture.pits.size).toBe(0);
    } finally {
      await fixture.close();
    }
  });

  it("surfaces an unsafe materialization window error without a retry", async () => {
    const fixture = await createElasticsearchHttpFixture({ window: 3 });
    try {
      await expect(
        fixture.driver.readTablePage({
          ...request,
          ...materializedOptions("filtered"),
          page: 1,
          pageSize: 25,
          skipCount: false,
        }),
      ).rejects.toMatchObject({
        name: "ResponseError",
        meta: { statusCode: 400 },
      });
      expect(fixture.searches).toHaveLength(1);
      expect(fixture.requests).toHaveLength(1);
      expect(fixture.activeReaders).toBe(0);
    } finally {
      await fixture.close();
    }
  });

  it("does not publish a successful materialization if cancellation wins during reader cleanup", async () => {
    const fixture = await createElasticsearchHttpFixture({ window: 10000 });
    try {
      const controller = new AbortController();
      const readers = (
        fixture.driver as unknown as { pitReaders: Set<unknown> }
      ).pitReaders;
      const remove = readers.delete.bind(readers);
      vi.spyOn(readers, "delete").mockImplementation((reader) => {
        controller.abort();
        return remove(reader);
      });
      await expect(
        fixture.driver.readTablePage({
          ...request,
          ...materializedOptions("sorted"),
          page: 1,
          pageSize: 25,
          skipCount: false,
          signal: controller.signal,
        }),
      ).rejects.toMatchObject({ name: "AbortError" });
      expect(fixture.activeReaders).toBe(0);
      expect(fixture.searches).toHaveLength(1);
    } finally {
      await fixture.close();
    }
  });
});
