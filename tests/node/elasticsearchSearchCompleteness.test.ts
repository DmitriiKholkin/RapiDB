import { describe, expect, it } from "vitest";
import { createElasticsearchPitMock } from "../support/elasticsearchPitMock";

describe("Elasticsearch search completeness", () => {
  it.each(
    (["timeout", "shards"] as const).flatMap((failure) =>
      (["sorted", "filtered", "index-query", "global-query"] as const).map(
        (scope) => ({ failure, scope }),
      ),
    ),
  )("rejects $failure partial results in $scope", async ({
    failure,
    scope,
  }) => {
    const { driver, client } = createElasticsearchPitMock(1);
    client.search.mockResolvedValueOnce({
      pit_id: undefined,
      timed_out: failure === "timeout",
      _shards: {
        total: 2,
        successful: 1,
        failed: failure === "shards" ? 1 : 0,
      },
      hits: {
        total: { value: 1, relation: "eq" },
        hits: [
          { _index: "records", _id: "one", _source: { ordinal: 1 }, sort: [1] },
        ],
      },
    });
    try {
      const operation = scope.endsWith("query")
        ? driver.query(
            scope === "index-query" ? "GET /records/_search" : "GET /_search",
          )
        : driver.readTablePage({
            database: "default",
            schema: "indices",
            table: "records",
            page: 1,
            pageSize: 25,
            sort:
              scope === "sorted" ? { column: "_id", direction: "asc" } : null,
            filters:
              scope === "filtered"
                ? [{ column: "_id", operator: "eq", value: "one" }]
                : [],
            skipCount: false,
          });
      await expect(operation).rejects.toThrow(/incomplete results/i);
      expect(
        (driver as unknown as { pitReaders: Set<unknown> }).pitReaders.size,
      ).toBe(0);
      expect(client.search).toHaveBeenCalledOnce();
    } finally {
      await driver.disconnect();
    }
  });
});
