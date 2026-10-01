import { createServer, type ServerResponse } from "node:http";
import { Client } from "@elastic/elasticsearch";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import {
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
} from "../../src/extension/dbDrivers/timeout";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { deferred } from "./elasticsearchPitMock";

/** Real SDK/HTTP requests, without a live Elasticsearch installation. */
export async function createElasticsearchHttpFixture(options?: {
  denySettings?: boolean;
  stallSearch?: boolean;
  window?: number;
}) {
  const window = options?.window ?? 3;
  const searchStarted = deferred<void>();
  const closedIds: string[] = [];
  const requests: string[] = [];
  const pendingSearches = new Set<ServerResponse>();
  const heldSearches: Array<() => void> = [];
  const searches: Array<{
    pit?: { id: string };
    size?: number;
    search_after?: number[];
    query?: unknown;
    sort?: unknown;
    track_total_hits?: boolean;
  }> = [];
  const pits = new Set<string>();
  let serial = 0;
  const server = createServer(async (request, response) => {
    requests.push(request.url ?? "");
    response.setHeader("x-elastic-product", "Elasticsearch");
    response.setHeader("content-type", "application/json");
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const body = chunks.length
      ? JSON.parse(Buffer.concat(chunks).toString())
      : {};
    const send = (value: unknown, status = 200) => {
      response.statusCode = status;
      response.end(JSON.stringify(value));
    };
    if (request.url?.includes("/_settings/")) {
      if (options?.denySettings) {
        send(
          {
            error: {
              type: "security_exception",
              reason: "missing view_index_metadata privilege",
            },
            status: 403,
          },
          403,
        );
      } else {
        send({
          records: {
            settings: { index: { max_result_window: String(window) } },
          },
        });
      }
    } else if (
      request.method === "POST" &&
      request.url?.startsWith("/records/_pit")
    ) {
      const id = `pit-${++serial}`;
      pits.add(id);
      send({ id });
    } else if (
      request.method === "DELETE" &&
      request.url?.startsWith("/_pit")
    ) {
      closedIds.push(body.id);
      pits.delete(body.id);
      send({ succeeded: true, num_freed: 1 });
    } else if (
      request.url?.startsWith("/_search") ||
      request.url?.startsWith("/records/_search")
    ) {
      searches.push(body);
      pendingSearches.add(response);
      response.on("close", () => pendingSearches.delete(response));
      searchStarted.resolve();
      const finishSearch = () => {
        if (response.destroyed) return;
        if (body.size > window) {
          send(
            {
              error: {
                type: "illegal_argument_exception",
                reason:
                  "Result window is too large, from + size must be less than or equal to the configured window",
              },
              status: 400,
            },
            400,
          );
          return;
        }
        const cursor = body.search_after?.[0] ?? -1;
        const hits = Array.from({ length: 17 }, (_, ordinal) => ({
          _id: `doc-${ordinal}`,
          _source: { ordinal },
          sort: [ordinal],
        }))
          .filter((hit) => hit.sort[0] > cursor)
          .slice(0, body.size ?? 1);
        let pit_id: string | undefined;
        if (body.pit) {
          pits.delete(body.pit.id);
          pit_id = `pit-${++serial}`;
          pits.add(pit_id);
        }
        send({
          pit_id,
          timed_out: false,
          _shards: { total: 1, successful: 1, failed: 0 },
          hits: { total: { value: 17, relation: "eq" }, hits },
        });
      };
      if (options?.stallSearch) heldSearches.push(finishSearch);
      else finishSearch();
    } else {
      send({ error: "unexpected request" }, 404);
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("Missing fixture port");
  const client = new Client({
    node: `http://127.0.0.1:${address.port}`,
    maxRetries: 0,
  });
  const timeoutSettings = () =>
    createDriverTimeoutSettingsSnapshot({ dbOperationTimeoutSeconds: 1 });
  const driver = new ElasticsearchDriver(
    { id: "es-http", name: "ES HTTP", type: "elasticsearch" },
    timeoutSettings,
  );
  Object.assign(driver, { client, connected: true });
  const wrappedDriver = createTimeoutAwareDriver(driver, timeoutSettings);
  const service = new TableReadService({
    getConnection: () => ({ id: "es-http" }),
    getDriver: () => wrappedDriver,
  } as unknown as ConnectionManager);
  return {
    client,
    driver,
    wrappedDriver,
    service,
    searchStarted,
    searches,
    pits,
    closedIds,
    requests,
    pendingSearches,
    get activeReaders() {
      return (driver as unknown as { pitReaders: Set<unknown> }).pitReaders
        .size;
    },
    releaseSearches() {
      for (const release of heldSearches.splice(0)) release();
    },
    async close() {
      await driver.disconnect();
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
