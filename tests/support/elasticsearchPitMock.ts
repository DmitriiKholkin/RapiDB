import { errors } from "@elastic/elasticsearch";
import { vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import type { DriverTimeoutSettingsProvider } from "../../src/extension/dbDrivers/timeout";
import { TableReadService } from "../../src/extension/table/tableReadService";

type Hit = {
  _index: string;
  _id: string;
  _source: { ordinal: number };
  sort: number[];
};

export function createElasticsearchPitMock(
  count: number,
  window = 10000,
  timeoutSettingsProvider?: DriverTimeoutSettingsProvider,
) {
  const documents: Hit[] = Array.from({ length: count }, (_, ordinal) => ({
    _index: "records",
    _id: `doc-${ordinal}`,
    _source: { ordinal },
    // Real PIT shard-doc values include the shard ordinal in the high bits.
    sort: [Math.floor(ordinal / 2) + (ordinal % 2) * 2 ** 32],
  })).sort((a, b) => a.sort[0] - b.sort[0]);
  const snapshots = new Map<string, Hit[]>();
  let serial = 0;
  const openPointInTime = vi.fn(
    async (
      _request: unknown,
      _options?: { signal?: AbortSignal; requestTimeout?: number },
    ) => {
      const id = `pit-${++serial}`;
      snapshots.set(id, structuredClone(documents));
      return { id };
    },
  );
  const search = vi.fn(
    async (
      request: {
        index?: string;
        pit?: { id: string; keep_alive: string };
        from?: number;
        size?: number;
        search_after?: number[];
        sort?: unknown;
        track_total_hits?: boolean;
      },
      options?: { signal?: AbortSignal; requestTimeout?: number },
    ) => {
      options?.signal?.throwIfAborted();
      const size = request.size ?? 10;
      if ((request.from ?? 0) + size > window) {
        throw elasticsearchResponseError(400, "Result window is too large");
      }
      const snapshot = request.pit ? snapshots.get(request.pit.id) : documents;
      if (!snapshot) throw new Error("Unknown or stale PIT ID");
      if (request.pit && (request.index || request.from !== undefined)) {
        throw new Error("PIT searches must not specify index/from");
      }
      const after = request.search_after?.[0] ?? -1;
      const hits = snapshot
        .filter((hit) => hit.sort[0] > after)
        .slice(request.from ?? 0, (request.from ?? 0) + size);
      let pit_id: string | undefined;
      if (request.pit) {
        snapshots.delete(request.pit.id);
        pit_id = `pit-${++serial}`;
        snapshots.set(pit_id, snapshot);
      }
      return {
        pit_id,
        timed_out: false,
        _shards: { total: 2, successful: 2, failed: 0 },
        hits: {
          total: { value: snapshot.length, relation: "eq" },
          hits: structuredClone(hits),
        },
      };
    },
  );
  const closePointInTime = vi.fn(
    async (
      { id }: { id: string },
      _options?: { signal?: AbortSignal; requestTimeout?: number },
    ) => {
      const succeeded = snapshots.delete(id);
      return { succeeded, num_freed: succeeded ? 2 : 0 };
    },
  );
  const close = vi.fn(async () => undefined);
  const getSettings = vi.fn(
    async (
      _request?: unknown,
      _options?: { signal?: AbortSignal; requestTimeout?: number },
    ) => ({
      records: { settings: { index: { max_result_window: String(window) } } },
    }),
  );
  const client = {
    openPointInTime,
    search,
    closePointInTime,
    close,
    indices: { getSettings },
  };
  const driver = new ElasticsearchDriver(
    {
      id: "es-pit",
      name: "ES PIT",
      type: "elasticsearch",
    },
    timeoutSettingsProvider,
  );
  Object.assign(driver, { connected: true, client });
  const manager = {
    getConnection: () => ({ id: "es-pit" }),
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  const service = new TableReadService(manager);
  return { driver, service, client, documents, snapshots };
}

export function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

export function required<T>(value: T | null | undefined): T {
  if (value == null) throw new Error("Missing required mock value");
  return value;
}

export function elasticsearchResponseError(statusCode: number, reason: string) {
  return new errors.ResponseError({
    statusCode,
    body: {
      error: {
        type: "illegal_argument_exception",
        reason,
        root_cause: [{ type: "illegal_argument_exception", reason }],
      },
    },
    headers: {},
    meta: {} as never,
    warnings: null,
  });
}
