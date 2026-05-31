import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { ELASTICSEARCH_READ_BUDGET } from "../../src/shared/safetyContracts";

const elasticDatetimeColumn: ColumnTypeMeta = {
  name: "created_at",
  type: "date",
  nativeType: "date",
  category: "datetime",
  nullable: true,
  defaultValue: undefined,
  isPrimaryKey: false,
  primaryKeyOrdinal: undefined,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

function createDriver() {
  const driver = new ElasticsearchDriver({
    id: "elasticsearch-data-test",
    name: "Elasticsearch Data Test",
    type: "elasticsearch",
    host: "localhost",
  });

  const search = vi.fn();
  const getMapping = vi.fn();
  const resolveIndex = vi.fn();
  const createIndex = vi.fn();
  const update = vi.fn().mockResolvedValue({ result: "updated" });
  const remove = vi.fn().mockResolvedValue({ result: "deleted" });
  const get = vi.fn();
  const index = vi.fn().mockResolvedValue({ result: "created", _id: "doc-3" });

  (
    driver as unknown as {
      client: {
        get: typeof get;
        search: typeof search;
        update: typeof update;
        delete: typeof remove;
        index: typeof index;
        indices: {
          resolveIndex: typeof resolveIndex;
          getMapping: typeof getMapping;
          create: typeof createIndex;
          get: typeof get;
          delete: ReturnType<typeof vi.fn>;
          putMapping: ReturnType<typeof vi.fn>;
          putSettings: ReturnType<typeof vi.fn>;
          updateAliases: ReturnType<typeof vi.fn>;
        };
      } | null;
      connected: boolean;
    }
  ).client = {
    get,
    search,
    update,
    delete: remove,
    index,
    indices: {
      resolveIndex,
      getMapping,
      create: createIndex,
      get: get.mockResolvedValue({}),
      delete: vi.fn().mockResolvedValue({ acknowledged: true }),
      putMapping: vi.fn().mockResolvedValue({ acknowledged: true }),
      putSettings: vi.fn().mockResolvedValue({ acknowledged: true }),
      updateAliases: vi.fn().mockResolvedValue({ acknowledged: true }),
    },
  };
  (driver as unknown as { connected: boolean }).connected = true;

  return {
    driver,
    search,
    getMapping,
    resolveIndex,
    createIndex,
    update,
    remove,
    get,
    index,
  };
}

describe("ElasticsearchDriver — metadata and pages", () => {
  it("lists sorted visible concrete indices via resolveIndex", async () => {
    const { driver, resolveIndex } = createDriver();
    resolveIndex.mockResolvedValue({
      indices: [
        { name: "orders", attributes: ["open"] },
        { name: ".internal", attributes: ["open", "hidden"] },
        { name: ".security", attributes: ["open", "system"] },
        { name: "logs-000001", attributes: ["open"], data_stream: "logs" },
        { name: "users", attributes: ["open"] },
      ],
      aliases: [{ name: "users-current", indices: ["users"] }],
      data_streams: [
        {
          name: "logs",
          timestamp_field: "@timestamp",
          backing_indices: ["logs-000001"],
        },
      ],
    });

    await expect(driver.listObjects()).resolves.toEqual([
      { schema: "indices", name: "orders", type: "table" },
      { schema: "indices", name: "users", type: "table" },
    ]);
    expect(resolveIndex).toHaveBeenCalledWith({
      name: "*",
      expand_wildcards: ["open", "closed", "hidden"],
      allow_no_indices: true,
      ignore_unavailable: true,
    });
  });

  it("returns fixed _id/_source columns and JSON row payloads for page reads", async () => {
    const { driver, search, getMapping } = createDriver();
    search.mockResolvedValue({
      hits: {
        total: { value: 2, relation: "eq" },
        hits: [
          {
            _id: "doc-2",
            _source: {
              created_at: "2026-04-02T10:30:00Z",
              email: "bravo@example.com",
              profile: { tier: "pro" },
              active: false,
              dynamic_seen: "yes",
            },
          },
          {
            _id: "doc-1",
            _source: {
              created_at: "2026-04-01T09:00:00Z",
              email: "alpha@example.com",
              profile: { tier: "free" },
              active: true,
              status: null,
              dynamic_seen: "no",
            },
          },
        ],
      },
    });
    getMapping.mockResolvedValue({});

    const described = await driver.describeColumns(
      "default",
      "indices",
      "users",
    );
    const page = await driver.readTablePage({
      database: "default",
      schema: "indices",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "_id", direction: "asc" },
      skipCount: false,
    });

    expect(described[0]).toEqual(
      expect.objectContaining({
        name: "_id",
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
      }),
    );
    expect(described).toEqual([
      expect.objectContaining({
        name: "_id",
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
      }),
      expect.objectContaining({
        name: "_source",
        category: "json",
      }),
    ]);
    expect(page.totalCount).toBe(2);
    expect(page.rows).toEqual([
      {
        _id: "doc-2",
        _source:
          '{"created_at":"2026-04-02T10:30:00Z","email":"bravo@example.com","profile":{"tier":"pro"},"active":false,"dynamic_seen":"yes"}',
      },
      {
        _id: "doc-1",
        _source:
          '{"created_at":"2026-04-01T09:00:00Z","email":"alpha@example.com","profile":{"tier":"free"},"active":true,"status":null,"dynamic_seen":"no"}',
      },
    ]);
    expect(search).toHaveBeenCalledTimes(1);
    expect(search).toHaveBeenCalledWith({
      index: "users",
      query: { match_all: {} },
      sort: [{ _id: { order: "asc" } }],
      from: 0,
      size: 10,
      track_total_hits: true,
    });
    expect(getMapping).not.toHaveBeenCalled();
  });

  it("keeps fixed _id/_source columns regardless mapping richness", async () => {
    const { driver, search, getMapping } = createDriver();
    search.mockResolvedValue({ hits: { hits: [] } });
    getMapping.mockResolvedValue({
      users: {
        mappings: {
          properties: {
            binary_value: { type: "binary" },
            boolean_flag: { type: "boolean" },
            keyword_value: { type: "keyword" },
            constant_keyword_value: { type: "constant_keyword" },
            wildcard_value: { type: "wildcard" },
            long_value: { type: "long" },
            scaled_value: { type: "scaled_float" },
            nanos_value: { type: "date_nanos" },
            alias_value: { type: "alias", path: "keyword_value" },
            object_value: {
              properties: {
                inner: { type: "integer" },
              },
            },
            flattened_value: { type: "flattened" },
            nested_value: {
              type: "nested",
              properties: {
                sku: { type: "keyword" },
              },
            },
            join_value: { type: "join" },
            passthrough_value: { type: "passthrough" },
            range_value: { type: "integer_range" },
            ip_value: { type: "ip" },
            version_value: { type: "version" },
            murmur3_value: { type: "murmur3" },
            aggregate_metric_value: { type: "aggregate_metric_double" },
            histogram_value: { type: "histogram" },
            exponential_histogram_value: { type: "exponential_histogram" },
            tdigest_value: { type: "tdigest" },
            text_value: { type: "text" },
            match_only_text_value: { type: "match_only_text" },
            search_as_you_type_value: { type: "search_as_you_type" },
            semantic_text_value: { type: "semantic_text" },
            token_count_value: { type: "token_count" },
            dense_vector_value: { type: "dense_vector" },
            sparse_vector_value: { type: "sparse_vector" },
            rank_feature_value: { type: "rank_feature" },
            rank_features_value: { type: "rank_features" },
            geo_point_value: { type: "geo_point" },
            point_value: { type: "point" },
            percolator_value: { type: "percolator" },
            completion_value: { type: "completion" },
          },
          runtime: {
            runtime_score: { type: "double" },
          },
        },
      },
    });

    const described = await driver.describeColumns(
      "default",
      "indices",
      "users",
    );

    expect(described).toEqual([
      expect.objectContaining({ name: "_id", isPrimaryKey: true }),
      expect.objectContaining({ name: "_source", category: "json" }),
    ]);
  });

  it("returns flattened rows from REST-style search queries", async () => {
    const { driver, search } = createDriver();
    search.mockResolvedValue({
      hits: {
        hits: [
          {
            _id: "doc-9",
            _source: {
              title: "Release note",
              tags: ["product", "beta"],
            },
          },
        ],
      },
    });

    const result = await driver.query(
      'POST /notes/_search\n{\n  "query": {\n    "match": {\n      "title": "Release"\n    }\n  },\n  "size": 1\n}',
    );

    expect(search).toHaveBeenCalledWith({
      index: "notes",
      size: 1,
      query: { match: { title: "Release" } },
    });
    expect(result.columns).toEqual(["_id", "_source"]);
    expect(result.rowCount).toBe(1);
    expect(result.rows).toEqual([
      {
        __col_0: "doc-9",
        __col_1: '{"title":"Release note","tags":["product","beta"]}',
      },
    ]);
  });

  it("clamps REST-style search body size to hard cap", async () => {
    const { driver, search } = createDriver();
    search.mockResolvedValue({ hits: { hits: [] } });

    await driver.query(
      `POST /notes/_search\n{\n  "query": {\n    "match_all": {}\n  },\n  "size": ${ELASTICSEARCH_READ_BUDGET.hardCap + 5000}\n}`,
    );

    expect(search).toHaveBeenCalledWith(
      expect.objectContaining({
        index: "notes",
        size: ELASTICSEARCH_READ_BUDGET.hardCap,
      }),
    );
  });

  it("clamps REST-style search query param size to hard cap", async () => {
    const { driver, search } = createDriver();
    search.mockResolvedValue({ hits: { hits: [] } });

    await driver.query(
      `POST /_search?size=${ELASTICSEARCH_READ_BUDGET.hardCap + 3000}\n{\n  "query": {\n    "match_all": {}\n  }\n}`,
    );

    expect(search).toHaveBeenCalledWith(
      expect.objectContaining({
        size: ELASTICSEARCH_READ_BUDGET.hardCap,
      }),
    );
  });

  it("executes REST-style update and delete document commands", async () => {
    const { driver, update, remove } = createDriver();

    await expect(
      driver.query(
        'POST /users/_update/doc-3?refresh=wait_for\n{\n  "doc": {\n    "email": "charlie+updated@example.com"\n  }\n}',
      ),
    ).resolves.toEqual(
      expect.objectContaining({
        columns: ["result", "index", "id"],
        rowCount: 1,
        affectedRows: 1,
      }),
    );
    expect(update).toHaveBeenCalledWith({
      index: "users",
      id: "doc-3",
      refresh: "wait_for",
      doc: {
        email: "charlie+updated@example.com",
      },
    });

    await expect(
      driver.query("DELETE /users/_doc/doc-3?refresh=wait_for"),
    ).resolves.toEqual(
      expect.objectContaining({
        columns: ["result", "index", "id"],
        rowCount: 1,
        affectedRows: 1,
      }),
    );
    expect(remove).toHaveBeenCalledWith({
      index: "users",
      id: "doc-3",
      refresh: "wait_for",
    });
  });

  it("executes multiple REST-style mutation statements from one editor buffer", async () => {
    const { driver, update } = createDriver();

    const result = await driver.query(
      'POST /app_logs/_update/acQgPZ4BXTUBdY62RrjQ?refresh=wait_for\n{\n  "doc": {\n    "host": "server-11.example.com"\n  }\n}\n\nPOST /app_logs/_update/asQgPZ4BXTUBdY62RrjQ?refresh=wait_for\n{\n  "doc": {\n    "message": "Database connection pool exhausted - all 111 connections in use"\n  }\n}',
    );

    expect(update).toHaveBeenNthCalledWith(1, {
      index: "app_logs",
      id: "acQgPZ4BXTUBdY62RrjQ",
      refresh: "wait_for",
      doc: {
        host: "server-11.example.com",
      },
    });
    expect(update).toHaveBeenNthCalledWith(2, {
      index: "app_logs",
      id: "asQgPZ4BXTUBdY62RrjQ",
      refresh: "wait_for",
      doc: {
        message:
          "Database connection pool exhausted - all 111 connections in use",
      },
    });
    expect(result.columns).toEqual(["results"]);
    expect(result.rowCount).toBe(1);
    expect(result.affectedRows).toBe(2);
    expect(result.rows[0]?.__col_0).toContain('"affectedRows":1');
    expect(result.rows[0]?.__col_0).toContain(
      "POST /app_logs/_update/acQgPZ4BXTUBdY62RrjQ?refresh=wait_for",
    );
  });

  it("executes REST-style PUT index DDL produced by Show DDL", async () => {
    const { driver, createIndex } = createDriver();
    createIndex.mockResolvedValue({
      acknowledged: true,
      shards_acknowledged: true,
      index: "app_logs_clone",
    });

    const result = await driver.query(
      'PUT /app_logs_clone\n{\n  "settings": {\n    "number_of_shards": "1",\n    "number_of_replicas": "0"\n  },\n  "mappings": {\n    "properties": {\n      "message": {\n        "type": "text"\n      },\n      "level": {\n        "type": "keyword"\n      }\n    }\n  }\n}',
    );

    expect(createIndex).toHaveBeenCalledWith({
      index: "app_logs_clone",
      settings: {
        number_of_shards: "1",
        number_of_replicas: "0",
      },
      mappings: {
        properties: {
          message: { type: "text" },
          level: { type: "keyword" },
        },
      },
    });
    expect(result.columns).toEqual([
      "acknowledged",
      "shards_acknowledged",
      "index",
    ]);
    expect(result.rows).toEqual([
      {
        __col_0: true,
        __col_1: true,
        __col_2: "app_logs_clone",
      },
    ]);
  });

  it("keeps fixed columns available when _source filters eliminate every row", async () => {
    const { driver, search, getMapping } = createDriver();
    search.mockResolvedValueOnce({
      hits: {
        total: { value: 0, relation: "eq" },
        hits: [],
      },
    });
    getMapping.mockResolvedValue({});

    const page = await driver.readTablePage({
      database: "default",
      schema: "indices",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [
        {
          column: "_source",
          operator: "like",
          value: "%missing@example.com%",
        },
      ],
      sort: { column: "_id", direction: "asc" },
      skipCount: false,
    });

    expect(page.rows).toEqual([]);
    expect(page.totalCount).toBe(0);
    expect(page.columns).toEqual([
      expect.objectContaining({ name: "_id", isPrimaryKey: true }),
      expect.objectContaining({ name: "_source", category: "json" }),
    ]);
  });

  it("uses _id as request metadata instead of indexing it inside the document body", async () => {
    const { driver, index } = createDriver();

    await expect(
      driver.insertRow({
        database: "default",
        schema: "indices",
        table: "users",
        values: {
          _id: "doc-3",
          email: "charlie@example.com",
          active: true,
        },
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    expect(index).toHaveBeenCalledWith({
      index: "users",
      id: "doc-3",
      document: {
        email: "charlie@example.com",
        active: true,
      },
      op_type: "create",
      refresh: "wait_for",
    });
    expect(
      driver.buildMutationPreviewStatement(
        "insert",
        "default",
        "indices",
        "users",
        {
          values: {
            _id: "doc-3",
            email: "charlie@example.com",
            active: true,
          },
        },
      ),
    ).toBe(
      'PUT /users/_doc/doc-3?op_type=create&refresh=wait_for\n{\n  "email": "charlie@example.com",\n  "active": true\n}',
    );
    expect(
      driver.buildMutationPreviewStatement(
        "update",
        "default",
        "indices",
        "users",
        {
          primaryKeys: { _id: "doc-3" },
          changes: { active: false },
        },
      ),
    ).toBe('PUT /users/_doc/doc-3?refresh=wait_for\n{\n  "active": false\n}');
    expect(
      driver.buildMutationPreviewStatement(
        "delete",
        "default",
        "indices",
        "users",
        {
          primaryKeys: { _id: "doc-3" },
        },
      ),
    ).toBe("DELETE /users/_doc/doc-3?refresh=wait_for");
  });

  it("normalizes viewer-formatted datetime edits back to REST payload syntax", () => {
    const { driver } = createDriver();

    const coerced = driver.coerceInputValue(
      "2026-04-01 09:00:00.123+0000",
      elasticDatetimeColumn,
    );

    expect(coerced).toBe("2026-04-01T09:00:00.123+00:00");
    expect(
      driver.buildMutationPreviewStatement(
        "update",
        "default",
        "indices",
        "users",
        {
          primaryKeys: { _id: "doc-3" },
          changes: { created_at: coerced },
        },
      ),
    ).toBe(
      'PUT /users/_doc/doc-3?refresh=wait_for\n{\n  "created_at": "2026-04-01T09:00:00.123+00:00"\n}',
    );
  });
});
