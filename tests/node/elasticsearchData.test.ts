import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";

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

  it("derives recursive mapped column metadata and flattened rows for page reads", async () => {
    const { driver, search, getMapping } = createDriver();
    search.mockResolvedValue({
      hits: {
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
    getMapping.mockResolvedValue({
      users: {
        mappings: {
          properties: {
            created_at: { type: "date" },
            email: {
              type: "keyword",
              fields: {
                text: { type: "text" },
              },
            },
            profile: {
              properties: {
                tier: { type: "keyword" },
              },
            },
            items: {
              type: "nested",
              properties: {
                sku: { type: "keyword" },
              },
            },
            active: { type: "boolean" },
            status: { type: "keyword", null_value: "unknown" },
            email_alias: { type: "alias", path: "email" },
          },
          runtime: {
            computed_score: { type: "double" },
          },
        },
      },
    });

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
    expect(described).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "created_at",
          type: "date",
          nativeType: "date",
          category: "datetime",
        }),
        expect.objectContaining({
          name: "email",
          type: "keyword",
          nativeType: "keyword",
          category: "text",
        }),
        expect.objectContaining({
          name: "email.text",
          type: "text",
          nativeType: "text",
          category: "text",
        }),
        expect.objectContaining({
          name: "profile",
          type: "object",
          nativeType: "object",
          category: "json",
        }),
        expect.objectContaining({
          name: "profile.tier",
          type: "keyword",
          nativeType: "keyword",
          category: "text",
        }),
        expect.objectContaining({
          name: "items",
          type: "nested",
          nativeType: "nested",
          category: "json",
        }),
        expect.objectContaining({
          name: "items.sku",
          type: "keyword",
          nativeType: "keyword",
          category: "text",
        }),
        expect.objectContaining({
          name: "computed_score",
          type: "double",
          nativeType: "double",
          category: "float",
        }),
        expect.objectContaining({
          name: "email_alias",
          type: "alias",
          nativeType: "alias",
          category: "text",
        }),
        expect.objectContaining({
          name: "dynamic_seen",
          type: "text",
        }),
        expect.objectContaining({
          name: "status",
          defaultValue: "unknown",
        }),
      ]),
    );
    expect(page.totalCount).toBe(2);
    expect(page.rows).toEqual([
      {
        _id: "doc-1",
        active: true,
        created_at: "2026-04-01 09:00:00Z",
        dynamic_seen: "no",
        email: "alpha@example.com",
        profile: '{"tier":"free"}',
        status: null,
      },
      {
        _id: "doc-2",
        active: false,
        created_at: "2026-04-02 10:30:00Z",
        dynamic_seen: "yes",
        email: "bravo@example.com",
        profile: '{"tier":"pro"}',
      },
    ]);
    expect(search).toHaveBeenCalledWith({
      index: "users",
      size: 1000,
      query: { match_all: {} },
      sort: ["_doc"],
    });
  });

  it("maps documented Elasticsearch field types to exact display types and UI categories", async () => {
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

    expect(described).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "binary_value",
          type: "binary",
          nativeType: "binary",
          category: "binary",
        }),
        expect.objectContaining({
          name: "boolean_flag",
          type: "boolean",
          nativeType: "boolean",
          category: "boolean",
        }),
        expect.objectContaining({
          name: "keyword_value",
          type: "keyword",
          nativeType: "keyword",
          category: "text",
        }),
        expect.objectContaining({
          name: "constant_keyword_value",
          type: "constant_keyword",
          nativeType: "constant_keyword",
          category: "text",
        }),
        expect.objectContaining({
          name: "wildcard_value",
          type: "wildcard",
          nativeType: "wildcard",
          category: "text",
        }),
        expect.objectContaining({
          name: "long_value",
          type: "long",
          nativeType: "long",
          category: "integer",
        }),
        expect.objectContaining({
          name: "scaled_value",
          type: "scaled_float",
          nativeType: "scaled_float",
          category: "float",
        }),
        expect.objectContaining({
          name: "nanos_value",
          type: "date_nanos",
          nativeType: "date_nanos",
          category: "datetime",
        }),
        expect.objectContaining({
          name: "alias_value",
          type: "alias",
          nativeType: "alias",
          category: "text",
        }),
        expect.objectContaining({
          name: "object_value",
          type: "object",
          nativeType: "object",
          category: "json",
        }),
        expect.objectContaining({
          name: "flattened_value",
          type: "flattened",
          nativeType: "flattened",
          category: "json",
        }),
        expect.objectContaining({
          name: "nested_value",
          type: "nested",
          nativeType: "nested",
          category: "json",
        }),
        expect.objectContaining({
          name: "join_value",
          type: "join",
          nativeType: "join",
          category: "json",
        }),
        expect.objectContaining({
          name: "passthrough_value",
          type: "passthrough",
          nativeType: "passthrough",
          category: "json",
        }),
        expect.objectContaining({
          name: "range_value",
          type: "integer_range",
          nativeType: "integer_range",
          category: "json",
        }),
        expect.objectContaining({
          name: "ip_value",
          type: "ip",
          nativeType: "ip",
          category: "text",
        }),
        expect.objectContaining({
          name: "version_value",
          type: "version",
          nativeType: "version",
          category: "text",
        }),
        expect.objectContaining({
          name: "murmur3_value",
          type: "murmur3",
          nativeType: "murmur3",
          category: "other",
        }),
        expect.objectContaining({
          name: "aggregate_metric_value",
          type: "aggregate_metric_double",
          nativeType: "aggregate_metric_double",
          category: "json",
        }),
        expect.objectContaining({
          name: "histogram_value",
          type: "histogram",
          nativeType: "histogram",
          category: "json",
        }),
        expect.objectContaining({
          name: "exponential_histogram_value",
          type: "exponential_histogram",
          nativeType: "exponential_histogram",
          category: "json",
        }),
        expect.objectContaining({
          name: "tdigest_value",
          type: "tdigest",
          nativeType: "tdigest",
          category: "json",
        }),
        expect.objectContaining({
          name: "text_value",
          type: "text",
          nativeType: "text",
          category: "text",
        }),
        expect.objectContaining({
          name: "match_only_text_value",
          type: "match_only_text",
          nativeType: "match_only_text",
          category: "text",
        }),
        expect.objectContaining({
          name: "search_as_you_type_value",
          type: "search_as_you_type",
          nativeType: "search_as_you_type",
          category: "text",
        }),
        expect.objectContaining({
          name: "semantic_text_value",
          type: "semantic_text",
          nativeType: "semantic_text",
          category: "text",
        }),
        expect.objectContaining({
          name: "token_count_value",
          type: "token_count",
          nativeType: "token_count",
          category: "integer",
        }),
        expect.objectContaining({
          name: "dense_vector_value",
          type: "dense_vector",
          nativeType: "dense_vector",
          category: "array",
        }),
        expect.objectContaining({
          name: "sparse_vector_value",
          type: "sparse_vector",
          nativeType: "sparse_vector",
          category: "json",
        }),
        expect.objectContaining({
          name: "rank_feature_value",
          type: "rank_feature",
          nativeType: "rank_feature",
          category: "float",
        }),
        expect.objectContaining({
          name: "rank_features_value",
          type: "rank_features",
          nativeType: "rank_features",
          category: "json",
        }),
        expect.objectContaining({
          name: "geo_point_value",
          type: "geo_point",
          nativeType: "geo_point",
          category: "spatial",
        }),
        expect.objectContaining({
          name: "point_value",
          type: "point",
          nativeType: "point",
          category: "spatial",
        }),
        expect.objectContaining({
          name: "percolator_value",
          type: "percolator",
          nativeType: "percolator",
          category: "json",
        }),
        expect.objectContaining({
          name: "completion_value",
          type: "completion",
          nativeType: "completion",
          category: "text",
        }),
        expect.objectContaining({
          name: "runtime_score",
          type: "double",
          nativeType: "double",
          category: "float",
        }),
      ]),
    );
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
    expect(result.columns).toEqual(["_id", "tags", "title"]);
    expect(result.rowCount).toBe(1);
    expect(result.rows).toEqual([
      {
        __col_0: "doc-9",
        __col_1: '["product","beta"]',
        __col_2: "Release note",
      },
    ]);
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

  it("keeps mapped columns available when filters eliminate every row", async () => {
    const { driver, search, getMapping } = createDriver();
    search.mockResolvedValue({
      hits: {
        hits: [
          {
            _id: "doc-1",
            _source: {
              email: "alpha@example.com",
              created_at: "2026-04-01T09:00:00Z",
            },
          },
        ],
      },
    });
    getMapping.mockResolvedValue({
      users: {
        mappings: {
          properties: {
            created_at: { type: "date" },
            email: { type: "keyword" },
          },
        },
      },
    });

    const page = await driver.readTablePage({
      database: "default",
      schema: "indices",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [
        {
          column: "email",
          operator: "eq",
          value: "missing@example.com",
        },
      ],
      sort: { column: "_id", direction: "asc" },
      skipCount: false,
    });

    expect(page.rows).toEqual([]);
    expect(page.totalCount).toBe(0);
    expect(page.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "_id", isPrimaryKey: true }),
        expect.objectContaining({ name: "email", type: "keyword" }),
        expect.objectContaining({ name: "created_at", type: "date" }),
      ]),
    );
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
    ).toBe(
      'POST /users/_update/doc-3?refresh=wait_for\n{\n  "doc": {\n    "active": false\n  }\n}',
    );
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
      'POST /users/_update/doc-3?refresh=wait_for\n{\n  "doc": {\n    "created_at": "2026-04-01T09:00:00.123+00:00"\n  }\n}',
    );
  });
});
