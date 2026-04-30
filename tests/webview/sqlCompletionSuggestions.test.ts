import { describe, expect, it } from "vitest";

import { buildSqlCompletionSuggestions } from "../../src/webview/components/sqlCompletionSuggestions";

describe("buildSqlCompletionSuggestions", () => {
  it("returns objects from every database but columns only for the configured database cache", () => {
    const schema = [
      {
        database: "app_db",
        schema: "public",
        object: "users",
        type: "table" as const,
        columns: [{ name: "id", type: "int" }],
      },
      {
        database: "archive_db",
        schema: "public",
        object: "users_archive",
        type: "table" as const,
        columns: [],
      },
    ];

    expect(
      buildSqlCompletionSuggestions(schema, "archive_db.public.").map(
        (item) => item.label,
      ),
    ).toContain("users_archive");

    expect(
      buildSqlCompletionSuggestions(schema, "archive_db.public.users_archive."),
    ).toEqual([]);

    expect(
      buildSqlCompletionSuggestions(schema, "app_db.public.users.").map(
        (item) => item.label,
      ),
    ).toContain("id");
  });

  it("surfaces newly expanded scope columns only after the shared cache merges them", () => {
    const partialSchema = [
      {
        database: "app_db",
        schema: "public",
        object: "users",
        type: "table" as const,
        columns: [{ name: "id", type: "int" }],
      },
      {
        database: "archive_db",
        schema: "audit",
        object: "sync_events",
        type: "table" as const,
        columns: [],
      },
    ];
    const mergedSchema = [
      partialSchema[0],
      {
        database: "archive_db",
        schema: "audit",
        object: "sync_events",
        type: "table" as const,
        columns: [{ name: "payload", type: "jsonb" }],
      },
    ];

    expect(
      buildSqlCompletionSuggestions(partialSchema, "archive_db.audit.").map(
        (item) => item.label,
      ),
    ).toContain("sync_events");

    expect(
      buildSqlCompletionSuggestions(
        partialSchema,
        "archive_db.audit.sync_events.",
      ),
    ).toEqual([]);

    expect(
      buildSqlCompletionSuggestions(
        mergedSchema,
        "archive_db.audit.sync_events.",
      ).map((item) => item.label),
    ).toContain("payload");
  });

  it("treats collapsed database/schema names as direct object containers", () => {
    const schema = [
      {
        database: "warehouse",
        schema: "warehouse",
        object: "orders",
        type: "table" as const,
        columns: [{ name: "order_id", type: "bigint" }],
      },
    ];

    expect(
      buildSqlCompletionSuggestions(schema, "warehouse.").map(
        (item) => item.label,
      ),
    ).toContain("orders");

    expect(
      buildSqlCompletionSuggestions(schema, "warehouse.orders.").map(
        (item) => item.label,
      ),
    ).toContain("order_id");
  });
});
