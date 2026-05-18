import { describe, expect, it } from "vitest";
import { formatMutationPreviewSql } from "../../src/extension/utils/mutationPreview";

describe("formatMutationPreviewSql", () => {
  it("keeps Redis and Elasticsearch previews unformatted and without SQL terminators", () => {
    expect(
      formatMutationPreviewSql(["SET app:key value", "DEL app:key"], {
        formatOnOpen: false,
        editorLanguage: "plaintext",
      }),
    ).toBe("SET app:key value\n\nDEL app:key");

    expect(
      formatMutationPreviewSql(
        ['PUT /users {"mappings":{"properties":{"id":{"type":"keyword"}}}}'],
        { formatOnOpen: false, editorLanguage: "plaintext" },
      ),
    ).toBe('PUT /users {"mappings":{"properties":{"id":{"type":"keyword"}}}}');
  });

  it("continues formatting SQL-like previews such as DynamoDB PartiQL", () => {
    expect(
      formatMutationPreviewSql(
        [
          'UPDATE "users" SET "email" = \'next@example.com\' WHERE "id" = \'u1\'',
        ],
        {
          formatOnOpen: true,
          editorLanguage: "sql",
          sqlDialect: "sql",
        },
      ),
    ).toContain('UPDATE "users"');
  });
});
