import { describe, expect, it } from "vitest";
import { formatMutationPreviewSql } from "../../src/extension/utils/mutationPreview";
import { splitDynamoPartiqlStatements } from "../../src/shared/dynamodbPartiql";

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

  it("skips formatting for DynamoDB PartiQL when formatting is disabled", () => {
    expect(
      formatMutationPreviewSql(
        [
          'UPDATE "users" SET "email" = \'next@example.com\' WHERE "id" = \'u1\'',
        ],
        {
          formatOnOpen: false,
          editorLanguage: "sql",
          sqlDialect: "sql",
          allowFormatting: false,
        },
      ),
    ).toBe(
      'UPDATE "users" SET "email" = \'next@example.com\' WHERE "id" = \'u1\'',
    );
  });

  it("splits multi-statement DynamoDB PartiQL without formatting it", () => {
    const sql =
      "UPDATE \"Users\" SET \"payload\" = {'country': 'RU'} WHERE \"userId\" = 'user-1' RETURNING ALL NEW *;\nUPDATE \"Users\" SET \"tags\" = ['one', 'two'] WHERE \"userId\" = 'user-2' RETURNING ALL NEW *;";

    expect(splitDynamoPartiqlStatements(sql)).toEqual([
      "UPDATE \"Users\" SET \"payload\" = {'country': 'RU'} WHERE \"userId\" = 'user-1' RETURNING ALL NEW *",
      "UPDATE \"Users\" SET \"tags\" = ['one', 'two'] WHERE \"userId\" = 'user-2' RETURNING ALL NEW *",
    ]);
  });
});
