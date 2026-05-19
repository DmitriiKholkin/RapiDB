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

  it("keeps DynamoDB native JSON previews stable when formatting is disabled", () => {
    expect(
      formatMutationPreviewSql(
        [
          JSON.stringify(
            {
              TableName: "users",
              Key: { id: { S: "u1" } },
              UpdateExpression: "SET #email = :email",
              ExpressionAttributeNames: { "#email": "email" },
              ExpressionAttributeValues: {
                ":email": { S: "next@example.com" },
              },
            },
            null,
            2,
          ),
        ],
        {
          formatOnOpen: false,
          editorLanguage: "json",
          allowFormatting: false,
        },
      ),
    ).toContain('"UpdateExpression": "SET #email = :email"');
  });

  it("joins multiple DynamoDB native preview requests with blank lines", () => {
    const previews = [
      JSON.stringify(
        {
          TableName: "Users",
          Key: { userId: { S: "user-1" } },
        },
        null,
        2,
      ),
      JSON.stringify(
        {
          TableName: "Users",
          Key: { userId: { S: "user-2" } },
        },
        null,
        2,
      ),
    ];

    expect(
      formatMutationPreviewSql(previews, {
        formatOnOpen: false,
        editorLanguage: "json",
        allowFormatting: false,
      }),
    ).toBe(`${previews[0]}\n\n${previews[1]}`);
  });
});
