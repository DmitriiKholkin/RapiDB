import { describe, expect, it } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { elasticsearchKeys } from "../support/elasticsearchIdentity";

const endpoint = process.env.RAPIDB_ES_SCRIPT_TEST_URL;

describe.skipIf(!endpoint)(
  "Elasticsearch source comparison — live Painless",
  () => {
    it("compares nested JSON numbers by value while rejecting real differences", async () => {
      const driver = new ElasticsearchDriver({
        id: "script-test",
        name: "ES",
        type: "elasticsearch",
      });
      const preview = driver.buildMutationPreviewStatement(
        "update",
        "default",
        "indices",
        "users",
        {
          primaryKeys: elasticsearchKeys(driver, "doc-1"),
          changes: { _source: '{"updated":true}' },
          originalValues: { _source: '{"n":1}' },
        },
      );
      const script = JSON.parse(preview.slice(preview.indexOf("\n") + 1)).script
        .source as string;
      const executable = `${script.replace(
        "if (!same(ctx._source, params.original,",
        "def ctx = ['_source': params.current, 'op': 'index'];\nif (!same(ctx._source, params.original,",
      )}\nreturn [ctx.op, ctx._source == params.next];`;
      expect(executable).toContain("def ctx = ['_source': params.current");

      const cases = [
        [
          '{"n":1.0,"nested":[1e3,{"x":2.00}]}',
          '{"n":1,"nested":[1000,{"x":2}]}',
          "index",
        ],
        ['{"n":1.0}', '{"n":2}', "none"],
        ['{"n":9007199254740993}', '{"n":9007199254740992}', "none"],
        ['{"n":1000000000000000100.0}', '{"n":1000000000000000100}', "none"],
        ['{"n":1000000000000000100.0}', '{"n":1000000000000000128}', "index"],
        ['{"n":1000000000000000100.0}', '{"n":1000000000000000128.0}', "index"],
        ['{"n":9007199254740991.0}', '{"n":9007199254740991}', "index"],
        ['{"n":9007199254740992.0}', '{"n":9007199254740992}', "index"],
        ['{"n":-9007199254740992.0}', '{"n":-9007199254740992}', "index"],
        ['{"n":2.5e16}', '{"n":25000000000000000}', "index"],
        ['{"n":2.5e16}', '{"n":25000000000000001}', "none"],
        ['{"n":1e20}', '{"n":100000000000000000000}', "index"],
        ['{"n":100000000000000000000}', '{"n":1e20}', "index"],
        ['{"n":1e20}', '{"n":100000000000000000001}', "none"],
        ['{"n":1e23}', '{"n":1e+23}', "index"],
        ['{"n":-1e23}', '{"n":-99999999999999991611392}', "index"],
        ['{"n":1e23}', '{"n":100000000000000000000000}', "none"],
        ['{"n":0.5}', '{"n":0}', "none"],
        ['{"n":9223372036854775807}', '{"n":9.223372036854776e18}', "none"],
        ['{"n":9223372036854775808}', '{"n":9.223372036854776e18}', "index"],
        [
          `{"n":${(2n ** 1000n).toString()}}`,
          '{"n":1.0715086071862673e+301}',
          "index",
        ],
        [
          `{"n":${(2n ** 1000n + 1n).toString()}}`,
          '{"n":1.0715086071862673e+301}',
          "none",
        ],
        ['{"n":1000000000000000100}', '{"n":1000000000000000101}', "none"],
        ['{"n":0.1}', '{"n":0.10000000000000002}', "none"],
        ['{"n":null}', "{}", "none"],
        ['{"n":[1,2]}', '{"n":[2,1]}', "none"],
        ['{"n":1}', '{"n":"1"}', "none"],
      ];
      for (const [current, original, expected] of cases) {
        const body = `{"script":{"source":${JSON.stringify(executable)},"params":{"current":${current},"original":${original},"next":{"updated":true}}}}`;
        const response = await fetch(`${endpoint}/_scripts/painless/_execute`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body,
        });
        const result = (await response.json()) as {
          result?: string;
          error?: unknown;
        };
        expect(result.error).toBeUndefined();
        expect(result.result).toBe(
          expected === "index" ? "[index, true]" : "[none, false]",
        );
      }

      const floats = `${"1.0,".repeat(999_999)}1.0`;
      const integers = "1,".repeat(999_999);
      for (const [last, expected] of [
        ["1", "index"],
        ["2", "none"],
      ]) {
        const body = `{"script":{"source":${JSON.stringify(executable)},"params":{"current":{"values":[${floats}]},"original":{"values":[${integers}${last}]},"next":{"updated":true}}}}`;
        const response = await fetch(`${endpoint}/_scripts/painless/_execute`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body,
        });
        const result = (await response.json()) as {
          result?: string;
          error?: unknown;
        };
        expect(result.error).toBeUndefined();
        expect(result.result).toBe(
          expected === "index" ? "[index, true]" : "[none, false]",
        );
      }
      const tooMany = `${"1,".repeat(2_000_000)}1`;
      const body = `{"script":{"source":${JSON.stringify(executable)},"params":{"current":{"values":[${tooMany}]},"original":{"values":[${tooMany}]},"next":{"updated":true}}}}`;
      const response = await fetch(`${endpoint}/_scripts/painless/_execute`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
      });
      expect(response.status).toBe(400);
      expect(await response.text()).toContain("2000000-node safety limit");
    }, 60000);
  },
);
