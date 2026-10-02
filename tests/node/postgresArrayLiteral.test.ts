import { types as pgTypes } from "pg";
import { describe, expect, it } from "vitest";
import "../../src/extension/dbDrivers/postgres";
import { jsonArrayLiteralToPgArrayLiteral } from "../../src/extension/utils/postgresArrayLiteral";

// pg's TypeId enum lists scalar OIDs only; dynamic numeric OIDs are supported.
const parser = (oid: number): ((text: string) => unknown[]) =>
  pgTypes.getTypeParser(oid);

describe("PostgreSQL array grammar in registered parsers", () => {
  const text = parser(1009);

  it("reads dimension prefixes without dropping the elements", () => {
    expect(parser(1007)("[0:1]={7,7}")).toEqual(["7", "7"]);
    expect(text('[0:1][-2:-1]={{"}",ok},{x,"{"}}')).toEqual([
      ["}", "ok"],
      ["x", "{"],
    ]);
  });

  it("keeps quoted braces, escaped quotes/backslashes and NULL distinct", () => {
    expect(text(String.raw`{{"}","{\"\\"},{NULL,"NULL"}}`)).toEqual([
      ["}", '{"\\'],
      [null, "NULL"],
    ]);
    expect(text(String.raw`{null,"null",N\ULL,a\,b,\{,\},"",a\ b}`)).toEqual([
      null,
      "null",
      "NULL",
      "a,b",
      "{",
      "}",
      "",
      "a b",
    ]);
  });

  it("uses PG escape semantics and retains real control characters", () => {
    expect(text(String.raw`{"a\nb","a\tb","a\rb"}`)).toEqual([
      "anb",
      "atb",
      "arb",
    ]);
    expect(text('{"a\nb","a\tb","a\rb"}')).toEqual(["a\nb", "a\tb", "a\rb"]);
  });

  it("ignores only unescaped outer whitespace and accepts empty arrays", () => {
    expect(text(' \n{  a b  , " a " , a\\ ,\\ a }\t')).toEqual([
      "a b",
      " a ",
      "a ",
      " a",
    ]);
    expect(text(" { } ")).toEqual([]);
  });

  it("decodes boolean OID 1000 only, including nested arrays", () => {
    expect(parser(1000)("{{t,f},{NULL,t}}")).toEqual([
      [true, false],
      [null, true],
    ]);
    expect(text("{t,f,NULL}")).toEqual(["t", "f", null]);
  });

  it("preserves bigint and numeric tokens exactly", () => {
    expect(parser(1016)("{9223372036854775807,-9223372036854775808}")).toEqual([
      "9223372036854775807",
      "-9223372036854775808",
    ]);
    expect(parser(1231)("{12345678901234567890.1234567890,13000.0}")).toEqual([
      "12345678901234567890.1234567890",
      "13000.0",
    ]);
  });

  it.each([
    "[2147483646:2147483646]={x}",
    "[-2147483648:-2147483648]={x}",
    "[-2147483648:-2147483647]={x,y}",
    " \t[0:1] \n= \t{x,y} \n",
    "[+0:+1]={x,y}",
  ])("accepts valid int32 bounds and external whitespace %s", (input) => {
    const expected = input.includes("y}") ? ["x", "y"] : ["x"];
    expect(text(input)).toEqual(expected);
    expect(jsonArrayLiteralToPgArrayLiteral(input)).toBe(input.trim());
  });

  it.each([
    "[2147483647:2147483647]={x}",
    "[2147483646:2147483647]={x,y}",
    "[-2147483649:-2147483649]={x}",
    "[-2147483648:-1]={x}",
    "[0 :1]={x,y}",
    "[0: 1]={x,y}",
    "[0:1 ]={x,y}",
    "[ 0:1]={x,y}",
  ])("rejects overflowing or internally spaced bounds %s", (input) => {
    expect(() => text(input)).toThrow();
    expect(() => jsonArrayLiteralToPgArrayLiteral(input)).toThrow();
  });

  it.each([
    "no array",
    "{",
    "{a,}",
    "{,a}",
    '{a b"c}',
    '{"unterminated}',
    '{"a"b}',
    "{a}tail",
    "{{a},{b,c}}",
    "{a,{b}}",
    "{a,,b}",
    "[0:2]={a,b}",
    "[0:1]{a,b}",
    "[2:1]={}",
    "[2147483648:2147483649]={a,b}",
    "{{}}",
    "{{{{{{{a}}}}}}}",
    '{"a\0b"}',
  ])("rejects malformed input %s instead of inventing values", (input) => {
    expect(() => text(input)).toThrow();
  });
});

describe("JSON to PostgreSQL array conversion", () => {
  it("writes real controls and escapes only quotes and backslashes", () => {
    const elements = ["a\nb", "a\tb", "a\rb", "\b\f\v", String.raw`\n`, '"{}'];
    const literal = jsonArrayLiteralToPgArrayLiteral(JSON.stringify(elements));
    expect(literal).toContain("a\nb");
    expect(literal).toContain("a\tb");
    expect(literal).toContain("a\rb");
    expect(parser(1009)(literal)).toEqual(elements);
  });

  it("keeps nested object elements as JSON text, including nulls and arrays", () => {
    const input =
      '[{"n":12345678901234567890.00,"items":[null,{"s":"a\\nb"}]}]';
    const [element] = parser(1009)(jsonArrayLiteralToPgArrayLiteral(input));
    expect(element).toBe(input.slice(1, -1));
    expect(JSON.parse(String(element))).toEqual(JSON.parse(input)[0]);
  });

  it("preserves multidimensional numbers and accepts native bounds", () => {
    expect(
      jsonArrayLiteralToPgArrayLiteral(
        "[[9223372036854775807,13000.0],[null,1e-20]]",
      ),
    ).toBe("{{9223372036854775807,13000.0},{NULL,1e-20}}");
    expect(jsonArrayLiteralToPgArrayLiteral("[0:1]={a,b}")).toBe("[0:1]={a,b}");
  });

  it.each([
    "[1]junk",
    "[1 2]",
    "[1,]",
    "[01]",
    "[1.]",
    "[.1]",
    "[true false]",
    '[{"a":1,}]',
    '[{"a":1 "b":2}]',
    '["bad\\q"]',
    '["real\nnewline"]',
    '["\\u0000"]',
    "[[1],[2,3]]",
    "[1,[2]]",
    "{a,}",
    "[0:2]={a,b}",
  ])("rejects malformed input %s", (input) => {
    expect(() => jsonArrayLiteralToPgArrayLiteral(input)).toThrow();
  });
});
