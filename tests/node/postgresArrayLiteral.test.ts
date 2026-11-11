import { types as pgTypes } from "pg";
import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { serializeJsonExportRecord } from "../../src/extension/utils/exportValueFormat";
import { jsonArrayLiteralToPgArrayLiteral } from "../../src/extension/utils/postgresArrayLiteral";

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

describe("PostgreSQL temporal, json, interval and point arrays keep raw element text", () => {
  const driver = new PostgresDriver({ id: "pg", name: "PG", type: "pg" });
  const arrayColumn = (nativeType: string) =>
    ({ name: "a", nativeType, type: nativeType, category: "array" }) as never;

  it.each([
    [1182, "date[]", "{2024-01-01,NULL}", ["2024-01-01", null]],
    [
      1115,
      "timestamp without time zone[]",
      '{"2024-01-01 00:30:00.123456"}',
      ["2024-01-01 00:30:00.123456"],
    ],
    [
      1185,
      "timestamp with time zone[]",
      '{"2024-01-01 00:30:00.123456+01"}',
      ["2024-01-01 00:30:00.123456+01"],
    ],
    [
      199,
      "json[]",
      String.raw`{"{\"n\": 12345678901234567890}","\"s\"","[1, 2]",NULL}`,
      ['{"n": 12345678901234567890}', '"s"', "[1, 2]", null],
    ],
    [
      3807,
      "jsonb[]",
      String.raw`{"{\"n\": 12345678901234567890}"}`,
      ['{"n": 12345678901234567890}'],
    ],
    [
      1187,
      "interval[]",
      '{"1 day 02:00:00",00:00:01.5}',
      ["1 day 02:00:00", "00:00:01.5"],
    ],
    [1017, "point[]", '{"(1.5,2)","(3,4)"}', ["(1.5,2)", "(3,4)"]],
  ])("parses OID %i (%s) without lossy conversion and round-trips the displayed text", (oid, nativeType, raw, expected) => {
    const parsed = parser(oid)(raw);
    expect(parsed).toEqual(expected);
    const shown = driver.formatOutputValue(parsed, arrayColumn(nativeType));
    expect(shown).toBe(JSON.stringify(expected));
    const written = driver.coerceInputValue(shown, arrayColumn(nativeType));
    expect(parser(1009)(String(written))).toEqual(expected);
    expect(
      driver.checkPersistedEdit(arrayColumn(nativeType), shown, {
        persistedValue: parsed,
      })?.ok,
    ).toBe(true);
  });

  it.each([
    "box[]",
    "_box",
  ])("keeps semicolon-delimited %s values as raw literal text", (nativeType) => {
    const raw = "{(1,1),(0,0);(2,2),(1,1)}";
    expect(parser(1020)(raw)).toBe(raw);
    const shown = driver.formatOutputValue(raw, arrayColumn(nativeType));
    expect(shown).toBe(raw);
    expect(driver.coerceInputValue(shown, arrayColumn(nativeType))).toBe(raw);
  });

  it("verifies jsonb[] edits against normalized persisted element text", () => {
    const column = arrayColumn("jsonb[]");
    const persistedValue = parser(3807)(
      String.raw`{"{\"a\": 12345678901234567890, \"b\": 1}"}`,
    );
    for (const expected of [
      '[{"b":1,"a":12345678901234567890}]',
      String.raw`["{\"b\":1,\"a\":12345678901234567890}"]`,
    ]) {
      expect(
        driver.checkPersistedEdit(column, expected, { persistedValue })?.ok,
      ).toBe(true);
    }
    expect(
      driver.checkPersistedEdit(column, '[{"b":1,"a":12345678901234567891}]', {
        persistedValue,
      })?.ok,
    ).toBe(false);
  });

  it("verifies json[] edits against the exact persisted element text", () => {
    const column = arrayColumn("json[]");
    const persistedValue = parser(199)(
      String.raw`{"{\"a\": 12345678901234567890}"}`,
    );
    expect(
      driver.checkPersistedEdit(column, '[{"a":12345678901234567890}]', {
        persistedValue,
      })?.ok,
    ).toBe(true);
    expect(
      driver.checkPersistedEdit(column, '[{"a":12345678901234567891}]', {
        persistedValue,
      })?.ok,
    ).toBe(false);
  });

  it.each([
    ["json[]", 199],
    ["jsonb[]", 3807],
  ])("keeps SQL NULL and JSON null elements distinct for %s", (nativeType, oid) => {
    const column = arrayColumn(nativeType);
    const sqlNull = { persistedValue: parser(oid)("{NULL}") };
    const jsonNull = { persistedValue: parser(oid)('{"null"}') };
    expect(driver.checkPersistedEdit(column, "[null]", sqlNull)?.ok).toBe(true);
    expect(driver.checkPersistedEdit(column, '["null"]', sqlNull)?.ok).toBe(
      false,
    );
    expect(driver.checkPersistedEdit(column, '["null"]', jsonNull)?.ok).toBe(
      true,
    );
    expect(driver.checkPersistedEdit(column, "[null]", jsonNull)?.ok).toBe(
      false,
    );
  });

  it("verifies two-dimensional jsonb[] edits element by element", () => {
    const column = arrayColumn("jsonb[]");
    const persistedValue = parser(3807)(
      String.raw`{{"{\"a\": 1}","[1, 2]"},{NULL,"\"s\""}}`,
    );
    expect(
      driver.checkPersistedEdit(
        column,
        String.raw`[[{"a":1},"[1,2]"],[null,"\"s\""]]`,
        { persistedValue },
      )?.ok,
    ).toBe(true);
    expect(
      driver.checkPersistedEdit(
        column,
        String.raw`[[{"a":1},"[2,1]"],[null,"\"s\""]]`,
        { persistedValue },
      )?.ok,
    ).toBe(false);
  });

  it("accepts jsonb[] exponent numbers that jsonb stores in expanded form", () => {
    const column = arrayColumn("jsonb[]");
    const persistedValue = parser(3807)(String.raw`{"{\"a\": 100}"}`);
    expect(
      driver.checkPersistedEdit(column, '[{"a":1e2}]', { persistedValue })?.ok,
    ).toBe(true);
    expect(
      driver.checkPersistedEdit(column, '[{"a":1e3}]', { persistedValue })?.ok,
    ).toBe(false);
    expect(
      driver.checkPersistedEdit(arrayColumn("json[]"), '[{"a":1e2}]', {
        persistedValue: parser(199)(String.raw`{"{\"a\":1e2}"}`),
      })?.ok,
    ).toBe(true);
  });

  it.each([
    [
      "timestamp without time zone[]",
      '["2024-01-01T10:00:00"]',
      1115,
      '{"2024-01-01 10:00:00"}',
    ],
    [
      "timestamp(3) with time zone[]",
      '["2024-01-01 10:00:00Z"]',
      1185,
      '{"2024-01-01 11:00:00+01"}',
    ],
    ["date[]", '["2024-1-1"]', 1182, "{2024-01-01}"],
    ["time with time zone[]", '["10:00Z"]', 1270, '{"10:00:00+00"}'],
    ["interval[]", '["24 hours"]', 1187, '{"24:00:00"}'],
    ["point[]", '["(1, 2)"]', 1017, '{"(1,2)"}'],
  ])("does not reject valid %s input that PostgreSQL stores in another text form", (nativeType, input, oid, stored) => {
    expect(
      driver.checkPersistedEdit(arrayColumn(nativeType), input, {
        persistedValue: parser(oid)(stored),
      }),
    ).toEqual({ ok: true, shouldVerify: false });
  });

  it.each([
    "daterange[]",
    "text[]",
  ])("still verifies non-temporal %s arrays", (nativeType) => {
    expect(
      driver.checkPersistedEdit(arrayColumn(nativeType), '["a"]', {
        persistedValue: ["b"],
      })?.ok,
    ).toBe(false);
  });

  it.each([
    ["numeric(10,2)[]", "[1.5]", 1231, "{1.50}", "[1.6]"],
    ["double precision[]", "[1.0]", 1022, "{1}", "[1.5]"],
    ["real[]", "[0.1]", 1021, "{0.1}", "[0.2]"],
    ["numeric[]", "[1e2]", 1231, "{100}", "[1e3]"],
    ["integer[]", "{+1}", 1007, "{1}", "{-1}"],
    [
      "bigint[]",
      '["9223372036854775807"]',
      1016,
      "{9223372036854775807}",
      "[9223372036854775806]",
    ],
    [
      "uuid[]",
      '["A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"]',
      2951,
      "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
      '["A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A12"]',
    ],
    ["character(3)[]", '["ab"]', 1014, '{"ab "}', '["ac"]'],
  ])("verifies %s edits using element-type canonical values", (nativeType, input, oid, stored, different) => {
    const column = arrayColumn(nativeType);
    const persistedValue = parser(oid)(stored);
    expect(driver.checkPersistedEdit(column, input)?.ok).toBe(true);
    expect(
      driver.checkPersistedEdit(column, input, { persistedValue })?.ok,
    ).toBe(true);
    expect(
      driver.checkPersistedEdit(column, different, { persistedValue })?.ok,
    ).toBe(false);
  });

  it("keeps numeric[] comparisons exact beyond double precision", () => {
    const column = arrayColumn("numeric[]");
    const persistedValue = parser(1231)("{12345678901234567890.10}");
    expect(
      driver.checkPersistedEdit(column, "[12345678901234567890.1]", {
        persistedValue,
      })?.ok,
    ).toBe(true);
    expect(
      driver.checkPersistedEdit(column, "[12345678901234567890.2]", {
        persistedValue,
      })?.ok,
    ).toBe(false);
    expect(
      driver.checkPersistedEdit(column, "[1.5,null]", {
        persistedValue: parser(1231)("{1.50,NULL}"),
      })?.ok,
    ).toBe(true);
    expect(
      driver.checkPersistedEdit(column, "[1.5,0]", {
        persistedValue: parser(1231)("{1.50,NULL}"),
      })?.ok,
    ).toBe(false);
  });

  const domainArrayColumn = (nativeType: string, baseType: string) =>
    ({ ...(arrayColumn(nativeType) as object), baseType }) as never;

  it.each([
    ["public.amount[]", "numeric(10,2)[]", "[1.5]", "{1.50}", "[1.6]"],
    ["public.amount[]", "numeric(10,2)[]", [1.5], "{1.50}", [1.6]],
    ["public.qty[]", "integer[]", "[1]", "{1}", "[2]"],
    ["public.qty[]", "integer[]", [1], "{1}", [2]],
    ["public.amount3[]", "numeric(10,2)[]", "[2.5]", "{2.50}", "[2.6]"],
    [
      "hidden.hamount[]",
      "numeric(8,3)[]",
      "[1.25,null]",
      "{1.250,NULL}",
      "[1.25,0]",
    ],
    ["public.label[]", "text[]", '["x"]', "{x}", '["y"]'],
    ["public.moods[]", "public.mood[]", '["sad"]', "{sad}", '["ok"]'],
  ])("verifies %s domain array edits using the %s base element type", (nativeType, baseType, input, stored, different) => {
    const column = domainArrayColumn(nativeType, baseType);
    expect(driver.checkPersistedEdit(column, input)?.ok).toBe(true);
    expect(
      driver.checkPersistedEdit(column, input, { persistedValue: stored }),
    ).toEqual({ ok: true, shouldVerify: true });
    expect(
      driver.checkPersistedEdit(column, different, { persistedValue: stored })
        ?.ok,
    ).toBe(false);
  });

  it.each([
    ['["{t,f}"]', '{"{t,f}"}', '["{t,t}"]'],
    [["{t,f}"], '{"{t,f}"}', ["{t,t}"]],
    ['["{t}",null]', '{"{t}",NULL}', '["{f}",null]'],
  ])("verifies arrays of domains over boolean[] with the generic comparison (%j)", (input, stored, different) => {
    const column = domainArrayColumn("public.flags[]", "public.flags[]");
    expect(
      driver.checkPersistedEdit(column, input, { persistedValue: stored }),
    ).toEqual({ ok: true, shouldVerify: true });
    expect(
      driver.checkPersistedEdit(column, different, { persistedValue: stored })
        ?.ok,
    ).toBe(false);
  });

  it("does not verify arrays of domains over temporal base types", () => {
    expect(
      driver.checkPersistedEdit(
        domainArrayColumn("public.day[]", "date[]"),
        '["2024-1-1"]',
        { persistedValue: "{2024-01-01}" },
      ),
    ).toEqual({ ok: true, shouldVerify: false });
  });

  it.each([
    "public.amount[]",
    '"audit"."amount"[]',
  ])("skips verification of %s when its element base type is unresolved", (nativeType) => {
    expect(
      driver.checkPersistedEdit(arrayColumn(nativeType), "[1.5]", {
        persistedValue: "{1.50}",
      }),
    ).toEqual({ ok: true, shouldVerify: false });
  });

  it("exports json[] and jsonb[] elements as nested JSON without precision loss", () => {
    for (const [nativeType, oid, expected] of [
      [
        "json[]",
        199,
        '{"a":[[{"n": 12345678901234567890},"s"],[null,[1.0, 2]],[null,null]]}',
      ],
      [
        "jsonb[]",
        3807,
        '{"a":[[{"n":12345678901234567890},"s"],[null,[1.0,2]],[null,null]]}',
      ],
    ] as const) {
      const shown = driver.formatOutputValue(
        parser(oid)(
          String.raw`{{"{\"n\": 12345678901234567890}","\"s\""},{NULL,"[1.0, 2]"},{"null",NULL}}`,
        ),
        arrayColumn(nativeType),
      );
      expect(
        serializeJsonExportRecord([
          {
            key: "a",
            sourceKey: "a",
            category: "array",
            nativeType,
            value: shown,
          },
        ]),
      ).toBe(expected);
    }
    expect(
      serializeJsonExportRecord([
        {
          key: "a",
          sourceKey: "a",
          category: "array",
          nativeType: "text[]",
          value: '["{\\"n\\": 1}"]',
        },
      ]),
    ).toBe('{"a":["{\\"n\\": 1}"]}');
  });
});
