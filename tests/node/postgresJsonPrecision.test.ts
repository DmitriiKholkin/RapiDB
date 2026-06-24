import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const driver = new PostgresDriver({
  id: "pg-json-precision",
  name: "PostgreSQL JSON precision",
  type: "pg",
  host: "127.0.0.1",
  port: 5432,
  database: "postgres",
  username: "postgres",
  password: "postgres",
} as ConnectionConfig);

const jsonbColumn: ColumnTypeMeta = {
  name: "data",
  type: "jsonb",
  nativeType: "jsonb",
  category: "json",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "neq", "like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

const jsonColumn: ColumnTypeMeta = {
  name: "meta",
  type: "json",
  nativeType: "json",
  category: "json",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "neq", "like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

const numericArrayColumn: ColumnTypeMeta = {
  name: "amounts",
  type: "numeric[]",
  nativeType: "numeric[]",
  category: "array",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

const textArrayColumn: ColumnTypeMeta = {
  name: "tags",
  type: "text[]",
  nativeType: "text[]",
  category: "array",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["like", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

describe("PostgreSQL JSON/Array precision", () => {
  describe("formatOutputValue", () => {
    it("preserves the raw JSONB text as-is", () => {
      const raw = '{"name_risk_group":"setB","amt_estimated_loan":13000.0}';
      const formatted = driver.formatOutputValue(raw, jsonbColumn);
      expect(formatted).toBe(raw);
    });

    it("preserves the raw JSON text as-is", () => {
      const raw = '{"rate":0.95,"value":13000.0}';
      const formatted = driver.formatOutputValue(raw, jsonColumn);
      expect(formatted).toBe(raw);
    });

    it("preserves precision in a numeric array (raw text)", () => {
      const formatted = driver.formatOutputValue(
        "{13000.0,42.5,NULL}",
        numericArrayColumn,
      );
      expect(formatted).toBe("[13000.0,42.5,null]");
    });

    it("serializes a parsed numeric array preserving raw tokens", () => {
      const formatted = driver.formatOutputValue(
        ["13000.0", "42.5", null],
        numericArrayColumn,
      );
      expect(formatted).toBe("[13000.0,42.5,null]");
    });

    it("serializes a parsed text array with quoted strings", () => {
      const formatted = driver.formatOutputValue(
        ["alpha", "beta", "with spaces"],
        textArrayColumn,
      );
      expect(formatted).toBe('["alpha","beta","with spaces"]');
    });

    it("normalizes a PG array string with quoted elements to JSON text", () => {
      const formatted = driver.formatOutputValue(
        '{"hello world","with quote\\"inside",42.5}',
        textArrayColumn,
      );
      expect(formatted).toBe('["hello world","with quote\\"inside",42.5]');
    });

    it("decodes PG array backslash escapes (n, t, r, b, f, v)", () => {
      const formatted = driver.formatOutputValue(
        '{"line1\\nline2","col1\\tcol2"}',
        textArrayColumn,
      );
      expect(formatted).toBe('["line1\\nline2","col1\\tcol2"]');
    });

    it("decodes boolean tokens (t/f) inside PG arrays", () => {
      const formatted = driver.formatOutputValue("{t,f,NULL}", textArrayColumn);
      expect(formatted).toBe("[true,false,null]");
    });
  });

  describe("coerceInputValue", () => {
    it("returns the JSON text unchanged for jsonb columns", () => {
      const raw = '{"x":13000.0}';
      expect(driver.coerceInputValue(raw, jsonbColumn)).toBe(raw);
    });

    it("converts a JSON array literal to a PG array literal for numeric[]", () => {
      const coerced = driver.coerceInputValue(
        "[13000.0,42.5]",
        numericArrayColumn,
      );
      expect(coerced).toBe("{13000.0,42.5}");
    });

    it("converts null elements inside a JSON array to PG NULL", () => {
      const coerced = driver.coerceInputValue(
        "[1,null,3.0]",
        numericArrayColumn,
      );
      expect(coerced).toBe("{1,NULL,3.0}");
    });

    it("escapes quotes and backslashes when converting text arrays", () => {
      const coerced = driver.coerceInputValue(
        '["hello","with \\"quote\\""]',
        textArrayColumn,
      );
      expect(coerced).toBe('{"hello","with \\"quote\\""}');
    });

    it("leaves PG array literals unchanged", () => {
      const raw = "{13000.0,42.5}";
      expect(driver.coerceInputValue(raw, numericArrayColumn)).toBe(raw);
    });
  });

  describe("buildFilterCondition", () => {
    it("preserves trailing zero in a jsonb equality filter", () => {
      const condition = driver.buildFilterCondition(
        jsonbColumn,
        "eq",
        '{"amt_estimated_loan":13000.0}',
        1,
      );
      expect(condition).toEqual({
        sql: '("data")::jsonb = $1::jsonb',
        params: ['{"amt_estimated_loan":13000.0}'],
      });
    });

    it("preserves trailing zero in a jsonb inequality filter", () => {
      const condition = driver.buildFilterCondition(
        jsonbColumn,
        "neq",
        '{"x":1.5}',
        1,
      );
      expect(condition).toEqual({
        sql: '("data")::jsonb <> $1::jsonb',
        params: ['{"x":1.5}'],
      });
    });

    it("preserves trailing zero in a jsonb like filter", () => {
      const condition = driver.buildFilterCondition(
        jsonbColumn,
        "like",
        '"amt_estimated_loan": 13000.0',
        1,
      );
      expect(condition).toEqual({
        sql: 'CAST("data" AS TEXT) ILIKE $1',
        params: ['%"amt_estimated_loan": 13000.0%'],
      });
    });
  });
});
