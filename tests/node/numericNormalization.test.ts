import { describe, expect, it } from "vitest";
import { normalizeNumericToken } from "../../src/shared/numericNormalization";

describe("normalizeNumericToken", () => {
  it.each([
    "9223372036854775807",
    "9".repeat(400),
    "1e999",
    "12345678901234567890.123456789",
  ])("preserves exact numeric text %s when JS range is not required", (value) => {
    expect(normalizeNumericToken(value, false)).toBe(value);
  });

  it.each([
    "abc123",
    "10xyz",
    "mis500",
    "Infinity",
    "-Infinity",
    "NaN",
  ])("still rejects invalid exact numeric text %s", (value) =>
    expect(normalizeNumericToken(value, false)).toBeNull());

  describe("plain numeric values", () => {
    it.each([
      ["0", "0"],
      ["1", "1"],
      ["42", "42"],
      ["-7", "-7"],
      ["+5", "+5"],
      ["3.14", "3.14"],
      ["-2.5", "-2.5"],
      [".5", ".5"],
      ["-.5", "-.5"],
      ["1e10", "1e10"],
      ["1.5e3", "1.5e3"],
      ["1.5e-3", "1.5e-3"],
    ])("normalizes %s -> %s", (input, expected) => {
      expect(normalizeNumericToken(input)).toBe(expected);
    });
  });

  describe("currency-prefixed money values", () => {
    it.each([
      ["$99.99", "99.99"],
      ["-$1.00", "-1.00"],
      ["+$1.00", "+1.00"],
      ["€50.00", "50.00"],
      ["£42.5", "42.5"],
      ["¥100", "100"],
      ["₽100.50", "100.50"],
      ["$0.5", "0.5"],
    ])("normalizes %s -> %s", (input, expected) => {
      expect(normalizeNumericToken(input)).toBe(expected);
    });
  });

  describe("currency-suffixed money values", () => {
    it.each([
      ["99.99$", "99.99"],
      ["1,234.56 ₽", "1234.56"],
      ["50.00€", "50.00"],
      ["99.99\u{1ECB0}\u00a0€\t", "99.99"],
      ["\u{1ECB0}99.99\u{1ECB0}", "99.99"],
      ["CHF 1'234.56\u{1ECB0}€", "1234.56"],
      ["1,234.56\u{1ECB0}€", "1234.56"],
    ])("normalizes %s -> %s", (input, expected) => {
      expect(normalizeNumericToken(input)).toBe(expected);
    });
  });

  describe("accounting parentheses (negative money)", () => {
    it.each([
      ["(99.99)", "-99.99"],
      ["($50.00)", "-50.00"],
      ["(€1,234.56)", "-1234.56"],
      ["(\u{1ECB0}1,234.56\u00a0€\u{1ECB0})", "-1234.56"],
    ])("normalizes %s -> %s", (input, expected) => {
      expect(normalizeNumericToken(input)).toBe(expected);
    });
  });

  describe("grouped thousands", () => {
    it.each([
      ["$1,000", "1000"],
      ["$1,000.00", "1000.00"],
      ["$1,234,567.89", "1234567.89"],
      ["1 000.50", "1000.50"],
      ["$1 234.56", "1234.56"],
    ])("normalizes %s -> %s", (input, expected) => {
      expect(normalizeNumericToken(input)).toBe(expected);
    });
  });

  describe("ISO currency code prefix", () => {
    it.each([
      ["CHF 1'234.56", "1234.56"],
      ["USD 99.99", "99.99"],
      ["usd123", "123"],
      ["10CHF", "10"],
    ])("normalizes %s -> %s", (input, expected) => {
      expect(normalizeNumericToken(input)).toBe(expected);
    });
  });

  describe("apostrophe-grouped numbers (Swiss style)", () => {
    it.each([
      ["1'000", "1000"],
      ["1'000.50", "1000.50"],
      ["1'234'567.89", "1234567.89"],
    ])("normalizes %s -> %s", (input, expected) => {
      expect(normalizeNumericToken(input)).toBe(expected);
    });
  });

  describe("invalid input rejected", () => {
    it.each([
      [""],
      ["abc"],
      ["abc123"],
      ["10xyz"],
      ["mis500"],
      ["99.99.99"],
      ["$"],
      ["NaN"],
      ["Infinity"],
      ["1e999"],
      ["$1.00 +"],
      ["-$ -1.00"],
      ["1.234,56"],
      ["1,5"],
      ["0.99 USD"],
      ["$1,234.56 USD"],
      ["99.99\udf80"],
      ["99.99\ud83b"],
    ])("rejects %s", (input) => {
      expect(normalizeNumericToken(input)).toBeNull();
    });
  });
});
