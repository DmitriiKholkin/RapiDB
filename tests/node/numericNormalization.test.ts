import { describe, expect, it } from "vitest";
import { normalizeNumericToken } from "../../src/shared/numericNormalization";

describe("normalizeNumericToken", () => {
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
    ])("normalizes %s -> %s", (input, expected) => {
      expect(normalizeNumericToken(input)).toBe(expected);
    });
  });

  describe("accounting parentheses (negative money)", () => {
    it.each([
      ["(99.99)", "-99.99"],
      ["($50.00)", "-50.00"],
      ["(€1,234.56)", "-1234.56"],
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
      ["99.99.99"],
      ["$"],
      ["NaN"],
      ["Infinity"],
      ["$1.00 +"],
      ["-$ -1.00"],
      ["1.234,56"],
      ["1,5"],
      ["0.99 USD"],
      ["$1,234.56 USD"],
    ])("rejects %s", (input) => {
      expect(normalizeNumericToken(input)).toBeNull();
    });
  });
});
