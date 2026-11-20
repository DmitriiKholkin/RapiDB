import { describe, expect, it } from "vitest";
import { hasDuplicateDynamoDbNumbers } from "../../src/shared/dynamodbCellInput";
import { compareExactNumericValues } from "../../src/shared/exactNumericComparison";

describe("exact numeric comparison", () => {
  it.each([
    ["1,234,", "1234"],
    ["1.2.", "12"],
    ["$1.", "1"],
    ["(€1.234.)", "-1234"],
    ["1,234, €", "1234"],
    ["1'234'-", "-1234"],
    ["$1,,234", "1234"],
    ["1.23.45", "12345"],
    ["€1.234,567,89", "1234567.89"],
    ["$1''234.56", "1234.56"],
  ])("does not treat malformed %s as the number %s", (malformed, number) => {
    expect(compareExactNumericValues(malformed, number)).toBe(1);
    expect(compareExactNumericValues(number, malformed)).toBe(-1);
  });

  it.each([
    ["1.", "1"],
    ["1.e2", "100"],
    ["$1,234.56", "1234.56"],
    ["(€1.234,56)", "-1234.56"],
    ["1\u202f234,56 €", "1234.56"],
    ["€1.234.567", "1234567"],
    ["₹12,34,567", "1234567"],
    ["₹12,34,567.89", "1234567.89"],
    ["1'234.56", "1234.56"],
    ["1,234.56 $-", "-1234.56"],
    ["€,5", "0.5"],
    [
      "$9,007,199,254,740,993.0000000000000001",
      "9007199254740993.0000000000000001",
    ],
  ])("compares valid %s as exactly %s", (formatted, number) => {
    expect(compareExactNumericValues(formatted, number)).toBe(0);
  });

  it("keeps adjacent formatted decimals distinct without rounding", () => {
    expect(
      compareExactNumericValues(
        "$9,007,199,254,740,993.0000000000000001",
        "$9,007,199,254,740,993.0000000000000002",
      ),
    ).toBe(-1);
  });

  it("does not report false DynamoDB number duplicates for malformed text", () => {
    expect(hasDuplicateDynamoDbNumbers(["1.2.", "12"])).toBe(false);
    expect(hasDuplicateDynamoDbNumbers(["12.0", "12"])).toBe(true);
  });
});
