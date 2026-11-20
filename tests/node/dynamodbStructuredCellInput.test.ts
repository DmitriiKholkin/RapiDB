import {
  DescribeTableCommand,
  PutItemCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import { NumberValueImpl } from "@aws-sdk/util-dynamodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import {
  DYNAMODB_NATIVE_CELL_VALUE_PREFIX,
  getDynamoDbNativeCellValidationError,
} from "../../src/shared/dynamodbCellInput";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

const driver = new DynamoDBDriver({ id: "qa", name: "QA", type: "dynamodb" });

function createWriteFixture(nativeType: string) {
  const config = { id: "qa", name: "QA", type: "dynamodb" as const };
  const localDriver = new DynamoDBDriver(config);
  const send = vi.fn(async (command: object) => {
    if (command instanceof DescribeTableCommand) {
      return {
        Table: {
          KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
          AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
        },
      };
    }
    if (command instanceof PutItemCommand) return {};
    if (command instanceof UpdateItemCommand)
      return { Attributes: { pk: { S: "one" } } };
    throw new Error("Unexpected DynamoDB SDK command");
  });
  Object.assign(localDriver, { client: { send }, connected: true });
  const column = {
    name: "value",
    nativeType,
    type: nativeType,
    category: nativeType === "map" ? "json" : "array",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
  } as ColumnTypeMeta;
  const key = {
    ...column,
    name: "pk",
    type: "string",
    nativeType: "string",
    category: "text",
    nullable: false,
    isPrimaryKey: true,
  } as ColumnTypeMeta;
  const columns = [key, column];
  const manager = {
    getConnection: () => config,
    getDriver: () => localDriver,
  } as unknown as ConnectionManager;
  const service = new TableMutationService(manager, {
    getColumns: async () => columns,
  });
  return { columns, manager, send, service };
}

describe("DynamoDB structured display/edit contract", () => {
  it.each([
    "[]",
    "[42]",
    '["not-base64!"]',
    '["0xGG"]',
  ])("rejects invalid binary set JSON %s before writing", (text) => {
    const column = {
      name: "bytes",
      nativeType: "binary set",
      category: "array",
    } as ColumnTypeMeta;
    expect(() => driver.coerceInputValue(text, column)).toThrow(/binary set/);
    expect(
      getDynamoDbNativeCellValidationError(text, "binary set"),
    ).toBeTruthy();
  });
  it.each([
    " ",
    "\t",
    "\n",
  ])("preserves a whitespace-prefixed String envelope through update, original condition and insert (%j)", async (space) => {
    const text = `${space}DynamoDB native value (nested sets/binary): {"N":"7"}`;
    const key = {
      name: "pk",
      type: "string",
      nativeType: "string",
      category: "text",
      isPrimaryKey: true,
      nullable: false,
    } as ColumnTypeMeta;
    const value = {
      ...key,
      name: "value",
      isPrimaryKey: false,
      nullable: true,
    };
    const columns = [key, value];
    const config = { id: "qa", name: "QA", type: "dynamodb" as const };
    const localDriver = new DynamoDBDriver(config);
    const manager = {
      getConnection: () => config,
      getDriver: () => localDriver,
    } as unknown as ConnectionManager;
    expect(localDriver.formatOutputValue(text, value)).toBe(text);
    for (const [before, after] of [
      ["before", text],
      [text, "after"],
    ]) {
      const prepared = prepareApplyChangesPlan(
        manager,
        "qa",
        "qa",
        "",
        "items",
        [
          {
            primaryKeys: { pk: "one" },
            changes: { value: after },
            originalValues: { value: before },
            originalTypes: {
              value: { nativeType: "string", category: "text" },
            },
          },
        ],
        columns,
      );
      if (!prepared.executable) throw new Error("Expected executable plan");
      expect(
        JSON.parse(prepared.plan.previewStatements[0])
          .ExpressionAttributeValues,
      ).toMatchObject({
        ":u0": { S: after },
        ":o0": { S: before },
      });
    }
    Object.assign(localDriver, {
      getTableSchema: vi.fn().mockResolvedValue({ keys: ["pk"] }),
    });
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    const insert = await service.prepareInsertRow("qa", "qa", "", "items", {
      pk: "two",
      value: text,
    });
    expect(JSON.parse(insert.previewStatements[0]).Item.value).toEqual({
      S: text,
    });
  });

  it.each([
    ["string set", "<<1, true>>"],
    ["string set", "<<'unclosed>>"],
    ["string set", '["tag", 9007199254740993]'],
    ["string set", '["tag", true]'],
    ["string set", '["tag", null]'],
    ["string set", '["tag", {"key":"value"}]'],
    ["string set", '["tag", ["nested"]]'],
    ["string set", "[]"],
    ["string set", '{"tag":"value"}'],
    ["string set", '["tag",]'],
    ["number set", "<<1, invalid>>"],
    ["number set", "[]"],
    ["number set", "[ ]"],
    ["number set", '["invalid"]'],
    ["list", "{}"],
    ["list", "null"],
    ["list", '"text"'],
    ["list", "1"],
    ["map", 'DynamoDB native value (nested sets/binary): {"N":"1"}'],
    ["map", ' DynamoDB native value (nested sets/binary): {"L":[]} '],
    ["list", 'DynamoDB native value (nested sets/binary): {"M":{}}'],
    [
      "number set",
      'DynamoDB native value (nested sets/binary): {"SS":["one"]}',
    ],
    ["number set", "<<1, 1>>"],
    ["number set", "<<1.0, 10e-1>>"],
    ["number set", "[1,1.0]"],
    ["number set", '["-0", "0e10"]'],
    [
      "number set",
      'DynamoDB native value (nested sets/binary): {"NS":["1","1.00"]}',
    ],
    ["binary set", '["AR=="]'],
    ["binary set", "<<AQJ=>>"],
    ["binary set", '["AQ"]'],
    ["binary set", '["AQ==\\nAQ=="]'],
    ["binary set", '[" "]'],
    ["binary set", "<<>>"],
    ["binary set", '["AQ==","AQ=="]'],
    ["binary set", '["0xAB","qw=="]'],
    ["binary set", "<<0xAB, \\xab>>"],
    ["binary set", '<<"", 0x>>'],
    ["binary set", `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"BS":[]}`],
    ["binary set", `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"BS":["AR=="]}`],
    ["binary set", `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"BS":["0x01ab"]}`],
    ["binary set", `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"BS":["",""]}`],
    [
      "map",
      `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"M":{"bytes":{"BS":["AQ==","AQ=="]}}}`,
    ],
    ["number set", "[1e10000]"],
    ["number set", "[1e126]"],
    ["number set", "<<-1e126>>"],
    ["number set", '[ "1e-131" ]'],
    ["number set", "<<0.9e-130>>"],
    ["number set", "1, -1e-131"],
    ["number set", "[01]"],
    ["number set", "[123456789012345678901234567890123456789]"],
    ["number set", `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"NS":["10e125"]}`],
    ["list", `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"L":[{"NS":["-1e-131"]}]}`],
  ])("rejects invalid %s input %s before issuing SDK commands", async (nativeType, input) => {
    expect(getDynamoDbNativeCellValidationError(input, nativeType)).toMatch(
      /DynamoDB/,
    );
    const { columns, manager, send, service } = createWriteFixture(nativeType);
    expect(() =>
      prepareApplyChangesPlan(
        manager,
        "qa",
        "qa",
        "",
        "items",
        [
          {
            primaryKeys: { pk: "one" },
            changes: { value: input },
          },
        ],
        columns,
      ),
    ).toThrow(/DynamoDB/);
    await expect(
      service.updateRow(
        "qa",
        "qa",
        "",
        "items",
        { pk: "one" },
        { value: input },
      ),
    ).rejects.toThrow(/DynamoDB/);
    await expect(
      service.insertRow("qa", "qa", "", "items", {
        pk: "two",
        value: input,
      }),
    ).rejects.toThrow(/DynamoDB/);
    expect(send).not.toHaveBeenCalled();
  });

  it.each([
    [
      "string set",
      ' ["  spaced  ", "", "9007199254740993", "true", "null", "a,b", "a\\\\b", "true"] ',
      {
        SS: [
          "  spaced  ",
          "",
          "9007199254740993",
          "true",
          "null",
          "a,b",
          "a\\b",
        ],
      },
    ],
    [
      "string set",
      "<<'O''Brien', \"a,b\", '', 'O''Brien'>>",
      { SS: ["O'Brien", "a,b", ""] },
    ],
    [
      "string set",
      `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"SS":["", "  spaced  ", ""]}`,
      { SS: ["", "  spaced  "] },
    ],
    ["binary set", '["", " 0xAB ", "AQI="]', { BS: ["", "qw==", "AQI="] }],
    ["binary set", "<<0x, \\xAB, 'AQI='>>", { BS: ["", "qw==", "AQI="] }],
    [
      "binary set",
      `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"BS":["","0xAB"]}`,
      { BS: ["", "0xAB"] },
    ],
    [
      "number set",
      "[9.9999999999999999999999999999999999999e125,-1e-130,9007199254740993,-0e10000]",
      {
        NS: [
          "9.9999999999999999999999999999999999999e125",
          "-1e-130",
          "9007199254740993",
          "-0e10000",
        ],
      },
    ],
    [
      "number set",
      "<<+000.10000e-129, -1e125, 7.500>>",
      { NS: ["+000.10000e-129", "-1e125", "7.500"] },
    ],
    ["number set", "1e-130, -1e125", { NS: ["1e-130", "-1e125"] }],
    [
      "number set",
      `${DYNAMODB_NATIVE_CELL_VALUE_PREFIX}{"NS":["1e-130","-1e125","-0"]}`,
      { NS: ["1e-130", "-1e125", "-0"] },
    ],
  ] as const)("writes valid %s input %s without changing the values", async (nativeType, input, expected) => {
    expect(getDynamoDbNativeCellValidationError(input, nativeType)).toBeNull();
    const { send, service } = createWriteFixture(nativeType);
    await service.insertRow("qa", "qa", "", "items", {
      pk: "two",
      value: input,
    });
    await service.updateRow(
      "qa",
      "qa",
      "",
      "items",
      { pk: "one" },
      { value: input },
    );
    const commands = send.mock.calls.map(([command]) => command);
    const put = commands.find((command) => command instanceof PutItemCommand);
    const update = commands.find(
      (command) => command instanceof UpdateItemCommand,
    );
    const attribute =
      "BS" in expected
        ? { BS: expected.BS.map((value) => Buffer.from(value, "base64")) }
        : expected;
    expect(put?.input.Item?.value).toEqual(attribute);
    expect(update?.input.ExpressionAttributeValues?.[":u0"]).toEqual(attribute);
  });

  it("classifies huge exponents, including signed zero, without losing valid compensated magnitudes", () => {
    const validate = (input: string) =>
      getDynamoDbNativeCellValidationError(input, "number set");
    const huge = "9".repeat(100_000);
    const leadingZeros = "0".repeat(100_000);
    expect(validate(`[1e${huge}]`)).toMatch(/overflow/);
    expect(validate(`[9.${"9".repeat(38)}e125]`)).toMatch(/overflow/);
    expect(validate(`<<-1e-${huge}>>`)).toMatch(/underflow/);
    expect(validate(`[-0e-${huge}, 1]`)).toBeNull();
    expect(validate(`[-0e-${huge}, 0]`)).toMatch(/unique/);
    expect(validate("[-123456789012345678901234567890123456789]")).toMatch(
      /38 significant digits/,
    );
    expect(validate(`<<0.${leadingZeros}1e100001>>`)).toBeNull();
    expect(validate(`[1e-${leadingZeros}130]`)).toBeNull();
    expect(validate(`["1${"0".repeat(150)}e-25"]`)).toBeNull();
  });

  it.each([
    [
      "[9007199254740992,9007199254740993]",
      ["9007199254740992", "9007199254740993"],
    ],
    [
      '["1.0000000000000000001","1.0000000000000000002"]',
      ["1.0000000000000000001", "1.0000000000000000002"],
    ],
  ])("keeps distinct exact Number Set values in %s", (input, expected) => {
    const column = {
      name: "n",
      nativeType: "number set",
      category: "array",
    } as ColumnTypeMeta;
    expect(
      getDynamoDbNativeCellValidationError(input as string, "number set"),
    ).toBeNull();
    const coerced = driver.coerceInputValue(input, column);
    expect(
      Reflect.get(driver, "toAttributeValue").call(driver, coerced),
    ).toEqual({ NS: expected });
  });

  it.each([
    ["string set", "array", new Set(["O'Brien", "a,b", "a\\b", ""])],
    [
      "number set",
      "array",
      new Set([
        NumberValueImpl.from("9007199254740993.1250"),
        NumberValueImpl.from("7.50"),
      ]),
    ],
    ["binary set", "array", new Set([Buffer.from([1]), Buffer.from([2])])],
    [
      "map",
      "json",
      {
        bytes: Buffer.from([1]),
        numbers: new Set([NumberValueImpl.from("9007199254740993")]),
      },
    ],
    ["list", "array", [Buffer.from([1]), new Set(["a", "b"])]],
  ] as const)("preserves native %s through frontend validation and backend coercion", (nativeType, category, value) => {
    const column = { name: "value", nativeType, category } as ColumnTypeMeta;
    const display = driver.formatOutputValue(value, column) as string;
    expect(
      getDynamoDbNativeCellValidationError(display, nativeType),
    ).toBeNull();
    const coerced = driver.coerceInputValue(display, column);
    const toAttributeValue = Reflect.get(driver, "toAttributeValue");
    expect(toAttributeValue.call(driver, coerced)).toEqual(
      toAttributeValue.call(driver, value),
    );
  });
});
