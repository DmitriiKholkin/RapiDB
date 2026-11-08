import { NumberValueImpl } from "@aws-sdk/util-dynamodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { getDynamoDbNativeCellValidationError } from "../../src/shared/dynamodbCellInput";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

const driver = new DynamoDBDriver({ id: "qa", name: "QA", type: "dynamodb" });
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
    expect(() => driver.coerceInputValue(text, column)).toThrow(/Binary Set/);
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
  ])("rejects invalid %s literals before update/insert, even without modal validation", async (nativeType, input) => {
    expect(getDynamoDbNativeCellValidationError(input, nativeType)).toMatch(
      /DynamoDB/,
    );
    const localDriver = new DynamoDBDriver({
      id: "qa",
      name: "QA",
      type: "dynamodb",
    });
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
      isPrimaryKey: true,
    } as ColumnTypeMeta;
    const manager = {
      getConnection: () => ({ type: "dynamodb" }),
      getDriver: () => localDriver,
    } as unknown as ConnectionManager;
    const update = vi.spyOn(localDriver, "updateRows");
    const insert = vi.spyOn(localDriver, "insertRow");
    const service = new TableMutationService(manager, {
      getColumns: async () => [key, column],
    });
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
            originalValues: {
              value: nativeType === "string set" ? "<<'old'>>" : "<<1>>",
            },
          },
        ],
        [key, column],
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
      service.prepareInsertRow("qa", "qa", "", "items", {
        pk: "two",
        value: input,
      }),
    ).rejects.toThrow(/DynamoDB/);
    expect(update).not.toHaveBeenCalled();
    expect(insert).not.toHaveBeenCalled();
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
