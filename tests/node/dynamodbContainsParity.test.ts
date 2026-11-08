import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import type { FilterExpression } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";

function harness(needle: string, sampleSet: boolean, sampleList = false) {
  const row = (pk: string, value: AttributeValue) => ({
    pk: { S: pk },
    value,
    marker: { L: [{ S: "row" }] },
  });
  const scalar = row("row:scalar", { S: needle });
  const set = row("row:set", { SS: [needle] });
  const list = row("row:list", { L: [{ S: needle }] });
  const items = [
    ...(sampleList
      ? [list, scalar, set]
      : [...(sampleSet ? [set, scalar] : [scalar, set]), list]),
    row("row:bool-list", { L: [{ BOOL: needle === "true" }] }),
  ];
  const send = vi.fn(
    async (command: {
      constructor: { name: string };
      input: Record<string, unknown>;
    }) => {
      if (command.constructor.name === "DescribeTableCommand")
        return {
          Table: {
            KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
            AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
          },
        };
      if (command.constructor.name !== "ScanCommand")
        throw new Error("Unexpected command");
      const input = command.input;
      const value = Object.values(
        (input.ExpressionAttributeValues ?? {}) as Record<
          string,
          AttributeValue
        >,
      )[0]?.S;
      const selected = input.FilterExpression
        ? items.filter(
            ({ value: actual }) =>
              actual.S?.includes(value ?? "") ||
              actual.SS?.includes(value ?? "") ||
              actual.L?.some((member) => member.S === value),
          )
        : items;
      return input.Select === "COUNT"
        ? { Count: selected.length }
        : { Items: selected };
    },
  );
  const driver = new DynamoDBDriver({
    id: "parity",
    name: "Parity",
    type: "dynamodb",
    awsRegion: "us-east-1",
  });
  Object.assign(driver, { connected: true, client: { send } });
  const manager = {
    getConnection: () => ({ type: "dynamodb" }),
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  return { read: new TableReadService(manager), send };
}

describe("DynamoDB Contains keeps its operand type across read plans", () => {
  it("keeps a plain true as a string when the first sampled value is a list", async () => {
    const { read } = harness("true", false, true);
    const page = await read.getPage(
      "parity",
      "us-east-1",
      "us-east-1",
      "probe",
      1,
      25,
      [{ column: "value", operator: "like", value: "true" }],
      { column: "pk", direction: "asc" },
      false,
    );
    expect(
      page.columns.find((column) => column.name === "value")?.nativeType,
    ).toBe("list");
    expect(page.rows.map((row) => row.pk).sort()).toEqual([
      "row:list",
      "row:scalar",
      "row:set",
    ]);
  });
  it.each([
    { needle: "true", sampleSet: false },
    { needle: "false", sampleSet: false },
    { needle: "true", sampleSet: true },
    { needle: "false", sampleSet: true },
    { needle: "BOOL(true)", sampleSet: false },
    { needle: '"true"', sampleSet: false },
    { needle: "N(3)", sampleSet: false },
  ])("preserves S/SS/L string matches: $needle, sampled set=$sampleSet", async ({
    needle,
    sampleSet,
  }) => {
    const { read, send } = harness(needle, sampleSet);
    const filters: FilterExpression[] = [
      { column: "value", operator: "like", value: needle },
    ];
    const expected = ["row:list", "row:scalar", "row:set"];
    for (const skipCount of [true, false]) {
      for (const mode of ["native", "sort", "client-filter"] as const) {
        const page = await read.getPage(
          "parity",
          "us-east-1",
          "us-east-1",
          "probe",
          1,
          25,
          mode === "client-filter"
            ? [...filters, { column: "marker", operator: "like", value: "row" }]
            : filters,
          mode === "sort" ? { column: "pk", direction: "asc" } : null,
          skipCount,
        );
        expect(page.columns.find((c) => c.name === "value")?.nativeType).toBe(
          sampleSet ? "string set" : "string",
        );
        expect(
          page.rows.map((row) => row.pk).sort(),
          `${mode}, skipCount=${skipCount}`,
        ).toEqual(expected);
        if (!skipCount) expect(page.totalCount).toBe(3);
      }
    }
    if (!sampleSet)
      expect(
        send.mock.calls.some(
          ([command]) =>
            (
              command.input.ExpressionAttributeValues as
                | Record<string, AttributeValue>
                | undefined
            )?.[":v0"]?.S === needle,
        ),
      ).toBe(true);
  });
});
