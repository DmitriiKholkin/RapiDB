import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { prepareApplyChangesPlan } from "../../src/extension/table/tableMutationExecution";
import { buildUpdateRowSql } from "../../src/extension/table/updateSql";

const driver = new MSSQLDriver({
  id: "variant-snapshot",
  name: "Variant snapshot",
  type: "mssql",
});
const variant: ColumnTypeMeta = {
  name: "V",
  type: "sql_variant",
  nativeType: "sql_variant",
  category: "other",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: false,
  filterOperators: [],
  valueSemantics: "plain",
};
const id: ColumnTypeMeta = {
  ...variant,
  name: "ID",
  type: "int",
  nativeType: "int",
  category: "integer",
  isPrimaryKey: true,
};
const manager = {
  getDriver: () => driver,
  getConnection: () => ({ id: "variant-snapshot", type: "mssql" }),
} as unknown as ConnectionManager;

function snapshot(overrides: Record<string, unknown> = {}) {
  return {
    __rapidbSqlVariant: 1,
    displayValue: "1",
    baseType: "int",
    precision: 10,
    scale: 0,
    maxLength: 4,
    collation: null,
    binaryHex: "01000000",
    ...overrides,
  };
}

function prepare(original: unknown, nextValue = "2") {
  return prepareApplyChangesPlan(
    manager,
    "variant-snapshot",
    "db",
    "dbo",
    "t",
    [
      {
        primaryKeys: { ID: 1 },
        changes: { V: nextValue },
        originalValues: { V: original },
      },
    ],
    [id, variant],
  );
}

describe("MSSQL sql_variant snapshot boundary", () => {
  it("rejects raw temporal metadata before preview or parameter binding", () => {
    const injected =
      "7)) WHERE 1=0; SELECT 424242 AS [variant_injection]; UPDATE [db].[dbo].[t] SET [V] = CAST(NULL AS datetime2(7";
    // Pre-fix this passes effective-column checkPersistedEdit, preview
    // materialization and the binder's placeholder-count check unchanged.
    expect(() =>
      prepare(
        snapshot({ baseType: "datetime2", scale: injected }),
        "2026-10-19 12:34:56",
      ),
    ).toThrow(TypeError);
  });

  const invalidNumbers = [
    "7",
    "7)); SELECT 424242; --",
    undefined,
    NaN,
    Infinity,
    -Infinity,
    1.5,
    Number.MAX_SAFE_INTEGER + 1,
    true,
    {},
    [],
  ];
  for (const field of ["precision", "scale", "maxLength"]) {
    it.each(
      invalidNumbers.map((value) => ({ value })),
    )(`rejects ${field}=$value even for an unused subtype property`, ({
      value,
    }) => {
      const forged = snapshot({ [field]: value });
      expect(() =>
        driver.buildMutationValueExpr(variant, "2", "update", forged),
      ).toThrow(TypeError);
      expect(() =>
        driver.buildOriginalValueComparisonWithParams(variant, forged, 1),
      ).toThrow(TypeError);
      expect(() => prepare(forged)).toThrow(TypeError);
      expect(
        driver.checkPersistedEdit(variant, "1", { persistedValue: forged })?.ok,
      ).toBe(false);
    });
  }

  it.each(
    [undefined, 123, false, {}, [], NaN].map((collation) => ({ collation })),
  )("rejects a non-string/non-null collation: $collation", ({ collation }) => {
    const forged = snapshot({ collation });
    expect(() => prepare(forged)).toThrow(TypeError);
    expect(
      driver.checkPersistedEdit(variant, "1", { persistedValue: forged })?.ok,
    ).toBe(false);
  });

  it.each([
    null,
    "",
    "Latin1_General_CI_AS; SELECT 424242",
    "[Latin1_General_CI_AS]",
  ])("requires a safe nonempty character collation: %s", (collation) => {
    expect(() =>
      prepare(snapshot({ baseType: "varchar", collation }), "abc"),
    ).toThrow(TypeError);
  });

  it.each([
    { baseType: "decimal", precision: 0 },
    { baseType: "decimal", precision: 39 },
    { baseType: "numeric", scale: -1 },
    { baseType: "numeric", precision: 5, scale: 6 },
    { baseType: "float", precision: 25 },
    { baseType: "binary", maxLength: 0 },
    { baseType: "varbinary", maxLength: 8001 },
    { baseType: "varchar", maxLength: -1 },
    { baseType: "char", maxLength: 8001 },
    { baseType: "nvarchar", maxLength: 3 },
    { baseType: "nchar", maxLength: 8001 },
    { baseType: "time", scale: -1 },
    { baseType: "datetime2", scale: 8 },
    { baseType: "datetimeoffset", scale: 8 },
    { baseType: "decimal", precision: null },
    { baseType: "numeric", scale: null },
    { baseType: "varbinary", maxLength: null },
    { baseType: "time", scale: null },
  ])("rejects out-of-range or missing required subtype metadata: %j", (metadata) => {
    // These pass the integer-or-null snapshot shape check; type construction
    // must still fail closed before trying to validate the edited value.
    expect(() =>
      driver.buildMutationValueExpr(variant, "1", "update", snapshot(metadata)),
    ).toThrow(TypeError);
  });

  it.each([
    {
      baseType: "decimal",
      precision: 38,
      scale: 2,
      value: "12.34",
      type: "decimal(38,2)",
    },
    {
      baseType: "numeric",
      precision: 1,
      scale: 0,
      value: "2",
      type: "decimal(1,0)",
    },
    { baseType: "float", precision: 24, value: "1.5", type: "float(24)" },
    { baseType: "float", precision: 53, value: "1.5", type: "float(53)" },
    { baseType: "binary", maxLength: 1, value: "0x01", type: "binary(1)" },
    {
      baseType: "varbinary",
      maxLength: 8000,
      value: "0x01",
      type: "varbinary(8000)",
    },
    {
      baseType: "varchar",
      maxLength: 8000,
      value: "abc",
      type: "varchar(8000)",
    },
    { baseType: "char", maxLength: 3, value: "abc", type: "char(3)" },
    {
      baseType: "nvarchar",
      maxLength: 8000,
      value: "雪'",
      type: "nvarchar(4000)",
    },
    { baseType: "nchar", maxLength: 2, value: "雪", type: "nchar(1)" },
    { baseType: "time", scale: 0, value: "12:34:56", type: "time(0)" },
    {
      baseType: "datetime2",
      scale: 7,
      value: "2026-10-19 12:34:56.1234567",
      type: "datetime2(7)",
    },
    {
      baseType: "datetimeoffset",
      scale: 7,
      value: "2026-10-19 12:34:56.1234567 +02:00",
      type: "datetimeoffset(7)",
    },
    {
      baseType: "int",
      precision: null,
      scale: null,
      maxLength: null,
      value: "2",
      type: "int",
    },
  ])("preserves valid $type snapshots and readback", ({
    value,
    type,
    ...metadata
  }) => {
    const original = snapshot({
      ...metadata,
      collation: "Latin1_General_CI_AS",
      displayValue: value,
    });
    const expression = driver.buildMutationValueExpr(
      variant,
      value,
      "update",
      original,
    );
    expect(expression?.sql).toContain(type);
    expect(expression?.params).toEqual([value]);
    const plan = prepare(original, value);
    expect(plan.executable).toBe(true);
    if (!plan.executable) throw new Error("Expected a plan");
    expect(plan.plan.previewStatements[0]).toContain(type);
    expect(
      driver.checkPersistedEdit(variant, value, {
        originalValue: original,
        persistedValue: JSON.stringify(original),
      })?.ok,
    ).toBe(true);
  });

  it("normalizes server numeric strings before validating snapshots", () => {
    const original = driver.formatTableDataValue(
      "12.34",
      variant,
      {
        __rapidb_sql_variant_0_base_type: "decimal",
        __rapidb_sql_variant_0_precision: "5",
        __rapidb_sql_variant_0_scale: "2",
        __rapidb_sql_variant_0_max_length: "5",
        __rapidb_sql_variant_0_collation: null,
        __rapidb_sql_variant_0_binary_hex: "0102",
      },
      0,
    );
    expect(original).toMatchObject({
      precision: 5,
      scale: 2,
      maxLength: 5,
      collation: null,
    });
    expect(
      driver.buildMutationValueExpr(variant, "12.35", "update", original)?.sql,
    ).toBe("CAST(? AS decimal(5,2))");
  });

  it("does not restrict SQL punctuation in safely quoted table identifiers", () => {
    const operation = buildUpdateRowSql(
      driver,
      "db",
      "dbo",
      "t]; -- ?",
      { ID: 1 },
      { V: "2" },
      [id, variant],
      { V: snapshot() },
    );
    if (!operation) throw new Error("Expected update");
    expect(
      driver.materializePreviewSql(operation.sql, operation.params),
    ).toContain("UPDATE [db].[dbo].[t]]; -- ?] SET [V] = CAST('2' AS int)");
  });
});
