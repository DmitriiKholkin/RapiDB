import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  ColumnTypeMeta,
  TypeCategory,
  ValueSemantics,
} from "../../src/extension/dbDrivers/types";
import { resolveFilterOperators } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { formatScalarValueForDisplay } from "../../src/webview/utils/valueFormatting";

type DriverUnderTest = {
  name: string;
  make: () => {
    mapTypeCategory: (nativeType: string) => TypeCategory;
    coerceInputValue: (value: unknown, column: ColumnTypeMeta) => unknown;
    formatOutputValue: (value: unknown, column: ColumnTypeMeta) => unknown;
    checkPersistedEdit: (
      column: ColumnTypeMeta,
      expectedValue: unknown,
      options?: { persistedValue: unknown },
    ) => { ok: boolean; shouldVerify: boolean; message?: string } | null;
  };
  cases: Array<readonly [nativeType: string, expectedCategory: TypeCategory]>;
};

type DriverProbe = ReturnType<DriverUnderTest["make"]> & {
  getValueSemantics: (
    nativeType: string,
    category: TypeCategory,
  ) => ValueSemantics;
  isFilterable: (nativeType: string, category: TypeCategory) => boolean;
};

function buildColumn(
  driver: DriverProbe,
  nativeType: string,
  category: TypeCategory,
): ColumnTypeMeta {
  const valueSemantics = driver.getValueSemantics(
    nativeType,
    category,
  ) as ValueSemantics;

  const filterable = Boolean(driver.isFilterable(nativeType, category));

  return {
    name: "probe_col",
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    defaultValue: undefined,
    isPrimaryKey: false,
    primaryKeyOrdinal: undefined,
    isForeignKey: false,
    isAutoIncrement: false,
    filterable,
    filterOperators: resolveFilterOperators(category, {
      filterable,
      nullable: true,
    }),
    valueSemantics,
  };
}

type FilterabilityExpectation = {
  nativeType: string;
  expectedFilterable: boolean;
  expectedOperators?: readonly string[];
};

const edgeFilterabilityExpectations: Record<
  DriverUnderTest["name"],
  FilterabilityExpectation[]
> = {
  postgres: [
    {
      nativeType: "point",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
    {
      nativeType: "integer[]",
      expectedFilterable: true,
      expectedOperators: ["like", "is_null", "is_not_null"],
    },
    {
      nativeType: "interval",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
  ],
  mysql: [
    {
      nativeType: "geometry",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
    {
      nativeType: "enum('A','B')",
      expectedFilterable: true,
      expectedOperators: ["like", "in", "is_null", "is_not_null"],
    },
  ],
  mssql: [
    {
      nativeType: "geometry",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
    {
      nativeType: "geography",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
    {
      nativeType: "xml",
      expectedFilterable: true,
      expectedOperators: ["like", "is_null", "is_not_null"],
    },
    {
      nativeType: "image",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
  ],
  oracle: [
    {
      nativeType: "SDO_GEOMETRY",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
    {
      nativeType: "INTERVAL DAY TO SECOND",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
    {
      nativeType: "XMLTYPE",
      expectedFilterable: true,
      expectedOperators: ["like", "is_null", "is_not_null"],
    },
    {
      nativeType: "BLOB",
      expectedFilterable: false,
      expectedOperators: ["is_null", "is_not_null"],
    },
  ],
  sqlite: [],
};

describe("resolveFilterOperators", () => {
  it("includes null operators only for nullable columns", () => {
    expect(
      resolveFilterOperators("text", { filterable: true, nullable: true }),
    ).toEqual(["like", "is_null", "is_not_null"]);
    expect(
      resolveFilterOperators("text", { filterable: true, nullable: false }),
    ).toEqual(["like"]);
  });

  it("returns null-only operators for nullable non-filterable columns", () => {
    expect(
      resolveFilterOperators("spatial", { filterable: false, nullable: true }),
    ).toEqual(["is_null", "is_not_null"]);
    expect(
      resolveFilterOperators("spatial", {
        filterable: false,
        nullable: false,
      }),
    ).toEqual([]);
  });
});

function sampleValueFor(
  category: TypeCategory,
  semantics: ValueSemantics,
  nativeType: string,
): string {
  if (semantics === "bit") {
    return "1";
  }

  const lowered = nativeType.toLowerCase();
  switch (category) {
    case "boolean":
      return "true";
    case "integer":
      return "123";
    case "decimal":
      return "1234.56";
    case "float":
      return "12.34";
    case "date":
      return "2026-04-23";
    case "time":
      return "12:34:56";
    case "datetime":
      return "2026-04-23T12:34:56Z";
    case "binary":
      return "\\x0a";
    case "json":
      return '{"a":1}';
    case "uuid":
      return "123e4567-e89b-12d3-a456-426614174000";
    case "array":
      return "[1,2,3]";
    case "interval":
      return "P1DT2H";
    case "spatial":
      if (lowered.includes("circle")) {
        return '{"x":1,"y":2,"radius":3}';
      }
      return "POINT(1 2)";
    case "enum":
      return "A";
    case "text":
    case "other":
      return "sample";
  }

  return "sample";
}

function persistedSampleFor(
  driverName: DriverUnderTest["name"],
  nativeType: string,
  category: TypeCategory,
  semantics: ValueSemantics,
): unknown {
  const lowered = nativeType.toLowerCase();

  if (semantics === "bit") {
    return lowered.includes("bit") ? 5 : 1;
  }

  switch (category) {
    case "boolean":
      return driverName === "postgres" ? true : 1;
    case "integer":
      return lowered.includes("bigint") ? 123n : 123;
    case "decimal":
      return driverName === "oracle" ? 1234.56 : "1234.56";
    case "float":
      return 12.34;
    case "date":
      if (driverName === "mssql" || driverName === "oracle") {
        return new Date(Date.UTC(2026, 3, 23, 0, 0, 0, 0));
      }
      return "2026-04-23";
    case "time":
      if (driverName === "mssql") {
        return new Date(Date.UTC(1970, 0, 1, 12, 34, 56, 120));
      }
      return "12:34:56";
    case "datetime":
      if (driverName === "mssql" || driverName === "oracle") {
        return new Date(Date.UTC(2026, 3, 23, 12, 34, 56, 123));
      }
      if (driverName === "postgres") {
        return "2026-04-23 12:34:56.123+00";
      }
      if (driverName === "sqlite") {
        return "2026-04-23 12:34:56.123";
      }
      return "2026-04-23 12:34:56.123";
    case "binary":
      return Buffer.from([0x0a, 0x0b, 0x0c]);
    case "json":
      return { a: 1, nested: { ok: true } };
    case "uuid":
      return "123e4567-e89b-12d3-a456-426614174000";
    case "array":
      return [1, 2, 3];
    case "interval":
      if (driverName === "oracle") {
        return "+01 02:03:04.000000";
      }
      return "P1DT2H3M4S";
    case "spatial":
      if (driverName === "postgres" && lowered === "point") {
        return { x: 1, y: 2 };
      }
      return "POINT(1 2)";
    case "enum":
      return "A";
    case "text":
      if (lowered === "xml" || lowered === "xmltype") {
        return "<root><value>1</value></root>";
      }
      return "sample";
    case "other":
      return lowered.includes("bit") ? "101" : "sample";
  }
}

function toEditString(value: unknown): string {
  return value == null ? "" : formatScalarValueForDisplay(value);
}

function buildFilterColumn(
  nativeType: string,
  category: TypeCategory,
): ColumnTypeMeta {
  return {
    name: "probe_col",
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    defaultValue: undefined,
    isPrimaryKey: false,
    primaryKeyOrdinal: undefined,
    isForeignKey: false,
    isAutoIncrement: false,
    filterable: true,
    filterOperators: resolveFilterOperators(category, {
      filterable: true,
      nullable: true,
    }),
    valueSemantics: "plain",
  };
}

const baseConfig = {
  id: "driver-type-coverage",
  name: "Driver Type Coverage",
  host: "127.0.0.1",
  port: 0,
  database: "db",
  username: "user",
  password: "pass",
};

const drivers: DriverUnderTest[] = [
  {
    name: "postgres",
    make: () =>
      new PostgresDriver({ ...baseConfig, type: "pg" } as ConnectionConfig),
    cases: [
      ["boolean", "boolean"],
      ["bool", "boolean"],
      ["smallint", "integer"],
      ["integer", "integer"],
      ["bigint", "integer"],
      ["serial", "integer"],
      ["bigserial", "integer"],
      ["smallserial", "integer"],
      ["oid", "integer"],
      ["xid", "integer"],
      ["cid", "integer"],
      ["real", "float"],
      ["double precision", "float"],
      ["float4", "float"],
      ["float8", "float"],
      ["numeric", "decimal"],
      ["decimal", "decimal"],
      ["money", "decimal"],
      ["date", "date"],
      ["time", "time"],
      ["timetz", "time"],
      ["time with time zone", "time"],
      ["time without time zone", "time"],
      ["timestamp", "datetime"],
      ["timestamp with time zone", "datetime"],
      ["bytea", "binary"],
      ["json", "json"],
      ["jsonb", "json"],
      ["uuid", "uuid"],
      ["point", "spatial"],
      ["line", "spatial"],
      ["lseg", "spatial"],
      ["box", "spatial"],
      ["path", "spatial"],
      ["polygon", "spatial"],
      ["circle", "spatial"],
      ["interval", "interval"],
      ["integer[]", "array"],
      ["_int4", "array"],
      ["array", "array"],
      ["bit", "other"],
      ["varbit", "other"],
      ["inet", "text"],
      ["cidr", "text"],
      ["macaddr", "text"],
      ["macaddr8", "text"],
      ["tsvector", "text"],
      ["tsquery", "text"],
      ["text", "text"],
      ["varchar", "text"],
      ["character varying", "text"],
      ["character", "text"],
      ["name", "text"],
      ["xml", "text"],
    ],
  },
  {
    name: "mysql",
    make: () =>
      new MySQLDriver({ ...baseConfig, type: "mysql" } as ConnectionConfig),
    cases: [
      ["bool", "boolean"],
      ["boolean", "boolean"],
      ["tinyint(1)", "boolean"],
      ["tinyint", "integer"],
      ["smallint", "integer"],
      ["mediumint", "integer"],
      ["int", "integer"],
      ["integer", "integer"],
      ["bigint", "integer"],
      ["bit", "integer"],
      ["year", "integer"],
      ["float", "float"],
      ["double", "float"],
      ["real", "float"],
      ["decimal", "decimal"],
      ["numeric", "decimal"],
      ["date", "date"],
      ["time", "time"],
      ["datetime", "datetime"],
      ["timestamp", "datetime"],
      ["binary", "binary"],
      ["varbinary", "binary"],
      ["tinyblob", "binary"],
      ["blob", "binary"],
      ["mediumblob", "binary"],
      ["longblob", "binary"],
      ["json", "json"],
      ["point", "spatial"],
      ["linestring", "spatial"],
      ["polygon", "spatial"],
      ["multipoint", "spatial"],
      ["multilinestring", "spatial"],
      ["multipolygon", "spatial"],
      ["geometrycollection", "spatial"],
      ["geometry", "spatial"],
      ["enum('A','B')", "enum"],
      ["set('A','B')", "enum"],
      ["char(10)", "text"],
      ["varchar(10)", "text"],
      ["tinytext", "text"],
      ["text", "text"],
      ["mediumtext", "text"],
      ["longtext", "text"],
    ],
  },
  {
    name: "mssql",
    make: () =>
      new MSSQLDriver({ ...baseConfig, type: "mssql" } as ConnectionConfig),
    cases: [
      ["bit", "integer"],
      ["tinyint", "integer"],
      ["smallint", "integer"],
      ["int", "integer"],
      ["bigint", "integer"],
      ["real", "float"],
      ["float", "float"],
      ["decimal(10,2)", "decimal"],
      ["numeric(10,2)", "decimal"],
      ["money", "decimal"],
      ["smallmoney", "decimal"],
      ["date", "date"],
      ["time", "time"],
      ["datetime", "datetime"],
      ["datetime2", "datetime"],
      ["datetimeoffset", "datetime"],
      ["smalldatetime", "datetime"],
      ["timestamp", "binary"],
      ["rowversion", "binary"],
      ["binary", "binary"],
      ["varbinary", "binary"],
      ["image", "binary"],
      ["uniqueidentifier", "uuid"],
      ["text", "text"],
      ["ntext", "text"],
      ["xml", "text"],
      ["geography", "spatial"],
      ["geometry", "spatial"],
      ["hierarchyid", "other"],
      ["sql_variant", "other"],
      ["nvarchar(100)", "text"],
    ],
  },
  {
    name: "oracle",
    make: () =>
      new OracleDriver({
        ...baseConfig,
        type: "oracle",
        serviceName: "FREEPDB1",
      } as ConnectionConfig),
    cases: [
      ["NUMBER", "decimal"],
      ["NUMBER(10,0)", "integer"],
      ["NUMBER(10,2)", "decimal"],
      ["INTEGER", "integer"],
      ["SMALLINT", "integer"],
      ["PLS_INTEGER", "integer"],
      ["BINARY_INTEGER", "integer"],
      ["FLOAT", "float"],
      ["BINARY_FLOAT", "float"],
      ["BINARY_DOUBLE", "float"],
      ["DATE", "datetime"],
      ["TIMESTAMP", "datetime"],
      ["TIMESTAMP WITH TIME ZONE", "datetime"],
      ["INTERVAL DAY TO SECOND", "interval"],
      ["BLOB", "binary"],
      ["RAW(16)", "binary"],
      ["LONG RAW", "binary"],
      ["CLOB", "text"],
      ["NCLOB", "text"],
      ["LONG", "text"],
      ["XMLTYPE", "text"],
      ["SDO_GEOMETRY", "spatial"],
      ["ROWID", "text"],
      ["UROWID", "text"],
      ["VARCHAR2(50)", "text"],
      ["NVARCHAR2(50)", "text"],
      ["CHAR(10)", "text"],
      ["NCHAR(10)", "text"],
    ],
  },
  {
    name: "sqlite",
    make: () =>
      new SQLiteDriver({
        ...baseConfig,
        type: "sqlite",
        filePath: ":memory:",
      } as ConnectionConfig),
    cases: [
      ["", "text"],
      ["TEXT", "text"],
      ["JSON", "json"],
      ["UUID", "uuid"],
      ["INTEGER", "integer"],
      ["INT", "integer"],
      ["BIGINT", "integer"],
      ["SMALLINT", "integer"],
      ["TINYINT", "integer"],
      ["MEDIUMINT", "integer"],
      ["REAL", "float"],
      ["DOUBLE", "float"],
      ["FLOAT", "float"],
      ["NUMERIC", "decimal"],
      ["DECIMAL", "decimal"],
      ["BOOLEAN", "boolean"],
      ["BOOL", "boolean"],
      ["BLOB", "binary"],
      ["DATE", "date"],
      ["TIME", "time"],
      ["DATETIME", "datetime"],
      ["TIMESTAMP", "datetime"],
      ["VARCHAR(50)", "text"],
      ["CLOB", "text"],
      ["DOUBLE PRECISION", "float"],
      ["FLOAT4", "float"],
      ["INT8", "integer"],
      ["UNKNOWN_TYPE", "other"],
    ],
  },
];

describe("driver native type coverage", () => {
  for (const driverCase of drivers) {
    describe(`${driverCase.name} native types`, () => {
      const driver = driverCase.make() as DriverProbe;
      const edgeCases = edgeFilterabilityExpectations[driverCase.name] ?? [];

      for (const [nativeType, expectedCategory] of driverCase.cases) {
        it(`handles "${nativeType}"`, () => {
          const category = driver.mapTypeCategory(nativeType) as TypeCategory;
          expect(category).toBe(expectedCategory);

          const column = buildColumn(driver, nativeType, category);
          expect(typeof column.filterable).toBe("boolean");

          const sampleInput = sampleValueFor(
            category,
            column.valueSemantics,
            nativeType,
          );

          const coerced = driver.coerceInputValue(sampleInput, column);
          const formatted = driver.formatOutputValue(coerced, column);

          const persistedSample = persistedSampleFor(
            driverCase.name,
            nativeType,
            category,
            column.valueSemantics,
          );
          const displayValue = driver.formatOutputValue(
            persistedSample,
            column,
          );
          const editString = toEditString(displayValue);
          const reverseCoerced = driver.coerceInputValue(editString, column);
          const displayAfterReverse = driver.formatOutputValue(
            reverseCoerced,
            column,
          );

          expect(toEditString(displayAfterReverse)).toBe(editString);

          expect(() =>
            driver.checkPersistedEdit(column, coerced, {
              persistedValue: formatted,
            }),
          ).not.toThrow();

          const check = driver.checkPersistedEdit(column, coerced, {
            persistedValue: formatted,
          });
          if (check) {
            expect(typeof check.ok).toBe("boolean");
            expect(typeof check.shouldVerify).toBe("boolean");
          }
        });
      }

      for (const edgeCase of edgeCases) {
        it(`filterability policy for "${edgeCase.nativeType}"`, () => {
          const category = driver.mapTypeCategory(
            edgeCase.nativeType,
          ) as TypeCategory;
          const column = buildColumn(driver, edgeCase.nativeType, category);

          expect(column.filterable).toBe(edgeCase.expectedFilterable);

          if (edgeCase.expectedOperators) {
            expect(column.filterOperators).toEqual(edgeCase.expectedOperators);
          }
        });
      }
    });
  }
});

describe("filter SQL compatibility for complex null-only types", () => {
  it("returns null for MySQL spatial like filters", () => {
    const driver = new MySQLDriver({
      ...baseConfig,
      type: "mysql",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("geometry", "spatial"),
        filterable: false,
        filterOperators: ["is_null", "is_not_null"],
      },
      "like",
      "POINT(1 2)",
      1,
    );

    expect(result).toBeNull();
  });

  it("returns null for MSSQL spatial like filters", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("geometry", "spatial"),
        filterable: false,
        filterOperators: ["is_null", "is_not_null"],
      },
      "like",
      "POINT(1 2)",
      1,
    );

    expect(result).toBeNull();
  });

  it("returns null for Oracle interval like filters", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("INTERVAL DAY TO SECOND", "interval"),
        filterable: false,
        filterOperators: ["is_null", "is_not_null"],
      },
      "like",
      "1 02:03:04",
      1,
    );

    expect(result).toBeNull();
  });

  it("returns null for Oracle RAW like filters", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("RAW(16)", "binary"),
        filterable: false,
        filterOperators: ["is_null", "is_not_null"],
      },
      "like",
      "0x0a0b",
      1,
    );

    expect(result).toBeNull();
  });

  it("returns null for Oracle spatial like filters", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("SDO_GEOMETRY", "spatial"),
        filterable: false,
        filterOperators: ["is_null", "is_not_null"],
      },
      "like",
      "POINT(1 2)",
      1,
    );

    expect(result).toBeNull();
  });

  it("builds PostgreSQL array like filters against the displayed JSON text", () => {
    const driver = new PostgresDriver({
      ...baseConfig,
      type: "pg",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("integer[]", "array"),
        filterable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
      },
      "like",
      "1",
      1,
    );

    expect(result).toEqual({
      sql: 'to_jsonb("probe_col")::text ILIKE $1',
      params: ["%1%"],
    });
  });

  it("builds MySQL array like filters against JSON text", () => {
    const driver = new MySQLDriver({
      ...baseConfig,
      type: "mysql",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("json", "array"),
        filterable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
      },
      "like",
      '"alpha"',
      1,
    );

    expect(result).toEqual({
      sql: "CAST(`probe_col` AS CHAR) LIKE ?",
      params: ['%"alpha"%'],
    });
  });

  it("builds SQLite array like filters against JSON text", () => {
    const driver = new SQLiteDriver({
      ...baseConfig,
      type: "sqlite",
      filePath: ":memory:",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("JSON", "array"),
        filterable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
      },
      "like",
      '"alpha"',
      1,
    );

    expect(result).toEqual({
      sql: '"probe_col" LIKE ?',
      params: ['%"alpha"%'],
    });
  });

  it("builds MSSQL array like filters against JSON text", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("nvarchar(max)", "array"),
        filterable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
      },
      "like",
      '"alpha"',
      1,
    );

    expect(result).toEqual({
      sql: "CHARINDEX(CAST(? AS NVARCHAR(MAX)), CAST([probe_col] AS NVARCHAR(MAX))) > 0",
      params: ['"alpha"'],
    });
  });

  it("builds Oracle array like filters against JSON text", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("JSON", "array"),
        filterable: true,
        filterOperators: ["like", "is_null", "is_not_null"],
      },
      "like",
      '"alpha"',
      1,
    );

    expect(result).toEqual({
      sql: 'UPPER("probe_col") LIKE UPPER(:1)',
      params: ['%"alpha"%'],
    });
  });

  it("returns null for Postgres point like filters", () => {
    const driver = new PostgresDriver({
      ...baseConfig,
      type: "pg",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("point", "spatial"),
        filterable: false,
        filterOperators: ["is_null", "is_not_null"],
      },
      "like",
      "(1.5, 2.5)",
      1,
    );

    expect(result).toBeNull();
  });

  it("returns null for Postgres polygon like filters", () => {
    const driver = new PostgresDriver({
      ...baseConfig,
      type: "pg",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      {
        ...buildFilterColumn("polygon", "spatial"),
        filterable: false,
        filterOperators: ["is_null", "is_not_null"],
      },
      "like",
      "((0, 0), (1, 0), (1, 1))",
      1,
    );

    expect(result).toBeNull();
  });

  it("passes bigint unsigned MAX value as string param to preserve precision", () => {
    const driver = new MySQLDriver({
      ...baseConfig,
      type: "mysql",
    } as ConnectionConfig);
    const BIGINT_U_MAX = "18446744073709551615";
    const result = driver.buildFilterCondition(
      buildFilterColumn("bigint unsigned", "integer"),
      "eq",
      BIGINT_U_MAX,
      1,
    );

    expect(result?.sql).toBe("`probe_col` = ?");
    expect(result?.params).toEqual([BIGINT_U_MAX]);
    expect(typeof result?.params[0]).toBe("string");
  });

  it("passes safe integer bigint value as number param", () => {
    const driver = new MySQLDriver({
      ...baseConfig,
      type: "mysql",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      buildFilterColumn("bigint unsigned", "integer"),
      "eq",
      "42",
      1,
    );

    expect(result?.params).toEqual([42]);
    expect(typeof result?.params[0]).toBe("number");
  });

  it("uses tolerant comparison for Oracle BINARY_FLOAT eq filters", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    const result = driver.buildFilterCondition(
      buildFilterColumn("BINARY_FLOAT", "float"),
      "eq",
      "3.141593",
      1,
    );

    expect(result?.sql).toContain("ABS(TO_BINARY_DOUBLE");
    expect(result?.sql).toContain("GREATEST(:2");
    expect(result?.sql).toContain("TO_BINARY_DOUBLE(:3)");
    expect(result?.params).toHaveLength(4);
    expect(result?.params[0]).toBe(Number.parseFloat("3.141593"));
    expect(typeof result?.params[1]).toBe("number");
    expect(result?.params[2]).toBe(Number.parseFloat("3.141593"));
    expect(typeof result?.params[3]).toBe("number");
  });

  it("formats Oracle BINARY_DOUBLE values with readable precision", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      serviceName: "FREEPDB1",
    } as ConnectionConfig);

    const formatted = driver.formatOutputValue(
      Number.parseFloat("3.141592653589793"),
      {
        ...buildFilterColumn("BINARY_DOUBLE", "float"),
        valueSemantics: "plain",
        nullable: true,
        defaultValue: undefined,
        isPrimaryKey: false,
        primaryKeyOrdinal: undefined,
        isForeignKey: false,
        isAutoIncrement: false,
      },
    );

    expect(formatted).toBe("3.14159265359");
  });
});
