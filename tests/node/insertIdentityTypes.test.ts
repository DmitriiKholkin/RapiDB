import mssql from "mssql";
import oracledb from "oracledb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import { prepareInsertVerification } from "../../src/extension/table/insertVerification";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

const key: ColumnTypeMeta = {
  name: "id",
  type: "NUMBER",
  nativeType: "NUMBER",
  category: "integer",
  nullable: false,
  isPrimaryKey: true,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};
const amount = {
  ...key,
  name: "amount",
  type: "NUMBER(12,2)",
  nativeType: "NUMBER(12,2)",
  category: "decimal" as const,
  isPrimaryKey: false,
};
afterEach(() => vi.restoreAllMocks());

describe("B02 INSERT identity type regressions", () => {
  it.each([
    ["DATE", "2026-10-02 12:34:56"],
    ["TIMESTAMP(9)", "2026-10-02 12:34:56.123456789"],
    ["TIMESTAMP(9) WITH TIME ZONE", "2026-10-02 12:34:56.123456789 +05:30"],
    [
      "TIMESTAMP(9) WITH LOCAL TIME ZONE",
      "2026-10-02 12:34:56.123456789 +05:30",
    ],
  ])("Oracle explicit composite %s/NUMBER PK remains supported", async (nativeType, value) => {
    const config = { id: "types", name: "types", type: "oracle" as const };
    const driver = new OracleDriver(config);
    const temporal = {
      ...key,
      name: "business_day",
      type: nativeType,
      nativeType,
      category: "datetime" as const,
    };
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
    } as never;
    const service = new TableMutationService(manager, {
      getColumns: async () => [temporal, { ...key, name: "metric_id" }, amount],
    });
    const plan = await service.prepareInsertRow(
      config.id,
      "",
      "APP",
      "daily_metrics",
      { business_day: value, metric_id: 7, amount: "1.25" },
    );
    expect(plan.operation.captureIdentity?.oracleOutTypes).toEqual([
      "string",
      "string",
    ]);
    expect(plan.operation.sql).toContain("RETURNING TO_CHAR(");
    if (nativeType.startsWith("TIMESTAMP"))
      expect(plan.operation.sql).toContain("FF9");
    if (nativeType.includes("TIME ZONE"))
      expect(plan.operation.sql).toContain("TIMESTAMP(9) WITH TIME ZONE");
    // A trigger may assign a more precise/different key than the input Date bind.
    // The final read must use RETURNING text verbatim, never re-coerce it to Date.
    const captured =
      nativeType === "DATE"
        ? "+2026-10-03 12:34:56"
        : "+2026-10-03 07:04:56.987654321";
    let mismatch = false;
    const closeCursor = vi.fn(async () => {});
    const execute = vi.fn(
      async (
        sql: string,
        binds: unknown[] = [],
        options?: oracledb.ExecuteOptions,
      ) => {
        if (sql.startsWith("ALTER SESSION")) return {};
        expect(options?.autoCommit).toBe(false);
        if (sql.startsWith("INSERT")) {
          expect(binds.slice(-2)).toEqual([
            { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 32767 },
            { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 32767 },
          ]);
          return { rowsAffected: 1, outBinds: [[captured], ["7"]] };
        }
        expect(binds).toEqual([captured, "7"]);
        expect(options?.resultSet).toBe(true);
        expect(sql).toContain(
          nativeType === "DATE"
            ? "SYYYY-MM-DD HH24:MI:SS"
            : "SYYYY-MM-DD HH24:MI:SS.FF9",
        );
        const metadata = [
          {
            name: "__col_0",
            dbType: oracledb.DB_TYPE_NUMBER,
            precision: 38,
            scale: 0,
          },
          {
            name: "__col_1",
            dbType: oracledb.DB_TYPE_NUMBER,
            precision: 12,
            scale: 2,
          },
        ];
        const values = ["7", mismatch ? "99" : "1.25"].map(
          (raw, index) =>
            options?.fetchTypeHandler?.(metadata[index])?.converter?.(raw) ??
            raw,
        );
        let fetched = false;
        return {
          resultSet: {
            metaData: metadata,
            getRows: vi.fn(async () => {
              const rows = fetched ? [] : [values];
              fetched = true;
              return rows;
            }),
            close: closeCursor,
          },
        };
      },
    );
    const connection = {
      execute,
      commit: vi.fn(async () => {}),
      rollback: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      break: vi.fn(async () => {}),
    };
    const lease = vi.fn(async () => connection);
    (driver as unknown as { pool: unknown }).pool = { getConnection: lease };
    const outside = vi
      .spyOn(driver, "query")
      .mockRejectedValue(new Error("Outside reads forbidden"));
    await service.executePreparedInsertPlan(plan);
    expect(connection.commit).toHaveBeenCalledOnce();
    expect(connection.rollback).not.toHaveBeenCalled();
    expect(closeCursor).toHaveBeenCalledOnce();
    expect(lease).toHaveBeenCalledOnce();
    expect(outside).not.toHaveBeenCalled();
    mismatch = true;
    await expect(service.executePreparedInsertPlan(plan)).rejects.toThrow(
      /INSERT verification failed/,
    );
    expect(connection.commit).toHaveBeenCalledOnce();
    expect(connection.rollback).toHaveBeenCalledOnce();
    expect(closeCursor).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["sysname", "nvarchar", 256, 0, 0, "nvarchar(128)", "item"],
    ["Item]Type;--", "varchar", 80, 0, 0, "varchar(80)", "item"],
    ["MetricType", "decimal", 17, 28, 10, "decimal(28,10)", "1.25"],
  ] as const)("MSSQL resolves %s PK through real catalog metadata and enrichment", async (alias, base, length, precision, scale, resolved, value) => {
    const config = { id: "types", name: "types", type: "mssql" as const };
    const driver = new MSSQLDriver(config);
    const metadataQuery = vi.fn(async (_sql: string) => ({
      recordset: [
        {
          COLUMN_NAME: "id",
          DATA_TYPE: alias,
          BASE_TYPE: base,
          COLLATION_NAME: base.includes("char")
            ? "Latin1_General_100_BIN2_UTF8"
            : null,
          max_length: length,
          precision,
          scale,
          IS_NULLABLE: 0,
          IS_PK: 1,
        },
        {
          COLUMN_NAME: "amount",
          DATA_TYPE: "decimal",
          BASE_TYPE: "decimal",
          max_length: 9,
          precision: 10,
          scale: 2,
          IS_NULLABLE: 0,
          IS_PK: 0,
        },
      ],
    }));
    const request = { input: vi.fn().mockReturnThis(), query: metadataQuery };
    (driver as unknown as { pool: unknown }).pool = {
      request: () => request,
      config: { options: {} },
    };
    const columns = await driver.describeColumns("db]name", "dbo", "t");
    const manager = {
      getConnection: () => config,
      getDriver: () => driver,
    } as never;
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    const plan = await service.prepareInsertRow(
      config.id,
      "db]name",
      "dbo",
      "t",
      { id: value, amount: "1.25" },
    );
    expect(columns[0].type).toBe(alias);
    expect(columns[0].nativeType).toBe(resolved);
    expect(plan.operation.sql).toContain(`[__col_0] ${resolved}`);
    expect(plan.operation.sql).not.toContain(alias);
    if (base.includes("char")) {
      expect(plan.operation.sql).toContain(
        "COLLATE [Latin1_General_100_BIN2_UTF8]",
      );
    }
    expect(metadataQuery.mock.calls[0]).toBeDefined();
    const catalogSql = vi.mocked(request.query).mock.calls[0][0];
    // Both alias and base lookups are scoped to the selected database.
    expect(catalogSql).toContain("[db]]name].sys.types ut");
    expect(catalogSql).toContain("[db]]name].sys.types bt");
    expect(catalogSql).not.toContain("TYPE_NAME(");
    vi.spyOn(driver, "getMutationAtomicityRisk").mockResolvedValue(null);
    vi.spyOn(mssql.Transaction.prototype, "begin").mockResolvedValue(
      undefined as never,
    );
    const commit = vi
      .spyOn(mssql.Transaction.prototype, "commit")
      .mockResolvedValue(undefined as never);
    const rollback = vi
      .spyOn(mssql.Transaction.prototype, "rollback")
      .mockResolvedValue(undefined as never);
    let mismatch = false;
    const execute = vi.fn(async (sql: string) =>
      sql.startsWith("DECLARE")
        ? {
            recordsets: [[{ __col_0: value }]],
            output: { __rapidb_affected_rows: 1 },
          }
        : {
            recordset: [[value, mismatch ? "99" : "1.25"]],
            columns: [[{ name: "id" }, { name: "amount" }]],
          },
    );
    vi.spyOn(mssql.Transaction.prototype, "request").mockImplementation(
      () => ({ input: vi.fn(), output: vi.fn(), query: execute }) as never,
    );
    await service.executePreparedInsertPlan(plan);
    expect(commit).toHaveBeenCalledOnce();
    expect(rollback).not.toHaveBeenCalled();
    mismatch = true;
    await expect(service.executePreparedInsertPlan(plan)).rejects.toThrow(
      /INSERT verification failed/,
    );
    expect(commit).toHaveBeenCalledOnce();
    expect(rollback).toHaveBeenCalledOnce();
  });

  it.each([
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "bit",
    "real",
    "float(53)",
    "decimal(38,18)",
    "numeric(28,10)",
    "money",
    "smallmoney",
    "char(20)",
    "varchar(100)",
    "nchar(20)",
    "nvarchar(100)",
    "sysname",
    "binary(16)",
    "varbinary(100)",
    "timestamp",
    "rowversion",
    "uniqueidentifier",
    "date",
    "time(7)",
    "datetime",
    "smalldatetime",
    "datetime2(7)",
    "datetimeoffset(7)",
    "hierarchyid",
    "sql_variant",
  ])("MSSQL capture supports indexable built-in %s", (nativeType) => {
    const driver = new MSSQLDriver({
      id: "types",
      name: "types",
      type: "mssql",
    });
    const column = {
      ...key,
      type: nativeType,
      nativeType,
      category: driver.mapTypeCategory(nativeType),
    };
    const cols = [
      column,
      { ...amount, type: "decimal(12,2)", nativeType: "decimal(12,2)" },
    ];
    const operation = buildInsertRowOperation(
      driver,
      "db",
      "dbo",
      "t",
      { amount: "1.25" },
      cols,
    );
    const verification = prepareInsertVerification(
      driver,
      "mssql",
      "db",
      "dbo",
      "t",
      { amount: "1.25" },
      cols,
      operation,
    );
    expect(operation.captureIdentity?.mssqlSelect).toBeDefined();
    expect(verification?.identity).toBeDefined();
    if (column.category === "float") {
      expect(operation.captureIdentity?.mssqlSelect).toContain(
        "CONVERT(varbinary(8)",
      );
      expect(verification?.sql).toContain("CONVERT(varbinary(8)");
    }
    if (nativeType === "sql_variant") {
      expect(operation.captureIdentity?.mssqlSelect).toContain(
        "SQL_VARIANT_PROPERTY",
      );
      expect(verification?.sql).toContain("SQL_VARIANT_PROPERTY");
    }
    if (nativeType === "hierarchyid") {
      expect(operation.captureIdentity?.mssqlSelect).toContain(".ToString()");
      expect(verification?.sql).toContain(".ToString()");
    }
  });

  it.each([
    "NUMBER",
    "NUMBER(38,0)",
    "NUMBER(38,18)",
    "INTEGER",
    "SMALLINT",
    "FLOAT(126)",
    "BINARY_FLOAT",
    "BINARY_DOUBLE",
    "CHAR(20)",
    "NCHAR(20)",
    "VARCHAR2(100)",
    "NVARCHAR2(100)",
    "RAW(16)",
    "DATE",
    "TIMESTAMP(9)",
    "TIMESTAMP(9) WITH TIME ZONE",
    "TIMESTAMP(9) WITH LOCAL TIME ZONE",
    "INTERVAL YEAR(9) TO MONTH",
    "INTERVAL DAY(9) TO SECOND(9)",
    "ROWID",
    "UROWID",
    "BOOLEAN",
  ])("Oracle capture supports scalar PK type %s", (nativeType) => {
    const driver = new OracleDriver({
      id: "types",
      name: "types",
      type: "oracle",
    });
    const column = {
      ...key,
      type: nativeType,
      nativeType,
      category: driver.mapTypeCategory(nativeType),
    };
    const operation = buildInsertRowOperation(
      driver,
      "",
      "APP",
      "t",
      { amount: "1.25" },
      [column, amount],
    );
    const verification = prepareInsertVerification(
      driver,
      "oracle",
      "",
      "APP",
      "t",
      { amount: "1.25" },
      [column, amount],
      operation,
    );
    expect(operation.captureIdentity?.oracleOutTypes).toEqual([
      nativeType === "RAW(16)" ? "buffer" : "string",
    ]);
    expect(verification?.identity).toBeDefined();
    const expression = operation.sql.split(" RETURNING ")[1].split(" INTO ")[0];
    expect(verification?.sql).toContain(`${expression} = :1`);
  });

  it.each([
    "int); DROP TABLE t;--",
    "nvarchar(100) COLLATE bad;--",
    "[dbo].[unresolved]",
  ])("MSSQL refuses arbitrary type SQL %s before DML", (nativeType) => {
    const driver = new MSSQLDriver({
      id: "types",
      name: "types",
      type: "mssql",
    });
    const column = { ...key, type: nativeType, nativeType };
    expect(() =>
      buildInsertRowOperation(
        driver,
        "db",
        "dbo",
        "t",
        { amount: "1.25" },
        [column, amount],
        { backend: "mssql", columns: [column] },
      ),
    ).toThrow(/cannot capture/);
  });

  it("MSSQL quotes catalog collation identifiers instead of interpolating type SQL", () => {
    const driver = new MSSQLDriver({
      id: "types",
      name: "types",
      type: "mssql",
    });
    const column = {
      ...key,
      type: "varchar(20)",
      nativeType: "varchar(20)",
      collation: "name]; DROP TABLE t;--",
    };
    const operation = buildInsertRowOperation(
      driver,
      "db",
      "dbo",
      "t",
      { amount: "1.25" },
      [column, amount],
      { backend: "mssql", columns: [column] },
    );
    expect(operation.sql).toContain("COLLATE [name]]; DROP TABLE t;--]");
  });
});
