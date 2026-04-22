import {
  CANONICAL_FIXTURE_SCHEMA,
  type DbEngineId,
} from "../contracts/testingContracts.ts";
import {
  CANONICAL_FIXTURE_DATASET,
  CHILD_RECORD_ROWS,
  COMPOSITE_LINK_ROWS,
  EXACT_NUMERIC_SAMPLE_ROWS,
  EXPORT_ROWS,
  FIXTURE_ROUTINE_NAMES,
  FIXTURE_TABLE_NAMES,
  PAGINATION_ROWS,
  PARENT_RECORD_ROWS,
  physicalizeFixtureIdentifier,
  resolveFixtureNamespace,
  TRANSACTION_PROBE_ROWS,
} from "./canonicalDataset.ts";

type SqlValueKind =
  | "integer"
  | "decimal"
  | "boolean"
  | "timestamp"
  | "string"
  | "text";

interface SeedColumnSpec {
  name: string;
  kind: SqlValueKind;
}

interface SeedTableSpec {
  logicalTableName: string;
  columns: readonly SeedColumnSpec[];
  rows: readonly object[];
  batchSize?: number;
}

export interface FixtureMaterializationPlan {
  engineId: DbEngineId;
  namespace: ReturnType<typeof resolveFixtureNamespace>;
  bootstrapStatements: readonly string[];
  resetStatements: readonly string[];
  seedStatements: readonly string[];
}

function quoteIdentifier(engineId: DbEngineId, identifier: string): string {
  switch (engineId) {
    case "postgres":
    case "sqlite":
      return `"${identifier.replace(/"/g, '""')}"`;
    case "mysql":
      return `\`${identifier.replace(/`/g, "``")}\``;
    case "mssql":
      return `[${identifier.replace(/]/g, "]]")}]`;
    case "oracle":
      return physicalizeFixtureIdentifier(engineId, identifier);
  }
}

function qualifiedName(engineId: DbEngineId, logicalName: string): string {
  const namespace = resolveFixtureNamespace(engineId);
  const physicalName = physicalizeFixtureIdentifier(engineId, logicalName);

  switch (engineId) {
    case "postgres":
    case "mssql":
      return `${quoteIdentifier(engineId, namespace.physicalSchemaName)}.${quoteIdentifier(engineId, physicalName)}`;
    case "mysql":
    case "oracle":
    case "sqlite":
      return quoteIdentifier(engineId, physicalName);
  }
}

function escapeString(value: string): string {
  return value.replace(/'/g, "''");
}

function normalizeTimestampLiteral(value: string): string {
  return value.replace("T", " ").replace(/Z$/, "");
}

function renderValue(
  engineId: DbEngineId,
  kind: SqlValueKind,
  value: unknown,
): string {
  if (value === null) {
    return "NULL";
  }

  switch (kind) {
    case "integer":
    case "decimal":
      return String(value);
    case "boolean":
      if (engineId === "postgres") {
        return value ? "TRUE" : "FALSE";
      }
      return value ? "1" : "0";
    case "timestamp": {
      const iso = String(value);
      const sqlTimestamp = normalizeTimestampLiteral(iso);
      switch (engineId) {
        case "postgres":
          return `TIMESTAMPTZ '${escapeString(iso)}'`;
        case "mysql":
        case "sqlite":
          return `'${escapeString(sqlTimestamp)}'`;
        case "mssql":
          return `CAST('${escapeString(sqlTimestamp)}' AS DATETIME2(3))`;
        case "oracle":
          return `TO_TIMESTAMP('${escapeString(sqlTimestamp)}', 'YYYY-MM-DD HH24:MI:SS.FF3')`;
      }
      throw new Error(
        `[RapiDB:testdb] Unsupported timestamp engine ${engineId}.`,
      );
    }
    case "string":
    case "text":
      return `'${escapeString(String(value))}'`;
  }
}

function buildInsertStatements(
  engineId: DbEngineId,
  specification: SeedTableSpec,
): string[] {
  const batchSize = specification.batchSize ?? 24;
  const target = qualifiedName(engineId, specification.logicalTableName);
  const columnList = specification.columns
    .map((column) =>
      quoteIdentifier(
        engineId,
        physicalizeFixtureIdentifier(engineId, column.name),
      ),
    )
    .join(", ");
  const statements: string[] = [];

  for (let index = 0; index < specification.rows.length; index += batchSize) {
    const batch = specification.rows.slice(index, index + batchSize);

    if (engineId === "oracle") {
      const intoClauses = batch
        .map((row) => {
          const record = row as Record<string, unknown>;
          const values = specification.columns
            .map((column) =>
              renderValue(engineId, column.kind, record[column.name]),
            )
            .join(", ");
          return `  INTO ${target} (${columnList}) VALUES (${values})`;
        })
        .join("\n");
      statements.push(`INSERT ALL\n${intoClauses}\nSELECT 1 FROM DUAL`);
      continue;
    }

    const valuesList = batch
      .map((row) => {
        const record = row as Record<string, unknown>;
        const values = specification.columns
          .map((column) =>
            renderValue(engineId, column.kind, record[column.name]),
          )
          .join(", ");
        return `(${values})`;
      })
      .join(",\n");

    statements.push(
      `INSERT INTO ${target} (${columnList}) VALUES\n${valuesList}`,
    );
  }

  return statements;
}

function createSchemaStatement(engineId: DbEngineId): string | null {
  const namespace = resolveFixtureNamespace(engineId);

  switch (engineId) {
    case "postgres":
      return `CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(engineId, namespace.physicalSchemaName)}`;
    case "mssql":
      return `IF SCHEMA_ID(N'${namespace.physicalSchemaName}') IS NULL EXEC(N'CREATE SCHEMA ${quoteIdentifier(engineId, namespace.physicalSchemaName)} AUTHORIZATION [dbo]')`;
    default:
      return null;
  }
}

function createResetStatements(engineId: DbEngineId): string[] {
  if (engineId === "postgres") {
    return [
      `DROP SCHEMA IF EXISTS ${quoteIdentifier(engineId, CANONICAL_FIXTURE_SCHEMA.schemaName)} CASCADE`,
      `CREATE SCHEMA ${quoteIdentifier(engineId, CANONICAL_FIXTURE_SCHEMA.schemaName)}`,
    ];
  }

  if (engineId === "sqlite") {
    return [
      `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.childRecords)}`,
      `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.parentRecords)}`,
      `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.compositeLinks)}`,
      `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.exactNumericSamples)}`,
      `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.paginationRows)}`,
      `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.exportRows)}`,
      `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.transactionProbe)}`,
      `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.fixtureRows)}`,
    ];
  }

  if (engineId === "oracle") {
    return [
      `BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE ${physicalizeFixtureIdentifier(engineId, FIXTURE_ROUTINE_NAMES.adjustBalance)}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP FUNCTION ${physicalizeFixtureIdentifier(engineId, FIXTURE_ROUTINE_NAMES.totalAmount)}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES.childRecords)} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES.parentRecords)} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES.compositeLinks)} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES.exactNumericSamples)} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES.paginationRows)} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES.exportRows)} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES.transactionProbe)} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
      `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${physicalizeFixtureIdentifier(engineId, FIXTURE_TABLE_NAMES.fixtureRows)} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
    ];
  }

  const resetStatements = [
    engineId === "mysql"
      ? `DROP PROCEDURE IF EXISTS ${quoteIdentifier(engineId, FIXTURE_ROUTINE_NAMES.adjustBalance)}`
      : `DROP PROCEDURE IF EXISTS ${qualifiedName(engineId, FIXTURE_ROUTINE_NAMES.adjustBalance)}`,
    engineId === "mysql"
      ? `DROP FUNCTION IF EXISTS ${quoteIdentifier(engineId, FIXTURE_ROUTINE_NAMES.totalAmount)}`
      : `DROP FUNCTION IF EXISTS ${qualifiedName(engineId, FIXTURE_ROUTINE_NAMES.totalAmount)}`,
    `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.childRecords)}`,
    `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.parentRecords)}`,
    `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.compositeLinks)}`,
    `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.exactNumericSamples)}`,
    `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.paginationRows)}`,
    `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.exportRows)}`,
    `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.transactionProbe)}`,
    `DROP TABLE IF EXISTS ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.fixtureRows)}`,
  ];

  const schemaStatement = createSchemaStatement(engineId);
  return schemaStatement
    ? [schemaStatement, ...resetStatements]
    : resetStatements;
}

function createTableStatements(engineId: DbEngineId): string[] {
  const tableType = {
    fixtureRowsAmount:
      engineId === "oracle"
        ? "NUMBER(18,2)"
        : engineId === "postgres"
          ? "NUMERIC(18,2)"
          : engineId === "mssql"
            ? "DECIMAL(18,2)"
            : "DECIMAL(18,2)",
    numericAmount:
      engineId === "oracle"
        ? "NUMBER(18,6)"
        : engineId === "postgres"
          ? "NUMERIC(18,6)"
          : engineId === "mssql"
            ? "DECIMAL(18,6)"
            : "DECIMAL(18,6)",
    ratioAmount:
      engineId === "oracle"
        ? "NUMBER(12,4)"
        : engineId === "postgres"
          ? "NUMERIC(12,4)"
          : engineId === "mssql"
            ? "DECIMAL(12,4)"
            : "DECIMAL(12,4)",
    boolType:
      engineId === "postgres"
        ? "BOOLEAN"
        : engineId === "mssql"
          ? "BIT"
          : engineId === "oracle"
            ? "NUMBER(1)"
            : engineId === "sqlite"
              ? "INTEGER"
              : "BOOLEAN",
    stringType:
      engineId === "mssql"
        ? "NVARCHAR(200)"
        : engineId === "oracle"
          ? "VARCHAR2(200 CHAR)"
          : "VARCHAR(200)",
    textType:
      engineId === "mssql"
        ? "NVARCHAR(MAX)"
        : engineId === "oracle"
          ? "CLOB"
          : "TEXT",
    timestampType:
      engineId === "postgres"
        ? "TIMESTAMPTZ"
        : engineId === "mssql"
          ? "DATETIME2(3)"
          : engineId === "oracle"
            ? "TIMESTAMP(3)"
            : engineId === "sqlite"
              ? "TEXT"
              : "DATETIME(3)",
  };
  const integerType = engineId === "mssql" ? "INT" : "INTEGER";
  const codeType = engineId === "mssql" ? "NVARCHAR(64)" : "VARCHAR(64)";
  const statusType = engineId === "mssql" ? "NVARCHAR(32)" : "VARCHAR(32)";

  const fixtureRowsTable =
    engineId === "oracle"
      ? `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.fixtureRows)} (ID NUMBER(10) NOT NULL, DISPLAY_NAME ${tableType.stringType} NOT NULL, AMOUNT ${tableType.fixtureRowsAmount} NOT NULL, IS_ACTIVE ${tableType.boolType} NOT NULL CHECK (IS_ACTIVE IN (0, 1)), CREATED_AT ${tableType.timestampType} NOT NULL, NOTES ${tableType.textType} NULL, CONSTRAINT PK_FIXTURE_ROWS PRIMARY KEY (ID))`
      : `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.fixtureRows)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "display_name"))} ${tableType.stringType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "amount"))} ${tableType.fixtureRowsAmount} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "is_active"))} ${tableType.boolType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "created_at"))} ${tableType.timestampType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "notes"))} ${tableType.textType} NULL, CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "pk_fixture_rows"))} PRIMARY KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))}))`;

  const parentRecordsTable =
    engineId === "oracle"
      ? `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.parentRecords)} (ID NUMBER(10) NOT NULL, CODE VARCHAR2(64 CHAR) NOT NULL, DISPLAY_NAME ${tableType.stringType} NOT NULL, CREATED_AT ${tableType.timestampType} NOT NULL, CONSTRAINT PK_PARENT_RECORDS PRIMARY KEY (ID), CONSTRAINT UQ_PARENT_RECORDS_CODE UNIQUE (CODE))`
      : `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.parentRecords)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "code"))} ${codeType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "display_name"))} ${tableType.stringType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "created_at"))} ${tableType.timestampType} NOT NULL, CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "pk_parent_records"))} PRIMARY KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))}), CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "uq_parent_records_code"))} UNIQUE (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "code"))}))`;

  const childRecordsTable =
    engineId === "oracle"
      ? `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.childRecords)} (ID NUMBER(10) NOT NULL, PARENT_ID NUMBER(10) NOT NULL, CHILD_NAME ${tableType.stringType} NOT NULL, STATUS VARCHAR2(32 CHAR) NOT NULL, AMOUNT ${tableType.fixtureRowsAmount} NOT NULL, CONSTRAINT PK_CHILD_RECORDS PRIMARY KEY (ID), CONSTRAINT FK_CHILD_PARENT FOREIGN KEY (PARENT_ID) REFERENCES ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.parentRecords)} (ID))`
      : `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.childRecords)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "parent_id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "child_name"))} ${tableType.stringType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "status"))} ${statusType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "amount"))} ${tableType.fixtureRowsAmount} NOT NULL, CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "pk_child_records"))} PRIMARY KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))}), CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "fk_child_parent"))} FOREIGN KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "parent_id"))}) REFERENCES ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.parentRecords)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))}))`;

  const compositeLinksTable =
    engineId === "oracle"
      ? `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.compositeLinks)} (TENANT_ID NUMBER(10) NOT NULL, EXTERNAL_ID NUMBER(10) NOT NULL, DESCRIPTION ${tableType.stringType} NOT NULL, CREATED_AT ${tableType.timestampType} NOT NULL, CONSTRAINT PK_COMP_LINKS PRIMARY KEY (TENANT_ID, EXTERNAL_ID))`
      : `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.compositeLinks)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "tenant_id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "external_id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "description"))} ${tableType.stringType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "created_at"))} ${tableType.timestampType} NOT NULL, CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "pk_comp_links"))} PRIMARY KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "tenant_id"))}, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "external_id"))}))`;

  const exactNumericTable =
    engineId === "oracle"
      ? `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.exactNumericSamples)} (ID NUMBER(10) NOT NULL, NUMERIC_LABEL VARCHAR2(64 CHAR) NOT NULL, EXACT_AMOUNT ${tableType.numericAmount} NOT NULL, RATIO ${tableType.ratioAmount} NOT NULL, CONSTRAINT PK_EXACT_NUMERIC PRIMARY KEY (ID))`
      : `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.exactNumericSamples)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "numeric_label"))} ${codeType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "exact_amount"))} ${tableType.numericAmount} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "ratio"))} ${tableType.ratioAmount} NOT NULL, CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "pk_exact_numeric"))} PRIMARY KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))}))`;

  const paginationTable =
    engineId === "oracle"
      ? `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.paginationRows)} (ID NUMBER(10) NOT NULL, PAGE_GROUP NUMBER(10) NOT NULL, TITLE ${tableType.stringType} NOT NULL, CREATED_AT ${tableType.timestampType} NOT NULL, CONSTRAINT PK_PAGINATION_ROWS PRIMARY KEY (ID))`
      : `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.paginationRows)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "page_group"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "title"))} ${tableType.stringType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "created_at"))} ${tableType.timestampType} NOT NULL, CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "pk_pagination_rows"))} PRIMARY KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))}))`;

  const exportRowsTable =
    engineId === "oracle"
      ? `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.exportRows)} (ID NUMBER(10) NOT NULL, EXPORT_CODE VARCHAR2(32 CHAR) NOT NULL, PAYLOAD ${tableType.textType} NOT NULL, DECIMAL_AMOUNT ${tableType.numericAmount} NOT NULL, CREATED_AT ${tableType.timestampType} NOT NULL, CONSTRAINT PK_EXPORT_ROWS PRIMARY KEY (ID))`
      : `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.exportRows)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "export_code"))} ${statusType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "payload"))} ${tableType.textType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "decimal_amount"))} ${tableType.numericAmount} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "created_at"))} ${tableType.timestampType} NOT NULL, CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "pk_export_rows"))} PRIMARY KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))}))`;

  const transactionProbeTable =
    engineId === "oracle"
      ? `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.transactionProbe)} (ID NUMBER(10) NOT NULL, ACCOUNT_NAME ${tableType.stringType} NOT NULL, BALANCE ${tableType.fixtureRowsAmount} NOT NULL, UPDATED_AT ${tableType.timestampType} NOT NULL, CONSTRAINT PK_TRANSACTION_PROBE PRIMARY KEY (ID))`
      : `CREATE TABLE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.transactionProbe)} (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))} ${integerType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "account_name"))} ${tableType.stringType} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "balance"))} ${tableType.fixtureRowsAmount} NOT NULL, ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "updated_at"))} ${tableType.timestampType} NOT NULL, CONSTRAINT ${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "pk_transaction_probe"))} PRIMARY KEY (${quoteIdentifier(engineId, physicalizeFixtureIdentifier(engineId, "id"))}))`;

  return [
    fixtureRowsTable,
    parentRecordsTable,
    childRecordsTable,
    compositeLinksTable,
    exactNumericTable,
    paginationTable,
    exportRowsTable,
    transactionProbeTable,
  ];
}

function createRoutineStatements(engineId: DbEngineId): string[] {
  switch (engineId) {
    case "sqlite":
      return [];
    case "postgres":
      return [
        `CREATE OR REPLACE FUNCTION ${qualifiedName(engineId, FIXTURE_ROUTINE_NAMES.totalAmount)}() RETURNS NUMERIC(18,2) LANGUAGE SQL AS $$ SELECT COALESCE(SUM(amount), 0) FROM ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.fixtureRows)} $$`,
        `CREATE OR REPLACE PROCEDURE ${qualifiedName(engineId, FIXTURE_ROUTINE_NAMES.adjustBalance)}(p_id INTEGER, p_delta NUMERIC(18,2)) LANGUAGE SQL AS $$ UPDATE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.transactionProbe)} SET balance = balance + p_delta, updated_at = CURRENT_TIMESTAMP WHERE id = p_id $$`,
      ];
    case "mysql":
      return [
        `CREATE FUNCTION ${quoteIdentifier(engineId, FIXTURE_ROUTINE_NAMES.totalAmount)}() RETURNS DECIMAL(18,2) DETERMINISTIC READS SQL DATA RETURN (SELECT COALESCE(SUM(amount), 0) FROM ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.fixtureRows)})`,
        `CREATE PROCEDURE ${quoteIdentifier(engineId, FIXTURE_ROUTINE_NAMES.adjustBalance)}(IN p_id INT, IN p_delta DECIMAL(18,2)) BEGIN UPDATE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.transactionProbe)} SET balance = balance + p_delta, updated_at = UTC_TIMESTAMP(3) WHERE id = p_id; END`,
      ];
    case "mssql":
      return [
        `CREATE OR ALTER FUNCTION ${qualifiedName(engineId, FIXTURE_ROUTINE_NAMES.totalAmount)}() RETURNS DECIMAL(18,2) AS BEGIN DECLARE @value DECIMAL(18,2); SELECT @value = COALESCE(SUM(amount), 0) FROM ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.fixtureRows)}; RETURN COALESCE(@value, 0); END`,
        `CREATE OR ALTER PROCEDURE ${qualifiedName(engineId, FIXTURE_ROUTINE_NAMES.adjustBalance)} @p_id INT, @p_delta DECIMAL(18,2) AS BEGIN SET NOCOUNT ON; UPDATE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.transactionProbe)} SET balance = balance + @p_delta, updated_at = SYSUTCDATETIME() WHERE id = @p_id; END`,
      ];
    case "oracle":
      return [
        `CREATE OR REPLACE FUNCTION ${qualifiedName(engineId, FIXTURE_ROUTINE_NAMES.totalAmount)} RETURN NUMBER IS v_total NUMBER(18,2); BEGIN SELECT NVL(SUM(AMOUNT), 0) INTO v_total FROM ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.fixtureRows)}; RETURN v_total; END;`,
        `CREATE OR REPLACE PROCEDURE ${qualifiedName(engineId, FIXTURE_ROUTINE_NAMES.adjustBalance)}(p_id IN NUMBER, p_delta IN NUMBER) IS BEGIN UPDATE ${qualifiedName(engineId, FIXTURE_TABLE_NAMES.transactionProbe)} SET BALANCE = BALANCE + p_delta, UPDATED_AT = SYSTIMESTAMP WHERE ID = p_id; END;`,
      ];
  }
}

function createSeedStatements(engineId: DbEngineId): string[] {
  const sessionStatements: string[] = [];
  if (engineId === "mysql") {
    sessionStatements.push("SET time_zone = '+00:00'");
  }
  if (engineId === "oracle") {
    sessionStatements.push("ALTER SESSION SET TIME_ZONE = '+00:00'");
  }

  const tables: SeedTableSpec[] = [
    {
      logicalTableName: FIXTURE_TABLE_NAMES.fixtureRows,
      columns: [
        { name: "id", kind: "integer" },
        { name: "display_name", kind: "string" },
        { name: "amount", kind: "decimal" },
        { name: "is_active", kind: "boolean" },
        { name: "created_at", kind: "timestamp" },
        { name: "notes", kind: "text" },
      ],
      rows: CANONICAL_FIXTURE_SCHEMA.seedRows,
      batchSize: 16,
    },
    {
      logicalTableName: FIXTURE_TABLE_NAMES.parentRecords,
      columns: [
        { name: "id", kind: "integer" },
        { name: "code", kind: "string" },
        { name: "display_name", kind: "string" },
        { name: "created_at", kind: "timestamp" },
      ],
      rows: PARENT_RECORD_ROWS,
      batchSize: 16,
    },
    {
      logicalTableName: FIXTURE_TABLE_NAMES.childRecords,
      columns: [
        { name: "id", kind: "integer" },
        { name: "parent_id", kind: "integer" },
        { name: "child_name", kind: "string" },
        { name: "status", kind: "string" },
        { name: "amount", kind: "decimal" },
      ],
      rows: CHILD_RECORD_ROWS,
      batchSize: 16,
    },
    {
      logicalTableName: FIXTURE_TABLE_NAMES.compositeLinks,
      columns: [
        { name: "tenant_id", kind: "integer" },
        { name: "external_id", kind: "integer" },
        { name: "description", kind: "string" },
        { name: "created_at", kind: "timestamp" },
      ],
      rows: COMPOSITE_LINK_ROWS,
      batchSize: 16,
    },
    {
      logicalTableName: FIXTURE_TABLE_NAMES.exactNumericSamples,
      columns: [
        { name: "id", kind: "integer" },
        { name: "numeric_label", kind: "string" },
        { name: "exact_amount", kind: "decimal" },
        { name: "ratio", kind: "decimal" },
      ],
      rows: EXACT_NUMERIC_SAMPLE_ROWS,
      batchSize: 16,
    },
    {
      logicalTableName: FIXTURE_TABLE_NAMES.paginationRows,
      columns: [
        { name: "id", kind: "integer" },
        { name: "page_group", kind: "integer" },
        { name: "title", kind: "string" },
        { name: "created_at", kind: "timestamp" },
      ],
      rows: PAGINATION_ROWS,
      batchSize: 24,
    },
    {
      logicalTableName: FIXTURE_TABLE_NAMES.exportRows,
      columns: [
        { name: "id", kind: "integer" },
        { name: "export_code", kind: "string" },
        { name: "payload", kind: "text" },
        { name: "decimal_amount", kind: "decimal" },
        { name: "created_at", kind: "timestamp" },
      ],
      rows: EXPORT_ROWS,
      batchSize: 24,
    },
    {
      logicalTableName: FIXTURE_TABLE_NAMES.transactionProbe,
      columns: [
        { name: "id", kind: "integer" },
        { name: "account_name", kind: "string" },
        { name: "balance", kind: "decimal" },
        { name: "updated_at", kind: "timestamp" },
      ],
      rows: TRANSACTION_PROBE_ROWS,
      batchSize: 16,
    },
  ];

  return [
    ...sessionStatements,
    ...tables.flatMap((table) => buildInsertStatements(engineId, table)),
    ...createRoutineStatements(engineId),
  ];
}

function createBootstrapStatements(engineId: DbEngineId): string[] {
  if (engineId !== "mssql") {
    return [];
  }

  return [
    `IF DB_ID(N'rapidb_mssql_db') IS NULL CREATE DATABASE [rapidb_mssql_db]`,
    `IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = N'rapidb_test_user') CREATE LOGIN [rapidb_test_user] WITH PASSWORD = N'mssql_pass123', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF`,
    `IF NOT EXISTS (SELECT 1 FROM [rapidb_mssql_db].sys.database_principals WHERE name = N'rapidb_test_user') EXEC(N'USE [rapidb_mssql_db]; CREATE USER [rapidb_test_user] FOR LOGIN [rapidb_test_user]')`,
    `IF NOT EXISTS (SELECT 1 FROM [rapidb_mssql_db].sys.database_role_members drm JOIN [rapidb_mssql_db].sys.database_principals roles ON roles.principal_id = drm.role_principal_id JOIN [rapidb_mssql_db].sys.database_principals members ON members.principal_id = drm.member_principal_id WHERE roles.name = N'db_owner' AND members.name = N'rapidb_test_user') EXEC(N'USE [rapidb_mssql_db]; ALTER ROLE [db_owner] ADD MEMBER [rapidb_test_user]')`,
  ];
}

export function buildFixtureMaterializationPlan(
  engineId: DbEngineId,
): FixtureMaterializationPlan {
  return {
    engineId,
    namespace: resolveFixtureNamespace(engineId),
    bootstrapStatements: createBootstrapStatements(engineId),
    resetStatements: [
      ...createResetStatements(engineId),
      ...createTableStatements(engineId),
    ],
    seedStatements: createSeedStatements(engineId),
  };
}

export const CANONICAL_FIXTURE_PLAN_MANIFEST = {
  datasetId: CANONICAL_FIXTURE_DATASET.datasetId,
  supportedEngines: ["sqlite", "postgres", "mysql", "mssql", "oracle"] as const,
};
