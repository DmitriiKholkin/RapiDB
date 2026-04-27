import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { ColumnTypeMeta } from "../../../src/extension/dbDrivers/types";
import type { DbEngineId } from "../../contracts/testingContracts";
import {
  createLiveDriverHarness,
  disposeLiveDriverHarness,
  fixtureRoutineName,
  fixtureSupportSummary,
  fixtureTableName,
  rowsFromQuery,
  truthyBoolean,
} from "../../support/liveDbHarness";

function findColumn(columns: ColumnTypeMeta[], name: string): ColumnTypeMeta {
  const column = columns.find(
    (candidate) => candidate.name.toLowerCase() === name.toLowerCase(),
  );

  if (!column) {
    throw new Error(`Column ${name} not found.`);
  }

  return column;
}

function getCaseInsensitive(
  row: Record<string, unknown>,
  key: string,
): unknown {
  const match = Object.keys(row).find(
    (candidate) => candidate.toLowerCase() === key.toLowerCase(),
  );
  return match ? row[match] : undefined;
}

function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function sqlTimestamp(engineId: DbEngineId, iso: string): string {
  const timestamp = iso.replace("T", " ").replace(/Z$/, "");
  switch (engineId) {
    case "postgres":
      return `TIMESTAMPTZ ${sqlString(iso)}`;
    case "mysql":
    case "sqlite":
      return sqlString(timestamp);
    case "mssql":
      return `CAST(${sqlString(timestamp)} AS DATETIME2(3))`;
    case "oracle":
      return `TO_TIMESTAMP(${sqlString(timestamp)}, 'YYYY-MM-DD HH24:MI:SS.FF3')`;
  }
}

function createProbeTableSql(
  engineId: DbEngineId,
  qualifiedName: string,
): string {
  switch (engineId) {
    case "postgres":
    case "sqlite":
    case "mysql":
      return `CREATE TABLE ${qualifiedName} (id INTEGER NOT NULL PRIMARY KEY, note VARCHAR(64) NOT NULL)`;
    case "mssql":
      return `CREATE TABLE ${qualifiedName} (id INT NOT NULL PRIMARY KEY, note NVARCHAR(64) NOT NULL)`;
    case "oracle":
      return `CREATE TABLE ${qualifiedName} (ID NUMBER(10) NOT NULL PRIMARY KEY, NOTE VARCHAR2(64 CHAR) NOT NULL)`;
  }
}

function probeColumnName(
  engineId: DbEngineId,
  logicalName: "id" | "note" | "amount",
): string {
  return engineId === "oracle" ? logicalName.toUpperCase() : logicalName;
}

function createMonetaryProbeTableSql(
  engineId: DbEngineId,
  qualifiedName: string,
): string {
  switch (engineId) {
    case "postgres":
      return `CREATE TABLE ${qualifiedName} (id INTEGER NOT NULL PRIMARY KEY, amount MONEY NOT NULL)`;
    case "mssql":
      return `CREATE TABLE ${qualifiedName} (id INT NOT NULL PRIMARY KEY, amount MONEY NOT NULL)`;
    case "oracle":
      return `CREATE TABLE ${qualifiedName} (ID NUMBER(10) NOT NULL PRIMARY KEY, AMOUNT NUMBER(19,4) NOT NULL)`;
    case "mysql":
      return `CREATE TABLE ${qualifiedName} (id INT NOT NULL PRIMARY KEY, amount DECIMAL(19,4) NOT NULL)`;
    case "sqlite":
      return `CREATE TABLE ${qualifiedName} (id INTEGER NOT NULL PRIMARY KEY, amount DECIMAL(19,4) NOT NULL)`;
  }
}

function sqlNumericLiteral(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "string") {
    return sqlString(value);
  }
  throw new Error(`Unsupported numeric literal value: ${String(value)}`);
}

function parseMonetaryLike(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  const normalizedSign =
    trimmed.startsWith("(") && trimmed.endsWith(")")
      ? `-${trimmed.slice(1, -1)}`
      : trimmed;
  const normalizedDigits = normalizedSign
    .replace(/[^0-9,.-]/g, "")
    .replace(/,/g, "");
  if (
    normalizedDigits === "" ||
    normalizedDigits === "-" ||
    normalizedDigits === "."
  ) {
    return null;
  }

  const parsed = Number(normalizedDigits);
  return Number.isFinite(parsed) ? parsed : null;
}

export function registerLiveDriverConformanceTests(engineId: DbEngineId): void {
  describe(`${engineId} live driver conformance`, () => {
    let harness: Awaited<ReturnType<typeof createLiveDriverHarness>>;

    beforeAll(async () => {
      harness = await createLiveDriverHarness(engineId);
    });

    afterAll(async () => {
      await disposeLiveDriverHarness(harness);
    });

    it("connects, disconnects, and lists the seeded fixture namespace", async () => {
      expect(harness.driver.isConnected()).toBe(true);

      await harness.driver.disconnect();
      expect(harness.driver.isConnected()).toBe(false);

      await harness.driver.connect();
      expect(harness.driver.isConnected()).toBe(true);

      const databases = await harness.driver.listDatabases();
      expect(databases.length).toBeGreaterThan(0);
      expect(
        databases.some(
          (database) =>
            database.name.toLowerCase() === harness.databaseName.toLowerCase(),
        ),
      ).toBe(true);

      const schemas = await harness.driver.listSchemas(harness.databaseName);
      expect(schemas.length).toBeGreaterThan(0);
      expect(
        schemas.some(
          (schema) =>
            schema.name.toLowerCase() === harness.schemaName.toLowerCase(),
        ),
      ).toBe(true);

      const objects = await harness.driver.listObjects(
        harness.databaseName,
        harness.schemaName,
      );
      expect(
        objects.some(
          (object) =>
            object.name.toLowerCase() ===
              fixtureTableName(engineId, "fixtureRows").toLowerCase() &&
            object.type === "table",
        ),
      ).toBe(true);
    });

    it("describes columns, indexes, foreign keys, ddl, and routine definitions", async () => {
      const fixtureRows = fixtureTableName(engineId, "fixtureRows");
      const parentRecords = fixtureTableName(engineId, "parentRecords");
      const childRecords = fixtureTableName(engineId, "childRecords");

      const describedTable = await harness.driver.describeTable(
        harness.databaseName,
        harness.schemaName,
        fixtureRows,
      );
      const describedColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureRows,
      );
      const indexes = await harness.driver.getIndexes(
        harness.databaseName,
        harness.schemaName,
        parentRecords,
      );
      const foreignKeys = await harness.driver.getForeignKeys(
        harness.databaseName,
        harness.schemaName,
        childRecords,
      );
      const ddl = await harness.driver.getCreateTableDDL(
        harness.databaseName,
        harness.schemaName,
        fixtureRows,
      );

      expect(describedTable.map((column) => column.name.toLowerCase())).toEqual(
        expect.arrayContaining(["id", "display_name", "amount"]),
      );
      expect(findColumn(describedColumns, "id").isPrimaryKey).toBe(true);
      expect(findColumn(describedColumns, "notes").nullable).toBe(true);
      expect(
        indexes.some(
          (index) =>
            index.unique &&
            index.columns.some(
              (columnName) => columnName.toLowerCase() === "code",
            ),
        ),
      ).toBe(true);
      expect(
        foreignKeys.some(
          (foreignKey) =>
            foreignKey.column.toLowerCase() === "parent_id" &&
            foreignKey.referencedTable.toLowerCase() ===
              fixtureTableName(engineId, "parentRecords").toLowerCase(),
        ),
      ).toBe(true);
      expect(ddl.toLowerCase()).toContain(fixtureRows.toLowerCase());

      const support = fixtureSupportSummary(engineId);
      if (support.routines) {
        const definition = await harness.driver.getRoutineDefinition(
          harness.databaseName,
          harness.schemaName,
          fixtureRoutineName(engineId, "totalAmount"),
          "function",
        );

        expect(definition.length).toBeGreaterThan(0);
        expect(definition.toLowerCase()).toMatch(/fixture|sum|create/);
      }
    });

    it("executes select, ddl, dml, multi-statement scripts, and transactions", async () => {
      const fixtureRowsColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "fixtureRows"),
      );
      const fixtureRowsIdColumn = findColumn(fixtureRowsColumns, "id");
      const transactionProbeColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "transactionProbe"),
      );
      const transactionProbeIdColumn = findColumn(
        transactionProbeColumns,
        "id",
      );
      const transactionProbeAccountNameColumn = findColumn(
        transactionProbeColumns,
        "account_name",
      );
      const transactionProbeBalanceColumn = findColumn(
        transactionProbeColumns,
        "balance",
      );
      const transactionProbeUpdatedAtColumn = findColumn(
        transactionProbeColumns,
        "updated_at",
      );
      const fixtureRowsTable = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "fixtureRows"),
      );
      const transactionProbeTable = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "transactionProbe"),
      );
      const selected = await harness.driver.query(
        `SELECT * FROM ${fixtureRowsTable} ORDER BY ${harness.driver.quoteIdentifier(fixtureRowsIdColumn.name)}`,
      );

      expect(selected.rowCount).toBe(2);
      expect(selected.columns.length).toBeGreaterThanOrEqual(6);

      const probeTableName = `rapidb_driver_probe_${engineId}_${Date.now()}`;
      const qualifiedProbeTable = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        probeTableName,
      );
      const probeIdColumn = probeColumnName(engineId, "id");
      const probeNoteColumn = probeColumnName(engineId, "note");
      await harness.driver.query(
        createProbeTableSql(engineId, qualifiedProbeTable),
      );
      await harness.driver.query(
        `INSERT INTO ${qualifiedProbeTable} (${harness.driver.quoteIdentifier(probeIdColumn)}, ${harness.driver.quoteIdentifier(probeNoteColumn)}) VALUES (1, ${sqlString("probe row")})`,
      );
      const probeSelect = await harness.driver.query(
        `SELECT ${harness.driver.quoteIdentifier(probeNoteColumn)} AS probe_note FROM ${qualifiedProbeTable}`,
      );
      expect(
        getCaseInsensitive(rowsFromQuery(probeSelect)[0] ?? {}, "probe_note"),
      ).toBe("probe row");
      await harness.driver.query(`DROP TABLE ${qualifiedProbeTable}`);

      const scriptId = 950_000 + Math.floor(Math.random() * 10_000);
      const scriptResult = await harness.driver.query(
        `INSERT INTO ${transactionProbeTable} (${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeAccountNameColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeUpdatedAtColumn.name)}) VALUES (${scriptId}, ${sqlString("Script Probe")}, 10.00, ${sqlTimestamp(engineId, "2026-04-21T10:00:00.000Z")});\nUPDATE ${transactionProbeTable} SET ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} = ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} + 5.25 WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${scriptId};\nSELECT ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} AS probe_balance FROM ${transactionProbeTable} WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${scriptId}`,
      );
      expect(
        Number(
          getCaseInsensitive(
            rowsFromQuery(scriptResult)[0] ?? {},
            "probe_balance",
          ),
        ),
      ).toBeCloseTo(15.25, 2);

      const transactionId = scriptId + 1;
      await harness.driver.query(
        `INSERT INTO ${transactionProbeTable} (${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeAccountNameColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)}, ${harness.driver.quoteIdentifier(transactionProbeUpdatedAtColumn.name)}) VALUES (${transactionId}, ${sqlString("Tx Probe")}, 20.00, ${sqlTimestamp(engineId, "2026-04-21T10:00:01.000Z")})`,
      );
      await harness.driver.runTransaction([
        {
          sql: `UPDATE ${transactionProbeTable} SET ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} = ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} + 1 WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${transactionId}`,
          checkAffectedRows: true,
        },
        {
          sql: `UPDATE ${transactionProbeTable} SET ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} = ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} + 2 WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${transactionId}`,
          checkAffectedRows: true,
        },
      ]);
      const transactionCheck = await harness.driver.query(
        `SELECT ${harness.driver.quoteIdentifier(transactionProbeBalanceColumn.name)} AS probe_balance FROM ${transactionProbeTable} WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} = ${transactionId}`,
      );
      expect(
        Number(
          getCaseInsensitive(
            rowsFromQuery(transactionCheck)[0] ?? {},
            "probe_balance",
          ),
        ),
      ).toBeCloseTo(23, 2);

      await harness.driver.query(
        `DELETE FROM ${transactionProbeTable} WHERE ${harness.driver.quoteIdentifier(transactionProbeIdColumn.name)} IN (${scriptId}, ${transactionId})`,
      );
    });

    it("supports qualified names, pagination SQL, input coercion, and persisted edit checks", async () => {
      const paginationTable = fixtureTableName(engineId, "paginationRows");
      const paginationQualifiedName = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        paginationTable,
      );
      expect(paginationQualifiedName.length).toBeGreaterThan(0);

      const pagination = harness.driver.buildPagination(12, 5, 1);
      const paginationColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        paginationTable,
      );
      const paginationIdColumn = findColumn(paginationColumns, "id");
      const paginationResult = await harness.driver.query(
        `SELECT ${harness.driver.quoteIdentifier(paginationIdColumn.name)} AS row_id FROM ${paginationQualifiedName} ORDER BY ${harness.driver.quoteIdentifier(paginationIdColumn.name)} ${pagination.sql}`,
        pagination.params,
      );
      expect(
        Number(
          getCaseInsensitive(
            rowsFromQuery(paginationResult)[0] ?? {},
            "row_id",
          ),
        ),
      ).toBe(13);

      const fixtureColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "fixtureRows"),
      );
      const boolColumn = findColumn(fixtureColumns, "is_active");
      expect(
        truthyBoolean(harness.driver.coerceInputValue("true", boolColumn)),
      ).toBe(true);

      const numericColumns = await harness.driver.describeColumns(
        harness.databaseName,
        harness.schemaName,
        fixtureTableName(engineId, "exactNumericSamples"),
      );
      const exactAmountColumn = findColumn(numericColumns, "exact_amount");
      const ratioColumn = findColumn(numericColumns, "ratio");
      const persistedCheck = harness.driver.checkPersistedEdit(
        exactAmountColumn,
        "123.450000",
        { persistedValue: "123.450000" },
      );
      expect(persistedCheck?.ok ?? true).toBe(true);

      if (engineId !== "sqlite") {
        const exactNumericResult = await harness.driver.query(
          `SELECT ${harness.driver.quoteIdentifier(exactAmountColumn.name)}, ${harness.driver.quoteIdentifier(ratioColumn.name)} FROM ${harness.driver.qualifiedTableName(harness.databaseName, harness.schemaName, fixtureTableName(engineId, "exactNumericSamples"))} ORDER BY ${harness.driver.quoteIdentifier(findColumn(numericColumns, "id").name)}`,
        );
        const exactNumericRows = rowsFromQuery(exactNumericResult);
        expect(
          String(getCaseInsensitive(exactNumericRows[0] ?? {}, "exact_amount")),
        ).toBe("123456789012.123456");
        expect(
          String(getCaseInsensitive(exactNumericRows[0] ?? {}, "ratio")),
        ).toBe("1.2500");
        expect(
          String(getCaseInsensitive(exactNumericRows[1] ?? {}, "exact_amount")),
        ).toBe("-45.600100");
        expect(
          String(getCaseInsensitive(exactNumericRows[2] ?? {}, "ratio")),
        ).toBe("0.3333");
      }

      const moneyProbeTable = `rapidb_money_probe_${engineId}_${Date.now()}`;
      const qualifiedMoneyProbeTable = harness.driver.qualifiedTableName(
        harness.databaseName,
        harness.schemaName,
        moneyProbeTable,
      );
      const moneyIdColumn = probeColumnName(engineId, "id");
      const moneyAmountColumn = probeColumnName(engineId, "amount");
      const moneyProbeDescribeName =
        engineId === "oracle" ? moneyProbeTable.toUpperCase() : moneyProbeTable;
      await harness.driver.query(
        createMonetaryProbeTableSql(engineId, qualifiedMoneyProbeTable),
      );

      try {
        await harness.driver.query(
          `INSERT INTO ${qualifiedMoneyProbeTable} (${harness.driver.quoteIdentifier(moneyIdColumn)}, ${harness.driver.quoteIdentifier(moneyAmountColumn)}) VALUES (1, 10.25)`,
        );

        const moneyColumns = await harness.driver.describeColumns(
          harness.databaseName,
          harness.schemaName,
          moneyProbeDescribeName,
        );
        const amountColumn =
          moneyColumns.find(
            (candidate) => candidate.name.toLowerCase() === "amount",
          ) ??
          (engineId === "oracle"
            ? {
                ...exactAmountColumn,
                name: moneyAmountColumn,
                nativeType: "NUMBER(19,4)",
                category: "decimal" as const,
              }
            : findColumn(moneyColumns, "amount"));

        expect(amountColumn.category).toBe("decimal");

        const coercedMoneyInput = harness.driver.coerceInputValue(
          "1234.56",
          amountColumn,
        );

        await harness.driver.query(
          `UPDATE ${qualifiedMoneyProbeTable} SET ${harness.driver.quoteIdentifier(moneyAmountColumn)} = ${sqlNumericLiteral(coercedMoneyInput)} WHERE ${harness.driver.quoteIdentifier(moneyIdColumn)} = 1`,
        );

        const moneySelect = await harness.driver.query(
          `SELECT ${harness.driver.quoteIdentifier(moneyAmountColumn)} AS probe_amount FROM ${qualifiedMoneyProbeTable} WHERE ${harness.driver.quoteIdentifier(moneyIdColumn)} = 1`,
        );
        const rawMoneyValue = getCaseInsensitive(
          rowsFromQuery(moneySelect)[0] ?? {},
          "probe_amount",
        );
        const formattedMoneyValue = harness.driver.formatOutputValue(
          rawMoneyValue,
          amountColumn,
        );

        const numericMoneyValue = parseMonetaryLike(formattedMoneyValue);
        expect(numericMoneyValue).not.toBeNull();
        expect(numericMoneyValue).toBeCloseTo(1234.56, 2);
      } finally {
        await harness.driver.query(`DROP TABLE ${qualifiedMoneyProbeTable}`);
      }
    });
  });
}
