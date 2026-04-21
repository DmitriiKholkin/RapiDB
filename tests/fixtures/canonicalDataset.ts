import {
  CANONICAL_FIXTURE_SCHEMA,
  type DbEngineId,
  TEST_DATABASE_NAMES,
  TEST_ORACLE_APP_USERNAME,
} from "../contracts/testingContracts.ts";

export const FIXTURE_TABLE_NAMES = {
  fixtureRows: "fixture_rows",
  parentRecords: "parent_records",
  childRecords: "child_records",
  compositeLinks: "composite_links",
  exactNumericSamples: "exact_numeric_samples",
  paginationRows: "pagination_rows",
  exportRows: "export_rows",
  transactionProbe: "transaction_probe",
} as const;

export const FIXTURE_ROUTINE_NAMES = {
  totalAmount: "fixture_total_amount",
  adjustBalance: "fixture_adjust_balance",
} as const;

export type FixtureTableKey = keyof typeof FIXTURE_TABLE_NAMES;
export type FixtureRoutineKey = keyof typeof FIXTURE_ROUTINE_NAMES;

export interface FixtureNamespace {
  logicalSchemaName: string;
  physicalDatabaseName: string | null;
  physicalSchemaName: string;
}

export interface FixtureTableManifest {
  key: FixtureTableKey;
  logicalName: (typeof FIXTURE_TABLE_NAMES)[FixtureTableKey];
  rowCount: number;
}

export interface FixtureRoutineManifest {
  key: FixtureRoutineKey;
  logicalName: (typeof FIXTURE_ROUTINE_NAMES)[FixtureRoutineKey];
  supportedEngines: readonly DbEngineId[];
}

interface ParentRecordRow {
  id: number;
  code: string;
  display_name: string;
  created_at: string;
}

interface ChildRecordRow {
  id: number;
  parent_id: number;
  child_name: string;
  status: string;
  amount: string;
}

interface CompositeLinkRow {
  tenant_id: number;
  external_id: number;
  description: string;
  created_at: string;
}

interface ExactNumericSampleRow {
  id: number;
  numeric_label: string;
  exact_amount: string;
  ratio: string;
}

interface PaginationRow {
  id: number;
  page_group: number;
  title: string;
  created_at: string;
}

interface ExportRow {
  id: number;
  export_code: string;
  payload: string;
  decimal_amount: string;
  created_at: string;
}

interface TransactionProbeRow {
  id: number;
  account_name: string;
  balance: string;
  updated_at: string;
}

function isoAt(
  month: number,
  day: number,
  hours: number,
  minutes: number,
  seconds: number,
  millis = 0,
): string {
  return new Date(
    Date.UTC(2026, month - 1, day, hours, minutes, seconds, millis),
  ).toISOString();
}

export const PARENT_RECORD_ROWS: readonly ParentRecordRow[] = [
  {
    id: 1,
    code: "PARENT-ALPHA",
    display_name: "Alpha Parent",
    created_at: isoAt(1, 10, 8, 15, 0),
  },
  {
    id: 2,
    code: "PARENT-BETA",
    display_name: "Beta Parent",
    created_at: isoAt(1, 11, 9, 30, 0),
  },
  {
    id: 3,
    code: "PARENT-GAMMA",
    display_name: "Gamma Parent",
    created_at: isoAt(1, 12, 10, 45, 0),
  },
] as const;

export const CHILD_RECORD_ROWS: readonly ChildRecordRow[] = [
  {
    id: 1,
    parent_id: 1,
    child_name: "Alpha Child One",
    status: "ready",
    amount: "5.25",
  },
  {
    id: 2,
    parent_id: 1,
    child_name: "Alpha Child Two",
    status: "draft",
    amount: "7.50",
  },
  {
    id: 3,
    parent_id: 2,
    child_name: "Beta Child One",
    status: "ready",
    amount: "12.00",
  },
  {
    id: 4,
    parent_id: 3,
    child_name: "Gamma Child One",
    status: "archived",
    amount: "1.75",
  },
] as const;

export const COMPOSITE_LINK_ROWS: readonly CompositeLinkRow[] = [
  {
    tenant_id: 10,
    external_id: 100,
    description: "Composite link A",
    created_at: isoAt(2, 1, 12, 0, 0),
  },
  {
    tenant_id: 10,
    external_id: 101,
    description: "Composite link B",
    created_at: isoAt(2, 1, 12, 5, 0),
  },
  {
    tenant_id: 11,
    external_id: 100,
    description: "Composite link C",
    created_at: isoAt(2, 1, 12, 10, 0),
  },
  {
    tenant_id: 11,
    external_id: 101,
    description: "Composite link D",
    created_at: isoAt(2, 1, 12, 15, 0),
  },
] as const;

export const EXACT_NUMERIC_SAMPLE_ROWS: readonly ExactNumericSampleRow[] = [
  {
    id: 1,
    numeric_label: "max-scale",
    exact_amount: "123456789012.123456",
    ratio: "1.2500",
  },
  {
    id: 2,
    numeric_label: "negative-scale",
    exact_amount: "-45.600100",
    ratio: "-0.5000",
  },
  {
    id: 3,
    numeric_label: "tiny-fraction",
    exact_amount: "0.000001",
    ratio: "0.3333",
  },
] as const;

export const PAGINATION_ROWS: readonly PaginationRow[] = Array.from(
  { length: 48 },
  (_, index) => ({
    id: index + 1,
    page_group: Math.floor(index / 12) + 1,
    title: `Pagination Row ${String(index + 1).padStart(2, "0")}`,
    created_at: new Date(Date.UTC(2026, 2, 1, 0, index, 0, 0)).toISOString(),
  }),
);

export const EXPORT_ROWS: readonly ExportRow[] = Array.from(
  { length: 128 },
  (_, index) => ({
    id: index + 1,
    export_code: `EXP-${String(index + 1).padStart(4, "0")}`,
    payload: `Export payload ${String(index + 1).padStart(4, "0")} :: ${"x".repeat(48)}`,
    decimal_amount: ((index + 1) * 1.125).toFixed(3),
    created_at: new Date(
      Date.UTC(2026, 3, 1, 6, index % 60, 0, 0),
    ).toISOString(),
  }),
);

export const TRANSACTION_PROBE_ROWS: readonly TransactionProbeRow[] = [
  {
    id: 1,
    account_name: "Cash",
    balance: "1500.00",
    updated_at: isoAt(4, 10, 8, 0, 0),
  },
  {
    id: 2,
    account_name: "Receivables",
    balance: "245.75",
    updated_at: isoAt(4, 10, 8, 5, 0),
  },
  {
    id: 3,
    account_name: "Deferred Revenue",
    balance: "-320.10",
    updated_at: isoAt(4, 10, 8, 10, 0),
  },
] as const;

export function physicalizeFixtureIdentifier(
  engineId: DbEngineId,
  logicalName: string,
): string {
  return engineId === "oracle" ? logicalName.toUpperCase() : logicalName;
}

export function resolveFixtureNamespace(
  engineId: DbEngineId,
): FixtureNamespace {
  switch (engineId) {
    case "sqlite":
      return {
        logicalSchemaName: CANONICAL_FIXTURE_SCHEMA.schemaName,
        physicalDatabaseName: null,
        physicalSchemaName: "main",
      };
    case "postgres":
      return {
        logicalSchemaName: CANONICAL_FIXTURE_SCHEMA.schemaName,
        physicalDatabaseName: TEST_DATABASE_NAMES.postgres,
        physicalSchemaName: CANONICAL_FIXTURE_SCHEMA.schemaName,
      };
    case "mysql":
      return {
        logicalSchemaName: CANONICAL_FIXTURE_SCHEMA.schemaName,
        physicalDatabaseName: TEST_DATABASE_NAMES.mysql,
        physicalSchemaName: TEST_DATABASE_NAMES.mysql,
      };
    case "mssql":
      return {
        logicalSchemaName: CANONICAL_FIXTURE_SCHEMA.schemaName,
        physicalDatabaseName: TEST_DATABASE_NAMES.mssql,
        physicalSchemaName: CANONICAL_FIXTURE_SCHEMA.schemaName,
      };
    case "oracle":
      return {
        logicalSchemaName: CANONICAL_FIXTURE_SCHEMA.schemaName,
        physicalDatabaseName: TEST_DATABASE_NAMES.oracle,
        physicalSchemaName: TEST_ORACLE_APP_USERNAME.toUpperCase(),
      };
  }
}

export const CANONICAL_FIXTURE_DATASET = {
  datasetId: "baseline-v2",
  logicalSchemaName: CANONICAL_FIXTURE_SCHEMA.schemaName,
  tables: [
    {
      key: "fixtureRows",
      logicalName: FIXTURE_TABLE_NAMES.fixtureRows,
      rowCount: CANONICAL_FIXTURE_SCHEMA.seedRows.length,
    },
    {
      key: "parentRecords",
      logicalName: FIXTURE_TABLE_NAMES.parentRecords,
      rowCount: PARENT_RECORD_ROWS.length,
    },
    {
      key: "childRecords",
      logicalName: FIXTURE_TABLE_NAMES.childRecords,
      rowCount: CHILD_RECORD_ROWS.length,
    },
    {
      key: "compositeLinks",
      logicalName: FIXTURE_TABLE_NAMES.compositeLinks,
      rowCount: COMPOSITE_LINK_ROWS.length,
    },
    {
      key: "exactNumericSamples",
      logicalName: FIXTURE_TABLE_NAMES.exactNumericSamples,
      rowCount: EXACT_NUMERIC_SAMPLE_ROWS.length,
    },
    {
      key: "paginationRows",
      logicalName: FIXTURE_TABLE_NAMES.paginationRows,
      rowCount: PAGINATION_ROWS.length,
    },
    {
      key: "exportRows",
      logicalName: FIXTURE_TABLE_NAMES.exportRows,
      rowCount: EXPORT_ROWS.length,
    },
    {
      key: "transactionProbe",
      logicalName: FIXTURE_TABLE_NAMES.transactionProbe,
      rowCount: TRANSACTION_PROBE_ROWS.length,
    },
  ] satisfies readonly FixtureTableManifest[],
  routines: [
    {
      key: "totalAmount",
      logicalName: FIXTURE_ROUTINE_NAMES.totalAmount,
      supportedEngines: ["postgres", "mysql", "mssql", "oracle"],
    },
    {
      key: "adjustBalance",
      logicalName: FIXTURE_ROUTINE_NAMES.adjustBalance,
      supportedEngines: ["postgres", "mysql", "mssql", "oracle"],
    },
  ] satisfies readonly FixtureRoutineManifest[],
  expected: {
    fixtureRowTotalAmount: "19.95",
    exportRowCount: EXPORT_ROWS.length,
    paginationRowCount: PAGINATION_ROWS.length,
  },
} as const;
