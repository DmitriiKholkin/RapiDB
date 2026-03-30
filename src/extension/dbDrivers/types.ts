export interface ColumnMeta {
  name: string;
  type: string;
  nullable: boolean;
  defaultValue?: string;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
}

export interface IndexMeta {
  name: string;
  columns: string[];
  unique: boolean;
  primary: boolean;
}

export interface ForeignKeyMeta {
  column: string;
  referencedSchema: string;
  referencedTable: string;
  referencedColumn: string;
  constraintName: string;
}

export interface TableInfo {
  schema: string;
  name: string;
  type: "table" | "view" | "function" | "procedure";
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
  affectedRows?: number;
}

export function colKey(index: number): string {
  return `__col_${index}`;
}

export interface DatabaseInfo {
  name: string;
  schemas: SchemaInfo[];
}

export interface SchemaInfo {
  name: string;
}

export interface IDBDriver {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  isConnected(): boolean;
  listDatabases(): Promise<DatabaseInfo[]>;
  listSchemas(database: string): Promise<SchemaInfo[]>;
  listObjects(database: string, schema: string): Promise<TableInfo[]>;
  describeTable(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]>;
  getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<IndexMeta[]>;
  getForeignKeys(
    database: string,
    schema: string,
    table: string,
  ): Promise<ForeignKeyMeta[]>;
  getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string>;
  getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    kind: "function" | "procedure",
  ): Promise<string>;
  query(sql: string, params?: unknown[]): Promise<QueryResult>;
  runTransaction(operations: TransactionOperation[]): Promise<void>;
}

export interface TransactionOperation {
  sql: string;
  params?: unknown[];
  checkAffectedRows?: boolean;
}
