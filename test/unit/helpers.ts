import { BaseDBDriver } from "../../src/extension/dbDrivers/BaseDBDriver";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  ForeignKeyMeta,
  IndexMeta,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TransactionOperation,
  TypeCategory,
  ValueSemantics,
} from "../../src/extension/dbDrivers/types";

/**
 * Minimal concrete driver that implements only the abstract methods.
 * Uses for testing BaseDBDriver default logic without DB connections.
 */
export class StubDriver extends BaseDBDriver {
  mapTypeCategory(nativeType: string): TypeCategory {
    const t = nativeType.toLowerCase().split("(")[0].trim();
    if (t === "boolean" || t === "bool") return "boolean";
    if (t === "integer" || t === "int" || t === "bigint" || t === "smallint")
      return "integer";
    if (t === "float" || t === "double" || t === "real") return "float";
    if (t === "numeric" || t === "decimal") return "decimal";
    if (t === "date") return "date";
    if (t === "time") return "time";
    if (t === "datetime" || t === "timestamp") return "datetime";
    if (t === "bytea" || t === "binary" || t === "blob") return "binary";
    if (t === "json" || t === "jsonb") return "json";
    if (t === "uuid") return "uuid";
    if (t === "text" || t === "varchar" || t === "char") return "text";
    return "other";
  }

  protected getValueSemantics(
    nativeType: string,
    _category: TypeCategory,
  ): ValueSemantics {
    const t = nativeType.toLowerCase().split("(")[0].trim();
    return t === "boolean" || t === "bool" ? "boolean" : "plain";
  }

  isDatetimeWithTime(nativeType: string): boolean {
    const t = nativeType.toLowerCase();
    return t === "datetime" || t.startsWith("timestamp");
  }

  async connect(): Promise<void> {}
  async disconnect(): Promise<void> {}
  isConnected(): boolean {
    return true;
  }
  async listDatabases(): Promise<DatabaseInfo[]> {
    return [];
  }
  async listSchemas(): Promise<SchemaInfo[]> {
    return [];
  }
  async listObjects(): Promise<TableInfo[]> {
    return [];
  }
  async describeTable(): Promise<ColumnMeta[]> {
    return [];
  }
  async getIndexes(): Promise<IndexMeta[]> {
    return [];
  }
  async getForeignKeys(): Promise<ForeignKeyMeta[]> {
    return [];
  }
  async getCreateTableDDL(): Promise<string> {
    return "";
  }
  async getRoutineDefinition(): Promise<string> {
    return "";
  }
  async query(): Promise<QueryResult> {
    return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
  }
  async runTransaction(): Promise<void> {}
}

/** Build a minimal ColumnTypeMeta for testing purposes. */
export function col(
  overrides: Partial<ColumnTypeMeta> & {
    name: string;
    type: string;
    isBoolean?: boolean;
  },
): ColumnTypeMeta {
  const valueSemantics =
    overrides.valueSemantics ?? (overrides.isBoolean ? "boolean" : "plain");

  return {
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    category: "text",
    nativeType: overrides.type,
    filterable: true,
    editable: true,
    filterOperators: [],
    valueSemantics,
    ...overrides,
  };
}
