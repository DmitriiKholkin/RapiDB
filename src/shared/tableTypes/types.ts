/**
 * Table-domain type definitions shared by extension and webview.
 *
 * This module is pure types — no runtime side effects, no I/O.
 * Keep the public surface stable; downstream modules import from
 * `../shared/tableTypes` (which resolves to this folder's index).
 */

export const NULL_SENTINEL = "\x00__NULL__\x00";

export type TypeCategory =
  | "text"
  | "integer"
  | "float"
  | "decimal"
  | "boolean"
  | "date"
  | "time"
  | "datetime"
  | "binary"
  | "json"
  | "uuid"
  | "spatial"
  | "interval"
  | "array"
  | "enum"
  | "lob"
  | "other";

export type ValueSemantics = "plain" | "boolean" | "bit";

export type GeneratedKind = "virtual" | "stored";

export type IdentityGenerationKind = "always" | "by_default" | "auto_increment";

export type ConstraintKind = "primary_key" | "foreign_key" | "unique" | "check";

export type TriggerTiming = "before" | "after" | "instead_of" | "unknown";

export type TriggerEvent =
  | "insert"
  | "update"
  | "delete"
  | "truncate"
  | "unknown";

export type TriggerOrientation = "row" | "statement" | "unknown";

export interface QueryColumnMeta {
  category: TypeCategory | null;
}

export type FilterOperator =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "between"
  | "like"
  | "ilike"
  | "in"
  | "is_null"
  | "is_not_null";

export type ScalarFilterOperator = Exclude<
  FilterOperator,
  "between" | "is_null" | "is_not_null"
>;

/** Lookup set of scalar operators; used by parsers for fast type guards. */
export const SCALAR_FILTER_OPERATORS: ReadonlySet<ScalarFilterOperator> =
  new Set(["eq", "neq", "gt", "gte", "lt", "lte", "like", "ilike", "in"]);

export type FilterExpression =
  | {
      column: string;
      operator: ScalarFilterOperator;
      value: string;
    }
  | {
      column: string;
      operator: "between";
      value: [string, string];
    }
  | {
      column: string;
      operator: "is_null" | "is_not_null";
    };

export type FilterDraft =
  | {
      operator: ScalarFilterOperator;
      value: string;
    }
  | {
      operator: "between";
      value: [string, string];
    }
  | {
      operator: "is_null" | "is_not_null";
    };

export type FilterDraftMap = Partial<Record<string, FilterDraft>>;

/**
 * Minimal column shape required to build a filter expression.
 * The bigger `ColumnTypeMeta` satisfies this automatically.
 */
export type FilterDraftColumn = Pick<
  ColumnTypeMeta,
  "name" | "filterable" | "filterOperators"
>;

export type PrimaryKeyRole = "partition" | "sort";

export interface ColumnMeta {
  name: string;
  type: string;
  nullable: boolean;
  defaultValue?: string;
  identityGeneration?: IdentityGenerationKind;
  onUpdateExpression?: string;
  isComputed?: boolean;
  computedExpression?: string;
  generatedKind?: GeneratedKind;
  isPersisted?: boolean;
  isPrimaryKey: boolean;
  primaryKeyOrdinal?: number;
  primaryKeyRole?: PrimaryKeyRole;
  isForeignKey: boolean;
}

export interface ColumnTypeMeta extends ColumnMeta {
  category: TypeCategory;
  nativeType: string;
  bsonSubtype?: number;
  filterable: boolean;
  filterOperators: FilterOperator[];
  valueSemantics: ValueSemantics;
}

export type IndexDdlSupport = "supported" | "unsupported";

export interface IndexMeta {
  name: string;
  columns: string[];
  unique: boolean;
  primary: boolean;
  ddlSupport?: IndexDdlSupport;
}

export interface ForeignKeyMeta {
  column: string;
  referencedSchema: string;
  referencedTable: string;
  referencedColumn: string;
  constraintName: string;
}

export interface TableConstraintMeta {
  name: string;
  kind: ConstraintKind;
  columns: string[];
  referencedSchema?: string;
  referencedTable?: string;
  referencedColumns?: string[];
  checkExpression?: string;
  source: "catalog" | "derived";
}

export interface TriggerMeta {
  name: string;
  timing: TriggerTiming;
  events: TriggerEvent[];
  orientation?: TriggerOrientation;
  enabled?: boolean;
  definition?: string;
}
