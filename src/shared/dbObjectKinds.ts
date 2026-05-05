export const DB_OBJECT_KINDS = [
  "table",
  "view",
  "materializedView",
  "function",
  "procedure",
  "sequence",
  "type",
] as const;

export type DbObjectKind = (typeof DB_OBJECT_KINDS)[number];

export const DATA_DB_OBJECT_KINDS = [
  "table",
  "view",
  "materializedView",
] as const;

export type DataDbObjectKind = (typeof DATA_DB_OBJECT_KINDS)[number];

export const ROUTINE_DB_OBJECT_KINDS = ["function", "procedure"] as const;

export type RoutineDbObjectKind = (typeof ROUTINE_DB_OBJECT_KINDS)[number];

export const DDL_ONLY_DB_OBJECT_KINDS = ["sequence", "type"] as const;

export type DdlOnlyDbObjectKind = (typeof DDL_ONLY_DB_OBJECT_KINDS)[number];

export const EXPLORER_CATEGORY_ORDER = [
  "tables",
  "views",
  "materializedViews",
  "functions",
  "procedures",
  "sequences",
  "types",
] as const;

export type ExplorerCategoryId = (typeof EXPLORER_CATEGORY_ORDER)[number];

export const EXPLORER_CATEGORY_CONFIG: Record<
  ExplorerCategoryId,
  { label: string; objectKinds: readonly DbObjectKind[] }
> = {
  tables: {
    label: "Tables",
    objectKinds: ["table"],
  },
  views: {
    label: "Views",
    objectKinds: ["view"],
  },
  materializedViews: {
    label: "Materialized Views",
    objectKinds: ["materializedView"],
  },
  functions: {
    label: "Functions",
    objectKinds: ["function"],
  },
  procedures: {
    label: "Procedures",
    objectKinds: ["procedure"],
  },
  sequences: {
    label: "Sequences",
    objectKinds: ["sequence"],
  },
  types: {
    label: "Types",
    objectKinds: ["type"],
  },
};

export function isDbObjectKind(value: string): value is DbObjectKind {
  return (DB_OBJECT_KINDS as readonly string[]).includes(value);
}

export function isDataDbObjectKind(
  kind: DbObjectKind,
): kind is DataDbObjectKind {
  return (DATA_DB_OBJECT_KINDS as readonly string[]).includes(kind);
}

export function isRoutineDbObjectKind(
  kind: DbObjectKind,
): kind is RoutineDbObjectKind {
  return (ROUTINE_DB_OBJECT_KINDS as readonly string[]).includes(kind);
}

export function isDdlOnlyDbObjectKind(
  kind: DbObjectKind,
): kind is DdlOnlyDbObjectKind {
  return (DDL_ONLY_DB_OBJECT_KINDS as readonly string[]).includes(kind);
}
