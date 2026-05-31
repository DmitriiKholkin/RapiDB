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

function includesKind<TKind extends DbObjectKind>(
  kinds: readonly TKind[],
  value: string,
): value is TKind {
  return (kinds as readonly string[]).includes(value);
}

export function isDbObjectKind(value: string): value is DbObjectKind {
  return includesKind(DB_OBJECT_KINDS, value);
}

export function isDataDbObjectKind(
  kind: DbObjectKind,
): kind is DataDbObjectKind {
  return includesKind(DATA_DB_OBJECT_KINDS, kind);
}

export function isRoutineDbObjectKind(
  kind: DbObjectKind,
): kind is RoutineDbObjectKind {
  return includesKind(ROUTINE_DB_OBJECT_KINDS, kind);
}

export function isDdlOnlyDbObjectKind(
  kind: DbObjectKind,
): kind is DdlOnlyDbObjectKind {
  return includesKind(DDL_ONLY_DB_OBJECT_KINDS, kind);
}

const DB_OBJECT_KIND_LABELS: Partial<Record<DbObjectKind, string>> = {
  materializedView: "materialized view",
};

function defaultDbObjectKindLabel(kind: DbObjectKind): string {
  return DB_OBJECT_KIND_LABELS[kind] ?? kind;
}

const TABLE_LABEL_BY_CONNECTION_TYPE: Record<
  string,
  { singular: string; plural: string }
> = {
  mongodb: { singular: "collection", plural: "Collections" },
  redis: { singular: "keyspace", plural: "Keyspaces" },
  elasticsearch: { singular: "index", plural: "Indices" },
};

function getTableLabel(
  connectionType: string | undefined,
  cardinality: "singular" | "plural",
): string | undefined {
  if (!connectionType) {
    return undefined;
  }
  return TABLE_LABEL_BY_CONNECTION_TYPE[connectionType]?.[cardinality];
}

export function getDbObjectKindDisplayLabel(
  connectionType: string | undefined,
  kind: DbObjectKind,
): string {
  if (kind !== "table") {
    return defaultDbObjectKindLabel(kind);
  }
  return getTableLabel(connectionType, "singular") ?? "table";
}

export function getDbObjectKindCategoryLabel(
  connectionType: string | undefined,
  categoryId: ExplorerCategoryId,
): string {
  if (categoryId !== "tables") {
    return EXPLORER_CATEGORY_CONFIG[categoryId].label;
  }
  return (
    getTableLabel(connectionType, "plural") ??
    EXPLORER_CATEGORY_CONFIG[categoryId].label
  );
}
