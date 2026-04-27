import type { SchemaObject } from "../store";

export type SqlCompletionSuggestionKind =
  | "class"
  | "field"
  | "function"
  | "keyword"
  | "module"
  | "value";

export interface SqlCompletionSuggestion {
  label: string;
  detail?: string;
  kind: SqlCompletionSuggestionKind;
  insertText: string;
  sortText: string;
}

const SQL_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "JOIN",
  "LEFT",
  "RIGHT",
  "INNER",
  "OUTER",
  "FULL",
  "CROSS",
  "ON",
  "AS",
  "AND",
  "OR",
  "NOT",
  "IN",
  "IS",
  "NULL",
  "LIKE",
  "BETWEEN",
  "EXISTS",
  "INSERT",
  "INTO",
  "VALUES",
  "UPDATE",
  "SET",
  "DELETE",
  "TRUNCATE",
  "CREATE",
  "ALTER",
  "DROP",
  "TABLE",
  "VIEW",
  "INDEX",
  "DATABASE",
  "SCHEMA",
  "GROUP BY",
  "ORDER BY",
  "HAVING",
  "LIMIT",
  "OFFSET",
  "DISTINCT",
  "ALL",
  "UNION",
  "WITH",
  "CASE",
  "WHEN",
  "THEN",
  "ELSE",
  "END",
  "CAST",
  "COALESCE",
  "NULLIF",
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "NOW",
  "CURRENT_TIMESTAMP",
  "CURRENT_DATE",
  "PRIMARY KEY",
  "FOREIGN KEY",
  "REFERENCES",
  "UNIQUE",
  "NOT NULL",
  "DEFAULT",
  "BEGIN",
  "COMMIT",
  "ROLLBACK",
  "TRANSACTION",
  "EXPLAIN",
  "ANALYZE",
  "ASC",
  "DESC",
  "TRUE",
  "FALSE",
  "RETURNING",
  "ILIKE",
  "SIMILAR TO",
  "CALL",
] as const;

function normalizeName(value: string): string {
  return value.toLowerCase();
}

function isRoutine(entry: SchemaObject): boolean {
  return entry.type === "function" || entry.type === "procedure";
}

function objectKindFor(
  type: SchemaObject["type"],
): SqlCompletionSuggestionKind {
  switch (type) {
    case "table":
      return "class";
    case "view":
      return "class";
    case "procedure":
      return "function";
    case "function":
      return "function";
    default:
      return "value";
  }
}

function objectDetailLabel(
  type: SchemaObject["type"],
  columnCount: number,
): string {
  if (type === "function" || type === "procedure") {
    return type;
  }

  if (type === "view") {
    return columnCount > 0 ? `view (${columnCount} cols)` : "view";
  }

  if (type === "table") {
    return columnCount > 0 ? `table (${columnCount} cols)` : "table";
  }

  if (columnCount > 0) {
    return `table (${columnCount} cols)`;
  }

  return type ?? "table";
}

function uniqueValues(values: readonly string[]): string[] {
  return [...new Set(values)];
}

function databaseSchemaMap(
  schema: readonly SchemaObject[],
): Map<string, string[]> {
  const byDatabase = new Map<string, string[]>();
  for (const entry of schema) {
    const existing = byDatabase.get(entry.database) ?? [];
    if (!existing.includes(entry.schema)) {
      existing.push(entry.schema);
      byDatabase.set(entry.database, existing);
    }
  }
  return byDatabase;
}

function isCollapsedSchemaDatabase(
  database: string,
  byDatabase: ReadonlyMap<string, string[]>,
): boolean {
  const schemaNames = byDatabase.get(database) ?? [];
  return (
    schemaNames.length === 1 &&
    normalizeName(schemaNames[0]) === normalizeName(database)
  );
}

function locationLabel(
  entry: SchemaObject,
  byDatabase: ReadonlyMap<string, string[]>,
): string {
  if (isCollapsedSchemaDatabase(entry.database, byDatabase)) {
    return entry.database;
  }
  return `${entry.database}.${entry.schema}`;
}

function schemaQualifiedObjectLabel(
  entry: SchemaObject,
  byDatabase: ReadonlyMap<string, string[]>,
): string {
  if (isCollapsedSchemaDatabase(entry.database, byDatabase)) {
    return `${entry.database}.${entry.object}`;
  }
  return `${entry.schema}.${entry.object}`;
}

function fullyQualifiedObjectLabel(
  entry: SchemaObject,
  byDatabase: ReadonlyMap<string, string[]>,
): string {
  if (isCollapsedSchemaDatabase(entry.database, byDatabase)) {
    return `${entry.database}.${entry.object}`;
  }
  return `${entry.database}.${entry.schema}.${entry.object}`;
}

function preferredEntry(
  entries: readonly SchemaObject[],
  primaryDatabase: string,
): SchemaObject | undefined {
  const normalizedPrimary = normalizeName(primaryDatabase);
  const withColumns = entries.filter((entry) => entry.columns.length > 0);
  return (
    withColumns.find(
      (entry) => normalizeName(entry.database) === normalizedPrimary,
    ) ??
    withColumns[0] ??
    entries.find(
      (entry) => normalizeName(entry.database) === normalizedPrimary,
    ) ??
    entries[0]
  );
}

function buildColumnSuggestions(
  entry: SchemaObject,
  objectPath: string,
): SqlCompletionSuggestion[] {
  return entry.columns.map((column, index) => ({
    label: column.name,
    detail: `${objectPath}.${column.name}  ${column.type}`,
    kind: "field",
    insertText: column.name,
    sortText: String(index).padStart(5, "0"),
  }));
}

function buildObjectSuggestions(
  entries: readonly SchemaObject[],
  byDatabase: ReadonlyMap<string, string[]>,
  primaryDatabase: string,
): SqlCompletionSuggestion[] {
  const normalizedPrimary = normalizeName(primaryDatabase);
  return entries.map((entry, index) => ({
    label: entry.object,
    detail: `${objectDetailLabel(entry.type, entry.columns.length)} in ${locationLabel(entry, byDatabase)}`,
    kind: objectKindFor(entry.type),
    insertText: entry.object,
    sortText: `${normalizeName(entry.database) === normalizedPrimary ? "0" : "1"}_${String(index).padStart(5, "0")}`,
  }));
}

export function buildSqlCompletionSuggestions(
  schema: readonly SchemaObject[],
  lineUpToCursor: string,
): SqlCompletionSuggestion[] {
  const byDatabase = databaseSchemaMap(schema);
  const primaryDatabase =
    schema.find((entry) => entry.columns.length > 0)?.database ??
    schema[0]?.database ??
    "";

  const databaseSchemaObjectMatch = lineUpToCursor.match(
    /(\w+)\.(\w+)\.(\w+)\.\s*(\w*)$/,
  );
  if (databaseSchemaObjectMatch) {
    const databaseHint = normalizeName(databaseSchemaObjectMatch[1]);
    const schemaHint = normalizeName(databaseSchemaObjectMatch[2]);
    const objectHint = normalizeName(databaseSchemaObjectMatch[3]);
    const matched = preferredEntry(
      schema.filter(
        (entry) =>
          normalizeName(entry.database) === databaseHint &&
          normalizeName(entry.schema) === schemaHint &&
          normalizeName(entry.object) === objectHint,
      ),
      primaryDatabase,
    );
    return matched
      ? buildColumnSuggestions(
          matched,
          fullyQualifiedObjectLabel(matched, byDatabase),
        )
      : [];
  }

  const twoSegmentMatch = lineUpToCursor.match(/(\w+)\.(\w+)\.\s*(\w*)$/);
  if (twoSegmentMatch) {
    const firstHint = normalizeName(twoSegmentMatch[1]);
    const secondHint = normalizeName(twoSegmentMatch[2]);

    const collapsedDatabaseObjectMatches = schema.filter(
      (entry) =>
        normalizeName(entry.database) === firstHint &&
        normalizeName(entry.object) === secondHint &&
        isCollapsedSchemaDatabase(entry.database, byDatabase),
    );
    const collapsedDatabaseObject = preferredEntry(
      collapsedDatabaseObjectMatches,
      primaryDatabase,
    );
    if (collapsedDatabaseObject) {
      return buildColumnSuggestions(
        collapsedDatabaseObject,
        fullyQualifiedObjectLabel(collapsedDatabaseObject, byDatabase),
      );
    }

    const databaseSchemaMatches = schema.filter(
      (entry) =>
        normalizeName(entry.database) === firstHint &&
        normalizeName(entry.schema) === secondHint,
    );
    if (databaseSchemaMatches.length > 0) {
      return buildObjectSuggestions(
        databaseSchemaMatches,
        byDatabase,
        primaryDatabase,
      );
    }

    const schemaObjectMatches = schema.filter(
      (entry) =>
        normalizeName(entry.schema) === firstHint &&
        normalizeName(entry.object) === secondHint,
    );
    const schemaObject = preferredEntry(schemaObjectMatches, primaryDatabase);
    if (schemaObject) {
      return buildColumnSuggestions(
        schemaObject,
        fullyQualifiedObjectLabel(schemaObject, byDatabase),
      );
    }

    return [];
  }

  const oneSegmentMatch = lineUpToCursor.match(/(\w+)\.(\w*)$/);
  if (oneSegmentMatch) {
    const hint = normalizeName(oneSegmentMatch[1]);
    const databaseMatches = schema.filter(
      (entry) => normalizeName(entry.database) === hint,
    );
    if (databaseMatches.length > 0) {
      const databaseName = databaseMatches[0].database;
      if (isCollapsedSchemaDatabase(databaseName, byDatabase)) {
        return buildObjectSuggestions(
          databaseMatches,
          byDatabase,
          primaryDatabase,
        );
      }

      const schemaNames = uniqueValues(
        databaseMatches.map((entry) => entry.schema),
      );
      return schemaNames.map((schemaName, index) => ({
        label: schemaName,
        detail: `schema in ${databaseName}`,
        kind: "module",
        insertText: schemaName,
        sortText: `0_${String(index).padStart(5, "0")}`,
      }));
    }

    const schemasWithHint = schema.filter(
      (entry) => normalizeName(entry.schema) === hint,
    );
    if (schemasWithHint.length > 0) {
      return buildObjectSuggestions(
        schemasWithHint,
        byDatabase,
        primaryDatabase,
      );
    }

    const objectMatches = schema.filter(
      (entry) => normalizeName(entry.object) === hint,
    );
    const objectEntry = preferredEntry(objectMatches, primaryDatabase);
    if (objectEntry) {
      return buildColumnSuggestions(
        objectEntry,
        fullyQualifiedObjectLabel(objectEntry, byDatabase),
      );
    }

    return [];
  }

  const suggestions: SqlCompletionSuggestion[] = [];

  SQL_KEYWORDS.forEach((keyword, index) => {
    suggestions.push({
      label: keyword,
      detail: undefined,
      kind: "keyword",
      insertText: keyword,
      sortText: `3_${String(index).padStart(5, "0")}`,
    });
  });

  uniqueValues(schema.map((entry) => entry.database)).forEach(
    (databaseName, index) => {
      suggestions.push({
        label: databaseName,
        detail: "database",
        kind: "module",
        insertText: databaseName,
        sortText: `0_${String(index).padStart(5, "0")}`,
      });
    },
  );

  const schemaUsage = new Map<string, string[]>();
  schema.forEach((entry) => {
    if (isCollapsedSchemaDatabase(entry.database, byDatabase)) {
      return;
    }
    const existing = schemaUsage.get(entry.schema) ?? [];
    if (!existing.includes(entry.database)) {
      existing.push(entry.database);
      schemaUsage.set(entry.schema, existing);
    }
  });
  [...schemaUsage.entries()].forEach(([schemaName, databases], index) => {
    suggestions.push({
      label: schemaName,
      detail: `schema in ${databases.join(", ")}`,
      kind: "module",
      insertText: schemaName,
      sortText: `1_${String(index).padStart(5, "0")}_schema`,
    });
  });

  schema.forEach((entry, index) => {
    const isPrimary =
      normalizeName(entry.database) === normalizeName(primaryDatabase);
    const isRoutineEntry = isRoutine(entry);
    const detailLabel = objectDetailLabel(entry.type, entry.columns.length);
    const schemaQualifiedLabel = schemaQualifiedObjectLabel(entry, byDatabase);
    const fullyQualifiedLabel = fullyQualifiedObjectLabel(entry, byDatabase);
    const location = locationLabel(entry, byDatabase);

    suggestions.push({
      label: entry.object,
      detail: `${detailLabel} in ${location}`,
      kind: objectKindFor(entry.type),
      insertText: entry.object,
      sortText: `2_${isPrimary ? "0" : "1"}_${String(index).padStart(5, "0")}_obj`,
    });

    suggestions.push({
      label: schemaQualifiedLabel,
      detail: `qualified ${detailLabel} in ${location}`,
      kind: objectKindFor(entry.type),
      insertText: schemaQualifiedLabel,
      sortText: `2_${isPrimary ? "0" : "1"}_${String(index).padStart(5, "0")}_schema`,
    });

    if (fullyQualifiedLabel !== schemaQualifiedLabel) {
      suggestions.push({
        label: fullyQualifiedLabel,
        detail: `fully qualified ${detailLabel}`,
        kind: objectKindFor(entry.type),
        insertText: fullyQualifiedLabel,
        sortText: `2_${isPrimary ? "0" : "1"}_${String(index).padStart(5, "0")}_db`,
      });
    }

    if (entry.columns.length > 0 && !isRoutineEntry) {
      entry.columns.forEach((column, columnIndex) => {
        suggestions.push({
          label: column.name,
          detail: `${fullyQualifiedLabel}.${column.name}  ${column.type}`,
          kind: "field",
          insertText: column.name,
          sortText: `2_${isPrimary ? "0" : "1"}_${String(index).padStart(5, "0")}_col_${String(columnIndex).padStart(5, "0")}`,
        });
      });
    }
  });

  return suggestions;
}
