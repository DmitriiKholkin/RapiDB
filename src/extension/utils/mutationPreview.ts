import { format as formatSql } from "sql-formatter";
import type { QueryEditorPresentation } from "../../shared/webviewContracts";

const FORMATTER_OPTIONS = {
  tabWidth: 2,
  keywordCase: "upper",
  linesBetweenQueries: 2,
} as const;

function usesSqlPreviewFormatting(
  editorPresentation: QueryEditorPresentation | undefined,
): boolean {
  if (editorPresentation?.editorLanguage === "javascript") return false;
  if (editorPresentation?.editorLanguage === "plaintext") return false;
  if (editorPresentation?.allowFormatting === false) return false;
  return true;
}

function ensureStatementTerminator(sql: string): string {
  const trimmed = sql.trim();
  if (trimmed === "") {
    return trimmed;
  }
  return trimmed.endsWith(";") ? trimmed : `${trimmed};`;
}

function nonEmptyTrimmed(statements: readonly string[]): string[] {
  return statements.map((s) => s.trim()).filter((s) => s.length > 0);
}

function formatSqlSafely(
  statement: string,
  language: NonNullable<QueryEditorPresentation["sqlDialect"]> | "sql",
): string {
  try {
    return formatSql(statement, { language, ...FORMATTER_OPTIONS }).trim();
  } catch {
    return statement.trim();
  }
}

export function formatMutationPreviewSql(
  statements: readonly string[],
  editorPresentation: QueryEditorPresentation | undefined,
): string {
  if (!usesSqlPreviewFormatting(editorPresentation)) {
    return nonEmptyTrimmed(statements).join("\n\n");
  }

  const language = editorPresentation?.sqlDialect ?? "sql";
  return nonEmptyTrimmed(statements)
    .map(ensureStatementTerminator)
    .filter((s) => s.length > 0)
    .map((s) => formatSqlSafely(s, language))
    .join("\n\n");
}
