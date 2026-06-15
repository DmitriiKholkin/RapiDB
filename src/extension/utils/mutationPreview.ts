/**
 * Format a list of SQL mutation statements for the preview dialog.
 *
 * Three small concerns, composed in one place:
 *  - decide whether to apply SQL formatting (skip for non-SQL editors,
 *    skip if formatting is explicitly disabled);
 *  - ensure each statement is terminated with `;` so the formatter
 *    treats it as a complete query;
 *  - join the formatted statements with blank lines.
 */

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

/**
 * Apply `sql-formatter` to a single statement, falling back to the
 * raw trimmed text when the formatter throws (e.g. on dialect-incompatible
 * syntax in the preview path).
 */
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
