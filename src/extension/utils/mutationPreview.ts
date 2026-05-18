import { format as formatSql } from "sql-formatter";
import type { QueryEditorPresentation } from "../../shared/webviewContracts";

function usesSqlPreviewFormatting(
  editorPresentation: QueryEditorPresentation | undefined,
): boolean {
  return (
    editorPresentation?.editorLanguage !== "javascript" &&
    editorPresentation?.editorLanguage !== "plaintext"
  );
}

function ensureStatementTerminator(sql: string): string {
  const trimmed = sql.trim();
  if (trimmed === "") {
    return trimmed;
  }
  return trimmed.endsWith(";") ? trimmed : `${trimmed};`;
}

export function formatMutationPreviewSql(
  statements: readonly string[],
  editorPresentation: QueryEditorPresentation | undefined,
): string {
  if (!usesSqlPreviewFormatting(editorPresentation)) {
    return statements
      .map((s) => s.trim())
      .filter((s) => s.length > 0)
      .join("\n\n");
  }

  const language = editorPresentation?.sqlDialect ?? "sql";

  return statements
    .map((statement) => ensureStatementTerminator(statement))
    .filter((statement) => statement.length > 0)
    .map((statement) => {
      try {
        return formatSql(statement, {
          language,
          tabWidth: 2,
          keywordCase: "upper",
          linesBetweenQueries: 2,
        }).trim();
      } catch {
        return statement.trim();
      }
    })
    .join("\n\n");
}
