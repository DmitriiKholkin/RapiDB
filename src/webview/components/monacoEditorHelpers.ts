/**
 * Monaco editor helper functions and constants.
 *
 * Extracted from MonacoEditor.tsx to reduce its size and improve testability.
 */
import * as monaco from "monaco-editor";
import { format as sqlFormatterFormat } from "sql-formatter";
import type { QueryEditorSqlDialect } from "../../shared/webviewContracts";
import { cssVar } from "../utils/cssVar";
import type { SqlCompletionSuggestionKind } from "../utils/sqlCompletionSuggestions";

// ─── Types ──────────────────────────────────────────────────────────────────

type SqlFormatterOptions = NonNullable<
  Parameters<typeof sqlFormatterFormat>[1]
>;
type SqlFormatterLanguage = NonNullable<SqlFormatterOptions["language"]>;

// ─── Constants ──────────────────────────────────────────────────────────────

export const RAPIDB_THEME = "rapidb-vscode";

// ─── Theme Helpers ──────────────────────────────────────────────────────────

export function themeBase(): "vs-dark" | "vs" | "hc-black" | "hc-light" {
  const k = document.body.dataset.vscodeThemeKind ?? "";
  if (k === "vscode-high-contrast") {
    return "hc-black";
  }
  if (k === "vscode-high-contrast-light") {
    return "hc-light";
  }
  if (k === "vscode-light") {
    return "vs";
  }
  return "vs-dark";
}

export function applyVSCodeTheme(): void {
  const base = themeBase();
  const isLight = base === "vs" || base === "hc-light";

  monaco.editor.defineTheme(RAPIDB_THEME, {
    base,
    inherit: true,

    rules: [],

    colors: {
      "editor.background":
        cssVar("--vscode-editor-background") ||
        (isLight ? "#ffffff" : "#1e1e1e"),
      "editor.foreground":
        cssVar("--vscode-editor-foreground") ||
        (isLight ? "#000000" : "#d4d4d4"),
    },
  });

  monaco.editor.setTheme(RAPIDB_THEME);
}

// ─── Completion Helpers ─────────────────────────────────────────────────────

export function monacoCompletionKindFor(
  kind: SqlCompletionSuggestionKind,
): monaco.languages.CompletionItemKind {
  switch (kind) {
    case "class":
      return monaco.languages.CompletionItemKind.Class;
    case "field":
      return monaco.languages.CompletionItemKind.Field;
    case "function":
      return monaco.languages.CompletionItemKind.Function;
    case "keyword":
      return monaco.languages.CompletionItemKind.Keyword;
    case "module":
      return monaco.languages.CompletionItemKind.Module;
    default:
      return monaco.languages.CompletionItemKind.Value;
  }
}

// ─── Formatting Helpers ─────────────────────────────────────────────────────

export function formatSQLSafe(sql: string, dialect = "sql"): string {
  if (!sql.trim()) {
    return sql;
  }
  return sqlFormatterFormat(sql, {
    language: dialect as SqlFormatterLanguage,
    tabWidth: 2,
    keywordCase: "upper",
    linesBetweenQueries: 1,
    indentStyle: "standard",
  });
}

export function formatSQLOrError(
  sql: string,
  dialect = "sql",
): { result: string } | { error: string } {
  if (!sql.trim()) {
    return { result: sql };
  }
  try {
    const result = sqlFormatterFormat(sql, {
      language: dialect as SqlFormatterLanguage,
      tabWidth: 2,
      keywordCase: "upper",
      linesBetweenQueries: 1,
      indentStyle: "standard",
    });
    return { result };
  } catch (err: unknown) {
    return {
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

export function formatJSONOrError(
  input: string,
): { result: string } | { error: string } {
  if (!input.trim()) {
    return { result: input };
  }

  try {
    return {
      result: JSON.stringify(JSON.parse(input) as unknown, null, 2),
    };
  } catch (err: unknown) {
    return {
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

export function formatEditorValueOrError(options: {
  value: string;
  language: string;
  dialect?: string;
}): { result: string } | { error: string } | null {
  const { value, language, dialect } = options;

  if (language === "sql") {
    return formatSQLOrError(value, dialect ?? "sql");
  }

  if (language === "json") {
    return formatJSONOrError(value);
  }

  return null;
}

// ─── Connection Type to Dialect ─────────────────────────────────────────────

export function connTypeToDialect(connType: string): QueryEditorSqlDialect {
  switch (connType) {
    case "mysql":
      return "mysql";
    case "pg":
      return "postgresql";
    case "sqlite":
      return "sqlite";
    case "mssql":
      return "transactsql";
    case "oracle":
      return "plsql";
    default:
      return "sql";
  }
}

// ─── Editor Format Application ──────────────────────────────────────────────

/**
 * Format the editor content and apply the result as an edit.
 *
 * Returns `null` on success (or when no formatting was needed),
 * or an error message string on failure.
 *
 * Centralises the format logic used by both the imperative `format()` handle
 * and the Shift+Alt+F keyboard shortcut.
 */
export function applyFormatToEditor(
  editor: monaco.editor.IStandaloneCodeEditor,
  options: { language: string; dialect?: string },
): string | null {
  if (editor.getOption(monaco.editor.EditorOption.readOnly)) {
    return null;
  }

  const model = editor.getModel();
  if (!model) {
    return null;
  }

  const raw = editor.getValue();
  const out = formatEditorValueOrError({
    value: raw,
    language: options.language,
    dialect: options.dialect,
  });

  if (!out) {
    return null;
  }

  if ("error" in out) {
    return out.error;
  }

  if (out.result === raw) {
    return null;
  }

  editor.executeEdits("format-sql", [
    {
      range: model.getFullModelRange(),
      text: out.result,
      forceMoveMarkers: true,
    },
  ]);
  editor.pushUndoStop();
  return null;
}
