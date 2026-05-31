import type { CSSProperties } from "react";
import type {
  QueryEditorLanguage,
  QueryEditorPresentation,
  QueryEditorSqlDialect,
} from "../../../shared/webviewContracts";
import type { ConnectionEntry } from "../../store";
import { buildButtonStyle } from "../../utils/buttonStyles";
import { buildSelectControlStyle } from "../../utils/controlStyles";
import { TOOLBAR_H } from "../../utils/layout";

export { TOOLBAR_H };

export const DIVIDER_H = 5;
export const MIN_EDITOR_H = 80;

export const DEFAULT_EDITOR_RATIO = 0.7;
export const DEFAULT_EDITOR_H = 400;

export const queryViewRootStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  height: "100vh",
  overflow: "hidden",
  background: "var(--vscode-editor-background)",
  color: "var(--vscode-foreground)",
};

export const queryToolbarStyle: CSSProperties = {
  height: TOOLBAR_H,
  flexShrink: 0,
  display: "flex",
  alignItems: "center",
  gap: 8,
  padding: "0 10px",
  borderBottom: "1px solid var(--vscode-panel-border)",
  background: "var(--vscode-editorGroupHeader-tabsBackground)",
};

function buildQueryButtonStyle(
  variant: "primary" | "ghost",
  disabled = false,
): CSSProperties {
  return buildButtonStyle(variant, { disabled, gap: 4, size: "sm" });
}

export function buildQueryPrimaryButtonStyle(disabled = false): CSSProperties {
  return buildQueryButtonStyle("primary", disabled);
}

export function buildQueryGhostButtonStyle(disabled = false): CSSProperties {
  return buildQueryButtonStyle("ghost", disabled);
}

export const querySelectStyle: CSSProperties = {
  ...buildSelectControlStyle("sm"),
  maxWidth: 220,
};

function resolveActiveEditorPresentation(
  resolvedConnectionId: string,
  initialConnectionId: string,
  activeConnection: ConnectionEntry | undefined,
  initialEditorPresentation?: QueryEditorPresentation,
): QueryEditorPresentation | undefined {
  return (
    activeConnection?.editorPresentation ??
    (resolvedConnectionId === initialConnectionId
      ? initialEditorPresentation
      : undefined)
  );
}

export interface QueryEditorState {
  activeEditorPresentation?: QueryEditorPresentation;
  allowSqlFormatting: boolean;
  canFormat: boolean;
  editorLabel: string;
  formatButtonTitle: string;
  formatErrorPrefix: string;
  monacoLanguage: QueryEditorLanguage;
  shouldFormatOnOpen: boolean;
  sqlDialect?: QueryEditorSqlDialect;
}

interface ResolveQueryEditorStateParams {
  activeConnection: ConnectionEntry | undefined;
  editorLanguage?: QueryEditorLanguage;
  editorPresentation?: QueryEditorPresentation;
  formatOnOpen: boolean;
  initialConnectionId: string;
  resolvedConnectionId: string;
}

export function resolveQueryEditorState({
  activeConnection,
  editorLanguage,
  editorPresentation,
  formatOnOpen,
  initialConnectionId,
  resolvedConnectionId,
}: ResolveQueryEditorStateParams): QueryEditorState {
  const activeEditorPresentation = resolveActiveEditorPresentation(
    resolvedConnectionId,
    initialConnectionId,
    activeConnection,
    editorPresentation,
  );

  const shouldFormatOnOpen =
    activeEditorPresentation?.formatOnOpen ??
    editorPresentation?.formatOnOpen ??
    formatOnOpen;
  const monacoLanguage =
    activeEditorPresentation?.editorLanguage ?? editorLanguage ?? "sql";
  const sqlDialect =
    monacoLanguage === "sql"
      ? (activeEditorPresentation?.sqlDialect ?? "sql")
      : undefined;
  const allowSqlFormatting =
    monacoLanguage === "sql"
      ? (activeEditorPresentation?.allowFormatting ?? true)
      : false;
  const canFormat =
    monacoLanguage === "json"
      ? true
      : monacoLanguage === "sql"
        ? allowSqlFormatting
        : false;

  return {
    activeEditorPresentation,
    allowSqlFormatting,
    canFormat,
    editorLabel: monacoLanguage === "sql" ? "SQL editor" : "Query editor",
    formatButtonTitle:
      monacoLanguage === "json"
        ? "Format JSON (Shift+Alt+F)"
        : "Format SQL (Shift+Alt+F)",
    formatErrorPrefix:
      monacoLanguage === "json" ? "JSON parse error" : "SQL format error",
    monacoLanguage,
    shouldFormatOnOpen,
    sqlDialect,
  };
}
