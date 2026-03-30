import * as monaco from "monaco-editor";
import React, {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useRef,
} from "react";
import { format as sqlFormatterFormat } from "sql-formatter";
import type { SchemaTable } from "../store";
import { onMessage, postMessage } from "../utils/messaging";

if (!(window as any).__monacoEnvSet) {
  (window as any).__monacoEnvSet = true;
  (self as any).MonacoEnvironment = {
    getWorker(): Worker {
      const blob = new Blob(["self.onmessage=function(){}"], {
        type: "application/javascript",
      });
      const url = URL.createObjectURL(blob);
      const worker = new Worker(url);

      URL.revokeObjectURL(url);
      return worker;
    },
  };
}

const HAPPYDB_THEME = "rapidb-vscode";

function cssVar(name: string): string {
  return getComputedStyle(document.body).getPropertyValue(name).trim();
}

function themeBase(): "vs-dark" | "vs" | "hc-black" | "hc-light" {
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

function applyVSCodeTheme(): void {
  const base = themeBase();
  const isLight = base === "vs" || base === "hc-light";

  monaco.editor.defineTheme(HAPPYDB_THEME, {
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

  monaco.editor.setTheme(HAPPYDB_THEME);
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
];

let providerDisposable: monaco.IDisposable | null = null;

let getActiveSchema: () => SchemaTable[] = () => [];

function ensureCompletionProvider() {
  if (providerDisposable) {
    return;
  }

  providerDisposable = monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: [" ", ".", "\n", "(", ","],

    provideCompletionItems(model, position) {
      const schema = getActiveSchema();

      const word = model.getWordUntilPosition(position);
      const range: monaco.IRange = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };

      const items: monaco.languages.CompletionItem[] = [];

      const lineUpToCursor = model
        .getLineContent(position.lineNumber)
        .slice(0, position.column - 1);

      const schemaDotTableDot = lineUpToCursor.match(/(\w+)\.(\w+)\.\s*(\w*)$/);
      if (schemaDotTableDot) {
        const schemaHint = schemaDotTableDot[1].toLowerCase();
        const tableHint = schemaDotTableDot[2].toLowerCase();
        const matched = schema.find(
          (t) =>
            t.schema.toLowerCase() === schemaHint &&
            t.table.toLowerCase() === tableHint,
        );
        if (matched) {
          matched.columns.forEach((col, i) => {
            items.push({
              label: col.name,
              detail: col.type,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col.name,
              range,
              sortText: String(i).padStart(5, "0"),
            });
          });
          return { suggestions: items };
        }
      }

      const dotMatch = lineUpToCursor.match(/(\w+)\.(\w*)$/);
      if (dotMatch) {
        const hint = dotMatch[1].toLowerCase();

        const schemasWithHint = schema.filter(
          (t) => t.schema.toLowerCase() === hint,
        );
        if (schemasWithHint.length > 0) {
          schemasWithHint.forEach((t, i) => {
            items.push({
              label: t.table,
              detail:
                t.columns.length > 0
                  ? `table (${t.columns.length} cols)`
                  : "table",
              kind: monaco.languages.CompletionItemKind.Class,
              insertText: t.table,
              range,
              sortText: String(i).padStart(5, "0"),
            });
          });
          return { suggestions: items };
        }

        const matchedTable = schema.find((t) => t.table.toLowerCase() === hint);
        if (matchedTable) {
          matchedTable.columns.forEach((col, i) => {
            items.push({
              label: col.name,
              detail: col.type,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col.name,
              range,
              sortText: String(i).padStart(5, "0"),
            });
          });
          return { suggestions: items };
        }

        return { suggestions: [] };
      }

      SQL_KEYWORDS.forEach((kw, i) => {
        items.push({
          label: kw,
          kind: monaco.languages.CompletionItemKind.Keyword,
          insertText: kw,
          range,
          sortText: `3_${String(i).padStart(5, "0")}`,
        });
      });

      const schemaNames = [...new Set(schema.map((t) => t.schema))];
      schemaNames.forEach((sn, i) => {
        items.push({
          label: sn,
          detail: "schema / database",
          kind: monaco.languages.CompletionItemKind.Module,
          insertText: sn,
          range,
          sortText: `1_${String(i).padStart(5, "0")}`,
        });
      });

      const primarySchema = schemaNames[0] ?? "";
      schema.forEach((t, ti) => {
        const isPrimary = t.schema === primarySchema;

        items.push({
          label: t.table,
          detail: isPrimary
            ? `table in ${t.schema} (${t.columns.length} cols)`
            : `table in ${t.schema}`,
          kind: monaco.languages.CompletionItemKind.Class,
          insertText: t.table,
          range,
          sortText: `2_${isPrimary ? "0" : "1"}_${String(ti).padStart(5, "0")}_tbl`,
        });

        items.push({
          label: `${t.schema}.${t.table}`,
          detail: isPrimary
            ? `qualified (${t.columns.length} cols)`
            : "qualified",
          kind: monaco.languages.CompletionItemKind.Class,
          insertText: `${t.schema}.${t.table}`,
          range,
          sortText: `2_${isPrimary ? "0" : "1"}_${String(ti).padStart(5, "0")}_qual`,
        });

        if (isPrimary) {
          t.columns.forEach((col, ci) => {
            items.push({
              label: col.name,
              detail: `${t.table}.${col.name}  ${col.type}`,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col.name,
              range,
              sortText: `2_0_${String(ti).padStart(5, "0")}_col_${String(ci).padStart(5, "0")}`,
            });
          });
        }
      });

      return { suggestions: items };
    },
  });
}

export interface MonacoEditorHandle {
  getValue(): string;
  getSelectionOrValue(): string;
  setValue(v: string): void;

  format(dialect?: string): string | null;
  focus(): void;
  layout(): void;

  placeCursor(): void;
}

export function connTypeToDialect(connType: string): string {
  switch (connType) {
    case "mysql":
      return "mysql";
    case "pg":
      return "postgresql";
    case "sqlite":
      return "sqlite";
    case "mssql":
      return "tsql";
    case "oracle":
      return "plsql";
    default:
      return "sql";
  }
}

export function formatSQLSafe(sql: string, dialect = "sql"): string {
  if (!sql.trim()) {
    return sql;
  }
  try {
    return sqlFormatterFormat(sql, {
      language: dialect as any,
      tabWidth: 2,
      keywordCase: "upper",
      linesBetweenQueries: 1,
      indentStyle: "standard",
    });
  } catch (err) {
    throw err;
  }
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
      language: dialect as any,
      tabWidth: 2,
      keywordCase: "upper",
      linesBetweenQueries: 1,
      indentStyle: "standard",
    });
    return { result };
  } catch (err: any) {
    return { error: err?.message ?? String(err) };
  }
}

interface Props {
  initialValue?: string;
  schema?: SchemaTable[];
  dialect?: string;
  onChange?: (value: string) => void;
  onExecute?: (value: string) => void;
  height?: string | number;
}

export const MonacoEditor = forwardRef<MonacoEditorHandle, Props>(
  function MonacoEditor(
    {
      initialValue = "",
      schema = [],
      dialect = "sql",
      onChange,
      onExecute,
      height = "100%",
    },
    ref,
  ) {
    const containerRef = useRef<HTMLDivElement>(null);
    const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null);

    const schemaRef = useRef<SchemaTable[]>([]);
    useEffect(() => {
      schemaRef.current = schema;
    }, [schema]);

    useEffect(() => {
      getActiveSchema = () => schemaRef.current;
      return () => {
        getActiveSchema = () => [];
      };
    }, []);

    const dialectRef = useRef(dialect);
    useEffect(() => {
      dialectRef.current = dialect;
    }, [dialect]);

    useImperativeHandle(ref, () => ({
      getValue: () => editorRef.current?.getValue() ?? "",
      getSelectionOrValue: () => {
        const editor = editorRef.current;
        if (!editor) return "";
        const model = editor.getModel();
        const selection = editor.getSelection();
        if (model && selection && !selection.isEmpty()) {
          return model.getValueInRange(selection);
        }
        return editor.getValue() ?? "";
      },
      setValue: (v) => editorRef.current?.setValue(v),
      format: (dialect = "sql") => {
        const editor = editorRef.current;
        if (!editor) {
          return null;
        }
        const model = editor.getModel();
        if (!model) {
          return null;
        }
        const raw = editor.getValue();
        const out = formatSQLOrError(raw, dialect);
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
      },
      focus: () => editorRef.current?.focus(),
      layout: () => editorRef.current?.layout(),
      placeCursor: () => {
        const editor = editorRef.current;
        if (!editor) {
          return;
        }
        const model = editor.getModel();
        if (!model) {
          editor.focus();
          return;
        }
        const content = editor.getValue();
        let position: monaco.IPosition;
        if (!content.trim()) {
          position = { lineNumber: 1, column: 1 };
        } else {
          const lastLine = model.getLineCount();
          position = {
            lineNumber: lastLine,
            column: model.getLineMaxColumn(lastLine),
          };
        }
        editor.setPosition(position);
        editor.revealPosition(position);
        editor.focus();
      },
    }));

    useEffect(() => {
      if (!containerRef.current) {
        return;
      }

      ensureCompletionProvider();

      applyVSCodeTheme();

      const editor = monaco.editor.create(containerRef.current, {
        value: initialValue,
        language: "sql",
        theme: HAPPYDB_THEME,
        fontSize: parseInt(
          getComputedStyle(document.body).getPropertyValue(
            "--vscode-editor-font-size",
          ) || "13",
        ),
        fontFamily:
          getComputedStyle(document.body)
            .getPropertyValue("--vscode-editor-font-family")
            .trim() || "Menlo, Monaco, 'Courier New', monospace",
        lineNumbers: "on",
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        automaticLayout: true,
        wordWrap: "on",
        tabSize: 2,
        insertSpaces: true,
        renderWhitespace: "selection",
        cursorBlinking: "smooth",
        smoothScrolling: true,
        contextmenu: false,
        folding: true,
        scrollbar: { verticalScrollbarSize: 8, horizontalScrollbarSize: 8 },
        overviewRulerLanes: 0,
        hideCursorInOverviewRuler: true,
        padding: { top: 8, bottom: 8 },
        suggestOnTriggerCharacters: true,
        quickSuggestions: { other: true, comments: false, strings: false },
        suggest: { showKeywords: true, showWords: false, filterGraceful: true },
      });
      editorRef.current = editor;

      const insertText = (text: string) => {
        if (!text) {
          return;
        }
        const model = editor.getModel();
        if (!model) {
          editor.trigger("keyboard", "type", { text });
          return;
        }
        const selection = editor.getSelection();
        const range = selection
          ? {
              startLineNumber: selection.startLineNumber,
              startColumn: selection.startColumn,
              endLineNumber: selection.endLineNumber,
              endColumn: selection.endColumn,
            }
          : (() => {
              const pos = editor.getPosition() ?? { lineNumber: 1, column: 1 };
              return {
                startLineNumber: pos.lineNumber,
                startColumn: pos.column,
                endLineNumber: pos.lineNumber,
                endColumn: pos.column,
              };
            })();
        editor.executeEdits("paste", [{ range, text, forceMoveMarkers: true }]);
        editor.pushUndoStop();
        const newPos = model.getPositionAt(
          model.getOffsetAt({
            lineNumber: range.startLineNumber,
            column: range.startColumn,
          }) + text.length,
        );
        editor.setPosition(newPos);
        editor.revealPosition(newPos);
      };

      const unsubClipboard = onMessage<string>("clipboardText", (text) => {
        insertText(text);
        editor.focus();
      });

      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyV, () =>
        postMessage("readClipboard"),
      );
      editor.addCommand(
        monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyV,
        () => postMessage("readClipboard"),
      );

      const domNode = editor.getDomNode();
      const nativePaste = (e: ClipboardEvent) => {
        e.preventDefault();
        e.stopImmediatePropagation();
        postMessage("readClipboard");
      };
      domNode?.addEventListener("paste", nativePaste, true);

      const getExecText = () => {
        const model = editor.getModel();
        const selection = editor.getSelection();
        if (model && selection && !selection.isEmpty()) {
          return model.getValueInRange(selection);
        }
        return editor.getValue();
      };

      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () =>
        onExecute?.(getExecText()),
      );
      editor.addCommand(monaco.KeyCode.F5, () => onExecute?.(getExecText()));

      editor.addCommand(
        monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF,
        () => {
          const model = editor.getModel();
          if (!model) {
            return;
          }
          const raw = editor.getValue();
          const out = formatSQLOrError(raw, dialectRef.current ?? "sql");
          if ("error" in out) {
            return;
          }
          if (out.result === raw) {
            return;
          }
          editor.executeEdits("format-sql", [
            {
              range: model.getFullModelRange(),
              text: out.result,
              forceMoveMarkers: true,
            },
          ]);
          editor.pushUndoStop();
        },
      );

      const changeDisposable = editor.onDidChangeModelContent(() => {
        onChange?.(editor.getValue());
      });

      const observer = new MutationObserver(() => {
        applyVSCodeTheme();
      });
      observer.observe(document.body, {
        attributes: true,
        attributeFilter: ["data-vscode-theme-kind", "class"],
      });

      return () => {
        observer.disconnect();
        changeDisposable.dispose();
        unsubClipboard();
        domNode?.removeEventListener("paste", nativePaste, true);
        editor.dispose();
      };
    }, []);

    return (
      <div
        ref={containerRef}
        style={{ width: "100%", height, overflow: "hidden" }}
      />
    );
  },
);
