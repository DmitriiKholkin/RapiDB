import * as monaco from "monaco-editor";
import React, {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useRef,
} from "react";
import { format as sqlFormatterFormat } from "sql-formatter";
import type { SchemaObject } from "../store";
import { onMessage, postMessage } from "../utils/messaging";
import {
  buildSqlCompletionSuggestions,
  type SqlCompletionSuggestionKind,
} from "./sqlCompletionSuggestions";

type MonacoHostWindow = Window & {
  __monacoEnvSet?: boolean;
};

type MonacoHostGlobal = typeof globalThis & {
  MonacoEnvironment?: {
    getWorker(): Worker;
  };
};

type SqlFormatterOptions = NonNullable<
  Parameters<typeof sqlFormatterFormat>[1]
>;
type SqlFormatterLanguage = NonNullable<SqlFormatterOptions["language"]>;

const monacoWindow = window as MonacoHostWindow;
const monacoGlobal = self as MonacoHostGlobal;

if (!monacoWindow.__monacoEnvSet) {
  monacoWindow.__monacoEnvSet = true;
  monacoGlobal.MonacoEnvironment = {
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

const RAPIDB_THEME = "rapidb-vscode";

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

let providerDisposable: monaco.IDisposable | null = null;

let getActiveSchema: () => SchemaObject[] = () => [];

function monacoCompletionKindFor(
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

function ensureCompletionProvider() {
  if (providerDisposable) {
    return;
  }

  providerDisposable = monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: [" ", ".", "(", ","],

    provideCompletionItems(model, position) {
      const schema = getActiveSchema();

      const word = model.getWordUntilPosition(position);
      const range: monaco.IRange = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };

      const lineUpToCursor = model
        .getLineContent(position.lineNumber)
        .slice(0, position.column - 1);
      return {
        suggestions: buildSqlCompletionSuggestions(schema, lineUpToCursor).map(
          (item) => ({
            ...item,
            kind: monacoCompletionKindFor(item.kind),
            range,
          }),
        ),
      };
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

interface Props {
  initialValue?: string;
  schema?: SchemaObject[];
  dialect?: string;
  onChange?: (value: string) => void;
  onExecute?: (value: string) => void;
  height?: string | number;
  readOnly?: boolean;
  ariaLabel?: string;
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
      readOnly = false,
      ariaLabel = "SQL editor",
    },
    ref,
  ) {
    const containerRef = useRef<HTMLDivElement>(null);
    const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null);
    const initialValueRef = useRef(initialValue);
    const onChangeRef = useRef(onChange);
    const onExecuteRef = useRef(onExecute);
    const readOnlyRef = useRef(readOnly);
    const ariaLabelRef = useRef(ariaLabel);

    useEffect(() => {
      initialValueRef.current = initialValue;
    }, [initialValue]);

    useEffect(() => {
      onChangeRef.current = onChange;
    }, [onChange]);

    useEffect(() => {
      onExecuteRef.current = onExecute;
    }, [onExecute]);

    useEffect(() => {
      readOnlyRef.current = readOnly;
      editorRef.current?.updateOptions({
        readOnly,
        domReadOnly: readOnly,
      });
    }, [readOnly]);

    useEffect(() => {
      ariaLabelRef.current = ariaLabel;
      editorRef.current?.updateOptions({
        ariaLabel,
      });
    }, [ariaLabel]);

    useEffect(() => {
      const editor = editorRef.current;
      if (!editor) {
        return;
      }
      if (editor.getValue() !== initialValue) {
        editor.setValue(initialValue);
      }
    }, [initialValue]);

    const schemaRef = useRef<SchemaObject[]>([]);
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
        if (!editor || editor.getOption(monaco.editor.EditorOption.readOnly)) {
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
        value: initialValueRef.current,
        language: "sql",
        theme: RAPIDB_THEME,
        ariaLabel: ariaLabelRef.current,
        fontSize: parseInt(
          getComputedStyle(document.body).getPropertyValue(
            "--vscode-editor-font-size",
          ) || "13",
          10,
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
        readOnly: readOnlyRef.current,
        domReadOnly: readOnlyRef.current,
      });
      editorRef.current = editor;

      const insertText = (text: string) => {
        if (readOnlyRef.current) {
          return;
        }
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

      const domNode = editor.getDomNode();
      const nativePaste = (e: ClipboardEvent) => {
        if (readOnlyRef.current) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
        postMessage("readClipboard");
      };

      const getExecText = () => {
        const model = editor.getModel();
        const selection = editor.getSelection();
        if (model && selection && !selection.isEmpty()) {
          return model.getValueInRange(selection);
        }
        return editor.getValue();
      };

      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyV, () => {
        if (readOnlyRef.current) {
          return;
        }
        postMessage("readClipboard");
      });
      editor.addCommand(
        monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyV,
        () => {
          if (readOnlyRef.current) {
            return;
          }
          postMessage("readClipboard");
        },
      );
      domNode?.addEventListener("paste", nativePaste, true);

      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
        if (readOnlyRef.current) {
          return;
        }
        onExecuteRef.current?.(getExecText());
      });
      editor.addCommand(monaco.KeyCode.F5, () => {
        if (readOnlyRef.current) {
          return;
        }
        onExecuteRef.current?.(getExecText());
      });

      editor.addCommand(
        monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF,
        () => {
          if (readOnlyRef.current) {
            return;
          }
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
        onChangeRef.current?.(editor.getValue());
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
