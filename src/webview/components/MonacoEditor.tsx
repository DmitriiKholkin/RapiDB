import * as monaco from "monaco-editor";
import React, {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useRef,
  useState,
} from "react";
import { format as sqlFormatterFormat } from "sql-formatter";
import type { QueryEditorSqlDialect } from "../../shared/webviewContracts";
import type { SchemaObject } from "../store";
import { onMessage, postMessage } from "../utils/messaging";
import {
  buildSqlCompletionSuggestions,
  type SqlCompletionSuggestionKind,
} from "../utils/sqlCompletionSuggestions";

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

  placeCursor(options?: { reveal?: boolean; preserveViewport?: boolean }): void;
  selectAllKeepCursorEndScrollTop(): void;
}

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

function formatEditorValueOrError(options: {
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

interface Props {
  initialValue?: string;
  schema?: SchemaObject[];
  dialect?: string;
  language?: string;
  onChange?: (value: string) => void;
  onExecute?: (value: string) => void;
  height?: string | number;
  readOnly?: boolean;
  ariaLabel?: string;
}

interface ContextMenuState {
  x: number;
  y: number;
  hasSelection: boolean;
  readOnly: boolean;
}

export const MonacoEditor = forwardRef<MonacoEditorHandle, Props>(
  function MonacoEditor(
    {
      initialValue = "",
      schema = [],
      dialect = "sql",
      language = "sql",
      onChange,
      onExecute,
      height = "100%",
      readOnly = false,
      ariaLabel = "SQL editor",
    },
    ref,
  ) {
    const rootRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null);
    const initialValueRef = useRef(initialValue);
    const onChangeRef = useRef(onChange);
    const onExecuteRef = useRef(onExecute);
    const readOnlyRef = useRef(readOnly);
    const ariaLabelRef = useRef(ariaLabel);
    const languageRef = useRef(language);
    const suppressChangeEventRef = useRef(false);
    const getSelectedTextRef = useRef<() => string>(() => "");
    const copySelectionRef = useRef<() => void>(() => {});
    const cutSelectionRef = useRef<() => void>(() => {});
    const pasteClipboardRef = useRef<() => void>(() => {});
    const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(
      null,
    );
    const [hoveredItem, setHoveredItem] = useState<
      "copy" | "cut" | "paste" | null
    >(null);
    const menuRef = useRef<HTMLDivElement>(null);

    const closeContextMenu = useCallback(() => {
      setHoveredItem(null);
      setContextMenu(null);
    }, []);

    const runContextAction = (action: () => void) => {
      action();
      closeContextMenu();
      editorRef.current?.focus();
    };

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
        suppressChangeEventRef.current = true;
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

    useEffect(() => {
      languageRef.current = language;
      const editor = editorRef.current;
      if (editor) {
        const model = editor.getModel();
        if (model) {
          monaco.editor.setModelLanguage(model, language);
        }
      }
    }, [language]);

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
      format: (requestedDialect?: string) => {
        const editor = editorRef.current;
        if (!editor || editor.getOption(monaco.editor.EditorOption.readOnly)) {
          return null;
        }
        const model = editor.getModel();
        if (!model) {
          return null;
        }
        const raw = editor.getValue();
        const out = formatEditorValueOrError({
          value: raw,
          language: languageRef.current,
          dialect: requestedDialect ?? dialectRef.current,
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
      },
      focus: () => editorRef.current?.focus(),
      layout: () => editorRef.current?.layout(),
      placeCursor: (options) => {
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

        const preserveViewport = options?.preserveViewport ?? false;
        const scrollTop = preserveViewport ? editor.getScrollTop() : null;
        const scrollLeft = preserveViewport ? editor.getScrollLeft() : null;

        editor.setPosition(position);
        editor.focus();

        if (options?.reveal ?? true) {
          editor.revealPosition(position);
        }

        if (preserveViewport) {
          if (scrollTop !== null) {
            editor.setScrollTop(scrollTop);
          }
          if (scrollLeft !== null) {
            editor.setScrollLeft(scrollLeft);
          }
        }
      },
      selectAllKeepCursorEndScrollTop: () => {
        const editor = editorRef.current;
        if (!editor) {
          return;
        }
        const model = editor.getModel();
        if (!model) {
          editor.focus();
          return;
        }

        const lastLine = model.getLineCount();
        const lastColumn = model.getLineMaxColumn(lastLine);
        editor.setSelection(new monaco.Selection(1, 1, lastLine, lastColumn));
        editor.setScrollTop(0);
        editor.setScrollLeft(0);
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
        language: languageRef.current,
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
      const getSelectedText = () => {
        const model = editor.getModel();
        const selection = editor.getSelection();
        if (!model || !selection || selection.isEmpty()) {
          return "";
        }
        return model.getValueInRange(selection);
      };
      const deleteSelectedText = () => {
        if (readOnlyRef.current) {
          return;
        }
        const selection = editor.getSelection();
        if (!selection || selection.isEmpty()) {
          return;
        }
        editor.executeEdits("native-cut", [
          {
            range: selection,
            text: "",
            forceMoveMarkers: true,
          },
        ]);
        editor.pushUndoStop();
      };
      const copySelection = () => {
        const selectedText = getSelectedText();
        if (!selectedText) {
          return;
        }
        postMessage("writeClipboard", { text: selectedText });
      };
      const cutSelection = () => {
        const selectedText = getSelectedText();
        if (!selectedText || readOnlyRef.current) {
          return;
        }
        postMessage("writeClipboard", { text: selectedText });
        deleteSelectedText();
      };
      const pasteClipboard = () => {
        if (readOnlyRef.current) {
          return;
        }
        postMessage("readClipboard");
      };

      getSelectedTextRef.current = getSelectedText;
      copySelectionRef.current = copySelection;
      cutSelectionRef.current = cutSelection;
      pasteClipboardRef.current = pasteClipboard;

      const nativeCopy = (e: ClipboardEvent) => {
        const selectedText = getSelectedText();
        if (!selectedText) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
        postMessage("writeClipboard", { text: selectedText });
      };
      const nativeCut = (e: ClipboardEvent) => {
        const selectedText = getSelectedText();
        if (!selectedText || readOnlyRef.current) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
        postMessage("writeClipboard", { text: selectedText });
        deleteSelectedText();
      };

      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyC, () => {
        copySelection();
      });

      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyX, () => {
        cutSelection();
      });

      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyV, () => {
        pasteClipboard();
      });

      editor.addCommand(
        monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyV,
        () => {
          pasteClipboard();
        },
      );

      domNode?.addEventListener("copy", nativeCopy, true);
      domNode?.addEventListener("cut", nativeCut, true);
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
          const out = formatEditorValueOrError({
            value: raw,
            language: languageRef.current,
            dialect: dialectRef.current,
          });
          if (!out) {
            return;
          }
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
        if (suppressChangeEventRef.current) {
          suppressChangeEventRef.current = false;
          return;
        }

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
        domNode?.removeEventListener("copy", nativeCopy, true);
        domNode?.removeEventListener("cut", nativeCut, true);
        domNode?.removeEventListener("paste", nativePaste, true);
        getSelectedTextRef.current = () => "";
        copySelectionRef.current = () => {};
        cutSelectionRef.current = () => {};
        pasteClipboardRef.current = () => {};
        editor.dispose();
      };
    }, []);

    useEffect(() => {
      const root = rootRef.current;
      if (!root) {
        return;
      }

      const handleContextMenu = (event: MouseEvent) => {
        event.preventDefault();

        const bounds = root.getBoundingClientRect();
        const menuWidth = 100;
        const menuHeight = 50;
        const x = Math.min(
          Math.max(event.clientX - bounds.left, 4),
          Math.max(bounds.width - menuWidth, 4),
        );
        const y = Math.min(
          Math.max(event.clientY - bounds.top, 4),
          Math.max(bounds.height - menuHeight, 4),
        );

        setContextMenu({
          x,
          y,
          hasSelection: getSelectedTextRef.current().length > 0,
          readOnly: readOnlyRef.current,
        });
      };

      root.addEventListener("contextmenu", handleContextMenu);

      return () => {
        root.removeEventListener("contextmenu", handleContextMenu);
      };
    }, []);

    useEffect(() => {
      if (!contextMenu) {
        return;
      }

      const handlePointerDown = (event: PointerEvent) => {
        const menu = menuRef.current;
        if (menu?.contains(event.target as Node)) {
          return;
        }

        closeContextMenu();
      };

      const handleEscape = (event: KeyboardEvent) => {
        if (event.key === "Escape") {
          closeContextMenu();
        }
      };

      window.addEventListener("pointerdown", handlePointerDown);
      window.addEventListener("keydown", handleEscape);
      window.addEventListener("blur", closeContextMenu);

      return () => {
        window.removeEventListener("pointerdown", handlePointerDown);
        window.removeEventListener("keydown", handleEscape);
        window.removeEventListener("blur", closeContextMenu);
      };
    }, [closeContextMenu, contextMenu]);

    return (
      <div
        ref={rootRef}
        style={{
          position: "relative",
          width: "100%",
          height,
          overflow: "hidden",
        }}
      >
        <div
          ref={containerRef}
          style={{ width: "100%", height: "100%", overflow: "hidden" }}
        />
        {contextMenu ? (
          <div
            ref={menuRef}
            role="menu"
            aria-label="Editor context menu"
            style={{
              position: "absolute",
              top: contextMenu.y,
              left: contextMenu.x,
              minWidth: 100,
              padding: 3,
              display: "flex",
              flexDirection: "column",
              gap: 0,
              background:
                cssVar("--vscode-menu-background") ||
                cssVar("--vscode-editorWidget-background") ||
                cssVar("--vscode-editor-background") ||
                "#252526",
              border: `1px solid ${
                cssVar("--vscode-menu-border") ||
                cssVar("--vscode-contrastBorder") ||
                "rgba(255, 255, 255, 0.12)"
              }`,
              borderRadius: 8,
              boxShadow:
                "0 10px 30px rgba(0, 0, 0, 0.24), 0 2px 8px rgba(0, 0, 0, 0.18)",
              zIndex: 20,
            }}
          >
            <button
              type="button"
              role="menuitem"
              disabled={!contextMenu.hasSelection || contextMenu.readOnly}
              onClick={() => {
                runContextAction(cutSelectionRef.current);
              }}
              onMouseEnter={() => {
                setHoveredItem("cut");
              }}
              onMouseLeave={() => {
                setHoveredItem(null);
              }}
              style={menuButtonStyle(
                !contextMenu.hasSelection || contextMenu.readOnly,
                hoveredItem === "cut",
              )}
            >
              Cut
            </button>
            <button
              type="button"
              role="menuitem"
              disabled={!contextMenu.hasSelection}
              onClick={() => {
                runContextAction(copySelectionRef.current);
              }}
              onMouseEnter={() => {
                setHoveredItem("copy");
              }}
              onMouseLeave={() => {
                setHoveredItem(null);
              }}
              style={menuButtonStyle(
                !contextMenu.hasSelection,
                hoveredItem === "copy",
              )}
            >
              Copy
            </button>
            <button
              type="button"
              role="menuitem"
              disabled={contextMenu.readOnly}
              onClick={() => {
                runContextAction(pasteClipboardRef.current);
              }}
              onMouseEnter={() => {
                setHoveredItem("paste");
              }}
              onMouseLeave={() => {
                setHoveredItem(null);
              }}
              style={menuButtonStyle(
                contextMenu.readOnly,
                hoveredItem === "paste",
              )}
            >
              Paste
            </button>
          </div>
        ) : null}
      </div>
    );
  },
);

function menuButtonStyle(
  disabled: boolean,
  hovered = false,
): React.CSSProperties {
  const hoverBg =
    cssVar("--vscode-menu-selectionBackground") || "rgba(255, 255, 255, 0.10)";

  return {
    appearance: "none",
    border: "none",
    background: hovered && !disabled ? hoverBg : "transparent",
    color: disabled
      ? cssVar("--vscode-disabledForeground") || "rgba(255, 255, 255, 0.4)"
      : hovered
        ? cssVar("--vscode-menu-selectionForeground") ||
          cssVar("--vscode-menu-foreground") ||
          cssVar("--vscode-foreground") ||
          "#ffffff"
        : cssVar("--vscode-menu-foreground") ||
          cssVar("--vscode-foreground") ||
          "#cccccc",
    borderRadius: 5,
    padding: "5px 10px",
    textAlign: "left",
    font: "inherit",
    cursor: disabled ? "default" : "pointer",
    opacity: disabled ? 0.6 : 1,
    width: "100%",
  };
}
