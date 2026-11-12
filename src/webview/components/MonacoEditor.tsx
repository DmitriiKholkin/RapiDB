import * as monaco from "monaco-editor";
import type React from "react";
import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useRef,
  useState,
} from "react";
import { format as sqlFormatterFormat } from "sql-formatter";
import type { ClipboardTextPayload } from "../../shared/webviewContracts";
import type { ClipboardWrittenPayload } from "../../shared/webviewContracts/table";
import type { SchemaObject } from "../store";
import { formatJsonPreservingTokens } from "../utils/jsonFormatting";
import { onMessage, postMessage } from "../utils/messaging";
import {
  buildSqlCompletionSuggestions,
  type SqlCompletionSuggestionKind,
} from "../utils/sqlCompletionSuggestions";
import {
  buildClipboardData,
  buildPasteEdits,
  type EditorClipboardData,
  rememberClipboardData,
  toPlainRange,
} from "./monacoClipboard";

type MonacoHostWindow = Window & {
  __monacoEnvSet?: boolean;
};

type MonacoHostGlobal = typeof globalThis & {
  MonacoEnvironment?: {
    getWorker(): Worker;
  };
};

// Monaco 0.53 preserves these runtime fields, omitted from monaco.d.ts.
// Tracking prevents operation reduction from dropping per-selection identifiers.
interface TrackedPasteOperation
  extends monaco.editor.IIdentifiedSingleEditOperation {
  identifier: { major: number; minor: number };
  _isTracked: true;
}

type IdentifiedInverseEdit = monaco.editor.IValidEditOperation & {
  identifier?: TrackedPasteOperation["identifier"] | null;
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

const schemaGetterByModel = new WeakMap<
  monaco.editor.ITextModel,
  () => SchemaObject[]
>();

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
      const schema = schemaGetterByModel.get(model)?.() ?? [];

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

function getSelectionOrValueFromEditor(
  editor: monaco.editor.IStandaloneCodeEditor,
): string | null {
  const model = editor.getModel();
  const selections = (editor.getSelections() ?? []).filter(
    (selection) => !selection.isEmpty(),
  );
  if (!model || selections.length === 0) {
    return editor.getValue();
  }
  if (selections.length > 1) {
    return null;
  }
  return model.getValueInRange(selections[0]);
}

export interface MonacoEditorHandle {
  getValue(): string;
  getSelectionOrValue(): string | null;
  setValue(v: string): void;
  clearValue(): void;

  format(dialect?: string): string | null;
  focus(): void;
  layout(): void;

  placeCursor(options?: { reveal?: boolean; preserveViewport?: boolean }): void;
  selectAllKeepCursorEndScrollTop(): void;
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
  return formatJsonPreservingTokens(input);
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
  onSelectionChange?: () => void;
  onExecute?: (value: string | null) => void;
  height?: string | number;
  readOnly?: boolean;
  allowFormatting?: boolean;
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
      onSelectionChange,
      onExecute,
      height = "100%",
      readOnly = false,
      allowFormatting = true,
      ariaLabel = "SQL editor",
    },
    ref,
  ) {
    const rootRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null);
    const initialValueRef = useRef(initialValue);
    const onChangeRef = useRef(onChange);
    const onSelectionChangeRef = useRef(onSelectionChange);
    const onExecuteRef = useRef(onExecute);
    const readOnlyRef = useRef(readOnly);
    const allowFormattingRef = useRef(allowFormatting);
    const ariaLabelRef = useRef(ariaLabel);
    const languageRef = useRef(language);
    const suppressChangeEventRef = useRef(false);
    const hasSelectionRef = useRef<() => boolean>(() => false);
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
      onSelectionChangeRef.current = onSelectionChange;
    }, [onSelectionChange]);

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
      allowFormattingRef.current = allowFormatting;
    }, [allowFormatting]);

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
        return getSelectionOrValueFromEditor(editor);
      },
      setValue: (v) => editorRef.current?.setValue(v),
      clearValue: () => {
        const editor = editorRef.current;
        const model = editor?.getModel();
        if (!editor || !model || model.getValueLength() === 0) return;
        editor.pushUndoStop();
        editor.executeEdits("rapidb-clear-query", [
          { range: model.getFullModelRange(), text: "" },
        ]);
        editor.pushUndoStop();
      },
      format: (requestedDialect?: string) => {
        const editor = editorRef.current;
        if (
          !editor ||
          !allowFormattingRef.current ||
          editor.getOption(monaco.editor.EditorOption.readOnly)
        ) {
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

      const editorModel = editor.getModel();
      const getSchema = () => schemaRef.current;
      if (editorModel) {
        schemaGetterByModel.set(editorModel, getSchema);
      }

      const getEditorSelections = (): monaco.Selection[] => {
        const selections = editor.getSelections();
        if (selections && selections.length > 0) {
          return selections;
        }
        const selection = editor.getSelection();
        return selection ? [selection] : [];
      };

      const getInsertRanges = (): monaco.IRange[] => {
        const selections = getEditorSelections();
        if (selections.length > 0) {
          return selections.map(toPlainRange);
        }
        const pos = editor.getPosition() ?? { lineNumber: 1, column: 1 };
        return [
          {
            startLineNumber: pos.lineNumber,
            startColumn: pos.column,
            endLineNumber: pos.lineNumber,
            endColumn: pos.column,
          },
        ];
      };

      const getClipboardData = (): EditorClipboardData | null => {
        const model = editor.getModel();
        const selections = getEditorSelections();
        if (!model || selections.length === 0) {
          return null;
        }
        return buildClipboardData(model, selections.map(toPlainRange));
      };

      const insertText = (text: string, ranges: monaco.IRange[]) => {
        if (readOnlyRef.current) {
          return;
        }
        const model = editor.getModel();
        if (!model) {
          editor.trigger("keyboard", "type", { text });
          return;
        }
        const edits = buildPasteEdits(text, ranges);
        const operations = edits.map(
          ({ range, text: editText }, index): TrackedPasteOperation => ({
            range,
            text: editText,
            forceMoveMarkers: true,
            identifier: { major: index, minor: 0 },
            _isTracked: true,
          }),
        );
        editor.executeEdits("paste", operations, (inverseEdits) => {
          // Touching inverses use document order, not input order. Auto-trim
          // edits may also be added, without an identifier. Match by identity.
          const insertedRanges = new Map<number, monaco.Range>();
          for (const edit of inverseEdits as IdentifiedInverseEdit[]) {
            if (edit.identifier?.minor === 0) {
              insertedRanges.set(edit.identifier.major, edit.range);
            }
          }
          const selections: monaco.Selection[] = [];
          for (const [index, edit] of edits.entries()) {
            const range = insertedRanges.get(index);
            if (!range) return null;
            const column = edit.keepColumn ?? range.endColumn;
            selections.push(
              new monaco.Selection(
                range.endLineNumber,
                column,
                range.endLineNumber,
                column,
              ),
            );
          }
          return selections;
        });
        editor.pushUndoStop();
        const newPos = editor.getPosition();
        if (newPos) {
          editor.revealPosition(newPos);
        }
      };

      const clipboardRecipient = `monaco:${crypto.randomUUID()}`;
      let pendingClipboardRequest: {
        requestId: string;
        model: monaco.editor.ITextModel | null;
        versionId: number | null;
        ranges: monaco.IRange[];
      } | null = null;
      const requestClipboard = () => {
        const requestId = crypto.randomUUID();
        const model = editor.getModel();
        pendingClipboardRequest = {
          requestId,
          model,
          versionId: model?.getVersionId() ?? null,
          ranges: getInsertRanges(),
        };
        postMessage("readClipboard", {
          requestId,
          recipient: clipboardRecipient,
        });
      };
      const unsubClipboard = onMessage<ClipboardTextPayload>(
        "clipboardText",
        (payload) => {
          const pending = pendingClipboardRequest;
          if (
            payload.recipient !== clipboardRecipient ||
            payload.requestId !== pending?.requestId
          )
            return;
          pendingClipboardRequest = null;
          if (payload.text === "") {
            return;
          }
          const model = editor.getModel();
          if (
            model !== pending.model ||
            (model && model.getVersionId() !== pending.versionId)
          ) {
            return;
          }
          insertText(payload.text, pending.ranges);
          editor.focus();
        },
      );
      let pendingCutRequest: {
        requestId: string;
        model: monaco.editor.ITextModel | null;
        versionId: number | null;
        ranges: monaco.IRange[];
      } | null = null;
      const unsubClipboardWritten = onMessage<ClipboardWrittenPayload>(
        "clipboardWritten",
        (payload) => {
          const pending = pendingCutRequest;
          if (
            payload.recipient !== clipboardRecipient ||
            payload.requestId !== pending?.requestId
          )
            return;
          pendingCutRequest = null;
          const model = editor.getModel();
          if (
            !payload.success ||
            readOnlyRef.current ||
            model !== pending.model ||
            (model && model.getVersionId() !== pending.versionId)
          ) {
            return;
          }
          editor.executeEdits(
            "native-cut",
            pending.ranges.map((range) => ({
              range,
              text: "",
              forceMoveMarkers: true,
            })),
          );
          editor.pushUndoStop();
        },
      );

      const domNode = editor.getDomNode();
      const isWidgetClipboardTarget = (target: EventTarget | null) =>
        target instanceof Element &&
        (target.closest(".overlayWidgets, .contentWidgets") !== null ||
          target instanceof HTMLInputElement ||
          (target instanceof HTMLTextAreaElement &&
            !target.matches(".inputarea, .ime-text-area")));
      const nativePaste = (e: ClipboardEvent) => {
        if (readOnlyRef.current || isWidgetClipboardTarget(e.target)) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
        requestClipboard();
      };

      const getExecText = () => getSelectionOrValueFromEditor(editor);
      const hasSelection = () =>
        getEditorSelections().some((selection) => !selection.isEmpty());
      const writeClipboardThenDelete = (data: EditorClipboardData) => {
        const requestId = crypto.randomUUID();
        const model = editor.getModel();
        pendingCutRequest = {
          requestId,
          model,
          versionId: model?.getVersionId() ?? null,
          ranges: data.cutRanges,
        };
        rememberClipboardData(data);
        postMessage("writeClipboard", {
          text: data.text,
          requestId,
          recipient: clipboardRecipient,
        });
      };
      const copySelection = () => {
        const data = getClipboardData();
        if (!data) {
          return false;
        }
        rememberClipboardData(data);
        postMessage("writeClipboard", { text: data.text });
        return true;
      };
      const cutSelection = () => {
        if (readOnlyRef.current) {
          return false;
        }
        const data = getClipboardData();
        if (!data) {
          return false;
        }
        writeClipboardThenDelete(data);
        return true;
      };
      const pasteClipboard = () => {
        if (readOnlyRef.current) {
          return;
        }
        requestClipboard();
      };

      hasSelectionRef.current = hasSelection;
      copySelectionRef.current = copySelection;
      cutSelectionRef.current = cutSelection;
      pasteClipboardRef.current = pasteClipboard;

      const nativeCopy = (e: ClipboardEvent) => {
        if (isWidgetClipboardTarget(e.target)) {
          return;
        }
        if (!copySelection()) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
      };
      const nativeCut = (e: ClipboardEvent) => {
        if (isWidgetClipboardTarget(e.target)) {
          return;
        }
        if (!cutSelection()) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
      };

      const actionDisposables = [
        editor.addAction({
          id: "rapidb.copy",
          label: "RapiDB: Copy",
          keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyC],
          keybindingContext: "editorTextFocus",
          run: () => {
            copySelection();
          },
        }),
        editor.addAction({
          id: "rapidb.cut",
          label: "RapiDB: Cut",
          precondition: "!editorReadonly",
          keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyX],
          keybindingContext: "editorTextFocus",
          run: () => {
            cutSelection();
          },
        }),
        editor.addAction({
          id: "rapidb.paste",
          label: "RapiDB: Paste",
          precondition: "!editorReadonly",
          keybindings: [
            monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyV,
            monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyV,
          ],
          keybindingContext: "editorTextFocus",
          run: () => {
            pasteClipboard();
          },
        }),
      ];

      domNode?.addEventListener("copy", nativeCopy, true);
      domNode?.addEventListener("cut", nativeCut, true);
      domNode?.addEventListener("paste", nativePaste, true);

      actionDisposables.push(
        editor.addAction({
          id: "rapidb.execute",
          label: "RapiDB: Execute Query",
          precondition: "!editorReadonly",
          keybindings: [
            monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
            monaco.KeyCode.F5,
          ],
          run: () => {
            if (readOnlyRef.current) {
              return;
            }
            onExecuteRef.current?.(getExecText());
          },
        }),
        editor.addAction({
          id: "rapidb.format",
          label: "RapiDB: Format Document",
          precondition: "!editorReadonly",
          keybindings: [
            monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF,
          ],
          run: () => {
            if (readOnlyRef.current || !allowFormattingRef.current) {
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
        }),
      );

      const changeDisposable = editor.onDidChangeModelContent(() => {
        if (suppressChangeEventRef.current) {
          suppressChangeEventRef.current = false;
          return;
        }

        onChangeRef.current?.(editor.getValue());
      });
      const selectionDisposable = editor.onDidChangeCursorSelection(() => {
        onSelectionChangeRef.current?.();
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
        selectionDisposable.dispose();
        unsubClipboard();
        unsubClipboardWritten();
        domNode?.removeEventListener("copy", nativeCopy, true);
        domNode?.removeEventListener("cut", nativeCut, true);
        domNode?.removeEventListener("paste", nativePaste, true);
        hasSelectionRef.current = () => false;
        copySelectionRef.current = () => {};
        cutSelectionRef.current = () => {};
        pasteClipboardRef.current = () => {};
        for (const disposable of actionDisposables) {
          disposable.dispose();
        }
        if (editorModel && schemaGetterByModel.get(editorModel) === getSchema) {
          schemaGetterByModel.delete(editorModel);
        }
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

        const menuWidth = 100;
        const menuHeight = 80;
        const x = Math.min(
          Math.max(event.clientX, 4),
          window.innerWidth - menuWidth - 4,
        );
        const y = Math.min(
          Math.max(event.clientY, 4),
          window.innerHeight - menuHeight - 4,
        );

        setContextMenu({
          x,
          y,
          hasSelection: hasSelectionRef.current(),
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
              position: "fixed",
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
