type ChangeListener = () => void;

interface MockEdit {
  text: string;
}

let mockSelectionText = "";
const selectionEmitters = new Set<() => void>();

export function __setMockSelectionText(text: string) {
  mockSelectionText = text;
  for (const emit of selectionEmitters) emit();
}

export function __resetMockMonacoState() {
  mockSelectionText = "";
  selectionEmitters.clear();
}

function createMockEditor(container: HTMLElement, initialValue: string) {
  let value = initialValue;
  const changeListeners = new Set<ChangeListener>();
  const domNode = document.createElement("div");
  let scrollTop = 0;
  let scrollLeft = 0;
  domNode.setAttribute("data-testid", "mock-monaco-dom");
  container.appendChild(domNode);

  const getSelection = () => {
    return {
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: 1,
      endColumn: mockSelectionText.length + 1,
      isEmpty: () => mockSelectionText.length === 0,
    };
  };
  const selectionListeners = new Set<
    (event: { selection: ReturnType<typeof getSelection> }) => void
  >();
  const emitSelection = () => {
    for (const listener of selectionListeners) {
      listener({ selection: getSelection() });
    }
  };
  selectionEmitters.add(emitSelection);
  let versionId = 1;
  const model = {
    getFullModelRange: () => ({
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: 1,
      endColumn: value.length + 1,
    }),
    getLineCount: () => 1,
    getLineMaxColumn: () => value.length + 1,
    getLineContent: () => value,
    getWordUntilPosition: (position: { column: number }) => ({
      word: "",
      startColumn: position.column,
      endColumn: position.column,
    }),
    getPositionAt: () => ({ lineNumber: 1, column: value.length + 1 }),
    getOffsetAt: () => 0,
    getValueInRange: () => mockSelectionText || value,
    getVersionId: () => versionId,
  };

  return {
    getValue: () => value,
    setValue: (nextValue: string) => {
      value = nextValue;
      versionId++;
      for (const listener of changeListeners) {
        listener();
      }
    },
    updateOptions: () => {},
    getModel: () => model,
    getSelection,
    getSelections: () => [getSelection()],
    getPosition: () => ({ lineNumber: 1, column: value.length + 1 }),
    setPosition: (_position: { lineNumber: number; column: number }) =>
      __setMockSelectionText(""),
    setSelection: (selection: Selection) => {
      const offsetAt = (lineNumber: number, column: number) =>
        value
          .split("\n")
          .slice(0, lineNumber - 1)
          .join("\n").length +
        (lineNumber > 1 ? 1 : 0) +
        column -
        1;
      const start = offsetAt(
        selection.selectionStartLineNumber,
        selection.selectionStartColumn,
      );
      const end = offsetAt(
        selection.positionLineNumber,
        selection.positionColumn,
      );
      __setMockSelectionText(
        value.slice(Math.min(start, end), Math.max(start, end)),
      );
    },
    revealPosition: () => {},
    focus: () => {},
    layout: () => {},
    getScrollTop: () => scrollTop,
    getScrollLeft: () => scrollLeft,
    setScrollTop: (next: number) => {
      scrollTop = next;
    },
    setScrollLeft: (next: number) => {
      scrollLeft = next;
    },
    addAction: (_descriptor: {
      id: string;
      label: string;
      precondition?: string;
      keybindings?: number[];
      keybindingContext?: string;
      run: () => void;
    }) => ({ dispose: () => {} }),
    trigger: () => {},
    getDomNode: () => domNode,
    getOption: () => false,
    executeEdits: (_source: string, edits: MockEdit[]) => {
      const nextText = edits[0]?.text;
      if (nextText === undefined) {
        return;
      }

      value = mockSelectionText
        ? value.replace(mockSelectionText, nextText)
        : nextText;
      versionId++;
    },
    pushUndoStop: () => {},
    onDidChangeModelContent: (listener: ChangeListener) => {
      changeListeners.add(listener);
      return {
        dispose: () => {
          changeListeners.delete(listener);
        },
      };
    },
    onDidChangeCursorSelection: (
      listener: (event: { selection: ReturnType<typeof getSelection> }) => void,
    ) => {
      selectionListeners.add(listener);
      return {
        dispose: () => {
          selectionListeners.delete(listener);
        },
      };
    },
    dispose: () => {
      changeListeners.clear();
      selectionListeners.clear();
      selectionEmitters.delete(emitSelection);
      domNode.remove();
    },
  };
}

export const editor = {
  create: (container: HTMLElement, options: { value?: string }) =>
    createMockEditor(container, options.value ?? ""),
  defineTheme: () => {},
  setTheme: () => {},
  setModelLanguage: () => {},
  EditorOption: {
    readOnly: 1,
  },
};

export const languages = {
  registerCompletionItemProvider: () => ({ dispose: () => {} }),
  CompletionItemKind: {
    Class: 0,
    Field: 1,
    Function: 2,
    Keyword: 3,
    Module: 4,
    Value: 5,
  },
};

export const KeyMod = {
  CtrlCmd: 1 << 11,
  Shift: 1 << 10,
  Alt: 1 << 9,
};

export const KeyCode = {
  Enter: 3,
  KeyC: 33,
  KeyF: 36,
  KeyV: 52,
  KeyX: 54,
  F5: 63,
};

export class Selection {
  constructor(
    public selectionStartLineNumber: number,
    public selectionStartColumn: number,
    public positionLineNumber: number,
    public positionColumn: number,
  ) {}
}
