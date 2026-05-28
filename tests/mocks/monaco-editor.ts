type ChangeListener = () => void;

function createMockEditor(initialValue: string) {
  let value = initialValue;
  const changeListeners = new Set<ChangeListener>();
  const domNode = document.createElement("div");
  let scrollTop = 0;
  let scrollLeft = 0;

  return {
    getValue: () => value,
    setValue: (nextValue: string) => {
      value = nextValue;
      for (const listener of changeListeners) {
        listener();
      }
    },
    updateOptions: () => {},
    getModel: () => ({
      getValueInRange: () => value,
      getFullModelRange: () => ({
        startLineNumber: 1,
        startColumn: 1,
        endLineNumber: 1,
        endColumn: value.length + 1,
      }),
      getLineCount: () => 1,
      getLineMaxColumn: () => value.length + 1,
      getPositionAt: () => ({ lineNumber: 1, column: value.length + 1 }),
      getOffsetAt: () => 0,
    }),
    getSelection: () => null,
    getPosition: () => ({ lineNumber: 1, column: value.length + 1 }),
    setPosition: () => {},
    setSelection: () => {},
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
    addCommand: () => {},
    trigger: () => {},
    getDomNode: () => domNode,
    getOption: () => false,
    executeEdits: () => {},
    pushUndoStop: () => {},
    onDidChangeModelContent: (listener: ChangeListener) => {
      changeListeners.add(listener);
      return {
        dispose: () => {
          changeListeners.delete(listener);
        },
      };
    },
    dispose: () => {},
  };
}

export const editor = {
  create: (_container: HTMLElement, options: { value?: string }) =>
    createMockEditor(options.value ?? ""),
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
  CtrlCmd: 1,
  Shift: 2,
  Alt: 4,
};

export const KeyCode = {
  KeyC: 0,
  KeyV: 1,
  KeyX: 5,
  Enter: 2,
  F5: 3,
  KeyF: 4,
};

export class Selection {
  constructor(
    public selectionStartLineNumber: number,
    public selectionStartColumn: number,
    public positionLineNumber: number,
    public positionColumn: number,
  ) {}
}
