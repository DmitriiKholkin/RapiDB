import { act, fireEvent, render, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

interface ActionDescriptor {
  id: string;
  label: string;
  precondition?: string;
  keybindings?: number[];
  keybindingContext?: string;
  run: () => void;
}

interface CompletionProvider {
  provideCompletionItems(
    model: unknown,
    position: { lineNumber: number; column: number },
  ): { suggestions: Array<{ label: string }> };
}

interface TrackedEditor {
  actions: ActionDescriptor[];
  disposeAction: ReturnType<typeof vi.fn<(id: string) => void>>;
  instance: {
    getModel(): {
      getVersionId(): number;
    };
    executeEdits: ReturnType<typeof vi.fn<(...args: unknown[]) => void>>;
    setValue(value: string): void;
    getSelection(): unknown;
  };
}

const monacoState = vi.hoisted(() => ({
  editors: [] as TrackedEditor[],
  provider: null as CompletionProvider | null,
}));

vi.mock("monaco-editor", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../mocks/monaco-editor")>();
  return {
    ...actual,
    editor: {
      ...actual.editor,
      create: (container: HTMLElement, options: { value?: string }) => {
        const base = actual.editor.create(container, options);
        const tracked = {
          actions: [] as ActionDescriptor[],
          disposeAction: vi.fn<(id: string) => void>(),
        } as unknown as TrackedEditor;
        const instance = {
          ...base,
          executeEdits: vi.fn(base.executeEdits),
          addAction: (descriptor: ActionDescriptor) => {
            tracked.actions.push(descriptor);
            return { dispose: () => tracked.disposeAction(descriptor.id) };
          },
        };
        tracked.instance = instance as unknown as TrackedEditor["instance"];
        monacoState.editors.push(tracked);
        return instance;
      },
    },
    languages: {
      ...actual.languages,
      registerCompletionItemProvider: (
        _language: string,
        provider: CompletionProvider,
      ) => {
        monacoState.provider = provider;
        return { dispose: () => {} };
      },
    },
  };
});

import { MonacoEditor } from "../../src/webview/components/MonacoEditor";
import type { SchemaObject } from "../../src/webview/store";
import { KeyCode, KeyMod } from "../mocks/monaco-editor";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

const schemaFor = (object: string): SchemaObject[] => [
  { database: "db", schema: "public", object, columns: [] },
];

function suggestionLabels(editor: TrackedEditor): string[] {
  const provider = monacoState.provider;
  if (!provider) throw new Error("completion provider was not registered");
  return provider
    .provideCompletionItems(editor.instance.getModel(), {
      lineNumber: 1,
      column: 1,
    })
    .suggestions.map((suggestion) => suggestion.label);
}

function requestPaste(root: HTMLElement) {
  fireEvent.contextMenu(root, { clientX: 24, clientY: 24 });
  fireEvent.click(within(root).getByRole("menuitem", { name: "Paste" }));
  return getPostedMessages()
    .filter((message) => message.type === "readClipboard")
    .at(-1)?.payload as { requestId: string; recipient: string };
}

describe("MonacoEditor lifecycle regressions", () => {
  beforeEach(() => {
    monacoState.editors = [];
    clearPostedMessages();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("registers per-editor disposable keybinding actions and disposes them on unmount", () => {
    const onExecute = vi.fn();
    render(<MonacoEditor initialValue="select 1" onExecute={onExecute} />);
    const secondary = render(
      <MonacoEditor initialValue="{}" language="json" readOnly />,
    );
    const [main, dialog] = monacoState.editors;

    const execute = main.actions.find(
      (action) => action.id === "rapidb.execute",
    );
    expect(execute?.keybindings).toEqual([
      KeyMod.CtrlCmd | KeyCode.Enter,
      KeyCode.F5,
    ]);
    const paste = main.actions.find((action) => action.id === "rapidb.paste");
    expect(paste?.keybindings).toEqual([
      KeyMod.CtrlCmd | KeyCode.KeyV,
      KeyMod.CtrlCmd | KeyMod.Shift | KeyCode.KeyV,
    ]);
    expect(paste?.keybindingContext).toBe("editorTextFocus");
    expect(
      main.actions.map(({ id, label, precondition }) => ({
        id,
        label,
        precondition,
      })),
    ).toEqual([
      { id: "rapidb.copy", label: "RapiDB: Copy", precondition: undefined },
      {
        id: "rapidb.cut",
        label: "RapiDB: Cut",
        precondition: "!editorReadonly",
      },
      {
        id: "rapidb.paste",
        label: "RapiDB: Paste",
        precondition: "!editorReadonly",
      },
      {
        id: "rapidb.execute",
        label: "RapiDB: Execute Query",
        precondition: "!editorReadonly",
      },
      {
        id: "rapidb.format",
        label: "RapiDB: Format Document",
        precondition: "!editorReadonly",
      },
    ]);
    expect(
      main.actions.find((action) => action.id === "rapidb.format")?.keybindings,
    ).toEqual([KeyMod.Shift | KeyMod.Alt | KeyCode.KeyF]);

    secondary.unmount();
    expect(dialog.disposeAction).toHaveBeenCalledTimes(dialog.actions.length);
    expect(main.disposeAction).not.toHaveBeenCalled();

    execute?.run();
    expect(onExecute).toHaveBeenCalledWith("select 1");
  });

  it("resolves completion schema per editor model and keeps it after another editor unmounts", () => {
    render(<MonacoEditor initialValue="" schema={schemaFor("users")} />);
    const secondary = render(
      <MonacoEditor initialValue="" schema={schemaFor("orders")} />,
    );
    const [main, dialog] = monacoState.editors;

    expect(suggestionLabels(main)).toContain("users");
    expect(suggestionLabels(main)).not.toContain("orders");
    expect(suggestionLabels(dialog)).toContain("orders");

    secondary.unmount();
    expect(suggestionLabels(main)).toContain("users");
  });

  it("drops a clipboard response when the content changed after the paste request", () => {
    const { container } = render(<MonacoEditor initialValue="select 1" />);
    const [editor] = monacoState.editors;
    const request = requestPaste(container.firstChild as HTMLElement);

    act(() => editor.instance.setValue("select 2"));
    act(() =>
      dispatchIncomingMessage("clipboardText", { ...request, text: "x" }),
    );

    expect(editor.instance.executeEdits).not.toHaveBeenCalled();
  });

  it("ignores a response for a superseded paste request and inserts only the latest one", () => {
    const { container } = render(<MonacoEditor initialValue="select 1" />);
    const [editor] = monacoState.editors;
    const root = container.firstChild as HTMLElement;
    const first = requestPaste(root);
    const second = requestPaste(root);
    expect(second.requestId).not.toBe(first.requestId);

    act(() =>
      dispatchIncomingMessage("clipboardText", { ...first, text: "old" }),
    );
    expect(editor.instance.executeEdits).not.toHaveBeenCalled();

    act(() =>
      dispatchIncomingMessage("clipboardText", { ...second, text: "new" }),
    );
    act(() =>
      dispatchIncomingMessage("clipboardText", { ...first, text: "old" }),
    );
    expect(editor.instance.executeEdits).toHaveBeenCalledTimes(1);
    expect(editor.instance.executeEdits.mock.calls[0][1]).toEqual([
      expect.objectContaining({ text: "new" }),
    ]);
  });

  it("does not insert clipboard text when the editor became read-only after the request", () => {
    const { container, rerender } = render(
      <MonacoEditor initialValue="select 1" />,
    );
    const [editor] = monacoState.editors;
    const request = requestPaste(container.firstChild as HTMLElement);

    rerender(<MonacoEditor initialValue="select 1" readOnly />);
    act(() =>
      dispatchIncomingMessage("clipboardText", { ...request, text: "x" }),
    );

    expect(editor.instance.executeEdits).not.toHaveBeenCalled();
  });

  it("pastes into the selection captured at request time", () => {
    let selection = {
      startLineNumber: 1,
      startColumn: 2,
      endLineNumber: 1,
      endColumn: 4,
      isEmpty: () => false,
    };
    const { container } = render(<MonacoEditor initialValue="select 1" />);
    const [editor] = monacoState.editors;
    editor.instance.getSelection = () => selection;
    const request = requestPaste(container.firstChild as HTMLElement);

    selection = {
      startLineNumber: 1,
      startColumn: 8,
      endLineNumber: 1,
      endColumn: 9,
      isEmpty: () => false,
    };
    act(() =>
      dispatchIncomingMessage("clipboardText", { ...request, text: "x" }),
    );

    expect(editor.instance.executeEdits).toHaveBeenCalledTimes(1);
    expect(editor.instance.executeEdits.mock.calls[0][1]).toEqual([
      {
        range: {
          startLineNumber: 1,
          startColumn: 2,
          endLineNumber: 1,
          endColumn: 4,
        },
        text: "x",
        forceMoveMarkers: true,
        identifier: { major: 0, minor: 0 },
        _isTracked: true,
      },
    ]);
  });
});
