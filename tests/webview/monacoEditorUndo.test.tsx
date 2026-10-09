import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

interface Edit {
  range: {
    startLineNumber: number;
    startColumn: number;
    endLineNumber: number;
    endColumn: number;
  };
  text: string;
}

interface UndoEditor {
  getValue(): string;
  getModel(): { getFullModelRange(): Edit["range"] };
  executeEdits: ReturnType<
    typeof vi.fn<(source: string, edits: Edit[]) => boolean>
  >;
  pushUndoStop: ReturnType<typeof vi.fn<() => boolean>>;
  setValue: ReturnType<typeof vi.fn<(value: string) => void>>;
  undo(): void;
  redo(): void;
}

const monacoState = vi.hoisted(() => ({ current: null as UndoEditor | null }));

vi.mock("monaco-editor", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../mocks/monaco-editor")>();
  return {
    ...actual,
    editor: {
      ...actual.editor,
      create: (container: HTMLElement, options: { value?: string }) => {
        const base = actual.editor.create(container, options);
        let value = options.value ?? "";
        const listeners = new Set<() => void>();
        const undo: string[] = [];
        const redo: string[] = [];
        let pendingBefore: string | undefined;
        const emit = () => {
          for (const listener of listeners) listener();
        };
        const offsetAt = (line: number, column: number) => {
          const preceding = value.split("\n").slice(0, line - 1);
          return (
            preceding.reduce((sum, text) => sum + text.length + 1, 0) +
            column -
            1
          );
        };
        const model = {
          ...base.getModel(),
          getValueLength: () => value.length,
          getLineCount: () => value.split("\n").length,
          getLineMaxColumn: (line: number) =>
            (value.split("\n")[line - 1]?.length ?? 0) + 1,
          getFullModelRange: () => ({
            startLineNumber: 1,
            startColumn: 1,
            endLineNumber: value.split("\n").length,
            endColumn: (value.split("\n").at(-1)?.length ?? 0) + 1,
          }),
        };
        // Model only the documented edit-stack contract: executeEdits emits
        // content changes and joins the open group; pushUndoStop separates
        // groups; setValue resets history. This is not the real Monaco engine.
        const editor = {
          ...base,
          getValue: () => value,
          getModel: () => model,
          setValue: vi.fn((next: string) => {
            value = next;
            undo.length = 0;
            redo.length = 0;
            pendingBefore = undefined;
            emit();
          }),
          executeEdits: vi.fn((_source: string, edits: Edit[]) => {
            pendingBefore ??= value;
            redo.length = 0;
            for (const edit of edits) {
              const start = offsetAt(
                edit.range.startLineNumber,
                edit.range.startColumn,
              );
              const end = offsetAt(
                edit.range.endLineNumber,
                edit.range.endColumn,
              );
              value = value.slice(0, start) + edit.text + value.slice(end);
            }
            emit();
            return true;
          }),
          pushUndoStop: vi.fn(() => {
            if (pendingBefore !== undefined) {
              if (pendingBefore !== value) undo.push(pendingBefore);
              pendingBefore = undefined;
            }
            return true;
          }),
          undo: () => {
            editor.pushUndoStop();
            const previous = undo.pop();
            if (previous === undefined) return;
            redo.push(value);
            value = previous;
            emit();
          },
          redo: () => {
            const next = redo.pop();
            if (next === undefined) return;
            undo.push(value);
            value = next;
            emit();
          },
          onDidChangeModelContent: (listener: () => void) => {
            listeners.add(listener);
            return { dispose: () => listeners.delete(listener) };
          },
        };
        monacoState.current = editor;
        return editor;
      },
    },
  };
});

vi.mock("../../src/webview/components/table/TableGrid", () => ({
  TableGrid: () => <div />,
}));

import { QueryView } from "../../src/webview/components/QueryView";
import {
  useConnectionStore,
  useQueryStore,
  useSchemaStore,
} from "../../src/webview/store";
import { clearPostedMessages } from "./testUtils";

describe("Clear with the actual MonacoEditor component", () => {
  beforeEach(() => {
    monacoState.current = null;
    clearPostedMessages();
    useConnectionStore.setState({ connections: [], activeConnectionId: "" });
    useQueryStore.setState({ status: "idle", result: null });
    useSchemaStore.setState({ schemaByConnection: {} });
  });

  it("isolates Clear for Undo/Redo without losing earlier or later edit history", () => {
    const initial = "select 1;\nselect 2;";
    const edited = "select 3;\nselect 4;";
    render(
      <QueryView
        panelId="undo-clear"
        connectionId="conn-1"
        initialQueryText={initial}
      />,
    );
    const editor = monacoState.current;
    if (!editor) throw new Error("Monaco editor was not created");
    // Leave the previous edit group open: Clear must close it before editing.
    act(() =>
      editor.executeEdits("typing", [
        { range: editor.getModel().getFullModelRange(), text: edited },
      ]),
    );
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(editor.getValue()).toBe("");
    expect(editor.setValue).not.toHaveBeenCalled();
    expect(editor.pushUndoStop).toHaveBeenCalledTimes(2);
    expect(editor.executeEdits).toHaveBeenLastCalledWith("rapidb-clear-query", [
      {
        range: {
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: 2,
          endColumn: 10,
        },
        text: "",
      },
    ]);
    // Clearing an already empty model is a no-op, including its undo stack.
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(editor.pushUndoStop).toHaveBeenCalledTimes(2);
    expect(editor.executeEdits).toHaveBeenCalledTimes(2);
    for (const [action, expected] of [
      ["undo", edited],
      ["undo", initial],
      ["redo", edited],
      ["redo", ""],
    ] as const) {
      act(() => editor[action]());
      expect(editor.getValue()).toBe(expected);
      const api = window.__vscode as NonNullable<Window["__vscode"]> & {
        setState: ReturnType<typeof vi.fn>;
      };
      expect(api.setState.mock.calls.at(-1)?.[0]).toMatchObject({
        queryDraft: { panelId: "undo-clear", text: expected },
      });
    }
    // The stop after Clear must prevent subsequent typing merging into it.
    act(() =>
      editor.executeEdits("typing", [
        { range: editor.getModel().getFullModelRange(), text: "select 5" },
      ]),
    );
    act(() => editor.undo());
    expect(editor.getValue()).toBe("");
    act(() => editor.undo());
    expect(editor.getValue()).toBe(edited);
  });
});
