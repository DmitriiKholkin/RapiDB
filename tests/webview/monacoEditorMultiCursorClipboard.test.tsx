import { act, fireEvent, render, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

interface Range {
  startLineNumber: number;
  startColumn: number;
  endLineNumber: number;
  endColumn: number;
}

interface Edit {
  range: Range;
  text: string;
}

type CursorStateComputer = (
  inverseEdits: Array<{
    range: Range;
    identifier: { major: number; minor: number };
  }>,
) => unknown[] | null;

interface ActionDescriptor {
  id: string;
  run: () => void;
}

interface MultiCursorEditor {
  getValue(): string;
  setValue(value: string): void;
  setSelections(ranges: Range[]): void;
  setEOL(eol: string): void;
  executeEdits: ReturnType<
    typeof vi.fn<
      (source: string, edits: Edit[], cursor?: CursorStateComputer) => boolean
    >
  >;
  actions: ActionDescriptor[];
  domNode: HTMLElement;
}

const monacoState = vi.hoisted(() => ({
  current: null as MultiCursorEditor | null,
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
        let value = options.value ?? "";
        let eol = "\n";
        let versionId = 1;
        let selections: Range[] = [
          {
            startLineNumber: 1,
            startColumn: 1,
            endLineNumber: 1,
            endColumn: 1,
          },
        ];
        const listeners = new Set<() => void>();
        const lines = () => value.split(eol);
        const offsetAt = (line: number, column: number) =>
          lines()
            .slice(0, line - 1)
            .reduce((sum, text) => sum + text.length + eol.length, 0) +
          column -
          1;
        const toSelection = (range: Range) => ({
          ...range,
          isEmpty: () =>
            range.startLineNumber === range.endLineNumber &&
            range.startColumn === range.endColumn,
        });
        const model = {
          ...base.getModel(),
          getEOL: () => eol,
          getVersionId: () => versionId,
          getLineCount: () => lines().length,
          getLineContent: (line: number) => lines()[line - 1] ?? "",
          getLineMaxColumn: (line: number) =>
            (lines()[line - 1]?.length ?? 0) + 1,
          getValueInRange: (range: Range) =>
            value.slice(
              offsetAt(range.startLineNumber, range.startColumn),
              offsetAt(range.endLineNumber, range.endColumn),
            ),
        };
        const actions: ActionDescriptor[] = [];
        const instance = {
          ...base,
          getValue: () => value,
          getModel: () => model,
          getSelection: () => toSelection(selections[0]),
          getSelections: () => selections.map(toSelection),
          setValue: (next: string) => {
            value = next;
            versionId++;
            for (const listener of listeners) listener();
          },
          addAction: (descriptor: ActionDescriptor) => {
            actions.push(descriptor);
            return { dispose: () => {} };
          },
          executeEdits: vi.fn(
            (_source: string, edits: Edit[], _cursor?: CursorStateComputer) => {
              const ordered = edits
                .map((edit) => ({
                  start: offsetAt(
                    edit.range.startLineNumber,
                    edit.range.startColumn,
                  ),
                  end: offsetAt(edit.range.endLineNumber, edit.range.endColumn),
                  text: edit.text,
                }))
                .sort((a, b) => b.start - a.start);
              for (const edit of ordered) {
                value =
                  value.slice(0, edit.start) +
                  edit.text +
                  value.slice(edit.end);
              }
              versionId++;
              for (const listener of listeners) listener();
              return true;
            },
          ),
          onDidChangeModelContent: (listener: () => void) => {
            listeners.add(listener);
            return { dispose: () => listeners.delete(listener) };
          },
        };
        monacoState.current = {
          getValue: () => value,
          setValue: instance.setValue,
          setSelections: (next) => {
            selections = next;
          },
          setEOL: (next) => {
            eol = next;
          },
          executeEdits: instance.executeEdits,
          actions,
          domNode: base.getDomNode(),
        };
        return instance;
      },
    },
  };
});

import { MonacoEditor } from "../../src/webview/components/MonacoEditor";
import { Selection } from "../mocks/monaco-editor";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

const range = (
  startLineNumber: number,
  startColumn: number,
  endLineNumber = startLineNumber,
  endColumn = startColumn,
): Range => ({ startLineNumber, startColumn, endLineNumber, endColumn });

function editor(): MultiCursorEditor {
  const current = monacoState.current;
  if (!current) throw new Error("Monaco editor was not created");
  return current;
}

function runAction(id: string) {
  const action = editor().actions.find((candidate) => candidate.id === id);
  if (!action) throw new Error(`missing action ${id}`);
  act(() => action.run());
}

function lastPayload<T>(type: string): T {
  return getPostedMessages()
    .filter((message) => message.type === type)
    .at(-1)?.payload as T;
}

function writtenTexts() {
  return getPostedMessages()
    .filter((message) => message.type === "writeClipboard")
    .map((message) => (message.payload as { text: string }).text);
}

function acknowledgeCut(success: boolean) {
  const request = lastPayload<{ requestId: string; recipient: string }>(
    "writeClipboard",
  );
  act(() =>
    dispatchIncomingMessage("clipboardWritten", {
      requestId: request.requestId,
      recipient: request.recipient,
      success,
    }),
  );
}

function answerPaste(text: string) {
  const request = lastPayload<{ requestId: string; recipient: string }>(
    "readClipboard",
  );
  act(() => dispatchIncomingMessage("clipboardText", { ...request, text }));
}

const QUERY = "select a\nfrom t\nwhere b";

describe("MonacoEditor multi-cursor clipboard", () => {
  beforeEach(() => {
    monacoState.current = null;
    clearPostedMessages();
  });

  it("copies every selection in document order joined with the model EOL", () => {
    render(<MonacoEditor initialValue={QUERY} />);
    editor().setSelections([range(2, 1, 2, 5), range(1, 1, 1, 7)]);

    runAction("rapidb.copy");
    expect(writtenTexts()).toEqual(["select\nfrom"]);

    clearPostedMessages();
    const input = document.createElement("textarea");
    input.className = "inputarea";
    editor().domNode.appendChild(input);
    expect(
      fireEvent(input, new Event("copy", { bubbles: true, cancelable: true })),
    ).toBe(false);
    expect(writtenTexts()).toEqual(["select\nfrom"]);
  });

  it("uses CRLF when the model EOL is CRLF", () => {
    render(<MonacoEditor initialValue={"select a\r\nfrom t"} />);
    editor().setEOL("\r\n");
    editor().setSelections([range(1, 1, 1, 7), range(2, 1, 2, 5)]);

    runAction("rapidb.copy");

    expect(writtenTexts()).toEqual(["select\r\nfrom"]);
  });

  it("enables the context menu Copy/Cut when only a secondary cursor has a selection", () => {
    const { container } = render(<MonacoEditor initialValue={QUERY} />);
    editor().setSelections([range(1, 1), range(2, 1, 2, 5)]);
    const root = container.firstChild as HTMLElement;

    fireEvent.contextMenu(root, { clientX: 24, clientY: 24 });
    const copy = within(root).getByRole("menuitem", { name: "Copy" });
    expect(copy).toHaveProperty("disabled", false);
    fireEvent.click(copy);

    expect(writtenTexts()).toEqual(["select a\nfrom"]);
  });

  it("cuts every selection only after the clipboard write is acknowledged", () => {
    render(<MonacoEditor initialValue={QUERY} />);
    editor().setSelections([range(1, 1, 1, 8), range(3, 7, 3, 8)]);

    runAction("rapidb.cut");
    expect(writtenTexts()).toEqual(["select \nb"]);
    expect(editor().getValue()).toBe(QUERY);

    acknowledgeCut(true);

    expect(editor().getValue()).toBe("a\nfrom t\nwhere ");
    expect(editor().executeEdits).toHaveBeenCalledTimes(1);
    expect(editor().executeEdits.mock.calls[0][1]).toHaveLength(2);
  });

  it("keeps every selection when the multi-cursor cut write fails or the model changed", () => {
    render(<MonacoEditor initialValue={QUERY} />);
    editor().setSelections([range(1, 1, 1, 8), range(3, 7, 3, 8)]);
    runAction("rapidb.cut");
    acknowledgeCut(false);
    expect(editor().getValue()).toBe(QUERY);

    runAction("rapidb.cut");
    act(() => editor().setValue(`${QUERY} `));
    acknowledgeCut(true);
    expect(editor().getValue()).toBe(`${QUERY} `);
    expect(editor().executeEdits).not.toHaveBeenCalled();
  });

  it("pastes copied multi-cursor pieces back to their own cursors", () => {
    render(<MonacoEditor initialValue={QUERY} />);
    editor().setSelections([range(1, 1, 1, 7), range(2, 1, 2, 5)]);
    runAction("rapidb.copy");

    editor().setSelections([range(3, 8), range(1, 9)]);
    runAction("rapidb.paste");
    answerPaste("select\nfrom");

    expect(editor().getValue()).toBe("select aselect\nfrom t\nwhere bfrom");
    const [, edits, computeCursorState] = editor().executeEdits.mock.calls[0];
    expect(edits.map((edit) => edit.text)).toEqual(["from", "select"]);
    expect(
      computeCursorState?.([
        { range: range(3, 8, 3, 12), identifier: { major: 0, minor: 0 } },
        { range: range(1, 9, 1, 15), identifier: { major: 1, minor: 0 } },
      ]),
    ).toEqual([new Selection(3, 12, 3, 12), new Selection(1, 15, 1, 15)]);
  });

  it("spreads external text across cursors when the line count matches and otherwise pastes it whole", () => {
    render(<MonacoEditor initialValue={"a\nb"} />);
    editor().setSelections([range(1, 2), range(2, 2)]);
    runAction("rapidb.paste");
    answerPaste("1\r\n2\r\n");
    expect(editor().getValue()).toBe("a1\nb2");

    editor().setSelections([range(1, 3), range(2, 3)]);
    runAction("rapidb.paste");
    answerPaste("x\ny\nz");
    expect(editor().getValue()).toBe("a1x\ny\nz\nb2x\ny\nz");
  });

  it("pastes into all selections captured at request time and drops stale responses", () => {
    render(<MonacoEditor initialValue={QUERY} />);
    editor().setSelections([range(1, 8, 1, 9), range(2, 6, 2, 7)]);
    runAction("rapidb.paste");
    editor().setSelections([range(3, 1)]);
    answerPaste("z");
    expect(editor().getValue()).toBe("select z\nfrom z\nwhere b");

    editor().setSelections([range(1, 1), range(2, 1)]);
    runAction("rapidb.paste");
    act(() => editor().setValue("changed"));
    answerPaste("q");
    expect(editor().getValue()).toBe("changed");
  });

  it("copies and cuts whole lines for empty selections and pastes a copied line above the cursor", () => {
    render(<MonacoEditor initialValue={QUERY} />);
    editor().setSelections([range(2, 3)]);
    runAction("rapidb.copy");
    expect(writtenTexts()).toEqual(["from t\n"]);

    editor().setSelections([range(3, 4)]);
    runAction("rapidb.paste");
    answerPaste("from t\n");
    expect(editor().getValue()).toBe("select a\nfrom t\nfrom t\nwhere b");
    const [, edits, computeCursorState] = editor().executeEdits.mock.calls[0];
    expect(edits[0].range).toEqual(range(3, 1));
    expect(
      computeCursorState?.([
        { range: range(3, 1, 4, 1), identifier: { major: 0, minor: 0 } },
      ]),
    ).toEqual([new Selection(4, 4, 4, 4)]);

    clearPostedMessages();
    editor().setSelections([range(1, 2), range(1, 5), range(4, 1)]);
    runAction("rapidb.cut");
    expect(writtenTexts()).toEqual(["select a\nwhere b\n"]);
    acknowledgeCut(true);
    expect(editor().getValue()).toBe("from t\nfrom t");
  });

  it("keeps read-only editors copy-only for multiple selections", () => {
    render(<MonacoEditor initialValue={QUERY} readOnly />);
    editor().setSelections([range(1, 1, 1, 7), range(2, 1, 2, 5)]);

    runAction("rapidb.copy");
    runAction("rapidb.cut");
    runAction("rapidb.paste");

    expect(getPostedMessages().map((message) => message.type)).toEqual([
      "writeClipboard",
    ]);
    expect(writtenTexts()).toEqual(["select\nfrom"]);
    expect(editor().getValue()).toBe(QUERY);
  });
});
