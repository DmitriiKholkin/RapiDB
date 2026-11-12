import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { createRef } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  MonacoEditor,
  type MonacoEditorHandle,
} from "../../src/webview/components/MonacoEditor";
import {
  __resetMockMonacoState,
  __setMockSelectionText,
  editor as mockEditor,
  Selection,
} from "../mocks/monaco-editor";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

afterEach(() => {
  __resetMockMonacoState();
  vi.restoreAllMocks();
});

type MockEditorInstance = ReturnType<typeof mockEditor.create>;

function spyOnEditorInstance(
  extend: (instance: MockEditorInstance) => void,
): () => MockEditorInstance {
  const create = mockEditor.create;
  const spy = vi.spyOn(mockEditor, "create").mockImplementation((...args) => {
    const instance = create(...args);
    extend(instance);
    return instance;
  });
  return () => spy.mock.results[0].value;
}

function clipboardMessages() {
  return getPostedMessages().filter(
    (message) =>
      message.type === "writeClipboard" || message.type === "readClipboard",
  );
}

describe("MonacoEditor", () => {
  it("notifies the current selection callback and disposes the subscription on unmount", () => {
    const create = mockEditor.create;
    const disposeSelection = vi.fn();
    const disposeEditor = vi.fn();
    vi.spyOn(mockEditor, "create").mockImplementation((...args) => {
      const instance = create(...args);
      const subscribe = instance.onDidChangeCursorSelection;
      vi.spyOn(instance, "onDidChangeCursorSelection").mockImplementation(
        (listener) => {
          const subscription = subscribe(listener);
          return {
            dispose: () => {
              disposeSelection();
              return subscription.dispose();
            },
          };
        },
      );
      const dispose = instance.dispose;
      vi.spyOn(instance, "dispose").mockImplementation(() => {
        disposeEditor();
        dispose();
      });
      return instance;
    });
    const firstCallback = vi.fn();
    const nextCallback = vi.fn();
    const ref = createRef<MonacoEditorHandle>();
    const { rerender, unmount } = render(
      <MonacoEditor
        ref={ref}
        initialValue="select 1"
        onSelectionChange={firstCallback}
      />,
    );
    const instance = vi.mocked(mockEditor.create).mock.results[0].value;
    expect(instance.onDidChangeCursorSelection).toHaveBeenCalledTimes(1);
    instance.setSelection(new Selection(1, 1, 1, 7));
    expect(firstCallback).toHaveBeenCalledTimes(1);
    expect(ref.current?.getSelectionOrValue()).toBe("select");

    rerender(
      <MonacoEditor
        ref={ref}
        initialValue="select 1"
        onSelectionChange={nextCallback}
      />,
    );
    __setMockSelectionText("1");
    expect(nextCallback).toHaveBeenCalledTimes(1);
    expect(ref.current?.getSelectionOrValue()).toBe("1");
    instance.setPosition({ lineNumber: 1, column: 9 });
    expect(nextCallback).toHaveBeenCalledTimes(2);
    expect(ref.current?.getSelectionOrValue()).toBe("select 1");
    expect(firstCallback).toHaveBeenCalledTimes(1);

    unmount();
    expect(disposeSelection).toHaveBeenCalledTimes(1);
    expect(disposeEditor).toHaveBeenCalledTimes(1);
    __setMockSelectionText("select");
    expect(nextCallback).toHaveBeenCalledTimes(2);
  });

  it("does not emit onChange when syncing a new initialValue", async () => {
    const handleChange = vi.fn();

    const { rerender } = render(
      <MonacoEditor initialValue='{"key":1}' onChange={handleChange} />,
    );

    rerender(<MonacoEditor initialValue="" onChange={handleChange} />);

    await waitFor(() => {
      expect(handleChange).not.toHaveBeenCalled();
    });
  });

  it("shows a custom context menu and copies selected Monaco text", async () => {
    __setMockSelectionText("select 1");

    const { container } = render(<MonacoEditor initialValue="select 1" />);

    fireEvent.contextMenu(container.firstChild as HTMLElement, {
      clientX: 24,
      clientY: 24,
    });

    const copyButton = await screen.findByRole("menuitem", { name: "Copy" });
    fireEvent.click(copyButton);

    expect(window.__vscode?.postMessage).toHaveBeenCalledWith({
      type: "writeClipboard",
      payload: { text: "select 1" },
    });
    expect(
      screen.queryByRole("menu", { name: "Editor context menu" }),
    ).toBeNull();
  });

  it("shows paste in the custom context menu and requests clipboard text", async () => {
    const { container } = render(<MonacoEditor initialValue="select 1" />);

    fireEvent.contextMenu(container.firstChild as HTMLElement, {
      clientX: 24,
      clientY: 24,
    });

    const pasteButton = await screen.findByRole("menuitem", { name: "Paste" });
    fireEvent.click(pasteButton);

    expect(window.__vscode?.postMessage).toHaveBeenCalledWith({
      type: "readClipboard",
      payload: expect.objectContaining({
        requestId: expect.any(String),
        recipient: expect.stringContaining("monaco:"),
      }),
    });
  });

  it("cuts selected Monaco text from the custom context menu", async () => {
    __setMockSelectionText("select 1");

    const ref = createRef<MonacoEditorHandle>();
    const { container } = render(
      <MonacoEditor ref={ref} initialValue="select 1" />,
    );

    fireEvent.contextMenu(container.firstChild as HTMLElement, {
      clientX: 24,
      clientY: 24,
    });

    const cutButton = await screen.findByRole("menuitem", { name: "Cut" });
    fireEvent.click(cutButton);

    expect(window.__vscode?.postMessage).toHaveBeenCalledWith({
      type: "writeClipboard",
      payload: {
        text: "select 1",
        requestId: expect.any(String),
        recipient: expect.stringContaining("monaco:"),
      },
    });
    expect(ref.current?.getValue()).toBe("select 1");
    const request = getPostedMessages().find(
      (message) => message.type === "writeClipboard",
    )?.payload as { requestId: string; recipient: string };
    act(() =>
      dispatchIncomingMessage("clipboardWritten", {
        requestId: request.requestId,
        recipient: request.recipient,
        success: true,
      }),
    );
    expect(ref.current?.getValue()).toBe("");
  });

  it("keeps cut text when the clipboard write is not confirmed", async () => {
    __setMockSelectionText("select 1");
    const ref = createRef<MonacoEditorHandle>();
    const { container } = render(
      <MonacoEditor ref={ref} initialValue="select 1" />,
    );
    fireEvent.contextMenu(container.firstChild as HTMLElement, {
      clientX: 24,
      clientY: 24,
    });
    fireEvent.click(await screen.findByRole("menuitem", { name: "Cut" }));
    const request = getPostedMessages().find(
      (message) => message.type === "writeClipboard",
    )?.payload as { requestId: string; recipient: string };
    act(() =>
      dispatchIncomingMessage("clipboardWritten", {
        requestId: request.requestId,
        recipient: request.recipient,
        success: false,
      }),
    );
    expect(ref.current?.getValue()).toBe("select 1");
  });

  it("does not replace the selection when the clipboard has no text", () => {
    const executeEdits = vi.fn();
    spyOnEditorInstance((instance) => {
      Object.assign(instance, { executeEdits });
    });
    __setMockSelectionText("important");
    const { container } = render(<MonacoEditor initialValue="important" />);
    fireEvent.contextMenu(container.firstChild as HTMLElement, {
      clientX: 24,
      clientY: 24,
    });
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    const request = getPostedMessages().find(
      (message) => message.type === "readClipboard",
    )?.payload as { requestId: string; recipient: string };

    act(() =>
      dispatchIncomingMessage("clipboardText", { ...request, text: "" }),
    );

    expect(executeEdits).not.toHaveBeenCalled();
  });

  it("limits clipboard shortcuts to editor text focus and ignores widget clipboard events", () => {
    const addAction = vi.fn(
      (_descriptor: {
        keybindings?: number[];
        keybindingContext?: string;
        run: () => void;
      }) => ({ dispose: () => {} }),
    );
    spyOnEditorInstance((instance) => {
      Object.assign(instance, { addAction });
    });
    __setMockSelectionText("select");
    render(<MonacoEditor initialValue="select 1" />);
    clearPostedMessages();

    const clipboardContexts = addAction.mock.calls.flatMap(([descriptor]) => {
      clearPostedMessages();
      descriptor.run();
      return clipboardMessages().length > 0
        ? (descriptor.keybindings ?? []).map(() => descriptor.keybindingContext)
        : [];
    });
    expect(clipboardContexts).toEqual([
      "editorTextFocus",
      "editorTextFocus",
      "editorTextFocus",
      "editorTextFocus",
    ]);

    __setMockSelectionText("select");
    const domNode = screen.getByTestId("mock-monaco-dom");
    const overlayWidgets = document.createElement("div");
    overlayWidgets.className = "overlayWidgets";
    const findInput = document.createElement("textarea");
    overlayWidgets.appendChild(findInput);
    const replaceInput = document.createElement("input");
    const contentWidgets = document.createElement("div");
    contentWidgets.className = "contentWidgets";
    const contentWidgetChild = document.createElement("span");
    contentWidgets.appendChild(contentWidgetChild);
    domNode.append(overlayWidgets, replaceInput, contentWidgets);
    clearPostedMessages();
    const widgetEvents = [findInput, replaceInput, contentWidgetChild].flatMap(
      (target) =>
        ["copy", "cut", "paste"].map((type) =>
          fireEvent(
            target,
            new Event(type, { bubbles: true, cancelable: true }),
          ),
        ),
    );
    expect(widgetEvents.every(Boolean)).toBe(true);
    expect(clipboardMessages()).toEqual([]);

    const editorInput = document.createElement("textarea");
    editorInput.className = "inputarea";
    domNode.appendChild(editorInput);
    expect(
      fireEvent(
        editorInput,
        new Event("copy", { bubbles: true, cancelable: true }),
      ),
    ).toBe(false);
    expect(
      fireEvent(
        editorInput,
        new Event("paste", { bubbles: true, cancelable: true }),
      ),
    ).toBe(false);
    expect(clipboardMessages()).toEqual([
      { type: "writeClipboard", payload: { text: "select" } },
      {
        type: "readClipboard",
        payload: expect.objectContaining({ requestId: expect.any(String) }),
      },
    ]);

    const editContext = document.createElement("div");
    editContext.className = "native-edit-context";
    domNode.appendChild(editContext);
    clearPostedMessages();
    expect(
      fireEvent(
        editContext,
        new Event("copy", { bubbles: true, cancelable: true }),
      ),
    ).toBe(false);
    expect(clipboardMessages()).toEqual([
      { type: "writeClipboard", payload: { text: "select" } },
    ]);
  });

  it("runs a secondary selection and refuses several non-empty selections", () => {
    const value = "select 1;\nselect 2;";
    let selections: Selection[] = [];
    const range = (startColumn: number, endColumn: number, lineNumber = 1) => ({
      startLineNumber: lineNumber,
      startColumn,
      endLineNumber: lineNumber,
      endColumn,
      isEmpty: () => startColumn === endColumn,
    });
    spyOnEditorInstance((instance) => {
      const getModel = instance.getModel;
      Object.assign(instance, {
        getSelections: () => selections,
        getModel: () => ({
          ...getModel(),
          getValueInRange: (selected: ReturnType<typeof range>) =>
            value
              .split("\n")
              [selected.startLineNumber - 1].slice(
                selected.startColumn - 1,
                selected.endColumn - 1,
              ),
        }),
      });
    });
    const onExecute = vi.fn();
    const ref = createRef<MonacoEditorHandle>();
    render(
      <MonacoEditor ref={ref} initialValue={value} onExecute={onExecute} />,
    );

    selections = [range(1, 1), range(1, 9, 2)] as unknown as Selection[];
    expect(ref.current?.getSelectionOrValue()).toBe("select 2");

    selections = [range(1, 1), range(1, 1, 2)] as unknown as Selection[];
    expect(ref.current?.getSelectionOrValue()).toBe(value);

    selections = [range(1, 9), range(1, 9, 2)] as unknown as Selection[];
    expect(ref.current?.getSelectionOrValue()).toBeNull();
  });

  it("places the cursor at the end of the inserted text reported by Monaco", () => {
    const executeEdits = vi.fn();
    const setPosition = vi.fn();
    const revealPosition = vi.fn();
    spyOnEditorInstance((instance) => {
      Object.assign(instance, {
        executeEdits,
        setPosition,
        revealPosition,
        getPosition: () => ({ lineNumber: 2, column: 4 }),
      });
    });
    const { container } = render(<MonacoEditor initialValue="" />);
    fireEvent.contextMenu(container.firstChild as HTMLElement, {
      clientX: 24,
      clientY: 24,
    });
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    const request = getPostedMessages().find(
      (message) => message.type === "readClipboard",
    )?.payload as { requestId: string; recipient: string };

    act(() =>
      dispatchIncomingMessage("clipboardText", {
        ...request,
        text: "a\r\nbcd",
      }),
    );

    expect(executeEdits).toHaveBeenCalledTimes(1);
    const computeCursorState = executeEdits.mock.calls[0][2] as (
      inverseEdits: Array<{
        range: { endLineNumber: number; endColumn: number };
        identifier: { major: number; minor: number };
      }>,
    ) => Selection[] | null;
    expect(
      computeCursorState([
        {
          range: { endLineNumber: 2, endColumn: 4 },
          identifier: { major: 0, minor: 0 },
        },
      ]),
    ).toEqual([new Selection(2, 4, 2, 4)]);
    expect(setPosition).not.toHaveBeenCalled();
    expect(revealPosition).toHaveBeenCalledWith({ lineNumber: 2, column: 4 });
  });
});
