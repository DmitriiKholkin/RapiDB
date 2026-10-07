import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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

afterEach(() => {
  __resetMockMonacoState();
});

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
      payload: { text: "select 1" },
    });
    expect(ref.current?.getValue()).toBe("");
  });
});
