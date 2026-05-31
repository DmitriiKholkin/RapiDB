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
} from "../mocks/monaco-editor";

afterEach(() => {
  __resetMockMonacoState();
});

describe("MonacoEditor", () => {
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
