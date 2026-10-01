import { act, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NULL_SENTINEL } from "../../src/extension/dbDrivers/types";
import type { ClipboardReadPayload } from "../../src/shared/webviewContracts";
import { EditInput } from "../../src/webview/components/table/EditInput";
import {
  dispatchIncomingMessage,
  getLastPostedMessage,
  getPostedMessages,
} from "./testUtils";

function requestPaste(input: HTMLInputElement): ClipboardReadPayload {
  fireEvent.contextMenu(input);
  const paste = screen.getByRole("menuitem", { name: "Paste" });
  fireEvent.mouseDown(paste);
  fireEvent.click(paste);
  const message = getLastPostedMessage();
  expect(message?.type).toBe("readClipboard");
  return message?.payload as ClipboardReadPayload;
}

function replyToPaste(request: ClipboardReadPayload, text: string): void {
  act(() => {
    dispatchIncomingMessage("clipboardText", { ...request, text });
  });
}

afterEach(() => {
  vi.useRealTimers();
});

describe("EditInput", () => {
  it("preserves typing while a context-menu clipboard read is pending", async () => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="start"
        nullable
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    input.setSelectionRange(5, 5);
    fireEvent.contextMenu(input);
    await user.click(screen.getByRole("menuitem", { name: "Paste" }));
    const request = getLastPostedMessage()?.payload as ClipboardReadPayload;
    expect(getLastPostedMessage()?.type).toBe("readClipboard");
    expect(document.activeElement).toBe(input);
    expect(onCommit).not.toHaveBeenCalled();

    await user.type(input, " typed", { skipClick: true });
    replyToPaste(request, " pasted");

    expect(input.value).toBe("start typed pasted");
    expect(document.activeElement).toBe(input);
    expect(input.selectionStart).toBe(input.value.length);
    expect(input.selectionEnd).toBe(input.value.length);
    await user.keyboard("{Enter}");
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("start typed pasted");
  });

  it.each([
    [2, 5, "abXf"],
    [3, 3, "abcXdef"],
  ])("uses the response-time selection %s..%s", (start, end, expected) => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="abcdef"
        nullable
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    const request = requestPaste(input);
    input.setSelectionRange(start, end);
    replyToPaste(request, "X");

    expect(input.value).toBe(expected);
    expect(input.selectionStart).toBe(start + 1);
    expect(input.selectionEnd).toBe(start + 1);
    fireEvent.blur(input);
    expect(onCommit).toHaveBeenCalledExactlyOnceWith(expected);
  });

  it.each([
    "old-first",
    "new-first",
  ] as const)("only applies the latest request with %s responses", (order) => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="base"
        nullable
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    input.setSelectionRange(4, 4);
    const oldRequest = requestPaste(input);
    const newRequest = requestPaste(input);
    expect(newRequest.requestId).not.toBe(oldRequest.requestId);
    expect(newRequest.recipient).toBe(oldRequest.recipient);
    fireEvent.change(input, { target: { value: "base typed" } });
    input.setSelectionRange(10, 10);

    if (order === "old-first") {
      replyToPaste(oldRequest, " old");
      expect(input.value).toBe("base typed");
    }
    replyToPaste(newRequest, " new");
    replyToPaste(oldRequest, " old");
    replyToPaste(newRequest, " duplicate");
    expect(input.value).toBe("base typed new");
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("base typed new");
  });

  it.each([
    "requestId",
    "recipient",
  ] as const)("ignores a mismatched %s without consuming the pending request", (field) => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="base"
        nullable
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    input.setSelectionRange(4, 4);
    const request = requestPaste(input);
    replyToPaste({ ...request, [field]: "another-editor-or-request" }, "bad");
    expect(input.value).toBe("base");
    expect(input.selectionStart).toBe(4);
    expect(input.selectionEnd).toBe(4);

    replyToPaste(request, " good");
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("base good");
  });

  it.each([
    "blur",
    "enter",
    "escape",
    "null",
    "default",
  ] as const)("ignores a late response after %s even before the parent unmounts", (action) => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const onSetDefault = vi.fn();
    render(
      <EditInput
        initial="base"
        nullable
        showDefaultButton
        onSetDefault={onSetDefault}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    const request = requestPaste(input);
    fireEvent.change(input, { target: { value: "typed" } });
    input.setSelectionRange(1, 3);
    if (action === "blur") fireEvent.blur(input);
    else if (action === "enter" || action === "escape") {
      fireEvent.keyDown(input, {
        key: action === "enter" ? "Enter" : "Escape",
      });
    } else {
      fireEvent.click(
        screen.getByRole("button", {
          name: action === "null" ? "NULL" : "DEF",
        }),
      );
    }
    replyToPaste(request, "late");

    expect(input.value).toBe("typed");
    expect(input.selectionStart).toBe(1);
    expect(input.selectionEnd).toBe(3);
    if (action === "blur" || action === "enter" || action === "null") {
      expect(onCommit).toHaveBeenCalledExactlyOnceWith(
        action === "null" ? NULL_SENTINEL : "typed",
      );
    } else expect(onCommit).not.toHaveBeenCalled();
    expect(onCancel).toHaveBeenCalledTimes(action === "escape" ? 1 : 0);
    expect(onSetDefault).toHaveBeenCalledTimes(action === "default" ? 1 : 0);

    fireEvent.contextMenu(input);
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    expect(getPostedMessages()).toHaveLength(1);
  });

  it("does not deliver an unmounted editor's response to a new editor", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const oldEditor = render(
      <EditInput
        initial="old"
        nullable
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const oldInput = screen.getByLabelText("Cell value") as HTMLInputElement;
    const oldRequest = requestPaste(oldInput);
    oldEditor.unmount();
    render(
      <EditInput
        initial="new"
        nullable
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    const request = requestPaste(input);
    expect(request.recipient).not.toBe(oldRequest.recipient);
    replyToPaste(oldRequest, "late");
    expect(oldInput.value).toBe("old");
    expect(input.value).toBe("new");
    expect(onCommit).not.toHaveBeenCalled();
    expect(onCancel).not.toHaveBeenCalled();
    replyToPaste(request, "current");
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("current");
  });

  it("disables context-menu paste for a read-only editor", () => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="base"
        nullable
        readOnly
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    fireEvent.contextMenu(input);
    const paste = screen.getByRole("menuitem", { name: "Paste" });
    expect((paste as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(paste);
    expect(getPostedMessages()).toHaveLength(0);
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("base");
  });

  it.each([
    false,
    true,
  ])("invalidates a pending paste on readOnly change (editable again: %s)", (editableAgain) => {
    const onCommit = vi.fn();
    const props = {
      initial: "base",
      nullable: true,
      onCommit,
      onCancel: vi.fn(),
    };
    const { rerender } = render(<EditInput {...props} />);
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    const request = requestPaste(input);
    fireEvent.change(input, { target: { value: "typed" } });
    input.setSelectionRange(1, 3);
    rerender(<EditInput {...props} readOnly />);
    if (editableAgain) rerender(<EditInput {...props} readOnly={false} />);
    replyToPaste(request, "late");
    expect(input.value).toBe("typed");
    expect(input.selectionStart).toBe(1);
    expect(input.selectionEnd).toBe(3);

    if (editableAgain) {
      replyToPaste(requestPaste(input), "X");
      expect(input.value).toBe("tXed");
    }
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith(
      editableAgain ? "tXed" : "typed",
    );
  });

  it.each([
    NULL_SENTINEL,
    "base",
  ])("treats an empty clipboard as a no-op for %s", (initial) => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={initial}
        nullable
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    const request = requestPaste(input);
    const value = input.value;
    const selectionEnd = input.selectionEnd;
    replyToPaste(request, "");
    replyToPaste(request, "duplicate");
    expect(input.value).toBe(value);
    expect(input.selectionStart).toBe(0);
    expect(input.selectionEnd).toBe(selectionEnd);
    expect(document.activeElement).toBe(input);
    if (initial === NULL_SENTINEL) expect(input.placeholder).toBe("NULL");
    fireEvent.blur(input);
    expect(onCommit).toHaveBeenCalledExactlyOnceWith(initial);
  });

  it.each([
    "\\x",
    "0X",
  ])("pastes into the current normalized binary value with %s prefix", (prefix) => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="0x00"
        nullable
        category="binary"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    const request = requestPaste(input);
    fireEvent.change(input, { target: { value: `${prefix}aabb` } });
    expect(input.value).toBe("0xaabb");
    input.setSelectionRange(4, 6);
    replyToPaste(request, "CC");
    expect(input.value).toBe("0xaaCC");
    expect(input.selectionStart).toBe(6);
    expect(input.selectionEnd).toBe(6);
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("0xaaCC");
  });

  it("normalizes pasted binary text and positions the caret after it", () => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        category="binary"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    replyToPaste(requestPaste(input), "\\xDead");
    expect(input.value).toBe("0xDead");
    expect(input.placeholder).toBe("0xHEX");
    expect(input.selectionStart).toBe(6);
    expect(input.selectionEnd).toBe(6);
    fireEvent.blur(input);
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("0xDead");
  });

  it("collapses the selection even when paste replaces text with itself", () => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="base"
        nullable
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    replyToPaste(requestPaste(input), "base");
    expect(input.value).toBe("base");
    expect(input.selectionStart).toBe(4);
    expect(input.selectionEnd).toBe(4);
    fireEvent.blur(input);
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("base");
  });

  it("collapses a binary prefix replacement that normalizes to the same value", () => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="0x00"
        nullable
        category="binary"
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "\\xAABB" } });
    input.setSelectionRange(0, 2);
    replyToPaste(requestPaste(input), "\\x");
    expect(input.value).toBe("0xAABB");
    expect(input.selectionStart).toBe(2);
    expect(input.selectionEnd).toBe(2);
    fireEvent.blur(input);
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("0xAABB");
  });

  it("does not overwrite a newer selection on a later animation frame", () => {
    vi.useFakeTimers();
    const onCommit = vi.fn();
    render(
      <EditInput
        initial="base"
        nullable
        onCommit={onCommit}
        onCancel={vi.fn()}
      />,
    );
    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    replyToPaste(requestPaste(input), "pasted");
    expect(input.selectionStart).toBe(6);
    expect(input.selectionEnd).toBe(6);
    input.setSelectionRange(1, 3);
    act(() => vi.advanceTimersByTime(50));
    expect(input.selectionStart).toBe(1);
    expect(input.selectionEnd).toBe(3);
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("pasted");
  });

  it.each([
    "blur",
    "enter",
  ] as const)("commits context-menu paste into a NULL value on %s", (action) => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        onCommit={onCommit}
        onCancel={() => undefined}
      />,
    );

    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    fireEvent.contextMenu(input);
    fireEvent.click(screen.getByRole("menuitem", { name: "Paste" }));
    const message = getLastPostedMessage();
    expect(message?.type).toBe("readClipboard");
    act(() => {
      dispatchIncomingMessage("clipboardText", {
        ...(message?.payload as { requestId: string; recipient: string }),
        text: "pasted value",
      });
    });
    expect(input.value).toBe("pasted value");
    if (action === "blur") fireEvent.blur(input);
    else fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledExactlyOnceWith("pasted value");
  });

  it("shows binary placeholders with 0x and preserves 0x hex edits", async () => {
    const user = userEvent.setup();
    const onCommit = vi.fn();

    render(
      <EditInput
        initial="0xDeAdBeEf"
        nullable
        category="binary"
        onCommit={onCommit}
        onCancel={() => undefined}
      />,
    );

    const input = screen.getByLabelText("Cell value") as HTMLInputElement;

    expect(input.value).toBe("0xDeAdBeEf");
    expect(input.placeholder).toBe("0xHEX");

    await user.clear(input);
    await user.type(input, "0xfeed");
    fireEvent.blur(input);

    expect(onCommit).toHaveBeenCalledWith("0xfeed");
  });

  it("selects the full value from the start when focused", () => {
    render(
      <EditInput
        initial="abcdefghijklmno"
        nullable
        onCommit={() => undefined}
        onCancel={() => undefined}
      />,
    );

    const input = screen.getByLabelText("Cell value") as HTMLInputElement;

    expect(input.selectionStart).toBe(0);
    expect(input.selectionEnd).toBe(input.value.length);
  });

  it.each([
    "blur",
    "enter",
  ] as const)("preserves an untouched NULL value on %s", (action) => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        onCommit={onCommit}
        onCancel={() => undefined}
      />,
    );

    const input = screen.getByLabelText("Cell value") as HTMLInputElement;
    expect(input.value).toBe("");
    expect(input.placeholder).toBe("NULL");

    if (action === "blur") {
      fireEvent.blur(input);
    } else {
      fireEvent.keyDown(input, { key: "Enter" });
    }

    expect(onCommit).toHaveBeenCalledWith(NULL_SENTINEL);
  });

  it("commits text entered into an initially NULL value", () => {
    const onCommit = vi.fn();
    render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable
        onCommit={onCommit}
        onCancel={() => undefined}
      />,
    );

    const input = screen.getByLabelText("Cell value");
    fireEvent.change(input, { target: { value: "value" } });
    fireEvent.blur(input);

    expect(onCommit).toHaveBeenCalledWith("value");
  });
});
