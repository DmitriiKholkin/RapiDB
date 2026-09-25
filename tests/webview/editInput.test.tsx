import { act, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { NULL_SENTINEL } from "../../src/extension/dbDrivers/types";
import { EditInput } from "../../src/webview/components/table/EditInput";
import { dispatchIncomingMessage, getLastPostedMessage } from "./testUtils";

describe("EditInput", () => {
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
