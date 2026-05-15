import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { EditInput } from "../../src/webview/components/table/EditInput";

describe("EditInput", () => {
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
});
