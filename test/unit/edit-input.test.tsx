/**
 * @vitest-environment jsdom
 */

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";
import {
  EditInput,
  valueToEditString,
} from "../../src/webview/components/table/EditInput";

afterEach(cleanup);

describe("valueToEditString", () => {
  it("converts boolean true to 'true'", () => {
    expect(valueToEditString(true)).toBe("true");
  });

  it("converts boolean false to 'false'", () => {
    expect(valueToEditString(false)).toBe("false");
  });

  it("keeps number 1 as a string", () => {
    expect(valueToEditString(1)).toBe("1");
  });

  it("keeps number 0 as a string", () => {
    expect(valueToEditString(0)).toBe("0");
  });

  it("converts null to NULL_SENTINEL", () => {
    expect(valueToEditString(null)).toBe(NULL_SENTINEL);
  });

  it("converts undefined to NULL_SENTINEL", () => {
    expect(valueToEditString(undefined)).toBe(NULL_SENTINEL);
  });

  it("converts strings as-is for non-boolean", () => {
    expect(valueToEditString("hello")).toBe("hello");
  });

  it("converts numbers as-is for non-boolean", () => {
    expect(valueToEditString(42)).toBe("42");
  });

  it("does not normalize float artifacts in the webview edit layer", () => {
    expect(valueToEditString(1.2000000476837158)).toBe("1.2000000476837158");
  });

  it("serializes dates generically for editing", () => {
    expect(valueToEditString(new Date("2026-04-15T10:20:30.000Z"))).toBe(
      "2026-04-15T10:20:30.000Z",
    );
  });

  it("stringifies unexpected objects generically", () => {
    expect(valueToEditString({ nested: true })).toBe('{"nested":true}');
  });
});

describe("EditInput", () => {
  it("renders an input with the initial value", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial="hello"
        nullable={false}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = screen.getByDisplayValue("hello");
    expect(input).toBeDefined();
  });

  it("commits on Enter", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial="hello"
        nullable={false}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = screen.getByDisplayValue("hello");
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledWith("hello");
  });

  it("cancels on Escape", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial="hello"
        nullable={false}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = screen.getByDisplayValue("hello");
    fireEvent.keyDown(input, { key: "Escape" });
    expect(onCancel).toHaveBeenCalled();
  });

  it("shows NULL button when nullable", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial="hello"
        nullable={true}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const nullBtn = screen.getByTitle("Set field to NULL");
    expect(nullBtn).toBeDefined();
  });

  it("does not show NULL button when not nullable", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial="hello"
        nullable={false}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    expect(screen.queryByTitle("Set field to NULL")).toBeNull();
  });

  it("commits NULL_SENTINEL when clicking NULL button", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const { container } = render(
      <EditInput
        initial="hello"
        nullable={true}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const nullBtn = container.querySelector(
      "[data-null-btn]",
    ) as HTMLButtonElement;
    expect(nullBtn).not.toBeNull();
    fireEvent.click(nullBtn);
    expect(onCommit).toHaveBeenCalledWith(NULL_SENTINEL);
  });

  it("renders a text input for boolean-category columns", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial="true"
        nullable={false}
        category="boolean"
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = screen.getByDisplayValue("true");
    expect(input.tagName).toBe("INPUT");
  });

  it("uses a text input for category boolean", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial="false"
        nullable={false}
        category="boolean"
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = screen.getByDisplayValue("false");
    expect(input.tagName).toBe("INPUT");
  });

  it("uses semantics-aware placeholder text without changing the editor control", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial=""
        nullable={false}
        category="boolean"
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );

    expect(screen.getByPlaceholderText("true / false").tagName).toBe("INPUT");
  });

  it("uses the integer placeholder for bit-style values while staying string-based", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial=""
        nullable={false}
        category="integer"
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );

    const input = screen.getByPlaceholderText("number");
    fireEvent.change(input, { target: { value: "13" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(onCommit).toHaveBeenCalledWith("13");
  });

  it("keeps NULL when a null text editor blurs without changes", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const { container } = render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable={true}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = container.querySelector("input") as HTMLInputElement;
    expect(input).not.toBeNull();
    fireEvent.blur(input);
    expect(onCommit).toHaveBeenCalledWith(NULL_SENTINEL);
  });

  it("allows typing a value into a null text editor", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const { container } = render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable={true}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = container.querySelector("input") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "hello" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onCommit).toHaveBeenCalledWith("hello");
  });

  it("keeps NULL for a null boolean-category editor on blur without changes", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const { container } = render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable={true}
        category="boolean"
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = container.querySelector("input") as HTMLInputElement;
    fireEvent.blur(input);
    expect(onCommit).toHaveBeenCalledWith(NULL_SENTINEL);
  });

  it("commits an explicit boolean text value after starting from NULL", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const { container } = render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable={true}
        category="boolean"
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const input = container.querySelector("input") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "true" } });
    fireEvent.blur(input);
    expect(onCommit).toHaveBeenCalledWith("true");
  });
});
