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
    expect(valueToEditString(true, true)).toBe("true");
  });

  it("converts boolean false to 'false'", () => {
    expect(valueToEditString(false, true)).toBe("false");
  });

  it("converts number 1 to 'true' for boolean column", () => {
    expect(valueToEditString(1, true)).toBe("true");
  });

  it("converts number 0 to 'false' for boolean column", () => {
    expect(valueToEditString(0, true)).toBe("false");
  });

  it("converts null to NULL_SENTINEL", () => {
    expect(valueToEditString(null, false)).toBe(NULL_SENTINEL);
  });

  it("converts undefined to NULL_SENTINEL", () => {
    expect(valueToEditString(undefined, false)).toBe(NULL_SENTINEL);
  });

  it("converts strings as-is for non-boolean", () => {
    expect(valueToEditString("hello", false)).toBe("hello");
  });

  it("converts numbers as-is for non-boolean", () => {
    expect(valueToEditString(42, false)).toBe("42");
  });

  it("does not normalize float artifacts in the webview edit layer", () => {
    expect(valueToEditString(1.2000000476837158, false)).toBe(
      "1.2000000476837158",
    );
  });

  it("serializes dates generically for editing", () => {
    expect(valueToEditString(new Date("2026-04-15T10:20:30.000Z"), false)).toBe(
      "2026-04-15T10:20:30.000Z",
    );
  });

  it("stringifies unexpected objects generically", () => {
    expect(valueToEditString({ nested: true }, false)).toBe(
      '{"nested":true}',
    );
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

  it("renders a select for boolean columns", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    render(
      <EditInput
        initial="true"
        nullable={false}
        isBoolean={true}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    // Boolean columns use a <select>
    const select = document.querySelector("select");
    expect(select).not.toBeNull();
  });

  it("renders a select for category boolean", () => {
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
    const select = document.querySelector("select");
    expect(select).not.toBeNull();
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

  it("keeps NULL for a null boolean editor on blur without selection", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const { container } = render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable={true}
        isBoolean={true}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const select = container.querySelector("select") as HTMLSelectElement;
    fireEvent.blur(select);
    expect(onCommit).toHaveBeenCalledWith(NULL_SENTINEL);
  });

  it("commits an explicit boolean value after starting from NULL", () => {
    const onCommit = vi.fn();
    const onCancel = vi.fn();
    const { container } = render(
      <EditInput
        initial={NULL_SENTINEL}
        nullable={true}
        isBoolean={true}
        onCommit={onCommit}
        onCancel={onCancel}
      />,
    );
    const select = container.querySelector("select") as HTMLSelectElement;
    fireEvent.change(select, { target: { value: "true" } });
    fireEvent.blur(select);
    expect(onCommit).toHaveBeenCalledWith("true");
  });
});
