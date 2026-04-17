/**
 * @vitest-environment jsdom
 */

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { CellDisplay } from "../../src/webview/components/table/CellDisplay";

afterEach(cleanup);

describe("CellDisplay", () => {
  it("renders NULL for null value", () => {
    render(<CellDisplay value={null} isPending={false} />);
    const nullText = screen.getByText("NULL");
    expect(nullText).toBeDefined();
    expect(nullText.style.fontStyle).toBe("italic");
  });

  it("renders NULL for undefined value", () => {
    render(<CellDisplay value={undefined} isPending={false} />);
    expect(screen.getByText("NULL")).toBeDefined();
  });

  it.each([
    [true, "true"],
    [false, "false"],
    [1, "true"],
    [0, "false"],
    ["1", "true"],
    ["false", "false"],
  ])("renders boolean %p as %s", (value, expected) => {
    render(<CellDisplay value={value} isPending={false} isBoolean={true} />);
    expect(screen.getByText(expected)).toBeDefined();
  });

  it("renders boolean true even with category='boolean'", () => {
    render(<CellDisplay value={true} isPending={false} category="boolean" />);
    expect(screen.getByText("true")).toBeDefined();
  });

  it("renders numbers with numeric category", () => {
    render(<CellDisplay value={42} isPending={false} category="integer" />);
    expect(screen.getByText("42")).toBeDefined();
  });

  it("renders float numbers", () => {
    render(<CellDisplay value={3.14} isPending={false} category="float" />);
    expect(screen.getByText("3.14")).toBeDefined();
  });

  it("normalizes Oracle BINARY_FLOAT artifacts for display", () => {
    render(
      <CellDisplay
        value={1.2000000476837158}
        isPending={false}
        category="float"
        nativeType="BINARY_FLOAT"
      />,
    );
    expect(screen.getByText("1.2")).toBeDefined();
  });

  it("renders JSON strings", () => {
    const json = '{"key": "value"}';
    render(<CellDisplay value={json} isPending={false} category="json" />);
    expect(screen.getByText(json)).toBeDefined();
  });

  it("truncates long JSON strings", () => {
    const longJson = '{"data": "' + "x".repeat(200) + '"}';
    render(<CellDisplay value={longJson} isPending={false} category="json" />);
    const rendered = screen.getByText(/^\{.*…$/);
    expect(rendered).toBeDefined();
  });

  it("renders binary hex values with category color", () => {
    const { container } = render(
      <CellDisplay value={"\\xDEADBEEF"} isPending={false} category="binary" />,
    );
    const span = container.querySelector("span");
    expect(span?.textContent).toBe("\\xDEADBEEF");
  });

  it("renders UUID values", () => {
    const uuid = "550e8400-e29b-41d4-a716-446655440000";
    render(<CellDisplay value={uuid} isPending={false} category="uuid" />);
    expect(screen.getByText(uuid)).toBeDefined();
  });

  it("renders plain text", () => {
    render(<CellDisplay value="hello world" isPending={false} />);
    expect(screen.getByText("hello world")).toBeDefined();
  });

  it("renders plain text with category text", () => {
    render(<CellDisplay value="hello" isPending={false} category="text" />);
    expect(screen.getByText("hello")).toBeDefined();
  });

  it("applies warning color when pending", () => {
    render(<CellDisplay value="test" isPending={true} />);
    const el = screen.getByText("test");
    expect(el.style.color).toContain("cca700");
  });

  it("renders date values", () => {
    render(
      <CellDisplay value="2024-01-15" isPending={false} category="date" />,
    );
    expect(screen.getByText("2024-01-15")).toBeDefined();
  });

  it("renders datetime values", () => {
    render(
      <CellDisplay
        value="2024-01-15 10:30:00"
        isPending={false}
        category="datetime"
      />,
    );
    expect(screen.getByText("2024-01-15 10:30:00")).toBeDefined();
  });

  it("renders time values", () => {
    render(<CellDisplay value="10:30:00" isPending={false} category="time" />);
    expect(screen.getByText("10:30:00")).toBeDefined();
  });
});
