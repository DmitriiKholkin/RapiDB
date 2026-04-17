/**
 * @vitest-environment jsdom
 */

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";
import { CellDisplay } from "../../src/webview/components/table/CellDisplay";

afterEach(cleanup);

describe("CellDisplay", () => {
  // ─── NULL values ───

  it("renders NULL for null value", () => {
    render(<CellDisplay value={null} isPk={false} isPending={false} />);
    expect(screen.getByText("NULL")).toBeDefined();
    expect(screen.getByText("NULL").style.fontStyle).toBe("italic");
  });

  it("renders NULL for undefined value", () => {
    render(<CellDisplay value={undefined} isPk={false} isPending={false} />);
    expect(screen.getByText("NULL")).toBeDefined();
  });

  // ─── Boolean display ───

  it("renders 'true' for boolean true", () => {
    render(
      <CellDisplay
        value={true}
        isPk={false}
        isPending={false}
        isBoolean={true}
      />,
    );
    expect(screen.getByText("true")).toBeDefined();
  });

  it("renders 'false' for boolean false", () => {
    render(
      <CellDisplay
        value={false}
        isPk={false}
        isPending={false}
        isBoolean={true}
      />,
    );
    expect(screen.getByText("false")).toBeDefined();
  });

  it("coerces 1 to true for boolean columns", () => {
    render(
      <CellDisplay value={1} isPk={false} isPending={false} isBoolean={true} />,
    );
    expect(screen.getByText("true")).toBeDefined();
  });

  it("coerces 0 to false for boolean columns", () => {
    render(
      <CellDisplay value={0} isPk={false} isPending={false} isBoolean={true} />,
    );
    expect(screen.getByText("false")).toBeDefined();
  });

  it("coerces string '1' to true for boolean columns", () => {
    render(
      <CellDisplay value="1" isPk={false} isPending={false} isBoolean={true} />,
    );
    expect(screen.getByText("true")).toBeDefined();
  });

  it("coerces string 'true' to true for boolean columns", () => {
    render(
      <CellDisplay
        value="true"
        isPk={false}
        isPending={false}
        isBoolean={true}
      />,
    );
    expect(screen.getByText("true")).toBeDefined();
  });

  it("renders boolean true even with category='boolean'", () => {
    render(
      <CellDisplay
        value={true}
        isPk={false}
        isPending={false}
        category="boolean"
      />,
    );
    expect(screen.getByText("true")).toBeDefined();
  });

  // ─── Numeric display ───

  it("renders numbers with numeric category", () => {
    render(
      <CellDisplay
        value={42}
        isPk={false}
        isPending={false}
        category="integer"
      />,
    );
    expect(screen.getByText("42")).toBeDefined();
  });

  it("renders float numbers", () => {
    render(
      <CellDisplay
        value={3.14}
        isPk={false}
        isPending={false}
        category="float"
      />,
    );
    expect(screen.getByText("3.14")).toBeDefined();
  });

  // ─── JSON display ───

  it("renders JSON strings", () => {
    const json = '{"key": "value"}';
    render(
      <CellDisplay
        value={json}
        isPk={false}
        isPending={false}
        category="json"
      />,
    );
    expect(screen.getByText(json)).toBeDefined();
  });

  it("truncates long JSON strings", () => {
    const longJson = '{"data": "' + "x".repeat(200) + '"}';
    render(
      <CellDisplay
        value={longJson}
        isPk={false}
        isPending={false}
        category="json"
      />,
    );
    const rendered = screen.getByText(/^\{.*…$/);
    expect(rendered).toBeDefined();
  });

  // ─── Binary display ───

  it("renders binary hex values with category color", () => {
    const { container } = render(
      <CellDisplay
        value={"\\xDEADBEEF"}
        isPk={false}
        isPending={false}
        category="binary"
      />,
    );
    const span = container.querySelector("span");
    expect(span?.textContent).toBe("\\xDEADBEEF");
  });

  // ─── UUID display ───

  it("renders UUID values", () => {
    const uuid = "550e8400-e29b-41d4-a716-446655440000";
    render(
      <CellDisplay
        value={uuid}
        isPk={false}
        isPending={false}
        category="uuid"
      />,
    );
    expect(screen.getByText(uuid)).toBeDefined();
  });

  // ─── Text display ───

  it("renders plain text", () => {
    render(<CellDisplay value="hello world" isPk={false} isPending={false} />);
    expect(screen.getByText("hello world")).toBeDefined();
  });

  it("renders plain text with category text", () => {
    render(
      <CellDisplay
        value="hello"
        isPk={false}
        isPending={false}
        category="text"
      />,
    );
    expect(screen.getByText("hello")).toBeDefined();
  });

  // ─── Pending state ───

  it("applies warning color when pending", () => {
    render(<CellDisplay value="test" isPk={false} isPending={true} />);
    const el = screen.getByText("test");
    expect(el.style.color).toContain("cca700");
  });

  // ─── Date/time display ───

  it("renders date values", () => {
    render(
      <CellDisplay
        value="2024-01-15"
        isPk={false}
        isPending={false}
        category="date"
      />,
    );
    expect(screen.getByText("2024-01-15")).toBeDefined();
  });

  it("renders datetime values", () => {
    render(
      <CellDisplay
        value="2024-01-15 10:30:00"
        isPk={false}
        isPending={false}
        category="datetime"
      />,
    );
    expect(screen.getByText("2024-01-15 10:30:00")).toBeDefined();
  });

  it("renders time values", () => {
    render(
      <CellDisplay
        value="10:30:00"
        isPk={false}
        isPending={false}
        category="time"
      />,
    );
    expect(screen.getByText("10:30:00")).toBeDefined();
  });
});
