import { fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { CellDisplay } from "../../src/webview/components/table/CellDisplay";
import { TableGrid } from "../../src/webview/components/table/TableGrid";
import {
  CELL_PREVIEW_LIMIT,
  CELL_PREVIEW_NOTICE,
} from "../../src/webview/utils/cellPreview";
import { calcColWidths } from "../../src/webview/utils/columnSizing";
import { formatScalarValueForDisplay } from "../../src/webview/utils/valueFormatting";
import { getPostedMessages } from "./testUtils";

// Use the real virtualizer with a deterministic JSDOM viewport.
vi.mock("@tanstack/react-virtual", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@tanstack/react-virtual")>();
  return {
    ...actual,
    useVirtualizer: (options: Parameters<typeof actual.useVirtualizer>[0]) =>
      actual.useVirtualizer({
        ...options,
        initialRect: { width: 800, height: 260 },
        observeElementRect: (_instance, callback) => {
          callback({ width: 800, height: 260 });
          return () => undefined;
        },
      }),
  };
});

vi.mock("../../src/webview/components/MonacoEditor", () => ({
  MonacoEditor: ({
    initialValue,
    ariaLabel,
  }: {
    initialValue: string;
    ariaLabel: string;
  }) => <textarea aria-label={ariaLabel} value={initialValue} readOnly />,
}));

const measureText = vi.fn((text: string) => ({ width: text.length * 8 }));
const huge = `${"x\ny".repeat(2_000_000)}tail`;

beforeEach(() => {
  measureText.mockClear();
  vi.mocked(HTMLCanvasElement.prototype.getContext).mockReturnValue({
    font: "12px monospace",
    measureText,
  } as unknown as CanvasRenderingContext2D);
});

function expectBoundedMeasurements() {
  expect(measureText).toHaveBeenCalled();
  for (const [text] of measureText.mock.calls) {
    expect(text.length).toBeLessThanOrEqual(CELL_PREVIEW_LIMIT);
  }
}

describe("large cell rendering", () => {
  it.each([
    "lob",
    "text",
    "binary",
    "uuid",
    "decimal",
    "json",
  ] as const)("bounds %s DOM text before newline replacement and binary formatting", (category) => {
    const { container } = render(
      <CellDisplay value={huge} category={category} isPending={false} />,
    );
    expect(container.textContent?.length).toBeLessThanOrEqual(
      CELL_PREVIEW_LIMIT,
    );
    expect(container.textContent?.endsWith("…")).toBe(true);
    expect(container.querySelector("span")?.title).toBe(CELL_PREVIEW_NOTICE);
  });

  it("bounds rendered query cells/titles/measurements while copy and preview keep full text", () => {
    const rows = Array.from({ length: 100 }, () =>
      Object.freeze({ __col_0: huge }),
    );
    const { container } = render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["clob"],
          columnMeta: [{ category: "lob" }],
          rows,
          rowCount: rows.length,
          executionTimeMs: 1,
        }}
      />,
    );
    const cells = Array.from(
      container.querySelectorAll<HTMLTableCellElement>("td[data-row]"),
    );
    expect(cells.length).toBeGreaterThan(0);
    expect(cells.length).toBeLessThan(rows.length);
    for (const cell of cells) {
      expect(cell.textContent?.length).toBeLessThanOrEqual(CELL_PREVIEW_LIMIT);
      expect(cell.title.length).toBeLessThanOrEqual(
        CELL_PREVIEW_LIMIT + CELL_PREVIEW_NOTICE.length + 1,
      );
      expect(cell.title).toContain(CELL_PREVIEW_NOTICE);
    }
    expectBoundedMeasurements();
    fireEvent.mouseDown(cells[0], { button: 0 });
    fireEvent.mouseUp(window);
    fireEvent.keyDown(cells[0], { key: "c", ctrlKey: true });
    const copied = getPostedMessages().find(
      (message) => message.type === "writeClipboard",
    );
    // TSV quotes multiline cells but preserves all original characters.
    expect(
      (copied?.payload as { text?: string } | undefined)?.text === `"${huge}"`,
    ).toBe(true);
    fireEvent.doubleClick(cells[0]);
    expect(
      (screen.getByLabelText("Cell data") as HTMLTextAreaElement).value,
    ).toBe(huge);
    expect(rows[0].__col_0).toBe(huge);
  });

  it("bounds persisted, pending, draft, and primary-key table cells without truncating dialog data", () => {
    const column: ColumnTypeMeta = {
      name: "clob",
      type: "CLOB",
      category: "lob",
      nativeType: "CLOB",
      nullable: true,
      isPrimaryKey: true,
      isForeignKey: false,
      filterable: false,
      filterOperators: [],
      valueSemantics: "plain",
    };
    const rows = Array.from({ length: 100 }, () =>
      Object.freeze({ clob: huge }),
    );
    const onOpenStructuredCell = vi.fn();
    const { container } = render(
      <TableGrid
        canEditRows
        canSelectAndDeleteRows={false}
        colSizes={{ clob: 160 }}
        columns={[column]}
        editCell={null}
        filterDrafts={{}}
        loading={false}
        loadingRef={{ current: false }}
        fetchEpochRef={{ current: 0 }}
        newRows={[{ clob: { value: huge } }]}
        onCancelEdit={vi.fn()}
        onBatchCellEdit={vi.fn()}
        onCommitCellEdit={vi.fn()}
        onCommitDraftCellEdit={vi.fn()}
        onMixedBatchEdit={vi.fn()}
        onFilterDraftChange={vi.fn()}
        onSelectionChange={vi.fn()}
        onSort={vi.fn()}
        onOpenStructuredCell={onOpenStructuredCell}
        onStartDraftEdit={vi.fn()}
        onStartEdit={vi.fn()}
        pendingEdits={new Map([[1, new Map([["clob", huge]])]])}
        rows={rows}
        scrollRef={React.createRef<HTMLDivElement>()}
        selected={new Set()}
        sort={null}
      />,
    );
    const cells = Array.from(
      container.querySelectorAll<HTMLTableCellElement>("td[data-row]"),
    );
    expect(cells.length).toBeGreaterThan(2);
    expect(cells.length).toBeLessThan(rows.length);
    for (const cell of cells) {
      expect(cell.textContent?.length).toBeLessThanOrEqual(CELL_PREVIEW_LIMIT);
      expect(cell.title.length).toBeLessThanOrEqual(
        CELL_PREVIEW_LIMIT + CELL_PREVIEW_NOTICE.length + 20,
      );
    }
    const persisted = container.querySelector('td[data-row="0"]');
    expect(persisted).not.toBeNull();
    fireEvent.mouseDown(persisted as HTMLElement, { button: 0 });
    fireEvent.mouseUp(window);
    fireEvent.keyDown(persisted as HTMLElement, { key: "c", ctrlKey: true });
    const copied = getPostedMessages().find(
      (message) => message.type === "writeClipboard",
    );
    expect(
      (copied?.payload as { text?: string } | undefined)?.text === `"${huge}"`,
    ).toBe(true);
    fireEvent.doubleClick(persisted as HTMLElement);
    expect(onOpenStructuredCell).toHaveBeenCalledWith(
      expect.objectContaining({
        currentValue: huge,
        originalValue: huge,
      }),
    );
    expect(
      onOpenStructuredCell.mock.calls[0][0].value.formattedText === huge,
    ).toBe(true);
    expect(rows[0].clob).toBe(huge);
  });

  it("renders bounded object and transported Buffer previews alongside unchanged timestamps", () => {
    const timestamp = new Date("2026-01-02T03:04:05Z");
    const object = { nested: { clob: huge } };
    const buffer = { type: "Buffer", data: new Array(1_000_000).fill(255) };
    const row = Object.freeze({
      __col_0: timestamp,
      __col_1: object,
      __col_2: buffer,
    });
    const { container } = render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["timestamp", "object", "buffer"],
          columnMeta: [
            { category: "datetime" },
            { category: "json" },
            { category: "binary" },
          ],
          rows: [row],
          rowCount: 1,
          executionTimeMs: 1,
        }}
      />,
    );
    const cells =
      container.querySelectorAll<HTMLTableCellElement>("td[data-row]");
    expect(cells.length).toBe(3);
    expect(cells[0].textContent).toBe(timestamp.toISOString());
    expect(cells[0].title).toBe(timestamp.toISOString());
    for (const cell of Array.from(cells).slice(1)) {
      expect(cell.textContent?.length).toBeLessThanOrEqual(CELL_PREVIEW_LIMIT);
      expect(cell.title.length).toBeLessThanOrEqual(
        CELL_PREVIEW_LIMIT + CELL_PREVIEW_NOTICE.length + 1,
      );
      expect(cell.textContent?.endsWith("…")).toBe(true);
    }
    expectBoundedMeasurements();
    expect(row.__col_1).toBe(object);
    expect(row.__col_2.data.length).toBe(1_000_000);
  });

  it("preserves circular fallback text in rendered query cells, titles, and measurements", () => {
    const value: Record<string, unknown> = { own: "small" };
    value.self = value;
    const expected = formatScalarValueForDisplay(value);
    const { container } = render(
      <TableGrid
        mode="query"
        status="success"
        result={{
          columns: ["cycle"],
          columnMeta: [{ category: "json" }],
          rows: [{ __col_0: value }],
          rowCount: 1,
          executionTimeMs: 1,
        }}
      />,
    );
    const cell = container.querySelector<HTMLTableCellElement>("td[data-row]");
    expect(cell?.textContent).toBe(expected);
    expect(cell?.title).toBe(expected);
    expect(cell?.querySelector("span")?.hasAttribute("title")).toBe(false);
    expect(measureText.mock.calls.some(([text]) => text === expected)).toBe(
      true,
    );
    expectBoundedMeasurements();
    expect(value.self).toBe(value);
  });

  it("measures only bounded previews, including header, timestamp, binary, and object values", () => {
    calcColWidths(
      [
        {
          name: huge,
          dataKey: "value",
          isPrimaryKey: false,
          isForeignKey: false,
        },
      ],
      [
        { value: huge },
        { value: { clob: huge } },
        { value: { type: "Buffer", data: new Array(1_000_000).fill(255) } },
        { value: new Date("2026-01-02T03:04:05Z") },
      ],
    );
    expectBoundedMeasurements();
    expect(
      measureText.mock.calls.some(
        ([text]) => text === "2026-01-02T03:04:05.000Z",
      ),
    ).toBe(true);
  });

  it("keeps ordinary text and newline display unchanged", () => {
    const { container } = render(
      <CellDisplay
        value={"hello\r\nworld 😀"}
        isPending={false}
        category="text"
      />,
    );
    expect(container.textContent).toBe("hello↵world 😀");
    expect(container.querySelector("span")?.getAttribute("title")).toBeNull();
  });
});
