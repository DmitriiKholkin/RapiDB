import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ColumnTypeMeta as ColumnMeta } from "../../src/shared/tableTypes";
import {
  type UsePasteHandlerOptions,
  usePasteHandler,
} from "../../src/webview/components/table/usePasteHandler";
import type { InsertDraftRow, Row } from "../../src/webview/types";

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

function makeColumn(
  overrides: Partial<ColumnMeta> & {
    name: string;
    category: ColumnMeta["category"];
  },
): ColumnMeta {
  const { name, category, ...rest } = overrides;
  return {
    name,
    category,
    type: rest.type ?? "text",
    nativeType: rest.nativeType ?? "text",
    nullable: rest.nullable ?? true,
    isPrimaryKey: rest.isPrimaryKey ?? false,
    isForeignKey: rest.isForeignKey ?? false,
    filterable: rest.filterable ?? true,
    filterOperators: rest.filterOperators ?? [
      "eq",
      "neq",
      "gt",
      "gte",
      "lt",
      "lte",
      "between",
      "in",
      "is_null",
      "is_not_null",
    ],
    valueSemantics: rest.valueSemantics ?? "plain",
    ...rest,
  };
}

function makeRow(values: Record<string, unknown>): Row {
  return values as Row;
}

interface HookOptions {
  canEditRows?: boolean;
  columns?: ColumnMeta[];
  rows?: Row[];
  selectedColumnOffset?: number;
  onBatchCellEdit?: ReturnType<typeof vi.fn>;
  onBatchDraftCellEdit?: ReturnType<typeof vi.fn>;
  onMixedBatchEdit?: ReturnType<typeof vi.fn>;
  selectionRange?: { anchorRow: number; anchorCol: number } | null;
  contextMenuCell?: { row: number; col: number } | null;
  newRow?: InsertDraftRow | null;
  handlePaste?: ReturnType<typeof vi.fn>;
}

function createHookOptions(overrides: HookOptions = {}) {
  const selectionRangeRef = { current: overrides.selectionRange ?? null };
  const contextMenuCellRef = { current: overrides.contextMenuCell ?? null };
  const newRowRef = { current: overrides.newRow ?? null };

  return {
    handlePaste: (overrides.handlePaste ?? vi.fn()) as () => void,
    canEditRows: overrides.canEditRows ?? true,
    columns: overrides.columns ?? [
      makeColumn({ name: "id", category: "integer", isPrimaryKey: true }),
      makeColumn({ name: "name", category: "text" }),
      makeColumn({ name: "value", category: "decimal", nullable: false }),
    ],
    rows: overrides.rows ?? [
      makeRow({ id: 1, name: "Alice", value: 100 }),
      makeRow({ id: 2, name: "Bob", value: 200 }),
    ],
    selectedColumnOffset: overrides.selectedColumnOffset ?? 0,
    onBatchCellEdit: (overrides.onBatchCellEdit ??
      vi.fn()) as UsePasteHandlerOptions["onBatchCellEdit"],
    onBatchDraftCellEdit: (overrides.onBatchDraftCellEdit ??
      vi.fn()) as UsePasteHandlerOptions["onBatchDraftCellEdit"],
    onMixedBatchEdit: (overrides.onMixedBatchEdit ??
      vi.fn()) as UsePasteHandlerOptions["onMixedBatchEdit"],
    selectionRangeRef,
    contextMenuCellRef,
    newRowRef,
  };
}

function dispatchClipboardText(text: string): void {
  window.dispatchEvent(
    new MessageEvent("message", {
      data: { type: "clipboardText", payload: text },
    }),
  );
}

/* ------------------------------------------------------------------ */
/*  Tests                                                             */
/* ------------------------------------------------------------------ */

describe("usePasteHandler", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  /* ---------- initial state ---------- */

  it("starts with empty pasteErrors", () => {
    const options = createHookOptions();
    const { result } = renderHook(() => usePasteHandler(options));

    expect(result.current.pasteErrors).toEqual([]);
  });

  /* ---------- handlePaste ---------- */

  it("handlePaste is called when keyboard paste event is triggered", () => {
    const handlePaste = vi.fn();
    const options = createHookOptions({
      handlePaste,
      selectionRange: { anchorRow: 0, anchorCol: 0 },
    });

    renderHook(() => usePasteHandler(options));

    act(() => {
      const event = new ClipboardEvent("paste", {
        bubbles: true,
        cancelable: true,
      });
      window.dispatchEvent(event);
    });

    expect(handlePaste).toHaveBeenCalled();
  });

  /* ---------- keyboard paste event ---------- */

  it("intercepts keyboard paste and calls handlePaste", () => {
    const handlePaste = vi.fn();
    const options = createHookOptions({
      handlePaste,
      selectionRange: { anchorRow: 0, anchorCol: 0 },
    });

    renderHook(() => usePasteHandler(options));

    act(() => {
      const event = new ClipboardEvent("paste", {
        bubbles: true,
        cancelable: true,
      });
      window.dispatchEvent(event);
    });

    expect(handlePaste).toHaveBeenCalled();
  });

  it("does not intercept paste when canEditRows is false", () => {
    const handlePaste = vi.fn();
    const options = createHookOptions({
      handlePaste,
      canEditRows: false,
      selectionRange: { anchorRow: 0, anchorCol: 0 },
    });

    renderHook(() => usePasteHandler(options));

    act(() => {
      const event = new ClipboardEvent("paste", {
        bubbles: true,
        cancelable: true,
      });
      window.dispatchEvent(event);
    });

    expect(handlePaste).not.toHaveBeenCalled();
  });

  it("does not intercept paste when selection range is null", () => {
    const handlePaste = vi.fn();
    const options = createHookOptions({
      handlePaste,
      selectionRange: null,
    });

    renderHook(() => usePasteHandler(options));

    act(() => {
      const event = new ClipboardEvent("paste", {
        bubbles: true,
        cancelable: true,
      });
      window.dispatchEvent(event);
    });

    expect(handlePaste).not.toHaveBeenCalled();
  });

  /* ---------- clipboard text message handling ---------- */

  describe("clipboard text handling", () => {
    it("parses TSV data and creates batch edits for persisted rows", () => {
      const onBatchCellEdit = vi.fn();
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 0, anchorCol: 1 },
      });

      renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("Alice\t500");
      });

      expect(onBatchCellEdit).toHaveBeenCalledWith([
        {
          rowIdx: 0,
          column: options.columns[1], // name column
          newVal: "Alice",
          originalVal: "Alice",
        },
        {
          rowIdx: 0,
          column: options.columns[2], // value column
          newVal: "500",
          originalVal: 100,
        },
      ]);
    });

    it("handles multi-row paste", () => {
      const onBatchCellEdit = vi.fn();
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 0, anchorCol: 1 },
      });

      renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("Alice\t500\nBob\t600");
      });

      expect(onBatchCellEdit).toHaveBeenCalledWith([
        {
          rowIdx: 0,
          column: options.columns[1],
          newVal: "Alice",
          originalVal: "Alice",
        },
        {
          rowIdx: 0,
          column: options.columns[2],
          newVal: "500",
          originalVal: 100,
        },
        {
          rowIdx: 1,
          column: options.columns[1],
          newVal: "Bob",
          originalVal: "Bob",
        },
        {
          rowIdx: 1,
          column: options.columns[2],
          newVal: "600",
          originalVal: 200,
        },
      ]);
    });

    it("sets paste errors for invalid values", () => {
      const onBatchCellEdit = vi.fn();
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 0, anchorCol: 1 },
      });

      const { result } = renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("Alice\tnot-a-number");
      });

      expect(result.current.pasteErrors.length).toBeGreaterThan(0);
      expect(result.current.pasteErrors[0]?.columnName).toBe("value");
      expect(result.current.pasteErrors[0]?.message).toContain(
        "Invalid number",
      );
      expect(onBatchCellEdit).not.toHaveBeenCalled();
    });

    it("sets paste errors when pasting into primary key column", () => {
      const onBatchCellEdit = vi.fn();
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 0, anchorCol: 0 },
      });

      const { result } = renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("999");
      });

      expect(result.current.pasteErrors.length).toBeGreaterThan(0);
      expect(result.current.pasteErrors[0]?.message).toContain("primary key");
      expect(onBatchCellEdit).not.toHaveBeenCalled();
    });

    it("sets paste errors when paste exceeds table bounds", () => {
      const onBatchCellEdit = vi.fn();
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 1, anchorCol: 1 },
        rows: [makeRow({ id: 1, name: "Alice", value: 100 })],
      });

      const { result } = renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("Alice\t500\nBob\t600");
      });

      expect(result.current.pasteErrors.length).toBeGreaterThan(0);
      expect(
        result.current.pasteErrors.some((e) =>
          e.message.includes("does not exist"),
        ),
      ).toBe(true);
      expect(onBatchCellEdit).not.toHaveBeenCalled();
    });

    it("ignores empty clipboard text", () => {
      const onBatchCellEdit = vi.fn();
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 0, anchorCol: 1 },
      });

      renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("");
      });

      expect(onBatchCellEdit).not.toHaveBeenCalled();
    });

    it("clears previous paste errors on successful paste", () => {
      const onBatchCellEdit = vi.fn();
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 0, anchorCol: 1 },
      });

      const { result } = renderHook(() => usePasteHandler(options));

      // First, trigger an error
      act(() => {
        dispatchClipboardText("Alice\tnot-a-number");
      });
      expect(result.current.pasteErrors.length).toBeGreaterThan(0);

      // Then, trigger a successful paste
      act(() => {
        dispatchClipboardText("Alice\t500");
      });
      expect(result.current.pasteErrors).toEqual([]);
    });
  });

  /* ---------- context menu paste ---------- */

  describe("context menu paste", () => {
    it("uses context menu cell position when available", () => {
      const onBatchCellEdit = vi.fn();
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 0, anchorCol: 0 },
        contextMenuCell: { row: 1, col: 1 },
      });

      renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("Bob");
      });

      expect(onBatchCellEdit).toHaveBeenCalledWith([
        {
          rowIdx: 1,
          column: options.columns[1], // name column
          newVal: "Bob",
          originalVal: "Bob",
        },
      ]);
    });

    it("clears context menu cell after use", () => {
      const onBatchCellEdit = vi.fn();
      const contextMenuCellRef = { current: { row: 1, col: 1 } };
      const options = createHookOptions({
        onBatchCellEdit,
        selectionRange: { anchorRow: 0, anchorCol: 0 },
      });
      // Override the contextMenuCellRef directly
      options.contextMenuCellRef = contextMenuCellRef;

      renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("Bob");
      });

      expect(contextMenuCellRef.current).toBeNull();
    });
  });

  /* ---------- draft row paste ---------- */

  describe("draft row paste", () => {
    it("creates draft edits when pasting into draft row", () => {
      const onBatchDraftCellEdit = vi.fn();
      const draft: InsertDraftRow = {
        name: { value: "" },
        value: { value: "" },
      };
      const options = createHookOptions({
        onBatchDraftCellEdit,
        selectionRange: { anchorRow: -1, anchorCol: 1 },
        newRow: draft,
      });

      renderHook(() => usePasteHandler(options));

      act(() => {
        dispatchClipboardText("Bob\t500");
      });

      expect(onBatchDraftCellEdit).toHaveBeenCalledWith([
        {
          column: options.columns[1], // name column
          newVal: "Bob",
        },
        {
          column: options.columns[2], // value column
          newVal: "500",
        },
      ]);
    });

    it("creates mixed edits when paste spans draft and persisted rows", () => {
      const onMixedBatchEdit = vi.fn();
      const draft: InsertDraftRow = {
        name: { value: "" },
        value: { value: "" },
      };
      const options = createHookOptions({
        onMixedBatchEdit,
        selectionRange: { anchorRow: -1, anchorCol: 1 },
        newRow: draft,
      });

      renderHook(() => usePasteHandler(options));

      // Paste two rows starting from draft row (-1)
      act(() => {
        dispatchClipboardText("Bob\t500\nCharlie\t600");
      });

      expect(onMixedBatchEdit).toHaveBeenCalled();
      const [draftEdits, persistedEdits] = onMixedBatchEdit.mock.calls[0];
      expect(draftEdits).toHaveLength(2);
      expect(persistedEdits).toHaveLength(2);
    });
  });

  /* ---------- selection column protection ---------- */

  it("sets error when pasting into selection column (col < 0)", () => {
    const options = createHookOptions({
      selectionRange: { anchorRow: 0, anchorCol: 0 },
      selectedColumnOffset: 1, // This makes startCol = 0 - 1 = -1
    });

    const { result } = renderHook(() => usePasteHandler(options));

    act(() => {
      dispatchClipboardText("test");
    });

    expect(result.current.pasteErrors).toHaveLength(1);
    expect(result.current.pasteErrors[0]?.message).toContain(
      "selection column",
    );
  });

  /* ---------- setPasteErrors ---------- */

  it("setPasteErrors updates paste errors", () => {
    const options = createHookOptions();
    const { result } = renderHook(() => usePasteHandler(options));

    act(() => {
      result.current.setPasteErrors([
        {
          rowIndex: 0,
          columnIndex: 0,
          columnName: "test",
          value: "test",
          message: "Test error",
        },
      ]);
    });

    expect(result.current.pasteErrors).toHaveLength(1);
    expect(result.current.pasteErrors[0]?.message).toBe("Test error");
  });

  it("setPasteErrors can clear errors", () => {
    const options = createHookOptions();
    const { result } = renderHook(() => usePasteHandler(options));

    act(() => {
      result.current.setPasteErrors([
        {
          rowIndex: 0,
          columnIndex: 0,
          columnName: "test",
          value: "test",
          message: "Test error",
        },
      ]);
    });

    act(() => {
      result.current.setPasteErrors([]);
    });

    expect(result.current.pasteErrors).toEqual([]);
  });
});
