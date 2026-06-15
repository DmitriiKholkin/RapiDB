/**
 * Smoke tests for the refactored `pendingEdits` utilities.
 *
 * The original co-located suite at `src/webview/utils/pendingEdits.test.ts`
 * is not picked up by the vitest workspace (which scans `tests/**`).
 * The full behavioural coverage is re-asserted here from the perspective
 * of the actual workspace, ensuring the refactor remains a true
 * drop-in replacement.
 */
import { describe, expect, it } from "vitest";
import {
  type ColumnTypeMeta as ColumnMeta,
  NULL_SENTINEL,
} from "../../src/shared/tableTypes";
import type { PendingEdits } from "../../src/webview/types";
import {
  applyCellEditsToPending,
  applyDraftEditsToInsertRow,
  clearAllPendingEdits,
  clearPendingEdits,
  createPendingEdit,
  filterEffectiveEdits,
  getPendingEditsForCell,
  mergePendingEdits,
  type PendingCellEdit,
  type PendingDraftEdit,
} from "../../src/webview/utils/pendingEdits";

// ─── Fixtures ────────────────────────────────────────────────────────────────

const makeColumn = (name: string, nullable = false): ColumnMeta =>
  ({
    name,
    type: "text",
    nativeType: "text",
    nullable,
    isPrimaryKey: name === "id",
    isForeignKey: false,
    category: "text",
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  }) as ColumnMeta;

const emptyPending = (): PendingEdits => new Map();

const pendingWith = (
  rows: Array<[number, Array<[string, unknown]>]>,
): PendingEdits => {
  const map = new Map<number, Map<string, unknown>>();
  for (const [rowIdx, cols] of rows) {
    map.set(rowIdx, new Map(cols));
  }
  return map;
};

// ─── createPendingEdit ───────────────────────────────────────────────────────

describe("createPendingEdit (refactored)", () => {
  it("adds a new edit", () => {
    const col = makeColumn("name");
    const result = createPendingEdit(emptyPending(), 0, col, "Alice", "Bob");
    expect(result.get(0)?.get("name")).toBe("Alice");
  });

  it("removes an edit when reverted to original", () => {
    const col = makeColumn("name");
    const pending = pendingWith([[0, [["name", "Alice"]]]]);
    const result = createPendingEdit(pending, 0, col, "Alice", "Alice");
    expect(result.has(0)).toBe(false);
  });

  it("stores null for NULL_SENTINEL", () => {
    const col = makeColumn("name", true);
    const result = createPendingEdit(
      emptyPending(),
      0,
      col,
      NULL_SENTINEL,
      "Bob",
    );
    expect(result.get(0)?.get("name")).toBeNull();
  });

  it("does not mutate the input map", () => {
    const col = makeColumn("name");
    const pending = pendingWith([[0, [["name", "Alice"]]]]);
    const snapshot = [...pending.keys()];
    createPendingEdit(pending, 0, col, "Charlie", "Bob");
    expect([...pending.keys()]).toEqual(snapshot);
    expect(pending.get(0)?.get("name")).toBe("Alice");
  });
});

// ─── applyCellEditsToPending ────────────────────────────────────────────────

describe("applyCellEditsToPending (refactored)", () => {
  it("returns same map for empty edits", () => {
    const pending = emptyPending();
    expect(applyCellEditsToPending(pending, [])).toBe(pending);
  });

  it("applies many edits in a single pass", () => {
    const name = makeColumn("name");
    const email = makeColumn("email");
    const edits: PendingCellEdit[] = [
      { rowIdx: 0, column: name, newVal: "Alice", originalVal: "Bob" },
      { rowIdx: 0, column: email, newVal: "a@b.com", originalVal: "" },
      { rowIdx: 1, column: name, newVal: "Charlie", originalVal: "" },
    ];
    const result = applyCellEditsToPending(emptyPending(), edits);
    expect(result.get(0)?.get("name")).toBe("Alice");
    expect(result.get(0)?.get("email")).toBe("a@b.com");
    expect(result.get(1)?.get("name")).toBe("Charlie");
  });

  it("removes edits that revert to original", () => {
    const col = makeColumn("name");
    const pending = pendingWith([[0, [["name", "Alice"]]]]);
    const result = applyCellEditsToPending(pending, [
      { rowIdx: 0, column: col, newVal: "Alice", originalVal: "Alice" },
    ]);
    expect(result.has(0)).toBe(false);
  });
});

// ─── getPendingEditsForCell ──────────────────────────────────────────────────

describe("getPendingEditsForCell", () => {
  it("returns the pending value", () => {
    const pending = pendingWith([[0, [["name", "Alice"]]]]);
    expect(getPendingEditsForCell(pending, 0, "name")).toBe("Alice");
  });

  it("returns undefined for non-pending cell", () => {
    expect(getPendingEditsForCell(emptyPending(), 0, "name")).toBeUndefined();
  });
});

// ─── clearPendingEdits / clearAllPendingEdits ───────────────────────────────

describe("clearPendingEdits", () => {
  it("removes a single cell entry but keeps siblings", () => {
    const pending = pendingWith([
      [
        0,
        [
          ["name", "Alice"],
          ["email", "a@b.com"],
        ],
      ],
    ]);
    const result = clearPendingEdits(pending, 0, "name");
    expect(result.get(0)?.has("name")).toBe(false);
    expect(result.get(0)?.get("email")).toBe("a@b.com");
  });

  it("removes the row when last cell is cleared", () => {
    const pending = pendingWith([[0, [["name", "Alice"]]]]);
    const result = clearPendingEdits(pending, 0, "name");
    expect(result.has(0)).toBe(false);
  });

  it("returns the same map for a non-pending cell", () => {
    const pending = emptyPending();
    expect(clearPendingEdits(pending, 0, "name")).toBe(pending);
  });
});

describe("clearAllPendingEdits", () => {
  it("returns an empty map", () => {
    const result = clearAllPendingEdits();
    expect(result.size).toBe(0);
    expect(result).toBeInstanceOf(Map);
  });
});

// ─── mergePendingEdits ──────────────────────────────────────────────────────

describe("mergePendingEdits", () => {
  it("merges two empty maps", () => {
    expect(mergePendingEdits(emptyPending(), emptyPending()).size).toBe(0);
  });

  it("merges disjoint rows", () => {
    const base = pendingWith([[0, [["name", "Alice"]]]]);
    const override = pendingWith([[0, [["email", "a@b.com"]]]]);
    const result = mergePendingEdits(base, override);
    expect(result.get(0)?.get("name")).toBe("Alice");
    expect(result.get(0)?.get("email")).toBe("a@b.com");
  });

  it("override takes precedence on collision", () => {
    const base = pendingWith([[0, [["name", "Alice"]]]]);
    const override = pendingWith([[0, [["name", "Charlie"]]]]);
    expect(mergePendingEdits(base, override).get(0)?.get("name")).toBe(
      "Charlie",
    );
  });
});

// ─── applyDraftEditsToInsertRow ─────────────────────────────────────────────

describe("applyDraftEditsToInsertRow", () => {
  it("returns same draft for empty edits", () => {
    const draft = { name: { value: "" } };
    expect(applyDraftEditsToInsertRow(draft, [])).toBe(draft);
  });

  it("applies edits immutably", () => {
    const draft = { name: { value: "" } };
    const col = makeColumn("name");
    const edits: PendingDraftEdit[] = [{ column: col, newVal: "Alice" }];
    const result = applyDraftEditsToInsertRow(draft, edits);
    expect(result.name.value).toBe("Alice");
    expect(draft.name.value).toBe("");
  });

  it("preserves NULL_SENTINEL", () => {
    const draft = { name: { value: "" } };
    const col = makeColumn("name", true);
    const result = applyDraftEditsToInsertRow(draft, [
      { column: col, newVal: NULL_SENTINEL },
    ]);
    expect(result.name.value).toBe(NULL_SENTINEL);
  });
});

// ─── filterEffectiveEdits ───────────────────────────────────────────────────

describe("filterEffectiveEdits", () => {
  it("returns empty when all edits already match pending state", () => {
    const col = makeColumn("name");
    const pending = pendingWith([[0, [["name", "Alice"]]]]);
    const edits: PendingCellEdit[] = [
      { rowIdx: 0, column: col, newVal: "Alice", originalVal: "Bob" },
    ];
    expect(filterEffectiveEdits(edits, pending)).toHaveLength(0);
  });

  it("includes edits that differ from current pending state", () => {
    const col = makeColumn("name");
    const pending = pendingWith([[0, [["name", "Alice"]]]]);
    const edits: PendingCellEdit[] = [
      { rowIdx: 0, column: col, newVal: "Charlie", originalVal: "Bob" },
    ];
    const result = filterEffectiveEdits(edits, pending);
    expect(result).toHaveLength(1);
    expect(result[0].newVal).toBe("Charlie");
  });

  it("includes edits that match original but are currently pending", () => {
    const col = makeColumn("name");
    const pending = pendingWith([[0, [["name", "Alice"]]]]);
    const edits: PendingCellEdit[] = [
      { rowIdx: 0, column: col, newVal: "Alice", originalVal: "Alice" },
    ];
    expect(filterEffectiveEdits(edits, pending)).toHaveLength(1);
  });

  it("returns empty for non-pending cells whose edit matches original", () => {
    const col = makeColumn("name");
    const edits: PendingCellEdit[] = [
      { rowIdx: 0, column: col, newVal: "Alice", originalVal: "Alice" },
    ];
    expect(filterEffectiveEdits(edits, emptyPending())).toHaveLength(0);
  });
});
