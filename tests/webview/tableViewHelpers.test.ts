import { describe, expect, it } from "vitest";
import {
  applyUndoRedoSnapshot,
  buildUndoRedoSnapshot,
} from "../../src/webview/components/table/tableViewHelpers";
import type {
  EditTarget,
  InsertDraftRow,
  MutationSnapshot,
  PendingEdits,
} from "../../src/webview/types";

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

/**
 * Type-narrowing helper that throws if the value is undefined.
 * Avoids non-null assertions (``!``) in test assertions.
 */
function assertPresent<T>(value: T | undefined, label?: string): T {
  if (value === undefined) {
    throw new Error(
      label ? `${label} is undefined` : "Expected value to be defined",
    );
  }
  return value;
}

function pendingEditsFrom(
  entries: Array<[number, Record<string, unknown>]>,
): PendingEdits {
  return new Map(
    entries.map(([row, cols]) => [row, new Map(Object.entries(cols))]),
  );
}

function draftRow(col: string, value: unknown): InsertDraftRow {
  return { [col]: { value } };
}

/* ------------------------------------------------------------------ */
/*  buildUndoRedoSnapshot                                              */
/* ------------------------------------------------------------------ */

describe("buildUndoRedoSnapshot", () => {
  it("captures pendingEdits, newRow, and editCell", () => {
    const pending = pendingEditsFrom([[0, { name: "Alice" }]]);
    const newRow = draftRow("name", "Bob");
    const editCell: EditTarget = { kind: "persisted", rowIdx: 0, col: "name" };

    const snapshot = buildUndoRedoSnapshot(pending, newRow, editCell);

    expect(snapshot.pendingEdits.get(0)?.get("name")).toBe("Alice");
    expect(snapshot.newRow?.name.value).toBe("Bob");
    expect(snapshot.editCell).toEqual(editCell);
  });

  it("returns empty pendingEdits when given an empty map", () => {
    const snapshot = buildUndoRedoSnapshot(new Map(), null, null);

    expect(snapshot.pendingEdits.size).toBe(0);
    expect(snapshot.newRow).toBeNull();
    expect(snapshot.editCell).toBeNull();
  });

  it("returns null newRow when newRow is null", () => {
    const snapshot = buildUndoRedoSnapshot(new Map(), null, null);
    expect(snapshot.newRow).toBeNull();
  });

  it("deep-clones pendingEdits so modifying the original does not affect the snapshot", () => {
    const pending = pendingEditsFrom([[0, { name: "original" }]]);
    const snapshot = buildUndoRedoSnapshot(pending, null, null);

    // Mutate original
    assertPresent(pending.get(0), "row 0").set("name", "mutated");

    expect(snapshot.pendingEdits.get(0)?.get("name")).toBe("original");
  });

  it("shallow-clones newRow: top-level keys independent, inner cell objects shared", () => {
    const row = draftRow("name", "original");
    const snapshot = buildUndoRedoSnapshot(new Map(), row, null);

    // Adding a key to the original does NOT appear in the snapshot (top-level independence)
    row.email = { value: "e@x.com" };
    expect(snapshot.newRow?.email).toBeUndefined();

    // But mutating an inner cell object IS shared (shallow clone, not deep)
    row.name.value = "mutated";
    expect(snapshot.newRow?.name.value).toBe("mutated");
  });

  it("clones each inner map independently for different rows", () => {
    const pending = pendingEditsFrom([
      [0, { a: 1 }],
      [1, { b: 2 }],
    ]);
    const snapshot = buildUndoRedoSnapshot(pending, null, null);

    assertPresent(pending.get(0), "row 0").set("a", 999);

    expect(snapshot.pendingEdits.get(0)?.get("a")).toBe(1);
    expect(snapshot.pendingEdits.get(1)?.get("b")).toBe(2);
  });
});

/* ------------------------------------------------------------------ */
/*  applyUndoRedoSnapshot                                              */
/* ------------------------------------------------------------------ */

describe("applyUndoRedoSnapshot", () => {
  it("returns the same values that were captured in the snapshot", () => {
    const pending = pendingEditsFrom([[0, { name: "Alice" }]]);
    const newRow = draftRow("name", "Bob");
    const editCell: EditTarget = { kind: "draft", col: "name" };

    const snapshot: MutationSnapshot = {
      pendingEdits: pending,
      newRow,
      editCell,
    };

    const result = applyUndoRedoSnapshot(snapshot);

    expect(result.pendingEdits.get(0)?.get("name")).toBe("Alice");
    expect(result.newRow?.name.value).toBe("Bob");
    expect(result.editCell).toEqual(editCell);
  });

  it("returns null newRow when snapshot.newRow is null", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: new Map(),
      newRow: null,
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    expect(result.newRow).toBeNull();
  });

  it("returns empty pendingEdits when snapshot has no edits", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: new Map(),
      newRow: null,
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    expect(result.pendingEdits.size).toBe(0);
  });

  it("deep-clones pendingEdits so modifying the returned map does not affect the snapshot", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: pendingEditsFrom([[0, { name: "original" }]]),
      newRow: null,
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    assertPresent(result.pendingEdits.get(0), "row 0").set("name", "mutated");

    expect(snapshot.pendingEdits.get(0)?.get("name")).toBe("original");
  });

  it("shallow-clones newRow: top-level keys independent, inner cell objects shared", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: new Map(),
      newRow: draftRow("name", "original"),
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    expect(result.newRow).not.toBeNull();

    // Adding a key to the result does NOT appear in the original
    result.newRow!.email = { value: "e@x.com" };
    expect(snapshot.newRow?.email).toBeUndefined();

    // But inner cell objects are shared references (shallow clone)
    result.newRow!.name.value = "mutated";
    expect(snapshot.newRow?.name.value).toBe("mutated");
  });

  it("passes through editCell by reference (shallow)", () => {
    const editCell: EditTarget = { kind: "persisted", rowIdx: 3, col: "email" };
    const snapshot: MutationSnapshot = {
      pendingEdits: new Map(),
      newRow: null,
      editCell,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    expect(result.editCell).toBe(editCell);
  });

  it("preserves all rows in pendingEdits", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: pendingEditsFrom([
        [0, { a: 1 }],
        [5, { b: 2 }],
        [99, { c: 3 }],
      ]),
      newRow: null,
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);

    expect(result.pendingEdits.size).toBe(3);
    expect(result.pendingEdits.get(0)?.get("a")).toBe(1);
    expect(result.pendingEdits.get(5)?.get("b")).toBe(2);
    expect(result.pendingEdits.get(99)?.get("c")).toBe(3);
  });
});

/* ------------------------------------------------------------------ */
/*  Round-trip: build -> apply preserves data                         */
/* ------------------------------------------------------------------ */

describe("snapshot round-trip", () => {
  it("building and applying a snapshot produces equivalent data", () => {
    const pending = pendingEditsFrom([
      [0, { name: "Alice", age: 30 }],
      [2, { name: "Bob" }],
    ]);
    const newRow = draftRow("name", "Charlie");
    const editCell: EditTarget = { kind: "persisted", rowIdx: 0, col: "name" };

    const snapshot = buildUndoRedoSnapshot(pending, newRow, editCell);
    const result = applyUndoRedoSnapshot(snapshot);

    expect(result.pendingEdits.size).toBe(2);
    expect(result.pendingEdits.get(0)?.get("name")).toBe("Alice");
    expect(result.pendingEdits.get(0)?.get("age")).toBe(30);
    expect(result.pendingEdits.get(2)?.get("name")).toBe("Bob");
    expect(result.newRow?.name.value).toBe("Charlie");
    expect(result.editCell).toEqual(editCell);
  });

  it("empty snapshot round-trips cleanly", () => {
    const snapshot = buildUndoRedoSnapshot(new Map(), null, null);
    const result = applyUndoRedoSnapshot(snapshot);

    expect(result.pendingEdits.size).toBe(0);
    expect(result.newRow).toBeNull();
    expect(result.editCell).toBeNull();
  });
});
