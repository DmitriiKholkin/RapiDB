import { describe, expect, it } from "vitest";
import {
  applyUndoRedoSnapshot,
  buildInsertValues,
  buildUndoRedoSnapshot,
  INSERT_DEFAULT_SENTINEL,
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

function pendingEditsFrom(
  entries: Array<[number, Record<string, unknown>]>,
): PendingEdits {
  return new Map(
    entries.map(([row, cols]) => [row, new Map(Object.entries(cols))]),
  );
}

function row(col: string, value: unknown): InsertDraftRow {
  return { [col]: { value } };
}

function rows(...cols: Array<[string, unknown]>): InsertDraftRow[] {
  return cols.map(([c, v]) => row(c, v));
}

/* ------------------------------------------------------------------ */
/*  buildUndoRedoSnapshot                                              */
/* ------------------------------------------------------------------ */

describe("buildUndoRedoSnapshot", () => {
  it("captures pendingEdits, newRows, and editCell", () => {
    const pending = pendingEditsFrom([[0, { name: "Alice" }]]);
    const snapshot = buildUndoRedoSnapshot(pending, rows(["name", "Bob"]), {
      kind: "persisted",
      rowIdx: 0,
      col: "name",
    });

    expect(snapshot.pendingEdits.get(0)?.get("name")).toBe("Alice");
    expect(snapshot.newRows[0]?.name.value).toBe("Bob");
    expect(snapshot.editCell).toEqual({
      kind: "persisted",
      rowIdx: 0,
      col: "name",
    });
  });

  it("returns empty pendingEdits and newRows when given empty inputs", () => {
    const snapshot = buildUndoRedoSnapshot(new Map(), [], null);

    expect(snapshot.pendingEdits.size).toBe(0);
    expect(snapshot.newRows).toEqual([]);
    expect(snapshot.editCell).toBeNull();
  });

  it("supports multiple draft rows in newRows", () => {
    const snapshot = buildUndoRedoSnapshot(
      new Map(),
      [row("name", "Alice"), row("name", "Bob"), row("name", "Charlie")],
      null,
    );

    expect(snapshot.newRows).toHaveLength(3);
    expect(snapshot.newRows[0].name.value).toBe("Alice");
    expect(snapshot.newRows[1].name.value).toBe("Bob");
    expect(snapshot.newRows[2].name.value).toBe("Charlie");
  });

  it("deep-clones pendingEdits so modifying the original does not affect the snapshot", () => {
    const pending = pendingEditsFrom([[0, { name: "original" }]]);
    const snapshot = buildUndoRedoSnapshot(pending, [], null);

    pending.get(0)?.set("name", "mutated");

    expect(snapshot.pendingEdits.get(0)?.get("name")).toBe("original");
  });

  it("shallow-clones each row: top-level keys independent, inner cell objects shared", () => {
    const r = rows(["name", "original"]);
    const snapshot = buildUndoRedoSnapshot(new Map(), r, null);

    // Adding a key to the original does NOT appear in the snapshot
    r[0].email = { value: "e@x.com" };
    expect(snapshot.newRows[0].email).toBeUndefined();

    // But mutating an inner cell object IS shared (shallow clone)
    r[0].name.value = "mutated";
    expect(snapshot.newRows[0].name.value).toBe("mutated");
  });

  it("clones each inner map independently for different rows", () => {
    const pending = pendingEditsFrom([
      [0, { a: 1 }],
      [1, { b: 2 }],
    ]);
    const snapshot = buildUndoRedoSnapshot(pending, [], null);

    pending.get(0)?.set("a", 999);

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
    const snapshot: MutationSnapshot = {
      pendingEdits: pending,
      newRows: rows(["name", "Bob"]),
      editCell: { kind: "draft", rowIdx: 0, col: "name" },
    };

    const result = applyUndoRedoSnapshot(snapshot);

    expect(result.pendingEdits.get(0)?.get("name")).toBe("Alice");
    expect(result.newRows[0]?.name.value).toBe("Bob");
    expect(result.editCell).toEqual({ kind: "draft", rowIdx: 0, col: "name" });
  });

  it("returns empty newRows when snapshot.newRows is empty", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: new Map(),
      newRows: [],
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    expect(result.newRows).toEqual([]);
  });

  it("restores multiple draft rows from snapshot", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: new Map(),
      newRows: [row("name", "Alice"), row("name", "Bob")],
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    expect(result.newRows).toHaveLength(2);
    expect(result.newRows[0].name.value).toBe("Alice");
    expect(result.newRows[1].name.value).toBe("Bob");
  });

  it("deep-clones pendingEdits so modifying the returned map does not affect the snapshot", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: pendingEditsFrom([[0, { name: "original" }]]),
      newRows: [],
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    result.pendingEdits.get(0)?.set("name", "mutated");

    expect(snapshot.pendingEdits.get(0)?.get("name")).toBe("original");
  });

  it("shallow-clones newRows: top-level keys independent, inner cell objects shared", () => {
    const snapshot: MutationSnapshot = {
      pendingEdits: new Map(),
      newRows: rows(["name", "original"]),
      editCell: null,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    expect(result.newRows).toHaveLength(1);

    const restoredRow = result.newRows[0];
    restoredRow.email = { value: "e@x.com" };
    expect(snapshot.newRows[0].email).toBeUndefined();

    // But inner cell objects are shared references (shallow clone)
    result.newRows[0].name.value = "mutated";
    expect(snapshot.newRows[0].name.value).toBe("mutated");
  });

  it("passes through editCell by reference (shallow)", () => {
    const editCell: EditTarget = { kind: "persisted", rowIdx: 3, col: "email" };
    const snapshot: MutationSnapshot = {
      pendingEdits: new Map(),
      newRows: [],
      editCell,
    };

    const result = applyUndoRedoSnapshot(snapshot);
    expect(result.editCell).toBe(editCell);
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
    const editCell: EditTarget = { kind: "persisted", rowIdx: 0, col: "name" };

    const snapshot = buildUndoRedoSnapshot(
      pending,
      rows(["name", "Charlie"]),
      editCell,
    );
    const result = applyUndoRedoSnapshot(snapshot);

    expect(result.pendingEdits.size).toBe(2);
    expect(result.pendingEdits.get(0)?.get("name")).toBe("Alice");
    expect(result.pendingEdits.get(0)?.get("age")).toBe(30);
    expect(result.pendingEdits.get(2)?.get("name")).toBe("Bob");
    expect(result.newRows[0].name.value).toBe("Charlie");
    expect(result.editCell).toEqual(editCell);
  });

  it("empty snapshot round-trips cleanly", () => {
    const snapshot = buildUndoRedoSnapshot(new Map(), [], null);
    const result = applyUndoRedoSnapshot(snapshot);

    expect(result.pendingEdits.size).toBe(0);
    expect(result.newRows).toEqual([]);
    expect(result.editCell).toBeNull();
  });

  it("multiple draft rows round-trip cleanly", () => {
    const snapshot = buildUndoRedoSnapshot(
      new Map(),
      [row("name", "Alice"), row("name", "Bob"), row("name", "Charlie")],
      { kind: "draft", rowIdx: 1, col: "name" },
    );
    const result = applyUndoRedoSnapshot(snapshot);

    expect(result.newRows).toHaveLength(3);
    expect(result.newRows[0].name.value).toBe("Alice");
    expect(result.newRows[1].name.value).toBe("Bob");
    expect(result.newRows[2].name.value).toBe("Charlie");
    expect(result.editCell).toEqual({ kind: "draft", rowIdx: 1, col: "name" });
  });
});

/* ------------------------------------------------------------------ */
/*  buildInsertValues                                                  */
/* ------------------------------------------------------------------ */

describe("buildInsertValues", () => {
  it("builds a values object from a draft row", () => {
    const draft = row("name", "Bob");
    expect(buildInsertValues(draft)).toEqual({ name: "Bob" });
  });

  it("omits columns with INSERT_DEFAULT_SENTINEL", () => {
    const draft: InsertDraftRow = {
      name: { value: "Bob" },
      age: { value: INSERT_DEFAULT_SENTINEL },
      active: { value: true },
    };
    expect(buildInsertValues(draft)).toEqual({ name: "Bob", active: true });
  });

  it("returns empty object when all values are sentinel", () => {
    const draft: InsertDraftRow = {
      name: { value: INSERT_DEFAULT_SENTINEL },
      age: { value: INSERT_DEFAULT_SENTINEL },
    };
    expect(buildInsertValues(draft)).toEqual({});
  });
});
