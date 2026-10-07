import { act, renderHook } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import type { ApplyResultPayload } from "../../src/shared/webviewContracts";
import { useTableMutationController } from "../../src/webview/components/table/useTableMutationController";
import { dispatchIncomingMessage, getPostedMessages } from "./testUtils";

it("B05: sends a DEFAULT-only draft as an empty insert and retains it on failure", () => {
  const idColumn: ColumnTypeMeta = {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    category: "integer",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
  const fetchPage = vi.fn();
  const { result } = renderHook(() =>
    useTableMutationController({
      canEditRows: true,
      loadingRef: { current: false },
      columnsRef: { current: [idColumn] },
      fetchPageRef: { current: fetchPage },
      pkColsRef: { current: ["id"] },
      preserveScrollPositionRef: { current: () => {} },
      rowsRef: { current: [] },
      mongoIdTypesRef: { current: [] },
      selected: new Set(),
    }),
  );
  act(() => result.current.startInsertRow());
  const drafts = result.current.newRows;
  act(() => result.current.applyChanges());
  const request = getPostedMessages().find(
    (message) => message.type === "applyChanges",
  );
  const operationId = (request?.payload as { operationId: string }).operationId;
  expect(request?.payload).toEqual({
    operationId,
    updates: [],
    insertValues: [{}],
  });

  act(() =>
    dispatchIncomingMessage("applyResult", {
      operationId,
      success: false,
      error: "Constraint failed",
    }),
  );
  expect(result.current.newRows).toEqual(drafts);
  expect(result.current.applying).toBe(false);
  expect(result.current.applyStatus?.tone).toBe("error");
  expect(fetchPage).not.toHaveBeenCalled();

  act(() => result.current.applyChanges());
  const retryOperationId = (
    getPostedMessages().at(-1)?.payload as { operationId: string }
  ).operationId;
  expect(getPostedMessages().at(-1)?.payload).toEqual({
    operationId: retryOperationId,
    updates: [],
    insertValues: [{}],
  });
  expect(retryOperationId).not.toBe(operationId);
  act(() =>
    dispatchIncomingMessage("applyResult", {
      operationId: retryOperationId,
      success: true,
      insertApplied: true,
    }),
  );
  expect(result.current.newRows).toEqual([]);
  expect(fetchPage).toHaveBeenCalledOnce();
});

it("M16 clears apply-busy state while retaining drafts for a correlated backend-unknown result", () => {
  const idColumn: ColumnTypeMeta = {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    category: "integer",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
  const fetchPage = vi.fn();
  const { result } = renderHook(() =>
    useTableMutationController({
      canEditRows: true,
      loadingRef: { current: false },
      columnsRef: { current: [idColumn] },
      fetchPageRef: { current: fetchPage },
      pkColsRef: { current: ["id"] },
      preserveScrollPositionRef: { current: () => {} },
      rowsRef: { current: [] },
      mongoIdTypesRef: { current: [] },
      selected: new Set(),
    }),
  );

  act(() => result.current.startInsertRow());
  const drafts = result.current.newRows;
  act(() => result.current.applyChanges());
  const request = getPostedMessages().at(-1);
  const operationId = (request?.payload as { operationId: string }).operationId;
  expect(request?.type).toBe("applyChanges");
  expect(drafts).toHaveLength(1);
  expect(result.current.applying).toBe(true);

  const backendError =
    "The INSERT write outcome is unknown after a backend timeout.";
  act(() =>
    dispatchIncomingMessage<ApplyResultPayload>("applyResult", {
      operationId,
      success: false,
      error: backendError,
      insertApplied: false,
      rowOutcomes: [],
      insertRowOutcomes: [{ rowIndex: 0, success: false, status: "unknown" }],
      changesPossible: true,
      outcomeUnknown: true,
    }),
  );

  expect(result.current.applying).toBe(false);
  expect(result.current.applyStatus?.tone).toBe("error");
  expect(result.current.applyStatus?.message).toContain(backendError);
  expect(result.current.applyStatus?.message).toContain(
    "The first refresh may precede a late write; refresh again and verify before retrying.",
  );
  expect(result.current.newRows).toEqual(drafts);
  expect(result.current.reconciliationRequired).toBe(true);
  expect(result.current.getMetadataRefreshState()).toMatchObject({
    busy: false,
    reconciliationPending: true,
  });
  expect(fetchPage).toHaveBeenCalledOnce();
});

it("M16 accepts a resent known backend result without unknown-write recovery", () => {
  const idColumn: ColumnTypeMeta = {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    category: "integer",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
  const fetchPage = vi.fn();
  const { result } = renderHook(() =>
    useTableMutationController({
      canEditRows: true,
      loadingRef: { current: false },
      columnsRef: { current: [idColumn] },
      fetchPageRef: { current: fetchPage },
      pkColsRef: { current: ["id"] },
      preserveScrollPositionRef: { current: () => {} },
      rowsRef: { current: [] },
      mongoIdTypesRef: { current: [] },
      selected: new Set(),
    }),
  );
  act(() => result.current.startInsertRow());
  act(() => result.current.applyChanges());
  const request = getPostedMessages().at(-1);
  const operationId = (request?.payload as { operationId: string }).operationId;
  expect(result.current.applying).toBe(true);

  // R5 resends the original execution evidence, not a transport-derived unknown.
  const knownResult: ApplyResultPayload = {
    operationId,
    success: true,
    insertApplied: true,
    rowOutcomes: [],
    insertRowOutcomes: [{ rowIndex: 0, success: true, status: "applied" }],
    changesPossible: true,
    outcomeUnknown: false,
  };
  act(() => dispatchIncomingMessage("applyResult", knownResult));
  expect(result.current.applying).toBe(false);
  expect(result.current.newRows).toEqual([]);
  expect(result.current.applyStatus).toBeNull();
  expect(result.current.reconciliationRequired).toBe(false);
  expect(result.current.retryReconciliation()).toBe(false);
  expect(result.current.getMetadataRefreshState()).toMatchObject({
    busy: false,
    reconciliationPending: false,
  });
  expect(fetchPage).toHaveBeenCalledOnce();

  act(() => dispatchIncomingMessage("applyResult", knownResult));
  expect(result.current.applying).toBe(false);
  expect(result.current.applyStatus).toBeNull();
  expect(result.current.newRows).toEqual([]);
  expect(fetchPage).toHaveBeenCalledOnce();
});

it("M16 ignores a delayed mutation reply from an earlier controller epoch", () => {
  const idColumn: ColumnTypeMeta = {
    name: "id",
    type: "INTEGER",
    nativeType: "INTEGER",
    category: "integer",
    nullable: false,
    isPrimaryKey: true,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
  const params = {
    canEditRows: true,
    loadingRef: { current: false },
    columnsRef: { current: [idColumn] },
    fetchPageRef: { current: vi.fn() },
    pkColsRef: { current: ["id"] },
    preserveScrollPositionRef: { current: () => {} },
    rowsRef: { current: [] },
    mongoIdTypesRef: { current: [] },
    selected: new Set<number>(),
  };
  const previous = renderHook(() => useTableMutationController(params));
  act(() => previous.result.current.startInsertRow());
  act(() => previous.result.current.applyChanges());
  const previousOperationId = (
    getPostedMessages().at(-1)?.payload as { operationId: string }
  ).operationId;
  previous.unmount();

  const current = renderHook(() => useTableMutationController(params));
  act(() => current.result.current.startInsertRow());
  act(() => current.result.current.applyChanges());
  const currentOperationId = (
    getPostedMessages().at(-1)?.payload as { operationId: string }
  ).operationId;
  expect(currentOperationId).not.toBe(previousOperationId);

  act(() =>
    dispatchIncomingMessage("applyResult", {
      operationId: previousOperationId,
      success: false,
      error: "Stale result",
    }),
  );
  expect(current.result.current.applying).toBe(true);

  act(() =>
    dispatchIncomingMessage("applyResult", {
      operationId: currentOperationId,
      success: false,
      error: "Current result",
    }),
  );
  expect(current.result.current.applying).toBe(false);
  expect(current.result.current.applyStatus?.message).toBe("Current result");
});
