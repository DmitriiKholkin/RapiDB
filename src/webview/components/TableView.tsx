import {
  type CellContext,
  flexRender,
  getCoreRowModel,
  type ColumnDef as TanColumnDef,
  useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import React, {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  type FilterDraft,
  type FilterDraftMap,
  isNumericCategory,
  NULL_SENTINEL,
  serializeFilterDrafts,
} from "../../shared/tableTypes";
import type {
  ApplyResultPayload,
  TableMutationPreviewPayload,
} from "../../shared/webviewContracts";
import type { PendingEdits, Row } from "../types";
import { type Column, calcColWidths } from "../utils/columnSizing";
import { onMessage, postMessage } from "../utils/messaging";
import { Icon } from "./Icon";
import { MonacoEditor } from "./MonacoEditor";
import { CellDisplay } from "./table/CellDisplay";
import { ColumnFilterControl } from "./table/ColumnFilterControl";
import { EditInput, valueToEditString } from "./table/EditInput";
import { NewRowForm } from "./table/NewRowForm";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  isView?: boolean;
  defaultPageSize?: number;
}

const PAGE_SIZES = [25, 100, 500, 1000];
const DEBOUNCE = 400;
const ROW_H = 26;
const HEADER_H = 28;
const FILTER_H = 30;
const TOOLBAR_H = 36;
const PREVIEW_DIALOG_EDITOR_H = "min(42vh, 360px)";
const SR_ONLY_STYLE: React.CSSProperties = {
  position: "absolute",
  width: 1,
  height: 1,
  padding: 0,
  margin: -1,
  overflow: "hidden",
  clip: "rect(0, 0, 0, 0)",
  whiteSpace: "nowrap",
  border: 0,
};

const TABLE_ROW_STYLE_ID = "rapidb-table-row-style";
if (
  typeof document !== "undefined" &&
  !document.getElementById(TABLE_ROW_STYLE_ID)
) {
  const s = document.createElement("style");
  s.id = TABLE_ROW_STYLE_ID;
  s.textContent = [
    `.rdb-trow { transition: background 60ms; }`,
    `.rdb-trow[data-even="true"]  { background: var(--vscode-editor-background); }`,
    `.rdb-trow[data-even="false"] { background: var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04)); }`,

    `.rdb-trow:not([data-selected="true"]):hover { background: var(--vscode-list-hoverBackground); }`,
  ].join("\n");
  document.head.appendChild(s);
}

const btn = (
  v: "primary" | "ghost" | "danger" | "warning" = "ghost",
  disabled = false,
): React.CSSProperties => ({
  padding: "3px 10px",
  fontSize: 12,
  borderRadius: 2,
  cursor: disabled ? "default" : "pointer",
  fontFamily: "inherit",
  opacity: disabled ? 0.45 : 1,
  whiteSpace: "nowrap",
  ...(v === "primary"
    ? {
        background: "var(--vscode-button-background)",
        color: "var(--vscode-button-foreground)",
        border: "none",
      }
    : v === "danger"
      ? {
          background:
            "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.2))",
          color: "var(--vscode-errorForeground)",
          border:
            "1px solid var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.4))",
        }
      : v === "warning"
        ? {
            background: "rgba(200,150,0,0.15)",
            color: "var(--vscode-editorWarning-foreground, #cca700)",
            border: "1px solid rgba(200,150,0,0.4)",
          }
        : {
            background: "transparent",
            color: "var(--vscode-foreground)",
            border: "1px solid var(--vscode-panel-border)",
          }),
});

function canEditColumn(column?: ColumnMeta): column is ColumnMeta {
  return !!column;
}

function clonePendingEdits(pendingEdits: PendingEdits): PendingEdits {
  return new Map(
    [...pendingEdits.entries()].map(([rowIdx, columnMap]) => [
      rowIdx,
      new Map(columnMap),
    ]),
  );
}

type PendingRestoreState = Map<string, Map<string, unknown>>;

function stablePrimaryKeyPart(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => stablePrimaryKeyPart(item));
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entryValue]) => [key, stablePrimaryKeyPart(entryValue)]),
    );
  }

  return value;
}

function rowPrimaryKeySignature(
  row: Row | undefined,
  primaryKeyColumns: readonly string[],
): string | null {
  if (!row || primaryKeyColumns.length === 0) {
    return null;
  }

  const keyEntries: Array<[string, unknown]> = [];
  for (const columnName of primaryKeyColumns) {
    if (!(columnName in row)) {
      return null;
    }

    keyEntries.push([columnName, stablePrimaryKeyPart(row[columnName])]);
  }

  return JSON.stringify(keyEntries);
}

function buildPendingRestoreState(
  pendingEdits: PendingEdits,
  rows: readonly Row[],
  primaryKeyColumns: readonly string[],
): PendingRestoreState {
  const restoreState: PendingRestoreState = new Map();

  for (const [rowIdx, columnMap] of pendingEdits.entries()) {
    const signature = rowPrimaryKeySignature(rows[rowIdx], primaryKeyColumns);
    if (!signature) {
      continue;
    }

    restoreState.set(signature, new Map(columnMap));
  }

  return restoreState;
}

function restorePendingEdits(
  restoreState: PendingRestoreState | null,
  rows: readonly Row[],
  primaryKeyColumns: readonly string[],
): PendingEdits {
  if (!restoreState || restoreState.size === 0) {
    return new Map();
  }

  const restored: PendingEdits = new Map();

  rows.forEach((row, rowIdx) => {
    const signature = rowPrimaryKeySignature(row, primaryKeyColumns);
    if (!signature) {
      return;
    }

    const columnMap = restoreState.get(signature);
    if (columnMap) {
      restored.set(rowIdx, new Map(columnMap));
    }
  });

  return restored;
}

function getRetainedPendingEdits(
  pendingEdits: PendingEdits,
  updateRowIndexes: readonly number[],
  rowOutcomes?: ApplyResultPayload["rowOutcomes"],
  failedRows?: readonly number[],
): PendingEdits {
  const retainedUpdateIndexes = new Set<number>();

  if (rowOutcomes && rowOutcomes.length > 0) {
    for (const outcome of rowOutcomes) {
      if (outcome.status !== "applied" && outcome.status !== "skipped") {
        retainedUpdateIndexes.add(outcome.rowIndex);
      }
    }
  } else if (failedRows && failedRows.length > 0) {
    for (const rowIndex of failedRows) {
      retainedUpdateIndexes.add(rowIndex);
    }
  }

  if (retainedUpdateIndexes.size === 0) {
    return new Map();
  }

  const nextPending: PendingEdits = new Map();
  for (const updateIndex of retainedUpdateIndexes) {
    const rowIdx = updateRowIndexes[updateIndex];
    if (rowIdx === undefined) {
      continue;
    }

    const rowPending = pendingEdits.get(rowIdx);
    if (rowPending) {
      nextPending.set(rowIdx, new Map(rowPending));
    }
  }

  return nextPending;
}

const DIALOG_FOCUSABLE_SELECTOR =
  'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

function getFocusableElements(dialog: HTMLDivElement | null): HTMLElement[] {
  if (!dialog) {
    return [];
  }

  return Array.from(
    dialog.querySelectorAll<HTMLElement>(DIALOG_FOCUSABLE_SELECTOR),
  );
}

function useDialogFocusTrap(options: {
  dialogRef: React.RefObject<HTMLDivElement | null>;
  initialFocusRef?: React.RefObject<HTMLElement | null>;
  onEscape?: () => void;
}) {
  const { dialogRef, initialFocusRef, onEscape } = options;

  useEffect(() => {
    const previousActiveElement =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;

    initialFocusRef?.current?.focus();

    return () => {
      previousActiveElement?.focus();
    };
  }, [initialFocusRef]);

  return useCallback(
    (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (event.key === "Escape") {
        onEscape?.();
        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const dialog = dialogRef.current;
      const focusableElements = getFocusableElements(dialog);

      if (focusableElements.length === 0) {
        event.preventDefault();
        dialog?.focus();
        return;
      }

      const firstElement = focusableElements[0];
      const lastElement = focusableElements[focusableElements.length - 1];
      const activeElement =
        document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;

      if (!activeElement || !dialog?.contains(activeElement)) {
        event.preventDefault();
        firstElement.focus();
        return;
      }

      if (event.shiftKey && activeElement === firstElement) {
        event.preventDefault();
        lastElement.focus();
        return;
      }

      if (!event.shiftKey && activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    },
    [dialogRef, onEscape],
  );
}

export function TableView({ table, isView = false, defaultPageSize }: Props) {
  const validSizes = PAGE_SIZES as readonly number[];
  const initialPageSize =
    defaultPageSize !== undefined && validSizes.includes(defaultPageSize)
      ? defaultPageSize
      : 25;

  const [columns, setColumns] = useState<ColumnMeta[]>([]);
  const [pkCols, setPkCols] = useState<string[]>([]);
  const [rows, setRows] = useState<Row[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filterError, setFilterError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(initialPageSize);
  const [filterDrafts, setFilterDrafts] = useState<FilterDraftMap>({});
  const [debouncedFilterDrafts, setDebouncedFilterDrafts] =
    useState<FilterDraftMap>({});
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [pendingEdits, setPending] = useState<PendingEdits>(new Map());
  const [editCell, setEditCell] = useState<{
    rowIdx: number;
    col: string;
  } | null>(null);
  const [applying, setApplying] = useState(false);
  const [applyStatus, setApplyStatus] = useState<{
    tone: "error" | "warning";
    message: string;
  } | null>(null);
  const [newRow, setNewRow] = useState<Row | null>(null);
  const [inserting, setInserting] = useState(false);
  const [mutErr, setMutErr] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [sort, setSort] = useState<{
    column: string;
    direction: "asc" | "desc";
  } | null>(null);
  const [mutationPreview, setMutationPreview] =
    useState<TableMutationPreviewPayload | null>(null);
  const [exportChoice, setExportChoice] = useState<{
    format: "csv" | "json";
    sort: { column: string; direction: "asc" | "desc" } | null;
    filters: unknown[];
  } | null>(null);

  const [readOnlyTable, setReadOnlyTable] = useState(isView);

  const [colSizes, setColSizes] = useState<Record<string, number>>({});

  const colSizesInitedRef = useRef(false);

  const columnsRef = useRef<ColumnMeta[]>([]);
  const applyPendingSnapshotRef = useRef<PendingEdits>(new Map());
  const applyRowIndexesRef = useRef<number[]>([]);
  const pendingRestoreRef = useRef<PendingRestoreState | null>(null);

  const scrollRef = useRef<HTMLDivElement>(null);
  const scrollPreserveRef = useRef<number | null>(null);
  const sortRef = useRef(sort);

  const selectedRef = useRef(selected);
  const rowsRef = useRef(rows);
  const pkColsRef = useRef(pkCols);

  const editCellRef = useRef(editCell);
  const pendingEditsRef = useRef(pendingEdits);
  editCellRef.current = editCell;
  pendingEditsRef.current = pendingEdits;

  selectedRef.current = selected;
  sortRef.current = sort;
  rowsRef.current = rows;
  pkColsRef.current = pkCols;

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const pendingCount = pendingEdits.size;
  const hasPrimaryKey = pkCols.length > 0;
  const canEditRows = !readOnlyTable && hasPrimaryKey;
  const canSelectAndDeleteRows = !readOnlyTable && hasPrimaryKey;

  const initializedRef = useRef(false);
  const fetchEpochRef = useRef(0);
  const [initTick, setInitTick] = useState(0);
  const showMissingPrimaryKeyNotice =
    !readOnlyTable && initializedRef.current && !hasPrimaryKey;

  const pageRef = useRef(page);
  const pageSizeRef = useRef(pageSize);
  const debouncedFilterDraftsRef = useRef(debouncedFilterDrafts);
  pageRef.current = page;
  pageSizeRef.current = pageSize;
  debouncedFilterDraftsRef.current = debouncedFilterDrafts;

  const fetchTrigger = useMemo(
    () =>
      JSON.stringify({
        initTick,
        page,
        pageSize,
        filters: debouncedFilterDrafts,
        sortColumn: sort?.column ?? null,
        sortDirection: sort?.direction ?? null,
      }),
    [debouncedFilterDrafts, initTick, page, pageSize, sort],
  );

  const fetchPage = useCallback(() => {
    if (!initializedRef.current) return;
    const epoch = ++fetchEpochRef.current;
    setLoading(true);
    const activeFilters = serializeFilterDrafts(
      columnsRef.current,
      debouncedFilterDraftsRef.current,
    );
    postMessage("fetchPage", {
      fetchId: epoch,
      page: pageRef.current,
      pageSize: pageSizeRef.current,
      filters: activeFilters,
      sort: sortRef.current,
    });
  }, []);

  useEffect(() => {
    setReadOnlyTable(isView);
  }, [isView]);

  useEffect(() => {
    const unInit = onMessage<{
      columns: ColumnMeta[];
      primaryKeyColumns: string[];
    }>("tableInit", ({ columns: cols, primaryKeyColumns }) => {
      columnsRef.current = cols;

      initializedRef.current = true;
      setColumns(cols);
      setPkCols(primaryKeyColumns);
      setReadOnlyTable(isView);

      setInitTick((t) => t + 1);
    });
    const unData = onMessage<{
      fetchId?: number;
      rows: Row[];
      totalCount: number;
    }>("tableData", ({ fetchId, rows: r, totalCount: t }) => {
      if (fetchId !== undefined && fetchId !== fetchEpochRef.current) return;
      if (!colSizesInitedRef.current && columnsRef.current.length > 0) {
        colSizesInitedRef.current = true;
        setColSizes(
          calcColWidths(
            columnsRef.current.map((c): Column => {
              return {
                name: c.name,
                isPrimaryKey: c.isPrimaryKey,
              };
            }),
            r,
          ),
        );
      }
      setRows(r);
      setTotalCount(t);
      setLoading(false);
      setFilterError(null);
      setSelected(new Set());
      const restoredPending = restorePendingEdits(
        pendingRestoreRef.current,
        r,
        pkColsRef.current,
      );
      pendingRestoreRef.current = null;
      setPending(restoredPending);
      setEditCell(null);

      const savedScroll = scrollPreserveRef.current;
      scrollPreserveRef.current = null;
      if (savedScroll !== null && savedScroll > 0) {
        requestAnimationFrame(() => {
          scrollRef.current?.scrollTo?.({ top: savedScroll });
        });
      } else {
        scrollRef.current?.scrollTo?.({ top: 0 });
      }
    });
    const unError = onMessage<{
      fetchId?: number;
      error: string;
      isFilterError?: boolean;
    }>("tableError", ({ fetchId, error: e, isFilterError }) => {
      if (fetchId !== undefined && fetchId !== fetchEpochRef.current) return;
      if (isFilterError) {
        setFilterError(e);
        setRows([]);
        setTotalCount(0);
      } else {
        setError(e);
      }
      setLoading(false);
    });

    const unApply = onMessage<ApplyResultPayload>(
      "applyResult",
      ({ success, error: e, warning, failedRows, rowOutcomes }) => {
        setApplying(false);
        if (success) {
          const nextPending = getRetainedPendingEdits(
            applyPendingSnapshotRef.current,
            applyRowIndexesRef.current,
            rowOutcomes,
            failedRows,
          );
          setPending(nextPending);
          setApplyStatus(
            warning
              ? {
                  tone: "warning",
                  message: warning,
                }
              : null,
          );
          const restoreState = buildPendingRestoreState(
            nextPending,
            rowsRef.current,
            pkColsRef.current,
          );
          pendingRestoreRef.current =
            restoreState.size > 0 ? restoreState : null;
          scrollPreserveRef.current = scrollRef.current?.scrollTop ?? null;
          fetchPage();
        } else {
          pendingRestoreRef.current = null;
          setApplyStatus({
            tone: "error",
            message: e ?? "Apply failed — all changes were rolled back",
          });
        }
        applyPendingSnapshotRef.current = new Map();
        applyRowIndexesRef.current = [];
      },
    );

    const unInsert = onMessage<{ success: boolean; error?: string }>(
      "insertResult",
      ({ success, error: e }) => {
        setInserting(false);
        if (success) {
          setNewRow(null);
          setMutErr(null);
          fetchPage();
        } else {
          setMutErr(e ?? "Insert failed");
        }
      },
    );

    const unDelete = onMessage<{ success: boolean; error?: string }>(
      "deleteResult",
      ({ success, error: e }) => {
        setDeleting(false);
        if (success) {
          setMutErr(null);
          fetchPage();
        } else {
          setMutErr(e ?? "Delete failed");
        }
      },
    );

    const unConfirm = onMessage<{ confirmed: boolean }>(
      "deleteConfirmed",
      ({ confirmed }) => {
        if (!confirmed) {
          return;
        }
        const toDelete = [...selectedRef.current].map((idx) => {
          const row = rowsRef.current[idx];
          return Object.fromEntries(pkColsRef.current.map((k) => [k, row[k]]));
        });
        setDeleting(true);
        postMessage("deleteRows", { primaryKeysList: toDelete });
      },
    );
    const unMutationPreview = onMessage<TableMutationPreviewPayload>(
      "tableMutationPreview",
      (payload) => {
        setMutationPreview(payload);
      },
    );

    postMessage("ready");
    return () => {
      unInit();
      unData();
      unError();
      unApply();
      unInsert();
      unDelete();
      unConfirm();
      unMutationPreview();
    };
  }, [fetchPage, isView]);

  useEffect(() => {
    if (!initializedRef.current || fetchTrigger === "") return;
    fetchPage();
  }, [fetchPage, fetchTrigger]);

  const filtersMountedRef = useRef(false);
  useEffect(() => {
    if (!filtersMountedRef.current) {
      filtersMountedRef.current = true;
      return;
    }
    const t = setTimeout(() => {
      setFilterError(null);
      setPage(1);
      setDebouncedFilterDrafts(filterDrafts);
    }, DEBOUNCE);
    return () => clearTimeout(t);
  }, [filterDrafts]);

  const handleSort = useCallback((column: string) => {
    setPage(1);
    setSort((prev) => {
      if (prev?.column === column) {
        if (prev.direction === "asc") {
          return { column, direction: "desc" as const };
        }
        return null;
      }
      return { column, direction: "asc" as const };
    });
  }, []);

  const updateFilterDraft = useCallback(
    (columnName: string, nextDraft: FilterDraft | undefined) => {
      setFilterDrafts((current) => {
        if (!nextDraft) {
          if (current[columnName] === undefined) {
            return current;
          }

          const next = { ...current };
          delete next[columnName];
          return next;
        }

        return {
          ...current,
          [columnName]: nextDraft,
        };
      });
    },
    [],
  );

  const applyChanges = useCallback(() => {
    if (pendingEdits.size === 0 || applying) {
      return;
    }
    setApplying(true);
    setApplyStatus(null);
    applyPendingSnapshotRef.current = clonePendingEdits(pendingEdits);
    applyRowIndexesRef.current = [...pendingEdits.keys()];
    const updates = [...pendingEdits.entries()].map(([rowIdx, colMap]) => ({
      primaryKeys: Object.fromEntries(
        pkColsRef.current.map((k) => [k, rowsRef.current[rowIdx][k]]),
      ),
      changes: Object.fromEntries(colMap),
    }));
    postMessage("applyChanges", { updates });
  }, [pendingEdits, applying]);

  const revertChanges = useCallback(() => {
    pendingRestoreRef.current = null;
    setPending(new Map());
    setEditCell(null);
    setApplyStatus(null);
  }, []);

  const commitCellEdit = useCallback(
    (
      rowIdx: number,
      column: ColumnMeta,
      newVal: string,
      originalVal: unknown,
    ) => {
      setEditCell(null);

      const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;

      const origStr = valueToEditString(originalVal);

      if (newVal === origStr) {
        setPending((prev) => {
          const rowMap = prev.get(rowIdx);
          if (!rowMap?.has(column.name)) {
            return prev;
          }
          const next = new Map(prev);
          const newRow = new Map(rowMap);
          newRow.delete(column.name);
          if (newRow.size === 0) {
            next.delete(rowIdx);
          } else {
            next.set(rowIdx, newRow);
          }
          return next;
        });
        return;
      }
      setPending((prev) => {
        const next = new Map(prev);
        const row = new Map(next.get(rowIdx) ?? []);
        row.set(column.name, coerced);
        next.set(rowIdx, row);
        return next;
      });
    },
    [],
  );

  const handleStartEdit = useCallback((rowIdx: number, col: ColumnMeta) => {
    if (!canEditColumn(col)) {
      return;
    }
    setEditCell({ rowIdx, col: col.name });
    setApplyStatus(null);
  }, []);

  const deleteSelected = useCallback(() => {
    if (
      selectedRef.current.size === 0 ||
      pkColsRef.current.length === 0 ||
      deleting
    ) {
      return;
    }
    postMessage("confirmDelete", { count: selectedRef.current.size });
  }, [deleting]);

  const commitNewRow = useCallback(() => {
    if (!newRow || inserting) {
      return;
    }
    setInserting(true);
    setMutErr(null);
    postMessage("insertRow", { values: newRow });
  }, [newRow, inserting]);

  const clearApplyRequestState = useCallback(() => {
    setApplying(false);
    applyPendingSnapshotRef.current = new Map();
    applyRowIndexesRef.current = [];
  }, []);

  const cancelMutationPreview = useCallback(() => {
    if (!mutationPreview) {
      return;
    }

    const { kind, previewToken } = mutationPreview;
    setMutationPreview(null);

    if (kind === "applyChanges") {
      clearApplyRequestState();
    } else {
      setInserting(false);
    }

    postMessage("cancelMutationPreview", { previewToken });
  }, [clearApplyRequestState, mutationPreview]);

  const confirmMutationPreview = useCallback(() => {
    if (!mutationPreview) {
      return;
    }

    const { previewToken } = mutationPreview;
    setMutationPreview(null);
    postMessage("confirmMutationPreview", { previewToken });
  }, [mutationPreview]);

  useEffect(() => {
    if (!mutationPreview) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") {
        return;
      }

      event.preventDefault();
      cancelMutationPreview();
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => {
      window.removeEventListener("keydown", handleKeyDown, true);
    };
  }, [cancelMutationPreview, mutationPreview]);

  const columnsMap = useMemo(
    () => new Map(columns.map((c) => [c.name, c])),
    [columns],
  );

  const tanColumns = useMemo<TanColumnDef<Row>[]>(
    () => [
      ...(canSelectAndDeleteRows
        ? [
            {
              id: "__sel",
              size: 36,
              header: () => (
                <input
                  type="checkbox"
                  aria-label="Select all rows"
                  checked={
                    rowsRef.current.length > 0 &&
                    selectedRef.current.size === rowsRef.current.length
                  }
                  ref={(el) => {
                    if (el)
                      el.indeterminate =
                        selectedRef.current.size > 0 &&
                        selectedRef.current.size < rowsRef.current.length;
                  }}
                  onChange={(e) =>
                    setSelected(
                      e.target.checked
                        ? new Set(rowsRef.current.map((_, i) => i))
                        : new Set(),
                    )
                  }
                  style={{
                    cursor: "pointer",
                    accentColor: "var(--vscode-button-background)",
                    margin: 0,
                  }}
                />
              ),
              cell: ({ row }: CellContext<Row, unknown>) => (
                <input
                  type="checkbox"
                  aria-label={`Select row ${row.index + 1}`}
                  checked={selectedRef.current.has(row.index)}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                    const next = new Set(selectedRef.current);
                    e.target.checked
                      ? next.add(row.index)
                      : next.delete(row.index);
                    setSelected(next);
                  }}
                  style={{
                    cursor: "pointer",
                    accentColor: "var(--vscode-button-background)",
                    margin: 0,
                  }}
                />
              ),
            } as TanColumnDef<Row>,
          ]
        : []),
      ...columns.map(
        (col): TanColumnDef<Row> => ({
          id: col.name,
          accessorKey: col.name,

          size: colSizes[col.name] ?? colSizes[`${col.name}key`] ?? 160,
          minSize: 40,
          maxSize: 800,
          header: () => (
            <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
              {col.name}
              {col.isPrimaryKey && (
                <Icon
                  name="key"
                  size={13}
                  color="var(--vscode-charts-yellow, #cca700)"
                  title="Primary Key"
                  style={{ marginLeft: 2 }}
                />
              )}
            </span>
          ),
          cell: ({ row, getValue }) => {
            const rowIdx = row.index;

            const ec = editCellRef.current;
            const pe = pendingEditsRef.current;
            const isEditing = ec?.rowIdx === rowIdx && ec.col === col.name;
            const pendingRow = pe.get(rowIdx);
            const hasPending = pendingRow?.has(col.name) ?? false;
            const pendingValue = pendingRow?.get(col.name);
            const displayVal = hasPending ? pendingValue : getValue();

            if (isEditing) {
              const startVal = hasPending ? pendingValue : getValue();
              const startStr = valueToEditString(startVal);
              return (
                <EditInput
                  initial={startStr}
                  nullable={col.nullable}
                  category={col.category}
                  readOnly={!canEditRows}
                  onCommit={(v) => commitCellEdit(rowIdx, col, v, getValue())}
                  onCancel={() => setEditCell(null)}
                />
              );
            }
            return (
              <CellDisplay
                value={displayVal}
                isPending={hasPending}
                category={col.category}
              />
            );
          },
        }),
      ),
    ],

    [canEditRows, canSelectAndDeleteRows, columns, colSizes, commitCellEdit],
  );

  const tanTable = useReactTable({
    data: rows,
    columns: tanColumns,
    getCoreRowModel: getCoreRowModel(),
    columnResizeMode: "onChange",
    enableColumnResizing: true,
  });
  const { rows: tableRows } = tanTable.getRowModel();
  const virt = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_H,
    overscan: 15,
  });
  const virtItems = virt.getVirtualItems();
  const totalVirtH = virt.getTotalSize();

  if (error) {
    return (
      <div
        style={{
          margin: 12,
          padding: "10px 14px",
          borderRadius: 3,
          fontSize: 13,
          background: "var(--vscode-inputValidation-errorBackground)",
          border: "1px solid var(--vscode-inputValidation-errorBorder)",
          color: "var(--vscode-errorForeground)",
        }}
      >
        <strong>Error:</strong> {error}
      </div>
    );
  }

  const busy = loading || applying || deleting || inserting;

  return (
    <main
      aria-label={`Table data for ${table}`}
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100vh",
        overflow: "hidden",
        position: "relative",
      }}
    >
      {filterError && (
        <div
          style={{
            flexShrink: 0,
            padding: "6px 12px",
            fontSize: 12,
            background:
              "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
            borderBottom:
              "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
            color: "var(--vscode-editorWarning-foreground, #CCA700)",
            display: "flex",
            alignItems: "center",
            gap: 8,
          }}
        >
          <span style={{ fontWeight: 600 }}>⚠ Filter:</span>
          <span style={{ flex: 1 }}>{filterError}</span>
          <button
            type="button"
            style={{
              background: "none",
              border: "none",
              cursor: "pointer",
              color: "inherit",
              opacity: 0.7,
              fontSize: 14,
              lineHeight: 1,
              padding: "0 2px",
            }}
            title="Dismiss"
            onClick={() => setFilterError(null)}
          >
            ×
          </button>
        </div>
      )}
      {showMissingPrimaryKeyNotice && (
        <div
          role="alert"
          style={{
            flexShrink: 0,
            padding: "6px 12px",
            fontSize: 12,
            background:
              "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
            borderBottom:
              "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
            color: "var(--vscode-editorWarning-foreground, #CCA700)",
            display: "flex",
            alignItems: "center",
            gap: 8,
          }}
        >
          <Icon name="warning" size={13} style={{ flexShrink: 0 }} />
          <span>
            Reduced table mode: no unique key was detected for row binding, so
            editing fields and deleting rows are disabled.
          </span>
        </div>
      )}
      {}
      <div
        style={{
          height: TOOLBAR_H,
          flexShrink: 0,
          display: "flex",
          alignItems: "center",
          gap: 6,
          padding: "0 10px",
          borderBottom: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editorGroupHeader-tabsBackground)",
        }}
      >
        {!readOnlyTable && (
          <>
            <button
              type="button"
              style={btn("primary", busy || !!newRow)}
              disabled={busy || !!newRow}
              onClick={() => {
                setNewRow({});
                setMutErr(null);
              }}
            >
              <>
                <Icon name="add" size={13} style={{ marginRight: 4 }} />
                Add Row
              </>
            </button>
            {canSelectAndDeleteRows && (
              <button
                type="button"
                style={btn("danger", selected.size === 0 || busy)}
                disabled={selected.size === 0 || busy}
                onClick={deleteSelected}
              >
                <>
                  <Icon name="trash" size={13} style={{ marginRight: 4 }} />
                  {deleting ? "Deleting…" : `Delete (${selected.size})`}
                </>
              </button>
            )}
          </>
        )}

        <button
          type="button"
          style={btn("ghost", busy)}
          disabled={busy}
          onClick={fetchPage}
        >
          <Icon name="refresh" size={13} style={{ marginRight: 4 }} />
          Refresh
        </button>
        <button
          type="button"
          style={btn("ghost")}
          onClick={() => {
            const activeFilters = serializeFilterDrafts(
              columns,
              debouncedFilterDrafts,
            );
            if (totalCount > rows.length) {
              setExportChoice({ format: "csv", sort, filters: activeFilters });
            } else {
              postMessage("exportCSV", { sort, filters: activeFilters });
            }
          }}
        >
          <Icon name="export" size={13} style={{ marginRight: 4 }} />
          Export CSV
        </button>
        <button
          type="button"
          style={btn("ghost")}
          onClick={() => {
            const activeFilters = serializeFilterDrafts(
              columns,
              debouncedFilterDrafts,
            );
            if (totalCount > rows.length) {
              setExportChoice({ format: "json", sort, filters: activeFilters });
            } else {
              postMessage("exportJSON", { sort, filters: activeFilters });
            }
          }}
        >
          <Icon name="export" size={13} style={{ marginRight: 4 }} />
          Export JSON
        </button>
        <div style={{ flex: 1 }} />
        <span style={{ fontSize: 11, opacity: 0.5 }}>
          {loading ? "Loading…" : `${totalCount.toLocaleString()} rows total`}
        </span>
      </div>

      {}
      {!readOnlyTable && (pendingCount > 0 || applyStatus) && (
        <div
          style={{
            flexShrink: 0,
            padding: "0 12px",
            minHeight: 36,
            display: "flex",
            alignItems: "center",
            gap: 8,
            background:
              applyStatus?.tone === "error"
                ? "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.1))"
                : "rgba(200,150,0,0.08)",
            borderBottom: `1px solid ${
              applyStatus?.tone === "error"
                ? "var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.4))"
                : "rgba(200,150,0,0.3)"
            }`,
          }}
        >
          {pendingCount > 0 && applyStatus?.tone !== "error" && (
            <span
              style={{
                fontSize: 12,
                color: "var(--vscode-editorWarning-foreground, #cca700)",
              }}
            >
              <Icon name="edit" size={12} style={{ marginRight: 4 }} />
              {pendingCount} row{pendingCount !== 1 ? "s" : ""} with unsaved
              changes
            </span>
          )}
          {applyStatus && (
            <span
              style={{
                fontSize: 12,
                color:
                  applyStatus.tone === "error"
                    ? "var(--vscode-errorForeground)"
                    : "var(--vscode-editorWarning-foreground, #cca700)",
                flex: 1,
              }}
            >
              <>
                <Icon name="warning" size={13} style={{ marginRight: 4 }} />
                {applyStatus.message}
              </>
            </span>
          )}
          {pendingCount > 0 && (
            <>
              <button
                type="button"
                style={btn("warning", applying)}
                disabled={applying}
                onClick={applyChanges}
              >
                {applying ? "Applying…" : "Apply Changes"}
              </button>
              <button
                type="button"
                style={btn("ghost", applying)}
                disabled={applying}
                onClick={revertChanges}
              >
                Revert All
              </button>
            </>
          )}
          {applyStatus && pendingCount === 0 && (
            <button
              type="button"
              style={btn("ghost")}
              onClick={() => setApplyStatus(null)}
              title="Dismiss"
            >
              <Icon name="close" size={13} />
            </button>
          )}
        </div>
      )}

      {}
      {!readOnlyTable && mutErr && (
        <div
          style={{
            padding: "5px 12px",
            fontSize: 12,
            flexShrink: 0,
            background: "var(--vscode-inputValidation-errorBackground)",
            color: "var(--vscode-errorForeground)",
            borderBottom: "1px solid var(--vscode-inputValidation-errorBorder)",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <span>
            <Icon name="warning" size={13} style={{ marginRight: 4 }} />
            {mutErr}
          </span>
          <button
            type="button"
            onClick={() => setMutErr(null)}
            title="Dismiss"
            style={{
              background: "none",
              border: "none",
              cursor: "pointer",
              color: "inherit",
              opacity: 0.7,
            }}
          >
            <Icon name="close" size={13} />
          </button>
        </div>
      )}

      {}
      {!readOnlyTable && newRow && (
        <NewRowForm
          columns={columns}
          newRow={newRow}
          setNewRow={setNewRow}
          inserting={inserting}
          onInsert={commitNewRow}
          onCancel={() => setNewRow(null)}
        />
      )}

      {}
      <div
        ref={scrollRef}
        style={{ flex: 1, overflow: "auto", position: "relative" }}
      >
        <table
          style={{
            width: tanTable.getTotalSize(),
            borderCollapse: "collapse",
            tableLayout: "fixed",
            fontSize: 12,
            fontFamily: "var(--vscode-editor-font-family, monospace)",
          }}
        >
          <thead>
            {tanTable.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>
                {hg.headers.map((h) => {
                  const isSel = h.column.id === "__sel";
                  const colId = h.column.id;
                  const isSorted = sort?.column === colId;
                  const sortDir = isSorted ? (sort?.direction ?? null) : null;
                  return (
                    <th
                      key={h.id}
                      style={{
                        width: h.getSize(),
                        height: HEADER_H,
                        padding: isSel ? "0 6px" : "0 8px",
                        textAlign: isSel ? "center" : "left",
                        background: isSorted
                          ? "var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.1))"
                          : "var(--vscode-editorGroupHeader-tabsBackground)",
                        borderRight: "1px solid var(--vscode-panel-border)",
                        position: "sticky",
                        top: 0,
                        zIndex: 2,
                        whiteSpace: "nowrap",
                        overflow: "hidden",
                        fontWeight: 600,
                        boxSizing: "border-box",
                        userSelect: "none",
                        cursor: isSel ? "default" : "pointer",
                      }}
                      onClick={() => {
                        if (!isSel) {
                          handleSort(colId);
                        }
                      }}
                      title={isSel ? undefined : `Sort by ${colId}`}
                    >
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: isSel ? "center" : "flex-start",
                          gap: 4,
                          overflow: "hidden",
                        }}
                      >
                        <span
                          style={{
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                          }}
                        >
                          {flexRender(
                            h.column.columnDef.header,
                            h.getContext(),
                          )}
                        </span>
                        {!isSel && (
                          <span
                            style={{
                              opacity: isSorted ? 1 : 0.25,
                              fontSize: 10,
                              flexShrink: 0,
                            }}
                          >
                            {sortDir === "asc" ? (
                              <Icon name="triangle-up" size={10} />
                            ) : sortDir === "desc" ? (
                              <Icon name="triangle-down" size={10} />
                            ) : (
                              <Icon name="unfold" size={10} />
                            )}
                          </span>
                        )}
                      </div>
                      {!isSel && h.column.getCanResize() && (
                        <button
                          type="button"
                          aria-label={`Resize ${colId} column`}
                          tabIndex={-1}
                          onMouseDown={(event) => {
                            event.stopPropagation();
                            h.getResizeHandler()(event);
                          }}
                          onClick={(e) => e.stopPropagation()}
                          style={{
                            position: "absolute",
                            right: 0,
                            top: 0,
                            height: "100%",
                            width: 5,
                            padding: 0,
                            border: "none",
                            cursor: "col-resize",
                            background: h.column.getIsResizing()
                              ? "var(--vscode-focusBorder)"
                              : "transparent",
                          }}
                        />
                      )}
                    </th>
                  );
                })}
              </tr>
            ))}
            {}
            <tr>
              {tanTable.getHeaderGroups()[0]?.headers.map((h) => {
                const isSel = h.column.id === "__sel";
                const col = columnsMap.get(h.column.id);
                return (
                  <th
                    key={h.id + "_f"}
                    style={{
                      width: h.getSize(),
                      height: FILTER_H,
                      padding: isSel ? 0 : "2px 4px",
                      background:
                        "var(--vscode-editorGroupHeader-tabsBackground)",
                      borderBottom: "2px solid var(--vscode-panel-border)",
                      borderRight: "1px solid var(--vscode-panel-border)",
                      position: "sticky",
                      top: HEADER_H,
                      zIndex: 2,
                      boxSizing: "border-box",

                      boxShadow:
                        "0 -1px 0 0 var(--vscode-editorGroupHeader-tabsBackground)",
                    }}
                  >
                    {isSel ? (
                      <span style={SR_ONLY_STYLE}>Selection column</span>
                    ) : col ? (
                      <ColumnFilterControl
                        column={col}
                        draft={filterDrafts[h.column.id]}
                        onChange={(nextDraft) =>
                          updateFilterDraft(h.column.id, nextDraft)
                        }
                      />
                    ) : null}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {virtItems.length > 0 && virtItems[0].start > 0 && (
              <tr>
                <td
                  colSpan={tanColumns.length}
                  style={{
                    height: virtItems[0].start,
                    padding: 0,
                    border: "none",
                  }}
                />
              </tr>
            )}
            {virtItems.map((vRow) => {
              const row = tableRows[vRow.index];
              const isSelected = selected.has(vRow.index);
              const editingCol =
                editCell?.rowIdx === vRow.index ? editCell.col : null;

              return (
                <TableRow
                  key={vRow.key}
                  row={row}
                  index={vRow.index}
                  isSelected={isSelected}
                  pendingCols={pendingEdits.get(vRow.index)}
                  columnsMap={columnsMap}
                  editingCol={editingCol}
                  onStartEdit={handleStartEdit}
                />
              );
            })}
            {virtItems.length > 0 &&
              (() => {
                const last = virtItems[virtItems.length - 1];
                const rem = totalVirtH - last.end;
                return rem > 0 ? (
                  <tr>
                    <td
                      colSpan={tanColumns.length}
                      style={{ height: rem, padding: 0, border: "none" }}
                    />
                  </tr>
                ) : null;
              })()}
          </tbody>
        </table>
        {!loading && rows.length === 0 && (
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              height: 200,
              opacity: 0.4,
              userSelect: "none",
            }}
          >
            <Icon name="inbox" size={28} style={{ opacity: 0.4 }} />
            <div style={{ fontSize: 13, marginTop: 8 }}>No rows found</div>
          </div>
        )}
      </div>

      <div
        style={{
          height: 34,
          flexShrink: 0,
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "0 12px",
          borderTop: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editorGroupHeader-tabsBackground)",
          fontSize: 12,
        }}
      >
        <button
          type="button"
          style={btn("ghost", page <= 1 || loading)}
          disabled={page <= 1 || loading}
          onClick={() => setPage((p) => Math.max(1, p - 1))}
        >
          ← Prev
        </button>
        <span style={{ opacity: 0.7 }}>
          Page {page} of {totalPages}
        </span>
        <button
          type="button"
          style={btn("ghost", page >= totalPages || loading)}
          disabled={page >= totalPages || loading}
          onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
        >
          Next →
        </button>
        <div style={{ flex: 1 }} />
        <span style={{ opacity: 0.6 }}>Rows per page:</span>
        <select
          aria-label="Rows per page"
          value={pageSize}
          onChange={(e) => {
            setPageSize(Number(e.target.value));
            setPage(1);
          }}
          style={{
            padding: "2px 4px",
            fontSize: 12,
            background:
              "var(--vscode-dropdown-background, var(--vscode-input-background))",
            color:
              "var(--vscode-dropdown-foreground, var(--vscode-foreground))",
            border:
              "1px solid var(--vscode-dropdown-border, var(--vscode-panel-border))",
            borderRadius: 2,
            fontFamily: "inherit",
            cursor: "pointer",
          }}
        >
          {PAGE_SIZES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </div>

      {mutationPreview && (
        <MutationPreviewDialog
          preview={mutationPreview}
          onCancel={cancelMutationPreview}
          onConfirm={confirmMutationPreview}
        />
      )}
      {exportChoice && (
        <ExportChoiceDialog
          format={exportChoice.format}
          visibleCount={rows.length}
          totalCount={totalCount}
          onExportVisible={() => {
            const { format, sort: s, filters } = exportChoice;
            setExportChoice(null);
            postMessage(format === "csv" ? "exportCSV" : "exportJSON", {
              sort: s,
              filters,
              limitToPage: { page, pageSize },
            });
          }}
          onExportAll={() => {
            const { format, sort: s, filters } = exportChoice;
            setExportChoice(null);
            postMessage(format === "csv" ? "exportCSV" : "exportJSON", {
              sort: s,
              filters,
            });
          }}
          onCancel={() => setExportChoice(null)}
        />
      )}
    </main>
  );
}

function ExportChoiceDialog({
  format,
  visibleCount,
  totalCount,
  onExportVisible,
  onExportAll,
  onCancel,
}: {
  format: "csv" | "json";
  visibleCount: number;
  totalCount: number;
  onExportVisible: () => void;
  onExportAll: () => void;
  onCancel: () => void;
}) {
  const titleId = useId();
  const descriptionId = useId();
  const dialogRef = useRef<HTMLDivElement>(null);
  const cancelButtonRef = useRef<HTMLButtonElement>(null);
  const handleKeyDown = useDialogFocusTrap({
    dialogRef,
    initialFocusRef: cancelButtonRef,
    onEscape: onCancel,
  });

  const ext = format.toUpperCase();

  return (
    <div
      style={{
        position: "absolute",
        inset: 0,
        zIndex: 30,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
        background: "rgba(0, 0, 0, 0.36)",
        backdropFilter: "blur(2px)",
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        tabIndex={-1}
        ref={dialogRef}
        onKeyDown={handleKeyDown}
        style={{
          width: "min(480px, calc(100vw - 48px))",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          borderRadius: 6,
          border: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editor-background)",
          boxShadow: "0 18px 48px rgba(0, 0, 0, 0.42)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
            padding: "14px 16px 12px",
            borderBottom: "1px solid var(--vscode-panel-border)",
            background: "var(--vscode-editorGroupHeader-tabsBackground)",
          }}
        >
          <div id={titleId} style={{ fontSize: 13, fontWeight: 600 }}>
            Export {ext}
          </div>
          <button
            type="button"
            ref={cancelButtonRef}
            onClick={onCancel}
            aria-label="Close export dialog"
            style={{
              background: "transparent",
              border: "none",
              color: "var(--vscode-foreground)",
              cursor: "pointer",
              padding: 0,
              opacity: 0.8,
            }}
          >
            <Icon name="close" size={14} />
          </button>
        </div>

        <div
          style={{
            padding: "16px",
            display: "flex",
            flexDirection: "column",
            gap: 16,
          }}
        >
          <p
            id={descriptionId}
            style={{ margin: 0, fontSize: 13, lineHeight: 1.5 }}
          >
            The table has <strong>{totalCount.toLocaleString()} rows</strong> in
            total, but only{" "}
            <strong>{visibleCount.toLocaleString()} rows</strong> are currently
            visible on this page. Choose what to export:
          </p>
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: 8,
            }}
          >
            <button
              type="button"
              style={{ ...btn("ghost"), textAlign: "left" }}
              onClick={onExportVisible}
            >
              Export visible ({visibleCount.toLocaleString()} rows)
            </button>
            <button
              type="button"
              style={{ ...btn("ghost"), textAlign: "left" }}
              onClick={onExportAll}
            >
              Export full table ({totalCount.toLocaleString()} rows){" "}
              <span style={{ opacity: 0.65, fontSize: 11 }}>
                ⚠ may be a heavy operation
              </span>
            </button>
          </div>
          <div style={{ display: "flex", justifyContent: "flex-end" }}>
            <button type="button" style={btn("ghost")} onClick={onCancel}>
              Cancel
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function MutationPreviewDialog({
  preview,
  onCancel,
  onConfirm,
}: {
  preview: TableMutationPreviewPayload;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const confirmLabel =
    preview.kind === "applyChanges" ? "Apply Changes" : "Insert Row";
  const titleId = useId();
  const descriptionId = useId();
  const dialogRef = useRef<HTMLDivElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const handleDialogKeyDown = useDialogFocusTrap({
    dialogRef,
    initialFocusRef: closeButtonRef,
  });

  return (
    <div
      style={{
        position: "absolute",
        inset: 0,
        zIndex: 30,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
        background: "rgba(0, 0, 0, 0.36)",
        backdropFilter: "blur(2px)",
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        tabIndex={-1}
        ref={dialogRef}
        onKeyDown={handleDialogKeyDown}
        style={{
          width: "min(920px, calc(100vw - 48px))",
          maxHeight: "calc(100vh - 48px)",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          borderRadius: 6,
          border: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editor-background)",
          boxShadow: "0 18px 48px rgba(0, 0, 0, 0.42)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "flex-start",
            justifyContent: "space-between",
            gap: 12,
            padding: "14px 16px 12px",
            borderBottom: "1px solid var(--vscode-panel-border)",
            background: "var(--vscode-editorGroupHeader-tabsBackground)",
          }}
        >
          <div style={{ minWidth: 0 }}>
            <div id={titleId} style={{ fontSize: 13, fontWeight: 600 }}>
              {preview.title}
            </div>
            <div
              id={descriptionId}
              style={{
                marginTop: 4,
                fontSize: 11,
                opacity: 0.75,
              }}
            >
              {preview.statementCount} statement
              {preview.statementCount === 1 ? "" : "s"} will be executed. The
              SQL below is read only and matches the prepared mutation plan.
            </div>
          </div>
          <button
            type="button"
            ref={closeButtonRef}
            onClick={onCancel}
            aria-label="Close SQL preview"
            style={{
              background: "transparent",
              border: "none",
              color: "var(--vscode-foreground)",
              cursor: "pointer",
              padding: 0,
              opacity: 0.8,
            }}
          >
            <Icon name="close" size={14} />
          </button>
        </div>

        <div
          style={{
            padding: "12px 16px 16px",
            display: "flex",
            flexDirection: "column",
            gap: 12,
          }}
        >
          <div
            style={{
              height: PREVIEW_DIALOG_EDITOR_H,
              border: "1px solid var(--vscode-panel-border)",
              borderRadius: 4,
              overflow: "hidden",
            }}
          >
            <MonacoEditor
              key={preview.previewToken}
              initialValue={preview.sql}
              height="100%"
              readOnly
              ariaLabel="SQL mutation preview"
            />
          </div>

          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              gap: 8,
            }}
          >
            <button type="button" style={btn("ghost")} onClick={onCancel}>
              Cancel
            </button>
            <button type="button" style={btn("primary")} onClick={onConfirm}>
              {confirmLabel}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

const TableRow = React.memo(function TableRow({
  row,
  index,
  isSelected,
  pendingCols,
  columnsMap,
  editingCol,
  onStartEdit,
}: {
  row: ReturnType<ReturnType<typeof useReactTable>["getRowModel"]>["rows"][0];
  index: number;
  isSelected: boolean;
  pendingCols?: Map<string, unknown>;

  columnsMap: Map<string, ColumnMeta>;
  editingCol: string | null;

  onStartEdit: (rowIndex: number, col: ColumnMeta) => void;
}) {
  return (
    <tr
      className="rdb-trow"
      data-even={String(index % 2 === 0)}
      data-editing-col={editingCol ?? ""}
      data-selected={String(isSelected)}
      style={{
        height: ROW_H,

        ...(isSelected
          ? { background: "var(--vscode-list-activeSelectionBackground)" }
          : {}),
      }}
    >
      {row.getVisibleCells().map((cell) => {
        const colId = cell.column.id;
        const colDef = columnsMap.get(colId);
        const isPk = colDef?.isPrimaryKey ?? false;
        const isSel = colId === "__sel";
        const isCellPending = pendingCols?.has(colId) ?? false;
        return (
          <td
            key={cell.id}
            style={{
              width: cell.column.getSize(),
              height: ROW_H,
              padding: isSel ? "0 6px" : "0 0 0 8px",
              textAlign: isSel
                ? "center"
                : colDef && isNumericCategory(colDef.category)
                  ? "right"
                  : "left",
              borderBottom: "1px solid var(--vscode-panel-border)",
              borderRight: "1px solid var(--vscode-panel-border)",
              borderLeft: isCellPending
                ? "3px solid var(--vscode-editorWarning-foreground, #cca700)"
                : "none",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              boxSizing: "border-box",
              verticalAlign: "middle",
              cursor: isSel || !canEditColumn(colDef) ? "default" : "pointer",

              userSelect: isSel ? "auto" : "none",
              background: isCellPending ? "rgba(200,150,0,0.07)" : undefined,
            }}
            title={isPk ? `PK: ${String(cell.getValue())}` : undefined}
            onDoubleClick={() => {
              if (colDef && !isSel && canEditColumn(colDef)) {
                onStartEdit(index, colDef);
              }
            }}
          >
            {flexRender(cell.column.columnDef.cell, cell.getContext())}
          </td>
        );
      })}
    </tr>
  );
});
