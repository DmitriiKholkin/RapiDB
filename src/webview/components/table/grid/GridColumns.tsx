import {
  type CellContext,
  type ColumnDef as TanColumnDef,
} from "@tanstack/react-table";
import React from "react";
import { type ColumnTypeMeta as ColumnMeta } from "../../../../shared/tableTypes";
import type { Row } from "../../../types";
import { Icon } from "../../Icon";
import { CellDisplay } from "../CellDisplay";
import { EditInput, valueToEditString } from "../EditInput";
import { buildColumnHeaderTitle, keyIconColor } from "../tableCellUtils";

interface BuildSelectionColumnOptions {
  rows: readonly Row[];
  selected: ReadonlySet<number>;
  onSelectionChange: (selection: Set<number>) => void;
}

export function buildSelectionColumn({
  rows,
  selected,
  onSelectionChange,
}: BuildSelectionColumnOptions): TanColumnDef<Row> {
  return {
    id: "__sel",
    size: 36,
    header: () => (
      <input
        type="checkbox"
        aria-label="Select all rows"
        checked={rows.length > 0 && selected.size === rows.length}
        ref={(element) => {
          if (element) {
            element.indeterminate =
              selected.size > 0 && selected.size < rows.length;
          }
        }}
        onChange={(event) =>
          onSelectionChange(
            event.target.checked
              ? new Set(rows.map((_, index) => index))
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
        checked={selected.has(row.index)}
        onChange={(event: React.ChangeEvent<HTMLInputElement>) => {
          const nextSelection = new Set(selected);
          if (event.target.checked) {
            nextSelection.add(row.index);
          } else {
            nextSelection.delete(row.index);
          }
          onSelectionChange(nextSelection);
        }}
        style={{
          cursor: "pointer",
          accentColor: "var(--vscode-button-background)",
          margin: 0,
        }}
      />
    ),
  } as TanColumnDef<Row>;
}

interface BuildDataColumnOptions {
  canEditRows: boolean;
  colSizes: Record<string, number>;
  editCell: { kind: string; rowIdx?: number; col?: string } | null;
  onCancelEdit: () => void;
  onCommitCellEdit: (
    rowIdx: number,
    column: ColumnMeta,
    newVal: string,
    originalVal: unknown,
  ) => void;
  pendingEdits: Map<number, Map<string, unknown>>;
}

export function buildDataColumn(
  column: ColumnMeta,
  {
    canEditRows,
    colSizes,
    editCell,
    onCancelEdit,
    onCommitCellEdit,
    pendingEdits,
  }: BuildDataColumnOptions,
): TanColumnDef<Row> {
  return {
    id: column.name,
    accessorKey: column.name,
    meta: column,
    size: colSizes[column.name] ?? colSizes[`${column.name}key`] ?? 160,
    minSize: 1,
    maxSize: 800,
    header: () => (
      <span
        title={buildColumnHeaderTitle(column)}
        style={{ display: "flex", alignItems: "center", gap: 4 }}
      >
        {column.name}
        {column.isPrimaryKey && (
          <Icon
            name="key"
            size={13}
            color={keyIconColor(column.primaryKeyRole)}
            style={{ marginLeft: 2 }}
          />
        )}
        {column.isForeignKey && (
          <Icon
            name="key"
            size={13}
            color="var(--vscode-foreground)"
            style={{ marginLeft: 2 }}
          />
        )}
      </span>
    ),
    cell: ({ row, getValue }) => {
      const rowIdx = row.index;
      const isEditing =
        editCell?.kind === "persisted" &&
        editCell.rowIdx === rowIdx &&
        editCell.col === column.name;
      const pendingRow = pendingEdits.get(rowIdx);
      const hasPending = pendingRow?.has(column.name) ?? false;
      const pendingValue = pendingRow?.get(column.name);
      const displayValue = hasPending ? pendingValue : getValue();

      if (isEditing) {
        const startValue = hasPending ? pendingValue : getValue();
        const startString = valueToEditString(startValue);
        return (
          <EditInput
            initial={startString}
            nullable={column.nullable}
            category={column.category}
            readOnly={!canEditRows}
            onCommit={(value) =>
              onCommitCellEdit(rowIdx, column, value, getValue())
            }
            onCancel={onCancelEdit}
          />
        );
      }

      return (
        <CellDisplay
          value={displayValue}
          isPending={hasPending}
          category={column.category}
          nativeType={column.nativeType}
        />
      );
    },
  };
}
