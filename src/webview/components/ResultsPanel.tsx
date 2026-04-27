import {
  type ColumnDef,
  type ColumnResizeMode,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import React, { useCallback, useMemo, useRef, useState } from "react";
import type { QueryResult, QueryStatus } from "../store";
import { buildButtonStyle } from "../utils/buttonStyles";
import { type Column, calcColWidths } from "../utils/columnSizing";
import { postMessage } from "../utils/messaging";
import { Icon } from "./Icon";
import { CellDisplay } from "./table/CellDisplay";
import { EditInput, valueToEditString } from "./table/EditInput";

interface Props {
  status: QueryStatus;
  result: QueryResult | null;
}

const HEADER_H = 28;
const ROW_H = 24;
const TOOLBAR_H = 28;

export function ResultsPanel({ status, result }: Props): React.ReactElement {
  if (status === "idle") {
    return (
      <EmptyState
        icon="run"
        primary="Run a query to see results"
        secondary="Ctrl+Enter or F5"
      />
    );
  }

  if (status === "running") {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "12px 16px",
          opacity: 0.7,
          fontSize: 13,
        }}
      >
        <Spinner /> Executing…
      </div>
    );
  }

  if (status === "error" || result?.error) {
    return (
      <div
        style={{
          margin: 10,
          padding: "10px 14px",
          borderRadius: 3,
          fontSize: 13,
          background:
            "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.15))",
          border:
            "1px solid var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.5))",
          color: "var(--vscode-errorForeground)",
          fontFamily: "var(--vscode-editor-font-family, monospace)",
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        <div style={{ fontWeight: 600, marginBottom: 4 }}>Error</div>
        <div style={{ opacity: 0.9 }}>{result?.error}</div>
      </div>
    );
  }

  if (!result || result.columns.length === 0) {
    return (
      <EmptyState
        icon="pass"
        primary="Query executed successfully"
        secondary={`${result?.rowCount ?? 0} rows affected · ${result?.executionTimeMs ?? 0} ms`}
      />
    );
  }

  return <DataTable result={result} />;
}

function DataTable({ result }: { result: QueryResult }) {
  const {
    columns: colNames,
    rows,
    rowCount,
    executionTimeMs,
    truncated,
    truncatedAt,
  } = result;

  const [sorting, setSorting] = useState<SortingState>([]);
  const [activeCell, setActiveCell] = useState<{
    rowIndex: number;
    columnId: string;
  } | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const truncatedCount = truncatedAt ?? rowCount;

  const colSizes = useMemo(
    () =>
      calcColWidths(
        colNames.map(
          (name, i): Column => ({
            name,
            dataKey: `__col_${i}`,
            isPrimaryKey: false,
          }),
        ),
        rows,
        { hPad: 19 },
      ),
    [colNames, rows],
  );

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      colNames.map((name, i) => {
        const key = `__col_${i}`;
        return {
          id: key,
          accessorKey: key,
          header: name,
          size: colSizes[key] ?? 160,
          minSize: 40,
          maxSize: 800,
          cell: (info) => (
            <CellDisplay value={info.getValue()} isPending={false} />
          ),
        };
      }),
    [colNames, colSizes],
  );

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    columnResizeMode: "onChange" as ColumnResizeMode,
    enableColumnResizing: true,
  });

  const { rows: tableRows } = table.getRowModel();

  const virtualizer = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_H,
    overscan: 20,
  });

  const totalHeight = virtualizer.getTotalSize();
  const virtItems = virtualizer.getVirtualItems();

  const exportCSV = useCallback(() => {
    postMessage("exportResultsCSV");
  }, []);

  const exportJSON = useCallback(() => {
    postMessage("exportResultsJSON");
  }, []);

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        overflow: "hidden",
        position: "relative",
      }}
    >
      {}
      <div
        style={{
          height: TOOLBAR_H,
          flexShrink: 0,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 10px",
          gap: 8,
          borderBottom: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editorGroupHeader-tabsBackground)",
          fontSize: 11,
        }}
      >
        <span style={{ opacity: 0.7 }}>
          {truncated
            ? `${truncatedCount.toLocaleString()} rows (truncated — query returned more)`
            : `${rowCount.toLocaleString()} row${rowCount !== 1 ? "s" : ""}`}
          <span style={{ opacity: 0.5, marginLeft: 6 }}>
            {executionTimeMs} ms
          </span>
        </span>
        <div style={{ display: "flex", gap: 4 }}>
          <ToolbarBtn onClick={exportCSV} title="Export results as CSV file">
            <Icon name="export" size={12} style={{ marginRight: 3 }} />
            Export CSV
          </ToolbarBtn>
          <ToolbarBtn onClick={exportJSON} title="Export results as JSON file">
            <Icon name="export" size={12} style={{ marginRight: 3 }} />
            Export JSON
          </ToolbarBtn>
        </div>
      </div>

      {}
      {truncated && (
        <div
          style={{
            flexShrink: 0,
            display: "flex",
            alignItems: "center",
            gap: 8,
            padding: "6px 12px",
            fontSize: 12,
            background:
              "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
            borderBottom:
              "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
            color: "var(--vscode-editorWarning-foreground, #CCA700)",
          }}
        >
          <Icon
            name="warning"
            size={12}
            style={{ opacity: 0.8, flexShrink: 0 }}
          />
          <span>
            Result limited to <strong>{truncatedCount.toLocaleString()}</strong>{" "}
            rows. The query returned more data. Use <code>LIMIT</code> in your
            query or increase <em>RapiDB: Query Row Limit</em> in settings.
          </span>
        </div>
      )}

      {}
      <div
        ref={scrollRef}
        style={{ flex: 1, overflow: "auto", position: "relative" }}
      >
        <table
          style={{
            width: table.getTotalSize(),
            borderCollapse: "collapse",
            tableLayout: "fixed",
            fontSize: 12,
            fontFamily: "var(--vscode-editor-font-family, monospace)",
          }}
        >
          {}
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>
                {hg.headers.map((header) => {
                  const sorted = header.column.getIsSorted();
                  return (
                    <th
                      key={header.id}
                      style={{
                        width: header.getSize(),
                        height: HEADER_H,
                        padding: "0 8px",
                        textAlign: "left",
                        background:
                          "var(--vscode-editorGroupHeader-tabsBackground)",
                        borderBottom: "2px solid var(--vscode-panel-border)",
                        borderRight: "1px solid var(--vscode-panel-border)",
                        position: "sticky",
                        top: 0,
                        zIndex: 2,
                        whiteSpace: "nowrap",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        userSelect: "none",
                        cursor: header.column.getCanSort()
                          ? "pointer"
                          : "default",
                        fontWeight: 600,
                        color: "var(--vscode-foreground)",
                        boxSizing: "border-box",
                      }}
                      onClick={header.column.getToggleSortingHandler()}
                    >
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
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
                            header.column.columnDef.header,
                            header.getContext(),
                          )}
                        </span>
                        {sorted === "asc" && (
                          <Icon
                            name="triangle-up"
                            size={10}
                            style={{ opacity: 0.7 }}
                          />
                        )}
                        {sorted === "desc" && (
                          <Icon
                            name="triangle-down"
                            size={10}
                            style={{ opacity: 0.7 }}
                          />
                        )}
                        {!sorted && header.column.getCanSort() && (
                          <Icon
                            name="unfold"
                            size={10}
                            style={{ opacity: 0.2 }}
                          />
                        )}
                      </div>

                      {}
                      {header.column.getCanResize() && (
                        <button
                          type="button"
                          aria-label={`Resize ${typeof header.column.columnDef.header === "string" ? header.column.columnDef.header : header.column.id} column`}
                          onMouseDown={header.getResizeHandler()}
                          onTouchStart={header.getResizeHandler()}
                          onClick={(e) => e.stopPropagation()}
                          style={{
                            position: "absolute",
                            right: 0,
                            top: 0,
                            height: "100%",
                            width: 5,
                            cursor: "col-resize",
                            userSelect: "none",
                            background: header.column.getIsResizing()
                              ? "var(--vscode-focusBorder)"
                              : "transparent",
                            zIndex: 1,
                            border: "none",
                            padding: 0,
                          }}
                        />
                      )}
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>

          {}
          <tbody>
            {}
            {virtItems.length > 0 && virtItems[0].start > 0 && (
              <tr style={{ height: virtItems[0].start }} />
            )}

            {virtItems.map((vRow) => {
              const row = tableRows[vRow.index];
              return (
                <VirtualRow
                  key={vRow.key}
                  row={row}
                  index={vRow.index}
                  activeCell={activeCell}
                  onActivateCell={(rowIndex, columnId) =>
                    setActiveCell({ rowIndex, columnId })
                  }
                  onDeactivateCell={() => setActiveCell(null)}
                />
              );
            })}

            {}
            {virtItems.length > 0 &&
              (() => {
                const last = virtItems[virtItems.length - 1];
                const remaining = totalHeight - last.end;
                return remaining > 0 ? (
                  <tr style={{ height: remaining }} />
                ) : null;
              })()}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const RESULTS_ROW_STYLE_ID = "rapidb-results-row-style";
if (
  typeof document !== "undefined" &&
  !document.getElementById(RESULTS_ROW_STYLE_ID)
) {
  const s = document.createElement("style");
  s.id = RESULTS_ROW_STYLE_ID;
  s.textContent = [
    `.rdb-rrow { transition: background 60ms; }`,
    `.rdb-rrow[data-even="true"]  { background: var(--vscode-editor-background); }`,
    `.rdb-rrow[data-even="false"] { background: var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04)); }`,
    `.rdb-rrow:hover { background: var(--vscode-list-hoverBackground); }`,
  ].join("\n");
  document.head.appendChild(s);
}

const VirtualRow = React.memo(function VirtualRow({
  row,
  index,
  activeCell,
  onActivateCell,
  onDeactivateCell,
}: {
  row: ReturnType<ReturnType<typeof useReactTable>["getRowModel"]>["rows"][0];
  index: number;
  activeCell: { rowIndex: number; columnId: string } | null;
  onActivateCell: (rowIndex: number, columnId: string) => void;
  onDeactivateCell: () => void;
}) {
  return (
    <tr
      className="rdb-rrow"
      data-even={String(index % 2 === 0)}
      style={{ height: ROW_H }}
    >
      {row.getVisibleCells().map((cell) => {
        const raw = cell.getValue();
        const isNull = raw === null || raw === undefined;
        const isNumeric = typeof raw === "number" || typeof raw === "bigint";
        const isEditing =
          activeCell?.rowIndex === index &&
          activeCell.columnId === cell.column.id;
        return (
          <td
            key={cell.id}
            style={{
              width: cell.column.getSize(),
              height: ROW_H,
              padding: isEditing ? "0" : "0 8px",
              border: "1px solid var(--vscode-panel-border)",
              textAlign: isNumeric ? "right" : "left",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              boxSizing: "border-box",
              verticalAlign: "middle",
              cursor: "pointer",
              userSelect: "text",
            }}
            title={isNull ? "" : String(raw)}
            onDoubleClick={() => onActivateCell(index, cell.column.id)}
          >
            {isEditing ? (
              <EditInput
                initial={valueToEditString(raw)}
                nullable
                readOnly
                onCommit={onDeactivateCell}
                onCancel={onDeactivateCell}
              />
            ) : (
              flexRender(cell.column.columnDef.cell, cell.getContext())
            )}
          </td>
        );
      })}
    </tr>
  );
});

function ToolbarBtn({
  onClick,
  title,
  children,
}: {
  onClick: () => void;
  title?: string;
  children: React.ReactNode;
}) {
  const [hov, setHov] = useState(false);
  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        ...buildButtonStyle("ghost", { size: "sm" }),
        height: 22,
        padding: "0 8px",
        fontSize: 11,
        background: hov
          ? "var(--vscode-button-secondaryHoverBackground, var(--vscode-list-hoverBackground))"
          : "transparent",
      }}
    >
      {children}
    </button>
  );
}

function EmptyState({
  icon,
  primary,
  secondary,
}: {
  icon: string;
  primary: string;
  secondary?: string;
}) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        height: "100%",
        gap: 6,
        opacity: 0.45,
        userSelect: "none",
      }}
    >
      <Icon name={icon} size={28} />
      <div style={{ fontSize: 13 }}>{primary}</div>
      {secondary && <div style={{ fontSize: 11 }}>{secondary}</div>}
    </div>
  );
}

function Spinner() {
  return <Icon name="sync" size={14} spin />;
}
