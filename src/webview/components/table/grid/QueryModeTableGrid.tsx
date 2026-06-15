import type { SortingState } from "@tanstack/react-table";
import React, { useEffect, useState } from "react";
import type { QueryResult, QueryStatus } from "../../../store";
import { postMessage } from "../../../utils/messaging";
import { Icon } from "../../Icon";
import { TableExportActions } from "../TableExportActions";
import { tableButtonStyle } from "../tableConstants";
import {
  QueryEmptyState,
  QueryResultsGrid,
  QuerySpinner,
} from "./QueryResultsGrid";

const QUERY_TOOLBAR_H = 28;

export function QueryModeTableGrid({
  status,
  result,
}: {
  status: QueryStatus;
  result: QueryResult | null;
}) {
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({});

  useEffect(() => {
    if (result?.columns) {
      setColumnOrder(result.columns.map((_, i) => `__col_${i}`));
      setSorting([]);
      setColumnSizing({});
    }
  }, [result?.columns]);

  if (status === "idle") {
    return (
      <QueryEmptyState
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
        <QuerySpinner /> Executing…
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
      <QueryEmptyState
        icon="pass"
        primary="Query executed successfully"
        secondary={`${result?.rowCount ?? 0} rows affected · ${result?.executionTimeMs ?? 0} ms`}
      />
    );
  }

  const { rowCount, executionTimeMs, truncated, truncatedAt } = result;
  const truncatedCount = truncatedAt ?? rowCount;

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
      <div
        style={{
          height: QUERY_TOOLBAR_H,
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
        <TableExportActions
          onExport={(format) => {
            const exportSort = sorting
              .map((s) => {
                const match = s.id.match(/^__col_(\d+)$/);
                if (!match || !result) return null;
                return {
                  column: result.columns[parseInt(match[1], 10)],
                  desc: s.desc,
                };
              })
              .filter((v): v is NonNullable<typeof v> => v !== null);

            const hiddenColIds = new Set(
              Object.entries(columnSizing)
                .filter(([, size]) => size <= 1)
                .map(([id]) => id),
            );

            const exportColumnOrder = columnOrder
              .filter((id) => !hiddenColIds.has(id))
              .map((id) => {
                const match = id.match(/^__col_(\d+)$/);
                return match ? result.columns[parseInt(match[1], 10)] : null;
              })
              .filter((v): v is string => v !== null);

            const payload: Record<string, unknown> = {};
            if (exportColumnOrder.length > 0) {
              payload.columnOrder = exportColumnOrder;
            }
            if (exportSort.length > 0) {
              payload.sort = exportSort;
            }

            postMessage(
              format === "csv" ? "exportResultsCSV" : "exportResultsJSON",
              payload,
            );
          }}
          titleByFormat={{
            csv: "Export results as CSV file",
            json: "Export results as JSON file",
          }}
          buttonStyle={() => ({
            ...tableButtonStyle("ghost"),
            height: 22,
            padding: "0 8px",
            fontSize: 11,
          })}
          iconSize={12}
          iconMarginRight={3}
        />
        <span style={{ opacity: 0.7 }}>
          {truncated
            ? `${truncatedCount.toLocaleString()} rows (truncated — query returned more)`
            : `${rowCount.toLocaleString()} row${rowCount !== 1 ? "s" : ""}`}
          <span style={{ opacity: 0.5, marginLeft: 6 }}>
            {executionTimeMs} ms
          </span>
        </span>
      </div>

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

      <QueryResultsGrid
        result={result}
        columnOrder={columnOrder}
        onColumnOrderChange={setColumnOrder}
        sorting={sorting}
        onSortingChange={setSorting}
        columnSizing={columnSizing}
        onColumnSizingChange={setColumnSizing}
      />
    </div>
  );
}
