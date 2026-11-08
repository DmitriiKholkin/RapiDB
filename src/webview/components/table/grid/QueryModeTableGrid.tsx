import type { SortingState } from "@tanstack/react-table";
import { useEffect, useState } from "react";
import type { QueryResultExportPayload } from "../../../../shared/webviewContracts";
import type { QueryResult, QueryStatus } from "../../../store";
import { postMessage } from "../../../utils/messaging";
import { queryRunShortcutLabel } from "../../../utils/platform";
import { Icon } from "../../Icon";
import { TableExportActions } from "../TableExportActions";
import { tableButtonStyle } from "../tableViewHelpers";
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
  const [activeSetSelection, setActiveSetSelection] = useState<{
    result: QueryResult | null;
    index: number;
  }>({ result, index: 0 });
  const activeResultSetIndex =
    activeSetSelection.result === result ? activeSetSelection.index : 0;
  const resultSets = result?.resultSets ?? [];
  const selectedResultSet = resultSets[activeResultSetIndex] ?? resultSets[0];
  const selectedResultSetIndex = selectedResultSet
    ? resultSets.indexOf(selectedResultSet)
    : 0;
  const displayedColumns = selectedResultSet?.columns ?? result?.columns;
  const displayedResult =
    selectedResultSet && result
      ? {
          ...result,
          ...selectedResultSet,
          columnMeta: selectedResultSet.columnMeta ?? [],
          resultSets: undefined,
        }
      : result;

  useEffect(() => {
    if (displayedColumns) {
      setColumnOrder(displayedColumns.map((_, i) => `__col_${i}`));
      setSorting([]);
      setColumnSizing({});
    }
  }, [displayedColumns]);

  if (status === "idle") {
    return (
      <QueryEmptyState
        icon="run"
        primary="Run a query to see results"
        secondary={`${queryRunShortcutLabel()} or F5`}
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

  const hasPartialResult =
    Boolean(result && result.columns.length > 0) || resultSets.length > 0;
  if ((status === "error" || result?.error) && !hasPartialResult) {
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

  const hasResponseMetadata = (result?.responseMetadata?.length ?? 0) > 0;
  if (
    !result ||
    !displayedResult ||
    (displayedResult?.columns.length === 0 &&
      !hasResponseMetadata &&
      resultSets.length === 0 &&
      !result.resultSetsTruncated)
  ) {
    return (
      <QueryEmptyState
        icon="pass"
        primary="Query executed successfully"
        secondary={`${result?.rowCount ?? 0} rows affected · ${result?.executionTimeMs ?? 0} ms`}
      />
    );
  }

  const { rowCount, executionTimeMs, truncated, truncatedAt } = displayedResult;
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
      {result.error && (
        <div
          role="alert"
          style={{
            flexShrink: 0,
            margin: "8px 10px",
            padding: "8px 12px",
            borderRadius: 3,
            fontSize: 12,
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
          <div style={{ fontWeight: 600, marginBottom: 4 }}>
            Statement error
          </div>
          <div>{result.error}</div>
          {resultSets.length > 0 && (
            <div style={{ marginTop: 4, opacity: 0.8 }}>
              Result sets received before the error are shown below. Remaining
              statements may not have run.
              {resultSets.some((resultSet) => resultSet.partial) && (
                <>
                  {" "}
                  Result sets marked partial may contain only rows received
                  before the error.
                </>
              )}
            </div>
          )}
        </div>
      )}
      {result.resultSetsTruncated && (
        <div
          role="status"
          style={{
            flexShrink: 0,
            margin: "8px 10px",
            padding: "8px 12px",
            borderRadius: 3,
            fontSize: 12,
            background:
              "var(--vscode-editorInfo-background, rgba(50,100,180,0.12))",
            border:
              "1px solid var(--vscode-editorInfo-border, rgba(50,100,180,0.4))",
            color: "var(--vscode-editor-foreground)",
          }}
        >
          Some result sets were omitted because the query exceeded the result
          metadata display limit.
        </div>
      )}
      {resultSets.length > 1 && (
        <div
          role="tablist"
          aria-label="Query result sets"
          style={{
            display: "flex",
            flexShrink: 0,
            overflowX: "auto",
            gap: 4,
            padding: "6px 10px",
            borderBottom: "1px solid var(--vscode-panel-border)",
          }}
        >
          {resultSets.map((resultSet, index) => (
            <button
              key={resultSet.id}
              type="button"
              role="tab"
              aria-selected={selectedResultSetIndex === index}
              onClick={() => setActiveSetSelection({ result, index })}
              style={{
                ...tableButtonStyle(
                  selectedResultSetIndex === index ? "primary" : "ghost",
                ),
                height: 24,
                flexShrink: 0,
                padding: "0 8px",
                fontSize: 11,
              }}
            >
              {`Result ${index + 1} (${resultSet.partial ? "partial, " : ""}${resultSet.rowCount.toLocaleString()})`}
            </button>
          ))}
        </div>
      )}
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
            const hiddenColIds = new Set(
              Object.entries(columnSizing)
                .filter(([, size]) => size <= 1)
                .map(([id]) => id),
            );

            const exportColumnOrder = columnOrder.filter(
              (id) => !hiddenColIds.has(id),
            );

            const payload: QueryResultExportPayload = {};
            if (resultSets.length > 0) {
              payload.resultSetIndex = selectedResultSetIndex;
            }
            if (columnOrder.length > 0) {
              payload.columnOrder = exportColumnOrder;
            }
            if (sorting.length > 0) {
              payload.sort = sorting;
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

      {result.responseMetadata && result.responseMetadata.length > 0 && (
        <section
          aria-label="DynamoDB response metadata"
          style={{
            flexShrink: 0,
            maxHeight: 180,
            overflow: "auto",
            padding: "6px 12px",
            borderBottom: "1px solid var(--vscode-panel-border)",
            background: "var(--vscode-editor-background)",
          }}
        >
          <div style={{ fontSize: 11, fontWeight: 600, opacity: 0.8 }}>
            DynamoDB response metadata
          </div>
          <pre
            style={{
              margin: "4px 0 0",
              fontFamily: "var(--vscode-editor-font-family, monospace)",
              fontSize: 11,
              whiteSpace: "pre-wrap",
              userSelect: "text",
            }}
          >
            {JSON.stringify(result.responseMetadata, null, 2)}
          </pre>
          <div style={{ fontSize: 10, opacity: 0.65 }}>
            Use LastEvaluatedKey as ExclusiveStartKey to continue a page.
          </div>
        </section>
      )}

      {displayedResult.columns.length === 0 ? (
        <QueryEmptyState
          icon="pass"
          primary="No rows in this response"
          secondary={`${rowCount} rows · ${executionTimeMs} ms`}
        />
      ) : (
        <QueryResultsGrid
          result={displayedResult}
          columnOrder={columnOrder}
          onColumnOrderChange={setColumnOrder}
          sorting={sorting}
          onSortingChange={setSorting}
          columnSizing={columnSizing}
          onColumnSizingChange={setColumnSizing}
        />
      )}
    </div>
  );
}
