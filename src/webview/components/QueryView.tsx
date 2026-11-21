import type React from "react";
import type { QueryDatabaseFields } from "../../shared/queryDatabaseTarget";
import type {
  QueryEditorLanguage,
  QueryEditorPresentation,
  QueryTargetBinding,
} from "../../shared/webviewContracts";
import { QueryEditorPanels } from "./query/QueryEditorPanels";
import { QueryToolbar } from "./query/QueryToolbar";
import { queryViewRootStyle } from "./query/queryViewHelpers";
import { useQueryViewController } from "./query/useQueryViewController";

interface Props extends QueryDatabaseFields {
  panelId?: string;
  connectionId: string;
  targetBinding?: QueryTargetBinding;
  queryRunStatus?: boolean;
  initialQueryText: string;
  formatOnOpen?: boolean;
  connectionType?: string;
  isBookmarked?: boolean;
  editorLanguage?: QueryEditorLanguage;
  editorPresentation?: QueryEditorPresentation;
}

export function QueryView({
  panelId = "query",
  connectionId,
  postgresDatabase,
  mssqlDatabase,
  targetBinding,
  queryRunStatus,
  initialQueryText,
  formatOnOpen,
  connectionType: _connectionType = "",
  isBookmarked: initialIsBookmarked = false,
  editorLanguage,
  editorPresentation,
}: Props): React.ReactElement {
  const pinnedDatabase = postgresDatabase ?? mssqlDatabase;
  const view = useQueryViewController({
    panelId,
    connectionId,
    pinned: pinnedDatabase !== undefined,
    postgresDatabase,
    mssqlDatabase,
    initialTargetBinding: targetBinding,
    queryRunStatus,
    editorLanguage,
    editorPresentation,
    formatOnOpen,
    initialIsBookmarked,
    initialQueryText,
  });

  return (
    <div ref={view.containerRef} style={queryViewRootStyle}>
      <QueryToolbar
        bookmarked={view.bookmarked}
        bookmarking={view.bookmarking}
        canFormat={view.editorState.canFormat}
        connectionId={connectionId}
        pinnedDatabase={pinnedDatabase}
        pinnedTargetUnavailable={view.pinnedTargetUnavailable}
        targetReviewRequired={
          view.targetReviewRequired ||
          view.targetChecking ||
          view.targetChangePending
        }
        connections={view.connections}
        formatButtonTitle={view.editorState.formatButtonTitle}
        schemaError={view.schemaError}
        schemaLoading={view.schemaLoading}
        selectedConnectionId={view.selectedConnectionId}
        status={view.status}
        runReviewRequired={!!view.unknownRun}
        onBookmark={view.handleBookmark}
        onCancel={view.cancelQuery}
        onClear={view.clearQuery}
        onConnectionChange={view.handleConnectionChange}
        onFormat={view.formatQuery}
        onRun={view.executeQuery}
      />

      {view.unknownRun && (
        <div
          role="alert"
          aria-label="Previous query outcome"
          style={{ padding: "6px 12px", flexShrink: 0 }}
        >
          Previous query outcome is unknown. It may still be running or may
          already have changed the original database. Verify the original target
          ({view.unknownRun.connectionId}
          {view.unknownRun.postgresDatabase !== undefined
            ? ` / ${JSON.stringify(view.unknownRun.postgresDatabase)} (PostgreSQL)`
            : view.unknownRun.mssqlDatabase !== undefined
              ? ` / ${JSON.stringify(view.unknownRun.mssqlDatabase)} (SQL Server)`
              : ""}
          ) before running again. No query was retried.
          <button
            type="button"
            style={{ marginLeft: 8 }}
            onClick={view.acknowledgeUnknown}
          >
            I checked the previous query
          </button>
        </div>
      )}
      {view.runNotice && (
        <div role="status" style={{ padding: "6px 12px", flexShrink: 0 }}>
          {view.runNotice}
        </div>
      )}

      {view.targetChangePending && (
        <div role="status" style={{ padding: "6px 12px", flexShrink: 0 }}>
          Verifying selected target…
        </div>
      )}
      {view.targetChangeError && (
        <div
          role="alert"
          aria-label="Target change error"
          style={{ padding: "6px 12px", flexShrink: 0 }}
        >
          Could not verify the selected target. Review the active connection or
          select again.
        </div>
      )}
      {view.targetReviewRequired &&
        !view.targetChecking &&
        !view.targetChangePending && (
          <div
            role="alert"
            aria-label="Query target review"
            style={{ padding: "6px 12px", flexShrink: 0 }}
          >
            {view.currentTargetAvailable
              ? "This draft's target changed or could not be verified. Review the connection settings and confirm the target before running."
              : pinnedDatabase !== undefined
                ? "The pinned target is unavailable. Your draft is preserved."
                : "The previous target is unavailable. Select a connection to use this draft."}
            {view.currentTargetAvailable && (
              <button
                type="button"
                style={{ marginLeft: 8 }}
                onClick={() =>
                  view.handleConnectionChange(view.activeConnectionId)
                }
              >
                Use current target
              </button>
            )}
          </div>
        )}

      {view.bookmarkError && (
        <div
          role="alert"
          aria-label="Bookmark error"
          style={{
            padding: "8px 12px",
            flexShrink: 0,
            color: "var(--vscode-errorForeground)",
            overflowWrap: "anywhere",
          }}
        >
          {view.bookmarkError}
        </div>
      )}

      <QueryEditorPanels
        allowFormatting={view.editorState.canFormat}
        editorHeight={view.editorHeight}
        editorLabel={view.editorState.editorLabel}
        editorRef={view.editorRef}
        initialQueryText={view.editorInitialValue}
        isResizing={view.isResizing}
        language={view.editorState.monacoLanguage}
        result={view.result}
        schema={view.schema}
        sqlDialect={view.editorState.sqlDialect}
        status={view.status}
        onEditorChange={view.handleEditorChange}
        onEditorSelectionChange={view.handleEditorSelectionChange}
        onExecute={view.executeQuery}
        onStartResizing={view.startResizing}
      />
    </div>
  );
}
