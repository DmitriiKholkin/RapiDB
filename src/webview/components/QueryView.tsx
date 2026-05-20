import React from "react";
import type {
  QueryEditorLanguage,
  QueryEditorPresentation,
} from "../../shared/webviewContracts";
import { QueryEditorPanels } from "./query/QueryEditorPanels";
import { QueryToolbar } from "./query/QueryToolbar";
import { queryViewRootStyle } from "./query/queryViewHelpers";
import { useQueryViewController } from "./query/useQueryViewController";

interface Props {
  connectionId: string;
  initialQueryText: string;
  formatOnOpen?: boolean;
  connectionType?: string;
  isBookmarked?: boolean;
  editorLanguage?: QueryEditorLanguage;
  editorPresentation?: QueryEditorPresentation;
}

export function QueryView({
  connectionId,
  initialQueryText,
  formatOnOpen = false,
  connectionType: _connectionType = "",
  isBookmarked: initialIsBookmarked = false,
  editorLanguage,
  editorPresentation,
}: Props): React.ReactElement {
  const view = useQueryViewController({
    connectionId,
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
        connections={view.connections}
        formatButtonTitle={view.editorState.formatButtonTitle}
        schemaLoading={view.schemaLoading}
        selectedConnectionId={view.activeConnectionId}
        status={view.status}
        onBookmark={view.handleBookmark}
        onClear={view.clearQuery}
        onConnectionChange={view.handleConnectionChange}
        onFormat={view.formatQuery}
        onRun={view.executeQuery}
      />

      <QueryEditorPanels
        editorHeight={view.editorHeight}
        editorLabel={view.editorState.editorLabel}
        editorRef={view.editorRef}
        initialQueryText={initialQueryText}
        isResizing={view.isResizing}
        language={view.editorState.monacoLanguage}
        result={view.result}
        schema={view.schema}
        sqlDialect={view.editorState.sqlDialect}
        status={view.status}
        onEditorChange={view.handleEditorChange}
        onExecute={view.executeQuery}
        onStartResizing={view.startResizing}
      />
    </div>
  );
}
