import type { RefObject } from "react";
import React from "react";
import type { QueryEditorSqlDialect } from "../../../shared/webviewContracts";
import type { QueryResult, QueryStatus, SchemaObject } from "../../store";
import { MonacoEditor, type MonacoEditorHandle } from "../MonacoEditor";
import { ResultsPanel } from "../ResultsPanel";

interface QueryEditorPanelsProps {
  editorHeight: number;
  editorLabel: string;
  editorRef: RefObject<MonacoEditorHandle | null>;
  initialQueryText: string;
  isResizing: boolean;
  language: string;
  result: QueryResult | null;
  schema: readonly SchemaObject[];
  sqlDialect?: QueryEditorSqlDialect;
  status: QueryStatus;
  onEditorChange: (value: string) => void;
  onExecute: () => void;
  onStartResizing: (event: React.MouseEvent<HTMLButtonElement>) => void;
}

export function QueryEditorPanels({
  editorHeight,
  editorLabel,
  editorRef,
  initialQueryText,
  isResizing,
  language,
  result,
  schema,
  sqlDialect,
  status,
  onEditorChange,
  onExecute,
  onStartResizing,
}: QueryEditorPanelsProps): React.ReactElement {
  return (
    <>
      <div style={{ height: editorHeight, flexShrink: 0, overflow: "hidden" }}>
        <MonacoEditor
          ref={editorRef}
          initialValue={initialQueryText || ""}
          schema={schema as SchemaObject[]}
          dialect={sqlDialect}
          language={language}
          ariaLabel={editorLabel}
          onExecute={onExecute}
          onChange={onEditorChange}
          height="100%"
        />
      </div>

      <button
        type="button"
        aria-label="Resize editor and results panels"
        onMouseDown={onStartResizing}
        style={{
          display: "block",
          width: "100%",
          height: 5,
          flexShrink: 0,
          cursor: "row-resize",
          background: isResizing
            ? "var(--vscode-focusBorder)"
            : "var(--vscode-panel-border)",
          transition: isResizing ? "none" : "background 150ms",
          userSelect: "none",
          border: "none",
          padding: 0,
        }}
        title="Drag to resize"
      />

      <div style={{ flex: 1, overflow: "hidden", minHeight: 0 }}>
        <ResultsPanel status={status} result={result} />
      </div>
    </>
  );
}
