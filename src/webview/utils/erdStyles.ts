/**
 * ERD (Entity Relationship Diagram) style constants.
 *
 * Extracted from ErdView.tsx to reduce component size and improve maintainability.
 * All styles are React.CSSProperties objects used by the ERD view components.
 */
import type React from "react";

// ─── Button Styles ───────────────────────────────────────────────────────────

export const miniButtonStyle: React.CSSProperties = {
  cursor: "pointer",
  fontSize: 10,
  padding: "3px 8px",
  border:
    "1px solid var(--vscode-button-secondaryBorder, var(--vscode-panel-border))",
  borderRadius: 4,
  color: "var(--vscode-button-secondaryForeground)",
  background: "var(--vscode-button-secondaryBackground)",
};

// ─── Node Header Styles ──────────────────────────────────────────────────────

export const nodeHeaderLabelStyle: React.CSSProperties = {
  flex: 1,
  display: "flex",
  alignItems: "center",
  minWidth: 0,
  overflow: "hidden",
};

export const tableTitleStyle: React.CSSProperties = {
  fontSize: 12,
  fontWeight: 600,
  textAlign: "left",
  minWidth: 0,
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
};

// ─── Toggle Label Styles ─────────────────────────────────────────────────────

export const toggleLabelStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 6,
  fontSize: 11,
  opacity: 0.85,
};

// ─── Column Badge Styles ─────────────────────────────────────────────────────

export const columnBadgeStyle: React.CSSProperties = {
  fontSize: 9,
  padding: "1px 4px",
  borderRadius: 3,
  border: "1px solid var(--vscode-panel-border)",
  background: "var(--vscode-editor-background)",
  opacity: 0.9,
};

export function primaryKeyBadgeStyle(
  role?: "partition" | "sort",
): React.CSSProperties {
  const isSortKey = role === "sort";
  return {
    ...columnBadgeStyle,
    color: isSortKey
      ? "var(--vscode-textLink-foreground, #2f6f9f)"
      : "var(--vscode-editorWarning-foreground, #8f5b00)",
    background: isSortKey
      ? "rgba(47, 111, 159, 0.16)"
      : "rgba(143, 91, 0, 0.16)",
  };
}

// ─── Column Layout Styles ────────────────────────────────────────────────────

export const columnNameAndBadgesStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 6,
  minWidth: 0,
  flex: "1 1 56%",
};

export const columnNameTextStyle: React.CSSProperties = {
  minWidth: 0,
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
};

export const columnTypeTextStyle: React.CSSProperties = {
  opacity: 0.7,
  minWidth: 0,
  flex: "0 1 44%",
  textAlign: "right",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
};

// ─── Row Handle Styles ───────────────────────────────────────────────────────

export const rowHandleStyle: React.CSSProperties = {
  width: 8,
  height: 8,
  opacity: 0,
  border: 0,
  background: "transparent",
  top: "50%",
  transform: "translateY(-50%)",
};

export const rowLeftHandleStyle: React.CSSProperties = {
  ...rowHandleStyle,
  left: -11,
};

export const rowRightHandleStyle: React.CSSProperties = {
  ...rowHandleStyle,
  right: -11,
};

// ─── ReactFlow Controls Theme ────────────────────────────────────────────────

export const reactFlowControlsThemeCss = `
.react-flow__controls { border: 1px solid var(--vscode-panel-border); border-radius: 4px; overflow: hidden; box-shadow: none; }
.react-flow__controls-button { background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border-bottom: 1px solid var(--vscode-panel-border); }
.react-flow__controls-button:last-child { border-bottom: 0; }
.react-flow__controls-button:hover { background: var(--vscode-list-hoverBackground); }
.react-flow__controls-button svg, .react-flow__controls-button path { fill: currentColor; stroke: currentColor; }
`;
