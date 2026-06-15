/**
 * Shared styles for context menu items.
 *
 * Used by MonacoEditor and EditInput to keep the visual appearance consistent.
 */
import React from "react";
import { cssVar } from "./cssVar";

export function contextMenuButtonStyle(options: {
  disabled?: boolean;
  hovered?: boolean;
}): React.CSSProperties {
  const { disabled = false, hovered = false } = options;
  const hoverBg =
    cssVar("--vscode-menu-selectionBackground") || "rgba(255, 255, 255, 0.10)";

  return {
    appearance: "none",
    border: "none",
    background: hovered && !disabled ? hoverBg : "transparent",
    color: disabled
      ? cssVar("--vscode-disabledForeground") || "rgba(255, 255, 255, 0.4)"
      : cssVar("--vscode-menu-foreground") ||
        cssVar("--vscode-foreground") ||
        "#cccccc",
    borderRadius: 4,
    padding: "4px 10px",
    fontSize: 12,
    textAlign: "left",
    cursor: disabled ? "default" : "pointer",
    width: "100%",
  };
}

export const CONTEXT_MENU_CONTAINER_STYLE: React.CSSProperties = {
  minWidth: 80,
  padding: 3,
  display: "flex",
  flexDirection: "column",
  gap: 0,
  background:
    cssVar("--vscode-menu-background") ||
    cssVar("--vscode-editorWidget-background") ||
    cssVar("--vscode-editor-background") ||
    "#252526",
  border: `1px solid ${
    cssVar("--vscode-menu-border") ||
    cssVar("--vscode-contrastBorder") ||
    "rgba(255, 255, 255, 0.12)"
  }`,
  borderRadius: 6,
  boxShadow: "0 10px 30px rgba(0, 0, 0, 0.24), 0 2px 8px rgba(0, 0, 0, 0.18)",
};
