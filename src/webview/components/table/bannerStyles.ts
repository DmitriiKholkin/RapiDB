/**
 * TableStatusBanners style constants.
 *
 * Extracted from TableStatusBanners.tsx to reduce component size and improve maintainability.
 * Contains styles for banner layouts, mutation status bar, and undo/redo buttons.
 */
import type React from "react";

// ─── Banner Layout Styles ────────────────────────────────────────────────────

/** Base layout for all status banners */
export const bannerLayoutStyle: React.CSSProperties = {
  flexShrink: 0,
  padding: "6px 12px",
  fontSize: 12,
  display: "flex",
  alignItems: "center",
  gap: 8,
};

/** Warning banner tone (yellow/orange) */
export const warningBannerToneStyle: React.CSSProperties = {
  background:
    "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
  borderBottom:
    "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
  color: "var(--vscode-editorWarning-foreground, #CCA700)",
};

/** Error banner tone (red) */
export const errorBannerToneStyle: React.CSSProperties = {
  background: "var(--vscode-inputValidation-errorBackground)",
  borderBottom: "1px solid var(--vscode-inputValidation-errorBorder)",
  color: "var(--vscode-errorForeground)",
};

/** Dismiss button for text-based banners */
export const dismissTextButtonStyle: React.CSSProperties = {
  background: "none",
  border: "none",
  cursor: "pointer",
  color: "inherit",
  opacity: 0.7,
  fontSize: 14,
  lineHeight: 1,
  padding: "0 2px",
};

// ─── Mutation Status Bar Styles ──────────────────────────────────────────────

/** Container for the mutation status bar */
export const mutationStatusBarStyle: React.CSSProperties = {
  flexShrink: 0,
  padding: "0 12px",
  minHeight: 36,
  display: "flex",
  alignItems: "center",
  gap: 8,
};

/** Error tone for mutation status bar */
export const mutationStatusBarErrorStyle: React.CSSProperties = {
  background:
    "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.1))",
  borderBottom:
    "1px solid var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.4))",
};

/** Default (warning) tone for mutation status bar */
export const mutationStatusBarWarningStyle: React.CSSProperties = {
  background: "rgba(200,150,0,0.08)",
  borderBottom: "1px solid rgba(200,150,0,0.3)",
};

// ─── Undo/Redo Button Styles ─────────────────────────────────────────────────

/** Base style for undo/redo buttons */
const undoRedoButtonBaseStyle: React.CSSProperties = {
  background: "none",
  border: "none",
  padding: "0 2px",
  display: "flex",
  alignItems: "center",
};

/** Undo button style (enabled) */
export const undoButtonEnabledStyle: React.CSSProperties = {
  ...undoRedoButtonBaseStyle,
  cursor: "pointer",
  color: "var(--vscode-editorWarning-foreground, #cca700)",
  opacity: 0.9,
};

/** Undo button style (disabled) */
export const undoButtonDisabledStyle: React.CSSProperties = {
  ...undoRedoButtonBaseStyle,
  cursor: "default",
  color: "var(--vscode-descriptionForeground, #888)",
  opacity: 0.4,
};

/** Redo button style (enabled) */
export const redoButtonEnabledStyle: React.CSSProperties = {
  ...undoRedoButtonBaseStyle,
  cursor: "pointer",
  color: "var(--vscode-editorWarning-foreground, #cca700)",
  opacity: 0.9,
};

/** Redo button style (disabled) */
export const redoButtonDisabledStyle: React.CSSProperties = {
  ...undoRedoButtonBaseStyle,
  cursor: "default",
  color: "var(--vscode-descriptionForeground, #888)",
  opacity: 0.4,
};

/** Separator line between undo/redo and unsaved count */
export const undoRedoSeparatorStyle: React.CSSProperties = {
  width: 1,
  height: 14,
  background: "rgba(200,150,0,0.3)",
  flexShrink: 0,
};

/** Unsaved changes count text */
export const unsavedCountStyle: React.CSSProperties = {
  fontSize: 12,
  color: "var(--vscode-editorWarning-foreground, #cca700)",
  flex: 1,
};

/** Apply status message text */
export const applyStatusTextStyle: React.CSSProperties = {
  fontSize: 12,
  flex: 1,
};

/** Apply status warning tone */
export const applyStatusWarningStyle: React.CSSProperties = {
  ...applyStatusTextStyle,
  color: "var(--vscode-editorWarning-foreground, #cca700)",
};

/** Apply status error tone */
export const applyStatusErrorStyle: React.CSSProperties = {
  ...applyStatusTextStyle,
  color: "var(--vscode-errorForeground)",
};

// ─── Error Status Bar Styles ─────────────────────────────────────────────────

/** Container for the mutation error bar */
export const errorStatusBarStyle: React.CSSProperties = {
  padding: "5px 12px",
  fontSize: 12,
  flexShrink: 0,
  background: "var(--vscode-inputValidation-errorBackground)",
  color: "var(--vscode-errorForeground)",
  borderBottom: "1px solid var(--vscode-inputValidation-errorBorder)",
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
};

/** Dismiss button for error status bar */
export const dismissErrorButtonStyle: React.CSSProperties = {
  background: "none",
  border: "none",
  cursor: "pointer",
  color: "inherit",
  opacity: 0.7,
};
