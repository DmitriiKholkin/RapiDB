import type React from "react";

export const bannerLayoutStyle: React.CSSProperties = {
  flexShrink: 0,
  padding: "6px 12px",
  fontSize: 12,
  display: "flex",
  alignItems: "center",
  gap: 8,
};

export const warningBannerToneStyle: React.CSSProperties = {
  background:
    "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
  borderBottom:
    "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
  color: "var(--vscode-editorWarning-foreground, #CCA700)",
};

export const errorBannerToneStyle: React.CSSProperties = {
  background: "var(--vscode-inputValidation-errorBackground)",
  borderBottom: "1px solid var(--vscode-inputValidation-errorBorder)",
  color: "var(--vscode-errorForeground)",
};

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

export const mutationStatusBarStyle: React.CSSProperties = {
  flexShrink: 0,
  padding: "0 12px",
  minHeight: 36,
  display: "flex",
  alignItems: "center",
  gap: 8,
};

export const mutationStatusBarErrorStyle: React.CSSProperties = {
  background:
    "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.1))",
  borderBottom:
    "1px solid var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.4))",
};

export const mutationStatusBarWarningStyle: React.CSSProperties = {
  background: "rgba(200,150,0,0.08)",
  borderBottom: "1px solid rgba(200,150,0,0.3)",
};

const undoRedoButtonBaseStyle: React.CSSProperties = {
  background: "none",
  border: "none",
  padding: "0 2px",
  display: "flex",
  alignItems: "center",
};

export const undoButtonEnabledStyle: React.CSSProperties = {
  ...undoRedoButtonBaseStyle,
  cursor: "pointer",
  color: "var(--vscode-editorWarning-foreground, #cca700)",
  opacity: 0.9,
};

export const undoButtonDisabledStyle: React.CSSProperties = {
  ...undoRedoButtonBaseStyle,
  cursor: "default",
  color: "var(--vscode-descriptionForeground, #888)",
  opacity: 0.4,
};

export const redoButtonEnabledStyle: React.CSSProperties = {
  ...undoRedoButtonBaseStyle,
  cursor: "pointer",
  color: "var(--vscode-editorWarning-foreground, #cca700)",
  opacity: 0.9,
};

export const redoButtonDisabledStyle: React.CSSProperties = {
  ...undoRedoButtonBaseStyle,
  cursor: "default",
  color: "var(--vscode-descriptionForeground, #888)",
  opacity: 0.4,
};

export const undoRedoSeparatorStyle: React.CSSProperties = {
  width: 1,
  height: 14,
  background: "rgba(200,150,0,0.3)",
  flexShrink: 0,
};

export const unsavedCountStyle: React.CSSProperties = {
  fontSize: 12,
  color: "var(--vscode-editorWarning-foreground, #cca700)",
  flex: 1,
};

export const applyStatusTextStyle: React.CSSProperties = {
  fontSize: 12,
  flex: 1,
};

export const applyStatusWarningStyle: React.CSSProperties = {
  ...applyStatusTextStyle,
  color: "var(--vscode-editorWarning-foreground, #cca700)",
};

export const applyStatusErrorStyle: React.CSSProperties = {
  ...applyStatusTextStyle,
  color: "var(--vscode-errorForeground)",
};

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

export const dismissErrorButtonStyle: React.CSSProperties = {
  background: "none",
  border: "none",
  cursor: "pointer",
  color: "inherit",
  opacity: 0.7,
};
