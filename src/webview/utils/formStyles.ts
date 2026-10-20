import type React from "react";

export const flexRowStyle: React.CSSProperties = {
  display: "flex",
  gap: 8,
  alignItems: "center",
};

export const flexRowFullWidthStyle: React.CSSProperties = {
  ...flexRowStyle,
  width: "100%",
};

export const flexRowWithGapStyle: React.CSSProperties = {
  display: "flex",
  gap: 8,
  marginBottom: 14,
};

export const toggleFlexRowStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 6,
};

export const buttonRowStyle: React.CSSProperties = {
  display: "flex",
  gap: 6,
  alignItems: "center",
};

export const tlsSectionStyle: React.CSSProperties = {
  borderTop: "1px solid var(--vscode-panel-border)",
  paddingTop: 14,
  marginTop: 4,
};

export const tlsConfigFieldsStyle: React.CSSProperties = {
  border: "1px solid var(--vscode-panel-border)",
  borderRadius: 6,
  padding: "10px 12px 0",
  marginBottom: 14,
  background:
    "var(--vscode-editorWidget-background, var(--vscode-input-background))",
};

export const tlsEncryptionLabelStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  textTransform: "uppercase",
  letterSpacing: 0.5,
  opacity: 0.45,
};

export const testStatusBaseStyle: React.CSSProperties = {
  marginBottom: 10,
  padding: "8px 12px",
  borderRadius: 5,
  fontSize: 12,
  display: "flex",
  alignItems: "center",
  gap: 7,
};

export const testStatusOkStyle: React.CSSProperties = {
  background: "rgba(50,180,50,0.1)",
  color: "var(--vscode-testing-iconPassed, #4ec94e)",
  border: "1px solid rgba(50,180,50,0.25)",
};

export const testStatusFailStyle: React.CSSProperties = {
  background: "var(--vscode-inputValidation-errorBackground)",
  color: "var(--vscode-errorForeground)",
  border: "1px solid var(--vscode-inputValidation-errorBorder)",
};

export const testStatusTestingStyle: React.CSSProperties = {
  opacity: 0.6,
};

export const colorPickerLabelStyle: React.CSSProperties = {
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  width: 28,
  height: 28,
  borderRadius: 8,
  border:
    "1px solid var(--vscode-input-border, var(--vscode-widget-border, #555))",
  background: "var(--vscode-input-background)",
  cursor: "pointer",
  position: "relative",
  flexShrink: 0,
  overflow: "hidden",
};

export const colorInputHiddenStyle: React.CSSProperties = {
  opacity: 0,
  position: "absolute",
  inset: 0,
  width: "100%",
  height: "100%",
  cursor: "pointer",
  border: "none",
  padding: 0,
};
