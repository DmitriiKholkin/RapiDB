import React from "react";

interface ErrorPanelProps {
  message: string;
  title?: string;
  style?: React.CSSProperties;
}

export function ErrorPanel({
  message,
  title = "Error",
  style,
}: ErrorPanelProps): React.ReactElement {
  return (
    <div
      style={{
        margin: 12,
        padding: "10px 14px",
        borderRadius: 3,
        fontSize: 13,
        background: "var(--vscode-inputValidation-errorBackground)",
        border: "1px solid var(--vscode-inputValidation-errorBorder)",
        color: "var(--vscode-errorForeground)",
        ...style,
      }}
    >
      <strong>{title}:</strong> {message}
    </div>
  );
}
