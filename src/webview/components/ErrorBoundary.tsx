// biome-ignore lint/style/useImportType: React needed for JSX
import React from "react";

interface Props {
  context?: string;
  children: React.ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo): void {
    const ctx = this.props.context ?? "unknown";
    console.error(
      `[RapiDB] Unhandled render error in <${ctx}>:`,
      error,
      info.componentStack,
    );
  }

  private handleRetry = (): void => {
    this.setState({ hasError: false, error: null });
  };

  render(): React.ReactNode {
    if (!this.state.hasError) {
      return this.props.children;
    }

    const ctx = this.props.context ?? "View";
    const msg = this.state.error?.message ?? "Unknown error";

    return (
      <div
        style={{
          margin: 16,
          padding: "14px 16px",
          borderRadius: 4,
          background: "var(--vscode-inputValidation-errorBackground)",
          border: "1px solid var(--vscode-inputValidation-errorBorder)",
          color: "var(--vscode-errorForeground)",
          fontFamily: "var(--vscode-font-family, system-ui, sans-serif)",
          fontSize: 13,
          display: "flex",
          flexDirection: "column",
          gap: 10,
        }}
      >
        <strong style={{ fontSize: 14 }}>⚠ {ctx} crashed</strong>
        <pre
          style={{
            margin: 0,
            padding: "8px 10px",
            borderRadius: 3,
            fontSize: 12,
            background: "rgba(0,0,0,0.15)",
            overflowX: "auto",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {msg}
        </pre>
        <div style={{ display: "flex", gap: 8 }}>
          <button
            type="button"
            onClick={this.handleRetry}
            style={{
              padding: "4px 14px",
              fontSize: 12,
              borderRadius: 2,
              cursor: "pointer",
              fontFamily: "inherit",
              background: "var(--vscode-button-background)",
              color: "var(--vscode-button-foreground)",
              border: "none",
            }}
          >
            Retry
          </button>
          <span style={{ fontSize: 11, opacity: 0.6, alignSelf: "center" }}>
            Check the Developer Console (Help → Toggle Developer Tools) for the
            full stack trace.
          </span>
        </div>
      </div>
    );
  }
}
