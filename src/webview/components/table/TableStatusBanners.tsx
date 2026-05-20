import React from "react";
import { Icon } from "../Icon";
import { type TableApplyStatus, tableButtonStyle } from "./tableViewHelpers";

interface TableStatusBannersProps {
  filterError: string | null;
  readError: string | null;
  showMissingPrimaryKeyNotice: boolean;
  onDismissFilterError: () => void;
  onDismissReadError: () => void;
}

export function TableStatusBanners({
  filterError,
  readError,
  showMissingPrimaryKeyNotice,
  onDismissFilterError,
  onDismissReadError,
}: TableStatusBannersProps) {
  return (
    <>
      {filterError && (
        <div
          role="status"
          aria-live="polite"
          style={{
            flexShrink: 0,
            padding: "6px 12px",
            fontSize: 12,
            background:
              "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
            borderBottom:
              "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
            color: "var(--vscode-editorWarning-foreground, #CCA700)",
            display: "flex",
            alignItems: "center",
            gap: 8,
          }}
        >
          <span style={{ fontWeight: 600 }}>⚠ Filter:</span>
          <span style={{ flex: 1 }}>{filterError}</span>
          <button
            type="button"
            aria-label="Dismiss filter error"
            style={{
              background: "none",
              border: "none",
              cursor: "pointer",
              color: "inherit",
              opacity: 0.7,
              fontSize: 14,
              lineHeight: 1,
              padding: "0 2px",
            }}
            title="Dismiss"
            onClick={onDismissFilterError}
          >
            ×
          </button>
        </div>
      )}
      {readError && (
        <div
          role="alert"
          style={{
            flexShrink: 0,
            padding: "6px 12px",
            fontSize: 12,
            background: "var(--vscode-inputValidation-errorBackground)",
            borderBottom: "1px solid var(--vscode-inputValidation-errorBorder)",
            color: "var(--vscode-errorForeground)",
            display: "flex",
            alignItems: "center",
            gap: 8,
          }}
        >
          <span style={{ fontWeight: 600 }}>Error:</span>
          <span style={{ flex: 1 }}>{readError}</span>
          <button
            type="button"
            aria-label="Dismiss read error"
            style={{
              background: "none",
              border: "none",
              cursor: "pointer",
              color: "inherit",
              opacity: 0.7,
              fontSize: 14,
              lineHeight: 1,
              padding: "0 2px",
            }}
            title="Dismiss"
            onClick={onDismissReadError}
          >
            ×
          </button>
        </div>
      )}
      {showMissingPrimaryKeyNotice && (
        <div
          role="alert"
          style={{
            flexShrink: 0,
            padding: "6px 12px",
            fontSize: 12,
            background:
              "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
            borderBottom:
              "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
            color: "var(--vscode-editorWarning-foreground, #CCA700)",
            display: "flex",
            alignItems: "center",
            gap: 8,
          }}
        >
          <Icon name="warning" size={13} style={{ flexShrink: 0 }} />
          <span>
            Reduced table mode: no unique key was detected for row binding, so
            editing fields and deleting rows are disabled.
          </span>
        </div>
      )}
    </>
  );
}

interface TableMutationStatusBarProps {
  applyStatus: TableApplyStatus | null;
  applying: boolean;
  inserting: boolean;
  insertValueCount: number;
  mutErr: string | null;
  newRowExists: boolean;
  readOnlyTable: boolean;
  unsavedRowCount: number;
  onApplyChanges: () => void;
  onDismissApplyStatus: () => void;
  onDismissMutationError: () => void;
  onRevertChanges: () => void;
}

export function TableMutationStatusBar({
  applyStatus,
  applying,
  inserting,
  insertValueCount,
  mutErr,
  newRowExists,
  readOnlyTable,
  unsavedRowCount,
  onApplyChanges,
  onDismissApplyStatus,
  onDismissMutationError,
  onRevertChanges,
}: TableMutationStatusBarProps) {
  if (readOnlyTable) {
    return null;
  }

  return (
    <>
      {(unsavedRowCount > 0 || applyStatus) && (
        <div
          style={{
            flexShrink: 0,
            padding: "0 12px",
            minHeight: 36,
            display: "flex",
            alignItems: "center",
            gap: 8,
            background:
              applyStatus?.tone === "error"
                ? "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.1))"
                : "rgba(200,150,0,0.08)",
            borderBottom: `1px solid ${
              applyStatus?.tone === "error"
                ? "var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.4))"
                : "rgba(200,150,0,0.3)"
            }`,
          }}
        >
          {unsavedRowCount > 0 && applyStatus?.tone !== "error" && (
            <span
              style={{
                fontSize: 12,
                color: "var(--vscode-editorWarning-foreground, #cca700)",
                flex: 1,
              }}
            >
              <Icon name="edit" size={12} style={{ marginRight: 4 }} />
              {unsavedRowCount} row{unsavedRowCount !== 1 ? "s" : ""} with
              unsaved changes
            </span>
          )}
          {applyStatus && (
            <span
              style={{
                fontSize: 12,
                color:
                  applyStatus.tone === "error"
                    ? "var(--vscode-errorForeground)"
                    : "var(--vscode-editorWarning-foreground, #cca700)",
                flex: 1,
              }}
            >
              <Icon name="warning" size={13} style={{ marginRight: 4 }} />
              {applyStatus.message}
            </span>
          )}
          {unsavedRowCount > 0 && (
            <>
              <button
                type="button"
                style={tableButtonStyle("warning", applying || inserting)}
                disabled={applying || inserting}
                title={
                  newRowExists && insertValueCount === 0
                    ? "Apply insert with database defaults, then updates"
                    : undefined
                }
                onClick={onApplyChanges}
              >
                {applying ? "Applying…" : "Apply Changes"}
              </button>
              <button
                type="button"
                style={tableButtonStyle("ghost", applying || inserting)}
                disabled={applying || inserting}
                onClick={onRevertChanges}
              >
                Revert All
              </button>
            </>
          )}
          {applyStatus && unsavedRowCount === 0 && (
            <button
              type="button"
              style={tableButtonStyle("ghost")}
              onClick={onDismissApplyStatus}
              title="Dismiss"
            >
              <Icon name="close" size={13} />
            </button>
          )}
        </div>
      )}

      {mutErr && (
        <div
          style={{
            padding: "5px 12px",
            fontSize: 12,
            flexShrink: 0,
            background: "var(--vscode-inputValidation-errorBackground)",
            color: "var(--vscode-errorForeground)",
            borderBottom: "1px solid var(--vscode-inputValidation-errorBorder)",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <span>
            <Icon name="warning" size={13} style={{ marginRight: 4 }} />
            {mutErr}
          </span>
          <button
            type="button"
            onClick={onDismissMutationError}
            title="Dismiss"
            style={{
              background: "none",
              border: "none",
              cursor: "pointer",
              color: "inherit",
              opacity: 0.7,
            }}
          >
            <Icon name="close" size={13} />
          </button>
        </div>
      )}
    </>
  );
}
