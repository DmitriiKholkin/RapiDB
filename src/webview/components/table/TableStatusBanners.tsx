import type React from "react";
import { Icon } from "../Icon";
import {
  type PendingRestoreEntry,
  type TableApplyStatus,
  tableButtonStyle,
} from "./tableViewHelpers";

const bannerLayoutStyle: React.CSSProperties = {
  flexShrink: 0,
  padding: "6px 12px",
  fontSize: 12,
  display: "flex",
  alignItems: "center",
  gap: 8,
};

const warningBannerToneStyle: React.CSSProperties = {
  background:
    "var(--vscode-inputValidation-warningBackground, rgba(180,120,0,0.15))",
  borderBottom:
    "1px solid var(--vscode-inputValidation-warningBorder, rgba(180,120,0,0.4))",
  color: "var(--vscode-editorWarning-foreground, #CCA700)",
};

const errorBannerToneStyle: React.CSSProperties = {
  background: "var(--vscode-inputValidation-errorBackground)",
  borderBottom: "1px solid var(--vscode-inputValidation-errorBorder)",
  color: "var(--vscode-errorForeground)",
};

const dismissTextButtonStyle: React.CSSProperties = {
  background: "none",
  border: "none",
  cursor: "pointer",
  color: "inherit",
  opacity: 0.7,
  fontSize: 14,
  lineHeight: 1,
  padding: "0 2px",
};

interface BannerDismissButtonProps {
  ariaLabel: string;
  onClick: () => void;
}

function BannerDismissButton({ ariaLabel, onClick }: BannerDismissButtonProps) {
  return (
    <button
      type="button"
      aria-label={ariaLabel}
      style={dismissTextButtonStyle}
      title="Dismiss"
      onClick={onClick}
    >
      ×
    </button>
  );
}

interface TableStatusBannersProps {
  schemaWarning?: string | null;
  rowMutationBlockReason?: string | null;
  filterError: string | null;
  readError: string | null;
  showMissingPrimaryKeyNotice: boolean;
  onDismissFilterError: () => void;
  onDismissReadError: () => void;
}

export function TableStatusBanners({
  schemaWarning,
  rowMutationBlockReason,
  filterError,
  readError,
  showMissingPrimaryKeyNotice,
  onDismissFilterError,
  onDismissReadError,
}: TableStatusBannersProps) {
  return (
    <>
      {schemaWarning && (
        <div
          role="alert"
          style={{ ...bannerLayoutStyle, ...warningBannerToneStyle }}
        >
          {schemaWarning}
        </div>
      )}
      {filterError && (
        <div
          role="status"
          aria-live="polite"
          style={{ ...bannerLayoutStyle, ...warningBannerToneStyle }}
        >
          <span style={{ fontWeight: 600 }}>⚠ Filter:</span>
          <span style={{ flex: 1 }}>{filterError}</span>
          <BannerDismissButton
            ariaLabel="Dismiss filter error"
            onClick={onDismissFilterError}
          />
        </div>
      )}
      {readError && (
        <div
          role="alert"
          style={{ ...bannerLayoutStyle, ...errorBannerToneStyle }}
        >
          <span style={{ fontWeight: 600 }}>Error:</span>
          <span style={{ flex: 1 }}>{readError}</span>
          <BannerDismissButton
            ariaLabel="Dismiss read error"
            onClick={onDismissReadError}
          />
        </div>
      )}
      {(rowMutationBlockReason || showMissingPrimaryKeyNotice) && (
        <div
          role="alert"
          style={{ ...bannerLayoutStyle, ...warningBannerToneStyle }}
        >
          <Icon name="warning" size={13} style={{ flexShrink: 0 }} />
          <span>
            {rowMutationBlockReason ??
              "Reduced table mode: no unique key was detected for row binding, so editing fields and deleting rows are disabled."}
          </span>
        </div>
      )}
    </>
  );
}

function platformShortcutLabel(): string {
  const isMac =
    typeof navigator !== "undefined" &&
    navigator.platform.toUpperCase().includes("MAC");
  return isMac ? "⌘" : "Ctrl+";
}

interface TableMutationStatusBarProps {
  schemaBlocked?: boolean;
  reconciliationPending?: boolean;
  applyStatus: TableApplyStatus | null;
  applying: boolean;
  loading: boolean;
  insertValueCount: number;
  mutErr: string | null;
  newRowExists: boolean;
  readOnlyTable: boolean;
  unsavedRowCount: number;
  unresolvedPendingCount?: number;
  unresolvedPendingColumns?: readonly string[];
  conflictDrafts?: readonly PendingRestoreEntry[];
  unmatchablePendingCount?: number;
  canUndo?: boolean;
  canRedo?: boolean;
  onApplyChanges: () => void;
  onDismissApplyStatus: () => void;
  onDismissMutationError: () => void;
  onAcknowledgeDeleteRecovery?: () => void;
  onRevertChanges: () => void;
  onDiscardUnresolved?: () => void;
  onUndo?: () => void;
  onRedo?: () => void;
}

export function TableMutationStatusBar({
  schemaBlocked = false,
  reconciliationPending = false,
  applyStatus,
  applying,
  loading,
  insertValueCount,
  mutErr,
  newRowExists,
  readOnlyTable,
  unsavedRowCount,
  unresolvedPendingCount = 0,
  unresolvedPendingColumns = [],
  conflictDrafts = [],
  unmatchablePendingCount = 0,
  canUndo = false,
  canRedo = false,
  onApplyChanges,
  onDismissApplyStatus,
  onDismissMutationError,
  onAcknowledgeDeleteRecovery,
  onRevertChanges,
  onDiscardUnresolved,
  onUndo,
  onRedo,
}: TableMutationStatusBarProps) {
  const shortcut = platformShortcutLabel();
  const hasHistory = !readOnlyTable && (canUndo || canRedo);

  return (
    <>
      {unresolvedPendingCount > 0 && (
        <div
          role="alert"
          style={{ ...bannerLayoutStyle, ...warningBannerToneStyle }}
        >
          <Icon name="warning" size={13} style={{ flexShrink: 0 }} />
          <span style={{ flex: 1 }}>
            {conflictDrafts.length === unresolvedPendingCount
              ? `${unresolvedPendingCount} conflicting row draft${unresolvedPendingCount === 1 ? " is" : "s are"} preserved and not attached to the refreshed rows. Apply Changes is blocked until you resolve them.`
              : unmatchablePendingCount === unresolvedPendingCount
                ? `${unresolvedPendingCount} row${unresolvedPendingCount === 1 ? "" : "s"} of unsaved changes lost the original row identity and cannot be matched again, even after refresh. Apply Changes is blocked; discard the unmatched changes or use Revert All.`
                : `Unable to match ${unresolvedPendingCount} row${unresolvedPendingCount === 1 ? "" : "s"} of unverified changes safely after refresh. The edits are preserved but not attached to a displayed row; Apply Changes is blocked. Refresh to check again or use Revert All to discard them.`}
            {unmatchablePendingCount > 0 &&
              unmatchablePendingCount < unresolvedPendingCount &&
              ` ${unmatchablePendingCount} of them lost the original row identity and cannot be matched by refreshing.`}
            {unresolvedPendingColumns.length > 0 && (
              <> Changed fields: {unresolvedPendingColumns.join(", ")}.</>
            )}
          </span>
          {onDiscardUnresolved && (
            <button
              type="button"
              style={tableButtonStyle("ghost", applying || loading)}
              disabled={applying || loading}
              onClick={onDiscardUnresolved}
            >
              Discard Unmatched
            </button>
          )}
        </div>
      )}
      {conflictDrafts.length > 0 && (
        <details
          style={{
            ...warningBannerToneStyle,
            flexShrink: 0,
            maxHeight: "25vh",
            overflow: "auto",
            padding: "6px 12px",
            fontSize: 12,
          }}
        >
          <summary>Review conflicting drafts</summary>
          <p>
            The database values are shown in the grid. A row may have moved,
            been removed, or been replaced. Copy any draft values you want to
            keep, discard the unmatched drafts, then edit the verified row
            again. Refresh does not apply or rebase these drafts.
          </p>
          <ol>
            {conflictDrafts.map((entry) => (
              <li key={entry.originalSignature}>
                Original row key: <code>{entry.originalSignature}</code>
                <ul>
                  {[...entry.changes].map(([column, draft]) => (
                    <li key={column}>
                      {column}: original{" "}
                      <code>
                        {formatConflictValue(
                          entry.originalValues?.get(column),
                        ) ??
                          (entry.originalMissingFields?.has(column)
                            ? "MISSING"
                            : "undefined")}
                      </code>
                      {" · "}draft{" "}
                      <code>{formatConflictValue(draft) ?? "undefined"}</code>
                    </li>
                  ))}
                </ul>
              </li>
            ))}
          </ol>
        </details>
      )}
      {(unsavedRowCount > 0 || hasHistory || applyStatus) && (
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
          {readOnlyTable &&
            unsavedRowCount > 0 &&
            applyStatus?.tone !== "error" && (
              <span
                style={{
                  fontSize: 12,
                  color: "var(--vscode-editorWarning-foreground, #cca700)",
                  flex: 1,
                }}
              >
                <Icon name="edit" size={12} style={{ marginRight: 4 }} />
                {`${unsavedRowCount} row${unsavedRowCount !== 1 ? "s" : ""} with unsaved changes. The table is read-only, so they cannot be applied; use Revert All to discard them.`}
              </span>
            )}
          {(hasHistory || (!readOnlyTable && unsavedRowCount > 0)) &&
            applyStatus?.tone !== "error" && (
              <>
                <button
                  type="button"
                  aria-label="Undo"
                  title={`Undo (${shortcut}Z)`}
                  disabled={
                    !canUndo || applying || loading || reconciliationPending
                  }
                  style={{
                    background: "none",
                    border: "none",
                    cursor:
                      canUndo && !applying && !loading && !reconciliationPending
                        ? "pointer"
                        : "default",
                    color: canUndo
                      ? "var(--vscode-editorWarning-foreground, #cca700)"
                      : "var(--vscode-descriptionForeground, #888)",
                    opacity: canUndo ? 0.9 : 0.4,
                    padding: "0 2px",
                    display: "flex",
                    alignItems: "center",
                  }}
                  onClick={onUndo}
                >
                  <Icon name="discard" size={14} />
                </button>
                <button
                  type="button"
                  aria-label="Redo"
                  title={`Redo (${shortcut}Shift+Z)`}
                  disabled={
                    !canRedo || applying || loading || reconciliationPending
                  }
                  style={{
                    background: "none",
                    border: "none",
                    cursor:
                      canRedo && !applying && !loading && !reconciliationPending
                        ? "pointer"
                        : "default",
                    color: canRedo
                      ? "var(--vscode-editorWarning-foreground, #cca700)"
                      : "var(--vscode-descriptionForeground, #888)",
                    opacity: canRedo ? 0.9 : 0.4,
                    padding: "0 2px",
                    display: "flex",
                    alignItems: "center",
                  }}
                  onClick={onRedo}
                >
                  <Icon name="redo" size={14} />
                </button>
                <span
                  style={{
                    width: 1,
                    height: 14,
                    background: "rgba(200,150,0,0.3)",
                    flexShrink: 0,
                  }}
                />
                <span
                  style={{
                    fontSize: 12,
                    color: "var(--vscode-editorWarning-foreground, #cca700)",
                    flex: 1,
                  }}
                >
                  <Icon name="edit" size={12} style={{ marginRight: 4 }} />
                  {unsavedRowCount > 0
                    ? `${unsavedRowCount} row${unsavedRowCount !== 1 ? "s" : ""} with unsaved changes`
                    : "Undo/redo history retained"}
                </span>
              </>
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
          {!readOnlyTable && unsavedRowCount > 0 && (
            <button
              type="button"
              style={tableButtonStyle(
                "warning",
                applying || loading || schemaBlocked,
              )}
              disabled={applying || loading || schemaBlocked}
              title={
                newRowExists && insertValueCount === 0
                  ? "Apply inserts with database defaults, then updates"
                  : undefined
              }
              onClick={onApplyChanges}
            >
              {applying ? "Applying…" : "Apply Changes"}
            </button>
          )}
          {(unsavedRowCount > 0 || hasHistory) && (
            <button
              type="button"
              style={tableButtonStyle("ghost", applying || loading)}
              disabled={applying || loading}
              onClick={onRevertChanges}
            >
              Revert All
            </button>
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
          {onAcknowledgeDeleteRecovery && (
            <button
              type="button"
              style={tableButtonStyle("ghost")}
              onClick={onAcknowledgeDeleteRecovery}
            >
              Acknowledge delete warning
            </button>
          )}
          <button
            type="button"
            onClick={onDismissMutationError}
            aria-label="Dismiss mutation error"
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

function formatConflictValue(value: unknown): string | undefined {
  return JSON.stringify(value, (_key, part) =>
    typeof part === "bigint" ? `${part}n` : part,
  );
}
