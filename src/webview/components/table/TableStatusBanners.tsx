import React from "react";
import { Icon } from "../Icon";
import { type TableApplyStatus, tableButtonStyle } from "./tableViewHelpers";

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
      {showMissingPrimaryKeyNotice && (
        <div
          role="alert"
          style={{ ...bannerLayoutStyle, ...warningBannerToneStyle }}
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

function platformShortcutLabel(): string {
  const isMac =
    typeof navigator !== "undefined" &&
    navigator.platform.toUpperCase().includes("MAC");
  return isMac ? "⌘" : "Ctrl+";
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
  canUndo?: boolean;
  canRedo?: boolean;
  onApplyChanges: () => void;
  onDismissApplyStatus: () => void;
  onDismissMutationError: () => void;
  onRevertChanges: () => void;
  onUndo?: () => void;
  onRedo?: () => void;
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
  canUndo = false,
  canRedo = false,
  onApplyChanges,
  onDismissApplyStatus,
  onDismissMutationError,
  onRevertChanges,
  onUndo,
  onRedo,
}: TableMutationStatusBarProps) {
  if (readOnlyTable) {
    return null;
  }

  const shortcut = platformShortcutLabel();

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
            <>
              <button
                type="button"
                aria-label="Undo"
                title={`Undo (${shortcut}Z)`}
                disabled={!canUndo || applying || inserting}
                style={{
                  background: "none",
                  border: "none",
                  cursor:
                    canUndo && !applying && !inserting ? "pointer" : "default",
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
                disabled={!canRedo || applying || inserting}
                style={{
                  background: "none",
                  border: "none",
                  cursor:
                    canRedo && !applying && !inserting ? "pointer" : "default",
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
                {unsavedRowCount} row{unsavedRowCount !== 1 ? "s" : ""} with
                unsaved changes
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
