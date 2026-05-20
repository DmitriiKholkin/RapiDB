import React, { useCallback, useEffect, useId, useRef } from "react";
import type { TableMutationPreviewPayload } from "../../../shared/webviewContracts";
import { Icon } from "../Icon";
import { MonacoEditor } from "../MonacoEditor";
import { PREVIEW_DIALOG_EDITOR_H, tableButtonStyle } from "./tableViewHelpers";

const DIALOG_FOCUSABLE_SELECTOR =
  'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

function getFocusableElements(dialog: HTMLDivElement | null): HTMLElement[] {
  if (!dialog) {
    return [];
  }

  return Array.from(
    dialog.querySelectorAll<HTMLElement>(DIALOG_FOCUSABLE_SELECTOR),
  );
}

function useDialogFocusTrap(options: {
  dialogRef: React.RefObject<HTMLDivElement | null>;
  initialFocusRef?: React.RefObject<HTMLElement | null>;
  onEscape?: () => void;
}) {
  const { dialogRef, initialFocusRef, onEscape } = options;

  useEffect(() => {
    const previousActiveElement =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;

    initialFocusRef?.current?.focus();

    return () => {
      previousActiveElement?.focus();
    };
  }, [initialFocusRef]);

  return useCallback(
    (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (event.key === "Escape") {
        onEscape?.();
        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const dialog = dialogRef.current;
      const focusableElements = getFocusableElements(dialog);

      if (focusableElements.length === 0) {
        event.preventDefault();
        dialog?.focus();
        return;
      }

      const firstElement = focusableElements[0];
      const lastElement = focusableElements[focusableElements.length - 1];
      const activeElement =
        document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;

      if (!activeElement || !dialog?.contains(activeElement)) {
        event.preventDefault();
        firstElement.focus();
        return;
      }

      if (event.shiftKey && activeElement === firstElement) {
        event.preventDefault();
        lastElement.focus();
        return;
      }

      if (!event.shiftKey && activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    },
    [dialogRef, onEscape],
  );
}

interface ExportChoiceDialogProps {
  format: "csv" | "json";
  visibleCount: number;
  totalCount: number;
  onCancel: () => void;
  onExportAll: () => void;
  onExportVisible: () => void;
}

function ExportChoiceDialog({
  format,
  visibleCount,
  totalCount,
  onCancel,
  onExportAll,
  onExportVisible,
}: ExportChoiceDialogProps) {
  const titleId = useId();
  const descriptionId = useId();
  const dialogRef = useRef<HTMLDivElement>(null);
  const cancelButtonRef = useRef<HTMLButtonElement>(null);
  const handleKeyDown = useDialogFocusTrap({
    dialogRef,
    initialFocusRef: cancelButtonRef,
    onEscape: onCancel,
  });
  const extension = format.toUpperCase();

  return (
    <div
      style={{
        position: "absolute",
        inset: 0,
        zIndex: 30,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
        background: "rgba(0, 0, 0, 0.36)",
        backdropFilter: "blur(2px)",
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        tabIndex={-1}
        ref={dialogRef}
        onKeyDown={handleKeyDown}
        style={{
          width: "min(480px, calc(100vw - 48px))",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          borderRadius: 6,
          border: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editor-background)",
          boxShadow: "0 18px 48px rgba(0, 0, 0, 0.42)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
            padding: "14px 16px 12px",
            borderBottom: "1px solid var(--vscode-panel-border)",
            background: "var(--vscode-editorGroupHeader-tabsBackground)",
          }}
        >
          <div id={titleId} style={{ fontSize: 13, fontWeight: 600 }}>
            Export {extension}
          </div>
          <button
            type="button"
            ref={cancelButtonRef}
            onClick={onCancel}
            aria-label="Close export dialog"
            style={{
              background: "transparent",
              border: "none",
              color: "var(--vscode-foreground)",
              cursor: "pointer",
              padding: 0,
              opacity: 0.8,
            }}
          >
            <Icon name="close" size={14} />
          </button>
        </div>

        <div
          style={{
            padding: "16px",
            display: "flex",
            flexDirection: "column",
            gap: 16,
          }}
        >
          <p
            id={descriptionId}
            style={{ margin: 0, fontSize: 13, lineHeight: 1.5 }}
          >
            The table has <strong>{totalCount.toLocaleString()} rows</strong> in
            total, but only{" "}
            <strong>{visibleCount.toLocaleString()} rows</strong> are currently
            visible on this page. Choose what to export:
          </p>
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: 8,
            }}
          >
            <button
              type="button"
              style={{
                ...tableButtonStyle("ghost"),
                justifyContent: "flex-start",
              }}
              onClick={onExportVisible}
            >
              Export visible ({visibleCount.toLocaleString()} rows)
            </button>
            <button
              type="button"
              style={{
                ...tableButtonStyle("ghost"),
                justifyContent: "flex-start",
              }}
              onClick={onExportAll}
            >
              Export full table ({totalCount.toLocaleString()} rows)
              <span style={{ opacity: 0.65, fontSize: 11, marginLeft: 6 }}>
                ⚠ may be a heavy operation
              </span>
            </button>
          </div>
          <div style={{ display: "flex", justifyContent: "flex-end" }}>
            <button
              type="button"
              style={tableButtonStyle("ghost")}
              onClick={onCancel}
            >
              Cancel
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

interface MutationPreviewDialogProps {
  preview: TableMutationPreviewPayload;
  onCancel: () => void;
  onConfirm: () => void;
}

function MutationPreviewDialog({
  preview,
  onCancel,
  onConfirm,
}: MutationPreviewDialogProps) {
  const confirmLabel =
    preview.kind === "applyChanges"
      ? "Apply Changes"
      : preview.kind === "insertRow"
        ? "Insert Row"
        : "Apply Changes";
  const titleId = useId();
  const descriptionId = useId();
  const dialogRef = useRef<HTMLDivElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const handleDialogKeyDown = useDialogFocusTrap({
    dialogRef,
    initialFocusRef: closeButtonRef,
  });

  return (
    <div
      style={{
        position: "absolute",
        inset: 0,
        zIndex: 30,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
        background: "rgba(0, 0, 0, 0.36)",
        backdropFilter: "blur(2px)",
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        tabIndex={-1}
        ref={dialogRef}
        onKeyDown={handleDialogKeyDown}
        style={{
          width: "min(920px, calc(100vw - 48px))",
          maxHeight: "calc(100vh - 48px)",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          borderRadius: 6,
          border: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editor-background)",
          boxShadow: "0 18px 48px rgba(0, 0, 0, 0.42)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "flex-start",
            justifyContent: "space-between",
            gap: 12,
            padding: "14px 16px 12px",
            borderBottom: "1px solid var(--vscode-panel-border)",
            background: "var(--vscode-editorGroupHeader-tabsBackground)",
          }}
        >
          <div style={{ minWidth: 0 }}>
            <div id={titleId} style={{ fontSize: 13, fontWeight: 600 }}>
              {preview.title}
            </div>
            <div
              id={descriptionId}
              style={{ marginTop: 4, fontSize: 11, opacity: 0.75 }}
            >
              {preview.statementCount} statement
              {preview.statementCount === 1 ? "" : "s"} will be executed. The
              preview below is read only and matches the prepared mutation plan.
            </div>
          </div>
          <button
            type="button"
            ref={closeButtonRef}
            onClick={onCancel}
            aria-label="Close mutation preview"
            style={{
              background: "transparent",
              border: "none",
              color: "var(--vscode-foreground)",
              cursor: "pointer",
              padding: 0,
              opacity: 0.8,
            }}
          >
            <Icon name="close" size={14} />
          </button>
        </div>

        <div
          style={{
            padding: "12px 16px 16px",
            display: "flex",
            flexDirection: "column",
            gap: 12,
          }}
        >
          <div
            style={{
              height: PREVIEW_DIALOG_EDITOR_H,
              border: "1px solid var(--vscode-panel-border)",
              borderRadius: 4,
              overflow: "hidden",
            }}
          >
            <MonacoEditor
              key={preview.previewToken}
              initialValue={preview.text ?? preview.sql}
              height="100%"
              readOnly
              language={
                preview.contentType === "application/json"
                  ? "json"
                  : preview.contentType === "application/sql"
                    ? "sql"
                    : "plaintext"
              }
              ariaLabel="Mutation preview"
            />
          </div>

          <div style={{ display: "flex", justifyContent: "flex-end", gap: 8 }}>
            <button
              type="button"
              style={tableButtonStyle("ghost")}
              onClick={onCancel}
            >
              Cancel
            </button>
            <button
              type="button"
              style={tableButtonStyle("primary")}
              onClick={onConfirm}
            >
              {confirmLabel}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

interface TableDialogsProps {
  exportChoice: {
    format: "csv" | "json";
  } | null;
  mutationPreview: TableMutationPreviewPayload | null;
  rowsLength: number;
  totalCount: number;
  onCancelExport: () => void;
  onCancelMutationPreview: () => void;
  onConfirmMutationPreview: () => void;
  onExportAll: () => void;
  onExportVisible: () => void;
}

export function TableDialogs({
  exportChoice,
  mutationPreview,
  rowsLength,
  totalCount,
  onCancelExport,
  onCancelMutationPreview,
  onConfirmMutationPreview,
  onExportAll,
  onExportVisible,
}: TableDialogsProps) {
  return (
    <>
      {mutationPreview && (
        <MutationPreviewDialog
          preview={mutationPreview}
          onCancel={onCancelMutationPreview}
          onConfirm={onConfirmMutationPreview}
        />
      )}
      {exportChoice && (
        <ExportChoiceDialog
          format={exportChoice.format}
          visibleCount={rowsLength}
          totalCount={totalCount}
          onExportVisible={onExportVisible}
          onExportAll={onExportAll}
          onCancel={onCancelExport}
        />
      )}
    </>
  );
}
