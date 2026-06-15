/**
 * Grid style injection — ensures table and query result styles are loaded.
 *
 * These styles were previously inlined at the top of TableGrid.tsx as
 * side-effectful module-level code. Extracting them here improves
 * readability and makes the dependency explicit.
 */

function injectStyles(id: string, css: string): void {
  if (typeof document === "undefined" || document.getElementById(id)) {
    return;
  }
  const el = document.createElement("style");
  el.id = id;
  el.textContent = css;
  document.head.appendChild(el);
}

// ─── Table Data Grid Styles ────────────────────────────────────────────────

injectStyles(
  "rapidb-table-row-style",
  [
    ".rdb-trow { transition: background 60ms; }",
    '.rdb-trow[data-even="true"]  { background: var(--vscode-editor-background); }',
    '.rdb-trow[data-even="false"] { background: var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04)); }',
    '.rdb-trow:not([data-selected="true"]):hover { background: var(--vscode-list-hoverBackground); }',
    ".rdb-tcell-selected { background: var(--vscode-editor-selectionBackground, rgba(38, 79, 120, 0.6)) !important; }",
    ".rdb-tcell-anchor { outline: 2px solid var(--vscode-focusBorder, #007fd4); outline-offset: -2px; }",
    ".rdb-tcell-border-top { box-shadow: inset 0 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-bottom { box-shadow: inset 0 -2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-left { box-shadow: inset 2px 0 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-right { box-shadow: inset -2px 0 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-top.rdb-tcell-border-left { box-shadow: inset 2px 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-top.rdb-tcell-border-right { box-shadow: inset -2px 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-bottom.rdb-tcell-border-left { box-shadow: inset 2px -2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-tcell-border-bottom.rdb-tcell-border-right { box-shadow: inset -2px -2px 0 var(--vscode-focusBorder, #007fd4); }",
  ].join("\n"),
);

// ─── Query Results Grid Styles ─────────────────────────────────────────────

injectStyles(
  "rapidb-results-row-style",
  [
    ".rdb-rrow { transition: background 60ms; }",
    '.rdb-rrow[data-even="true"]  { background: var(--vscode-editor-background); }',
    '.rdb-rrow[data-even="false"] { background: var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04)); }',
    ".rdb-rrow:hover { background: var(--vscode-list-hoverBackground); }",
    ".rdb-rcell-selected { background: var(--vscode-editor-selectionBackground, rgba(38, 79, 120, 0.6)) !important; }",
    ".rdb-rcell-anchor { outline: 2px solid var(--vscode-focusBorder, #007fd4); outline-offset: -2px; }",
    ".rdb-rcell-border-top { box-shadow: inset 0 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-bottom { box-shadow: inset 0 -2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-left { box-shadow: inset 2px 0 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-right { box-shadow: inset -2px 0 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-top.rdb-rcell-border-left { box-shadow: inset 2px 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-top.rdb-rcell-border-right { box-shadow: inset -2px 2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-bottom.rdb-rcell-border-left { box-shadow: inset 2px -2px 0 var(--vscode-focusBorder, #007fd4); }",
    ".rdb-rcell-border-bottom.rdb-rcell-border-right { box-shadow: inset -2px -2px 0 var(--vscode-focusBorder, #007fd4); }",
  ].join("\n"),
);

// ─── Column Drag Styles ────────────────────────────────────────────────────

injectStyles(
  "rapidb-column-drag-style",
  [
    "th[data-column-id] { cursor: grab; }",
    "th[data-column-id]:active { cursor: grabbing; }",
    '[data-column-dragging="true"] {',
    "  cursor: grabbing !important;",
    "  user-select: none !important;",
    "  background: var(--vscode-editor-selectionBackground, rgba(38, 79, 120, 0.6)) !important;",
    "  border-color: transparent !important;",
    "}",
    ".rapidb-column-drag-ghost {",
    "  position: fixed;",
    "  z-index: 10000;",
    "  pointer-events: none;",
    "  background: var(--vscode-editorGroupHeader-tabsBackground);",
    "  border: 1px solid var(--vscode-focusBorder);",
    "  box-shadow: 0 4px 12px rgba(0,0,0,0.3);",
    "  display: flex;",
    "  align-items: center;",
    "  gap: 4px;",
    "  padding: 0 8px;",
    "  height: 26px;",
    "  font-size: 12px;",
    "  font-weight: 600;",
    "  font-family: var(--vscode-editor-font-family, monospace);",
    "  white-space: nowrap;",
    "  opacity: 0.95;",
    "  border-radius: 3px;",
    "}",
  ].join("\n"),
);
