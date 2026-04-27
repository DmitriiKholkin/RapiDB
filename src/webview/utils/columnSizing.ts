let _ctx: CanvasRenderingContext2D | null = null;

function getCtx(): CanvasRenderingContext2D | null {
  if (_ctx) return _ctx;
  try {
    const canvas = document.createElement("canvas");
    const c = canvas.getContext("2d");
    if (c) {
      c.font = "12px monospace";
      _ctx = c;
    }
  } catch {}
  return _ctx;
}

function syncCtxFont(): void {
  const ctx = getCtx();
  if (!ctx) {
    return;
  }
  try {
    const style = getComputedStyle(document.documentElement);
    const size = style.getPropertyValue("--vscode-editor-font-size").trim();
    const family = style.getPropertyValue("--vscode-editor-font-family").trim();
    if (size && family) {
      ctx.font = `${size} ${family}`;
    }
  } catch {}
}

function measureText(s: string): number {
  const c = getCtx();
  return c ? c.measureText(s).width : s.length * 10;
}

export const COL_MIN = 100;
export const COL_MAX = 420;
const H_PAD = 16;
const SORT_ICON_W = 20;
const SAMPLE_ROWS = 50;

export interface ColSizeOpts {
  min?: number;
  max?: number;

  sampleRows?: number;
  hPad?: number;
}

export function calcColWidth(
  colName: string,
  isPrimaryKey: boolean,
  rows: Record<string, unknown>[],
  opts: ColSizeOpts = {},
  dataKey?: string,
): number {
  const min = opts.min ?? COL_MIN;
  const max = opts.max ?? COL_MAX;
  const sampleRows = opts.sampleRows ?? SAMPLE_ROWS;

  let maxContentW =
    measureText(isPrimaryKey ? `${colName}key` : colName) + SORT_ICON_W;

  const rowKey = dataKey ?? colName;
  const limit = Math.min(rows.length, sampleRows);
  for (let i = 0; i < limit; i++) {
    const val = rows[i][rowKey];
    const s = val === null || val === undefined ? "NULL" : String(val);
    const w = measureText(s);
    if (w > maxContentW) maxContentW = w;
  }

  const pad = opts.hPad ?? H_PAD;
  return Math.max(min, Math.min(max, Math.ceil(maxContentW + pad)));
}

export interface Column {
  name: string;

  dataKey?: string;
  isPrimaryKey: boolean;
}

export function calcColWidths(
  columns: Column[],
  rows: Record<string, unknown>[],
  opts?: ColSizeOpts,
): Record<string, number> {
  syncCtxFont();
  const result: Record<string, number> = {};
  for (const column of columns) {
    result[column.dataKey ?? column.name] = calcColWidth(
      column.name,
      column.isPrimaryKey,
      rows,
      opts,
      column.dataKey,
    );
  }
  return result;
}
