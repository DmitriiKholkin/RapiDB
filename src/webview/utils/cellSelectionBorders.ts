export interface CellRange {
  anchorRow: number;
  anchorCol: number;
  activeRow: number;
  activeCol: number;
}

export interface CellBorders {
  top: boolean;
  bottom: boolean;
  left: boolean;
  right: boolean;
}

export interface CellSelectionState extends CellBorders {
  selected: boolean;
  anchor: boolean;
}

function normalizeBounds(range: CellRange | null): {
  minRow: number;
  maxRow: number;
  minCol: number;
  maxCol: number;
} | null {
  if (!range) {
    return null;
  }
  return {
    minRow: Math.min(range.anchorRow, range.activeRow),
    maxRow: Math.max(range.anchorRow, range.activeRow),
    minCol: Math.min(range.anchorCol, range.activeCol),
    maxCol: Math.max(range.anchorCol, range.activeCol),
  };
}

export function classifyCellSelection(
  range: CellRange | null,
  rowIndex: number,
  colIndex: number,
  isSelected: boolean,
  isAnchor: boolean,
): CellSelectionState {
  const bounds = normalizeBounds(range);
  if (!bounds) {
    return {
      selected: false,
      anchor: false,
      top: false,
      bottom: false,
      left: false,
      right: false,
    };
  }
  return {
    selected: isSelected,
    anchor: isAnchor,
    top: isSelected && rowIndex === bounds.minRow,
    bottom: isSelected && rowIndex === bounds.maxRow,
    left: isSelected && colIndex === bounds.minCol,
    right: isSelected && colIndex === bounds.maxCol,
  };
}

const TABLE_SELECTED_CLASS = "rdb-tcell-selected";
const TABLE_ANCHOR_CLASS = "rdb-tcell-anchor";
const TABLE_TOP_BORDER_CLASS = "rdb-tcell-border-top";
const TABLE_BOTTOM_BORDER_CLASS = "rdb-tcell-border-bottom";
const TABLE_LEFT_BORDER_CLASS = "rdb-tcell-border-left";
const TABLE_RIGHT_BORDER_CLASS = "rdb-tcell-border-right";

const RESULTS_SELECTED_CLASS = "rdb-rcell-selected";
const RESULTS_ANCHOR_CLASS = "rdb-rcell-anchor";
const RESULTS_TOP_BORDER_CLASS = "rdb-rcell-border-top";
const RESULTS_BOTTOM_BORDER_CLASS = "rdb-rcell-border-bottom";
const RESULTS_LEFT_BORDER_CLASS = "rdb-rcell-border-left";
const RESULTS_RIGHT_BORDER_CLASS = "rdb-rcell-border-right";

export type CellSelectionClassSet = "table" | "results";

export function buildCellSelectionClassName(
  state: CellSelectionState,
  options:
    | string
    | { baseClass?: string; classSet?: CellSelectionClassSet } = "table",
): string {
  const { baseClass = "rdb-trow-cell", classSet = "table" } =
    typeof options === "string" ? { baseClass: options } : options;

  const isResults = classSet === "results";
  const selectedClass = isResults
    ? RESULTS_SELECTED_CLASS
    : TABLE_SELECTED_CLASS;
  const anchorClass = isResults ? RESULTS_ANCHOR_CLASS : TABLE_ANCHOR_CLASS;
  const topClass = isResults
    ? RESULTS_TOP_BORDER_CLASS
    : TABLE_TOP_BORDER_CLASS;
  const bottomClass = isResults
    ? RESULTS_BOTTOM_BORDER_CLASS
    : TABLE_BOTTOM_BORDER_CLASS;
  const leftClass = isResults
    ? RESULTS_LEFT_BORDER_CLASS
    : TABLE_LEFT_BORDER_CLASS;
  const rightClass = isResults
    ? RESULTS_RIGHT_BORDER_CLASS
    : TABLE_RIGHT_BORDER_CLASS;

  const classes: string[] = [baseClass];
  if (state.selected) classes.push(selectedClass);
  if (state.anchor) classes.push(anchorClass);
  if (state.top) classes.push(topClass);
  if (state.bottom) classes.push(bottomClass);
  if (state.left) classes.push(leftClass);
  if (state.right) classes.push(rightClass);
  return classes.join(" ");
}
