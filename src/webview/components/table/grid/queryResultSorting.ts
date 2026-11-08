import type { SortingFn } from "@tanstack/react-table";
import { compareExactNumericValues } from "../../../../shared/exactNumericComparison";

export const exactNumericSortingFn: SortingFn<Record<string, unknown>> = (
  rowA,
  rowB,
  columnId,
) =>
  compareExactNumericValues(rowA.getValue(columnId), rowB.getValue(columnId));
