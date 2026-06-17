import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

export interface TableColumnHeaderHandle {
  columnId: string;
  element: HTMLTableCellElement;
}

export function findTableHeaderByName(
  label: string,
): HTMLTableCellElement | null {
  const cell = screen.getAllByText(label).find((el) => el.closest("th")) as
    | HTMLElement
    | undefined;
  return cell instanceof HTMLTableCellElement ? cell : null;
}

export function getTableHeaders(): HTMLTableCellElement[] {
  const tables = document.querySelectorAll('[data-testid="workflow-table"]');
  if (tables.length === 0) {
    return [];
  }
  const table = tables[tables.length - 1];
  return Array.from(table.querySelectorAll("thead tr:first-child th"));
}

export function findResizeHandle(columnId: string): HTMLButtonElement | null {
  return screen.queryByRole("button", {
    name: `Resize ${columnId} column`,
  });
}

export function findRowSelectCheckbox(
  rowIndex: number,
): HTMLInputElement | null {
  return screen.queryByRole("checkbox", {
    name: new RegExp(`^Select row ${rowIndex + 1}$`),
  });
}

export function getHeaderColumnId(cell: HTMLTableCellElement): string | null {
  return cell.getAttribute("data-column-id");
}

export function dragResizeHandle(
  handle: HTMLElement,
  deltaX: number,
  startX = 200,
): void {
  fireEvent.mouseDown(handle, { clientX: startX, buttons: 1 });
  fireEvent.mouseMove(document, { clientX: startX + deltaX, buttons: 1 });
  fireEvent.mouseUp(document, { clientX: startX + deltaX, buttons: 0 });
}

export function resizeColumn(
  columnId: string,
  deltaX: number,
  startX = 200,
): void {
  const handle = findResizeHandle(columnId);
  if (!handle) {
    throw new Error(`No resize handle found for column ${columnId}`);
  }
  dragResizeHandle(handle, deltaX, startX);
}

export function hideColumn(columnId: string): void {
  resizeColumn(columnId, -10_000);
}

export async function reorderColumnByHeaderDrag(
  fromId: string,
  toId: string,
  user: ReturnType<typeof userEvent.setup>,
): Promise<void> {
  const fromHeader = getTableHeaders().find(
    (th) => getHeaderColumnId(th) === fromId,
  );
  const toHeader = getTableHeaders().find(
    (th) => getHeaderColumnId(th) === toId,
  );
  if (!fromHeader || !toHeader) {
    throw new Error(`Cannot find headers ${fromId} / ${toId} for drag`);
  }
  const fromRect = fromHeader.getBoundingClientRect();
  const toRect = toHeader.getBoundingClientRect();
  await user.pointer([
    { target: fromHeader, keys: "[MouseLeft>]", coords: { x: 5, y: 12 } },
    { coords: { x: toRect.left - fromRect.left + toRect.width / 2, y: 12 } },
    { keys: "[/MouseLeft]" },
  ]);
}

export function selectRowCheckbox(rowIndex: number, selected: boolean): void {
  const checkbox = findRowSelectCheckbox(rowIndex);
  if (!checkbox) {
    throw new Error(`No checkbox for row ${rowIndex}`);
  }
  if (checkbox.checked !== selected) {
    fireEvent.click(checkbox);
  }
}

export function clickCell(rowIndex: number, columnId: string): void {
  const table = screen.getByRole("table");
  const headers = Array.from(table.querySelectorAll("thead tr:first-child th"));
  const columnIndex = headers.findIndex(
    (h) => h.getAttribute("data-column-id") === columnId,
  );
  if (columnIndex < 0) {
    throw new Error(`Column ${columnId} not in table`);
  }
  const cell = table.querySelectorAll(`tbody tr:nth-child(${rowIndex + 1}) td`)[
    columnIndex
  ] as HTMLElement | undefined;
  if (!cell) {
    throw new Error(`No cell at row ${rowIndex}, column ${columnId}`);
  }
  fireEvent.click(cell);
}

export function getBodyCell(
  columnId: string,
  rowIndex = 0,
): HTMLTableCellElement {
  const table = screen.getByRole("table");
  const headers = Array.from(table.querySelectorAll("thead tr:first-child th"));
  const columnIndex = headers.findIndex(
    (h) => h.getAttribute("data-column-id") === columnId,
  );
  if (columnIndex < 0) {
    throw new Error(`Column ${columnId} not in table`);
  }
  const cell = table.querySelectorAll(`tbody tr:nth-child(${rowIndex + 1}) td`)[
    columnIndex
  ];
  if (!(cell instanceof HTMLTableCellElement)) {
    throw new Error(`Cell at (${rowIndex}, ${columnId}) is not TD`);
  }
  return cell;
}

export function readTableState(): {
  columns: string[];
  rows: Array<Record<string, string>>;
} {
  const table = screen.getByRole("table");
  const headers = Array.from(table.querySelectorAll("thead tr:first-child th"));
  const columns = headers
    .map((h) => h.getAttribute("data-column-id"))
    .filter((id): id is string => Boolean(id));
  const rows = Array.from(table.querySelectorAll("tbody tr")).map((tr) => {
    const row: Record<string, string> = {};
    Array.from(tr.querySelectorAll("td")).forEach((td, index) => {
      const columnId = columns[index];
      if (columnId) {
        row[columnId] = (td.textContent ?? "").trim();
      }
    });
    return row;
  });
  return { columns, rows };
}

export function waitForColumn(
  columnId: string,
  options: { timeout?: number } = {},
): Promise<void> {
  return waitFor(
    () => {
      const found = getTableHeaders().some(
        (h) => getHeaderColumnId(h) === columnId,
      );
      if (!found) {
        throw new Error(`Column ${columnId} not yet visible`);
      }
    },
    { timeout: options.timeout ?? 2000 },
  );
}

export function waitForRowCount(
  expected: number,
  options: { timeout?: number } = {},
): Promise<void> {
  return waitFor(
    () => {
      const tables = document.querySelectorAll(
        '[data-testid="workflow-table"]',
      );
      const lastTable = tables[tables.length - 1];
      const count = lastTable?.querySelectorAll("tbody tr").length ?? 0;
      if (count < expected) {
        throw new Error(`Expected at least ${expected} rows, got ${count}`);
      }
    },
    { timeout: options.timeout ?? 2000 },
  );
}
