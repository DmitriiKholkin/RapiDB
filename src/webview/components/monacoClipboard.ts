import type * as monaco from "monaco-editor";

// Mirrors Monaco's own clipboard semantics (emptySelectionClipboard: true,
// multiCursorPaste: "spread"), which RapiDB replaces with its host clipboard bridge.

export interface EditorClipboardData {
  text: string;
  multicursorText: string[] | null;
  isFromEmptySelection: boolean;
  cutRanges: monaco.IRange[];
}

export interface PasteEdit {
  range: monaco.IRange;
  text: string;
  keepColumn?: number;
}

type ClipboardModel = Pick<
  monaco.editor.ITextModel,
  | "getEOL"
  | "getLineContent"
  | "getLineCount"
  | "getLineMaxColumn"
  | "getValueInRange"
>;

let lastClipboardMetadata: EditorClipboardData | null = null;

export function rememberClipboardData(data: EditorClipboardData) {
  lastClipboardMetadata = data;
}

export function toPlainRange(range: monaco.IRange): monaco.IRange {
  return {
    startLineNumber: range.startLineNumber,
    startColumn: range.startColumn,
    endLineNumber: range.endLineNumber,
    endColumn: range.endColumn,
  };
}

const comparePositions = (
  aLine: number,
  aColumn: number,
  bLine: number,
  bColumn: number,
) => aLine - bLine || aColumn - bColumn;

const compareRanges = (a: monaco.IRange, b: monaco.IRange) =>
  comparePositions(
    a.startLineNumber,
    a.startColumn,
    b.startLineNumber,
    b.startColumn,
  ) ||
  comparePositions(a.endLineNumber, a.endColumn, b.endLineNumber, b.endColumn);

const isEmptyRange = (range: monaco.IRange) =>
  range.startLineNumber === range.endLineNumber &&
  range.startColumn === range.endColumn;

function wholeLineCutRange(
  model: ClipboardModel,
  lineNumber: number,
): monaco.IRange {
  if (lineNumber < model.getLineCount()) {
    return {
      startLineNumber: lineNumber,
      startColumn: 1,
      endLineNumber: lineNumber + 1,
      endColumn: 1,
    };
  }
  if (lineNumber > 1) {
    return {
      startLineNumber: lineNumber - 1,
      startColumn: model.getLineMaxColumn(lineNumber - 1),
      endLineNumber: lineNumber,
      endColumn: model.getLineMaxColumn(lineNumber),
    };
  }
  return {
    startLineNumber: lineNumber,
    startColumn: 1,
    endLineNumber: lineNumber,
    endColumn: model.getLineMaxColumn(lineNumber),
  };
}

function mergeRanges(ranges: monaco.IRange[]): monaco.IRange[] {
  const merged: Array<{ -readonly [K in keyof monaco.IRange]: number }> = [];
  for (const range of [...ranges].sort(compareRanges)) {
    const last = merged.at(-1);
    if (
      last &&
      comparePositions(
        range.startLineNumber,
        range.startColumn,
        last.endLineNumber,
        last.endColumn,
      ) <= 0
    ) {
      if (
        comparePositions(
          range.endLineNumber,
          range.endColumn,
          last.endLineNumber,
          last.endColumn,
        ) > 0
      ) {
        last.endLineNumber = range.endLineNumber;
        last.endColumn = range.endColumn;
      }
      continue;
    }
    merged.push(toPlainRange(range));
  }
  return merged.filter((range) => !isEmptyRange(range));
}

export function buildClipboardData(
  model: ClipboardModel,
  selections: monaco.IRange[],
): EditorClipboardData | null {
  if (selections.length === 0) {
    return null;
  }
  const eol = model.getEOL();
  const ranges = [...selections].sort(compareRanges);
  const parts: string[] = [];
  const cutRanges: monaco.IRange[] = [];
  const allEmpty = ranges.every(isEmptyRange);
  let previousLine = 0;
  for (const range of ranges) {
    if (isEmptyRange(range)) {
      if (range.startLineNumber !== previousLine) {
        const line = model.getLineContent(range.startLineNumber);
        parts.push(allEmpty ? line + eol : line);
        cutRanges.push(wholeLineCutRange(model, range.startLineNumber));
      }
    } else {
      parts.push(model.getValueInRange(range));
      cutRanges.push(range);
    }
    previousLine = range.startLineNumber;
  }
  return {
    text: parts.join(allEmpty ? "" : eol),
    multicursorText: !allEmpty && parts.length > 1 ? parts : null,
    isFromEmptySelection: allEmpty && selections.length === 1,
    cutRanges: mergeRanges(cutRanges),
  };
}

function distributePasteText(
  text: string,
  cursorCount: number,
  metadata: EditorClipboardData | null,
): string[] | null {
  if (cursorCount <= 1) {
    return null;
  }
  if (metadata?.multicursorText?.length === cursorCount) {
    return metadata.multicursorText;
  }
  let spread = text;
  if (spread.endsWith("\n")) spread = spread.slice(0, -1);
  if (spread.endsWith("\r")) spread = spread.slice(0, -1);
  const lines = spread.split(/\r\n|\r|\n/);
  return lines.length === cursorCount ? lines : null;
}

export function buildPasteEdits(
  text: string,
  ranges: monaco.IRange[],
): PasteEdit[] {
  const metadata =
    lastClipboardMetadata?.text === text ? lastClipboardMetadata : null;
  const fromWholeLine = metadata?.isFromEmptySelection === true;
  // Monaco's PasteOperation suppresses spreading for a whole-line clipboard,
  // even when _simplePaste later falls back to ordinary replacement. Decide
  // that fallback for the entire batch, not separately for each cursor: moving
  // an empty cursor to column 1 can otherwise put it inside another selection.
  const pasteOnNewLine =
    fromWholeLine &&
    text.indexOf("\n") === text.length - 1 &&
    ranges.every(isEmptyRange);
  const distributed = fromWholeLine
    ? null
    : distributePasteText(text, ranges.length, metadata);
  const order = ranges
    .map((_, index) => index)
    .sort((a, b) => compareRanges(ranges[a], ranges[b]));
  const texts: string[] = new Array(ranges.length).fill(text);
  if (distributed) {
    order.forEach((rangeIndex, position) => {
      texts[rangeIndex] = distributed[position];
    });
  }
  return ranges.map((range, index) => {
    if (pasteOnNewLine && isEmptyRange(range)) {
      return {
        range: {
          startLineNumber: range.startLineNumber,
          startColumn: 1,
          endLineNumber: range.startLineNumber,
          endColumn: 1,
        },
        text: texts[index],
        keepColumn: range.startColumn,
      };
    }
    return { range, text: texts[index] };
  });
}
