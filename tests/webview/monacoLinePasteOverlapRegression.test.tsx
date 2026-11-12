import type * as Monaco from "monaco-editor";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  buildClipboardData,
  buildPasteEdits,
  rememberClipboardData,
  toPlainRange,
} from "../../src/webview/components/monacoClipboard";
import {
  loadRealMonaco,
  type RealMonacoRuntime,
} from "./monacoLinePasteRealRuntime";

const range = (
  startLineNumber: number,
  startColumn: number,
  endLineNumber = startLineNumber,
  endColumn = startColumn,
): Monaco.IRange => ({
  startLineNumber,
  startColumn,
  endLineNumber,
  endColumn,
});

describe("whole-line paste overlap regressions (installed Monaco runtime)", () => {
  let monaco: RealMonacoRuntime;
  const models: Monaco.editor.ITextModel[] = [];

  beforeAll(() => {
    monaco = loadRealMonaco();
  });

  afterEach(() => {
    for (const model of models.splice(0)) model.dispose();
  });

  function model(text: string) {
    const value = monaco.editor.createModel(text, "plaintext");
    models.push(value);
    return value;
  }

  function copyLine(eol = "\n") {
    const data = buildClipboardData(model(`x${eol}y`), [range(1, 1)]);
    if (!data) throw new Error("missing copied line");
    rememberClipboardData(data);
    return data.text;
  }

  function nativeEdits(
    target: Monaco.editor.ITextModel,
    text: string,
    ranges: Monaco.IRange[],
    wholeLine: boolean,
    multicursorText: string[] | null = null,
  ) {
    const edits: Monaco.editor.IIdentifiedSingleEditOperation[] = [];
    const result = monaco.PasteOperation.getEdits(
      { multiCursorPaste: "spread" },
      target,
      ranges.map(
        (r) =>
          new monaco.Selection(
            r.startLineNumber,
            r.startColumn,
            r.endLineNumber,
            r.endColumn,
          ),
      ),
      text,
      wholeLine,
      multicursorText,
    );
    for (const command of result.commands) {
      command.getEditOperations(target, {
        addTrackedEditOperation: (r, inserted) => {
          edits.push({ range: toPlainRange(r), text: inserted });
        },
        trackSelection: () => "tracked-selection",
      });
    }
    return edits;
  }

  function assertNonOverlapping(edits: Array<{ range: Monaco.IRange }>) {
    const position = (line: number, column: number) => [line, column];
    const compare = (a: number[], b: number[]) => a[0] - b[0] || a[1] - b[1];
    const sorted = edits
      .map(({ range: r }) => r)
      .sort(
        (a, b) =>
          compare(
            position(a.startLineNumber, a.startColumn),
            position(b.startLineNumber, b.startColumn),
          ) ||
          compare(
            position(a.endLineNumber, a.endColumn),
            position(b.endLineNumber, b.endColumn),
          ),
      );
    for (let i = 1; i < sorted.length; i++) {
      expect(
        compare(
          position(sorted[i - 1].endLineNumber, sorted[i - 1].endColumn),
          position(sorted[i].startLineNumber, sorted[i].startColumn),
        ),
      ).toBeLessThanOrEqual(0);
    }
  }

  it("replaces the primary selection and inserts at the extra cursor without overlap", () => {
    const text = copyLine();
    const target = model("abc\ndef");
    const ranges = [range(1, 2, 2, 2), range(2, 4)];
    const edits = buildPasteEdits(text, ranges);
    target.applyEdits(edits);
    expect(edits.map((edit) => edit.range)).toEqual(ranges);
    expect(target.getValue()).toBe("ax\nefx\n");
    expect(
      edits.map(({ range: r, text: inserted }) => ({
        range: r,
        text: inserted,
      })),
    ).toEqual(nativeEdits(model("abc\ndef"), text, ranges, true));
  });

  it("executes Monaco's own model overlap check, not the permissive test mock", () => {
    const target = model("abc\ndef");
    expect(() =>
      target.applyEdits([
        { range: range(1, 2, 2, 2), text: "x\n" },
        { range: range(2, 1), text: "x\n" },
      ]),
    ).toThrow("Overlapping ranges are not allowed");
    expect(target.getValue()).toBe("abc\ndef");
  });

  it.each([
    { source: "LF", sourceEOL: "\n", target: "LF", targetEOL: "\n" },
    { source: "LF", sourceEOL: "\n", target: "CRLF", targetEOL: "\r\n" },
    { source: "CRLF", sourceEOL: "\r\n", target: "LF", targetEOL: "\n" },
    { source: "CRLF", sourceEOL: "\r\n", target: "CRLF", targetEOL: "\r\n" },
  ])("normalizes $source clipboard EOL into $target models", ({
    sourceEOL,
    targetEOL,
  }) => {
    const text = copyLine(sourceEOL);
    const target = model(`abc${targetEOL}def`);
    const ranges = [range(1, 2, 2, 2), range(2, 4)];
    const edits = buildPasteEdits(text, ranges);
    assertNonOverlapping(edits);
    expect(edits).toEqual(nativeEdits(target, text, ranges, true));
    target.applyEdits(edits);
    expect(target.getValue()).toBe(`ax${targetEOL}efx${targetEOL}`);
  });

  it.each([
    false,
    true,
  ])("uses one ordinary-replacement mode for mixed selections regardless of primary order (reversed=%s)", (reversed) => {
    const text = copyLine();
    const target = model("abc\ndef");
    const ranges = [range(1, 2, 2, 2), range(2, 4)];
    if (reversed) ranges.reverse();
    const edits = buildPasteEdits(text, ranges);
    expect(edits).toEqual(ranges.map((r) => ({ range: r, text })));
    assertNonOverlapping(edits);
    target.applyEdits(edits);
    expect(target.getValue()).toBe("ax\nefx\n");
  });

  it.each([
    {
      name: "cursor touching the start",
      ranges: [range(1, 2, 2, 2), range(1, 2)],
      expected: "ax\nx\nef",
    },
    {
      name: "cursor touching the end",
      ranges: [range(1, 2, 2, 2), range(2, 2)],
      expected: "ax\nx\nef",
    },
    {
      name: "touching nonempty selections",
      ranges: [range(1, 2, 1, 3), range(1, 3, 1, 4)],
      expected: "ax\nx\n\ndef",
    },
    {
      name: "empty cursors before and after a selection",
      ranges: [range(1, 1), range(1, 2, 2, 2), range(2, 4)],
      expected: "x\nax\nefx\n",
    },
  ])("preserves every edit for $name", ({ ranges, expected }) => {
    const text = copyLine();
    const target = model("abc\ndef");
    const edits = buildPasteEdits(text, ranges);
    expect(edits).toHaveLength(ranges.length);
    expect(edits.map((edit) => edit.range)).toEqual(ranges);
    expect(edits.every((edit) => edit.text === text)).toBe(true);
    assertNonOverlapping(edits);
    target.applyEdits(edits);
    expect(target.getValue()).toBe(expected);
  });

  it.each([
    "\n",
    "\r\n",
  ])("keeps all whole-line insertions for empty cursors on the same line (%j)", (eol) => {
    const text = copyLine(eol);
    const target = model(`abc${eol}def`);
    const ranges = [range(1, 2), range(1, 4), range(2, 3)];
    const edits = buildPasteEdits(text, ranges);
    expect(edits.map((edit) => edit.keepColumn)).toEqual([2, 4, 3]);
    expect(
      edits.map(({ range: r, text: inserted }) => ({
        range: r,
        text: inserted,
      })),
    ).toEqual(nativeEdits(target, text, ranges, true));
    assertNonOverlapping(edits);
    target.applyEdits(edits);
    expect(target.getValue()).toBe(`x${eol}x${eol}abc${eol}x${eol}def`);
  });

  it("does not silently drop or merge originally overlapping selections", () => {
    const text = copyLine();
    const ranges = [range(1, 2, 2, 2), range(2, 1, 2, 3)];
    const edits = buildPasteEdits(text, ranges);
    expect(edits).toEqual(ranges.map((r) => ({ range: r, text })));
    const target = model("abc\ndef");
    expect(() => target.applyEdits(edits)).toThrow(
      "Overlapping ranges are not allowed",
    );
    expect(target.getValue()).toBe("abc\ndef");
  });

  it("does not move an empty cursor already inside a selection or discard either edit", () => {
    const text = copyLine();
    const ranges = [range(1, 2, 2, 2), range(1, 3)];
    const edits = buildPasteEdits(text, ranges);
    expect(edits).toEqual(ranges.map((r) => ({ range: r, text })));
    expect(() => model("abc\ndef").applyEdits(edits)).toThrow(
      "Overlapping ranges are not allowed",
    );
  });

  it("keeps multi-line selected copy metadata distributed in document order", () => {
    const data = buildClipboardData(model("aa\nbb\ncc\ndd"), [
      range(3, 1, 4, 3),
      range(1, 1, 2, 3),
    ]);
    if (!data) throw new Error("missing selected clipboard");
    rememberClipboardData(data);
    const ranges = [range(2, 4), range(1, 2, 2, 2)];
    const target = model("abc\ndef");
    const edits = buildPasteEdits(data.text, ranges);
    expect(edits.map((edit) => edit.text)).toEqual(["cc\ndd", "aa\nbb"]);
    expect(edits.map((edit) => edit.range)).toEqual(ranges);
    assertNonOverlapping(edits);
    target.applyEdits(edits);
    expect(target.getValue()).toBe("aaa\nbbefcc\ndd");
  });

  it.each([
    { text: "y\nz\n", expected: "ayefz", texts: ["y", "z"] },
    {
      text: "external\n",
      expected: "aexternal\nefexternal\n",
      texts: ["external\n", "external\n"],
    },
  ])("ignores stale whole-line metadata for external clipboard $text", ({
    text,
    expected,
    texts,
  }) => {
    copyLine();
    const ranges = [range(1, 2, 2, 2), range(2, 4)];
    const target = model("abc\ndef");
    const edits = buildPasteEdits(text, ranges);
    expect(edits).toEqual(nativeEdits(target, text, ranges, false));
    expect(edits.map((edit) => edit.text)).toEqual(texts);
    assertNonOverlapping(edits);
    target.applyEdits(edits);
    expect(target.getValue()).toBe(expected);
  });

  it("falls back without spreading when whole-line metadata accompanies multiple lines", () => {
    const text = "x\ny\n";
    // Native PasteOperation accepts this metadata/payload combination as well:
    // whole-line provenance suppresses spread before the multiline fallback.
    rememberClipboardData({
      text,
      isFromEmptySelection: true,
      multicursorText: null,
      cutRanges: [],
    });
    const target = model("abc\ndef");
    const ranges = [range(1, 2), range(2, 4)];
    const edits = buildPasteEdits(text, ranges);
    expect(edits).toEqual(nativeEdits(target, text, ranges, true));
    target.applyEdits(edits);
    expect(target.getValue()).toBe("ax\ny\nbc\ndefx\ny\n");
  });

  it("spreads whole lines copied from multiple empty selections as Monaco does", () => {
    const data = buildClipboardData(model("x\ny"), [range(1, 1), range(2, 1)]);
    if (!data) throw new Error("missing multi-cursor copy");
    rememberClipboardData(data);
    const target = model("abc\ndef");
    const ranges = [range(1, 2, 2, 2), range(2, 4)];
    const edits = buildPasteEdits(data.text, ranges);
    expect(edits).toEqual(nativeEdits(target, data.text, ranges, false));
    target.applyEdits(edits);
    expect(target.getValue()).toBe("axefy");
  });

  it("spreads a selected multiline copy without treating its final newline as whole-line provenance", () => {
    const data = buildClipboardData(model("x\ny\n"), [range(1, 1, 3, 1)]);
    if (!data) throw new Error("missing multiline copy");
    rememberClipboardData(data);
    const target = model("abc\ndef");
    const ranges = [range(1, 2, 2, 2), range(2, 4)];
    const edits = buildPasteEdits(data.text, ranges);
    expect(edits).toEqual(nativeEdits(target, data.text, ranges, false));
    expect(edits.map((edit) => edit.text)).toEqual(["x", "y"]);
    target.applyEdits(edits);
    expect(target.getValue()).toBe("axefy");
  });

  it("pastes the full payload at every cursor when metadata and line counts both mismatch", () => {
    const data = buildClipboardData(model("a\nb"), [
      range(1, 1, 1, 2),
      range(2, 1, 2, 2),
    ]);
    if (!data) throw new Error("missing multi-selection copy");
    rememberClipboardData(data);
    const target = model("abc");
    const ranges = [range(1, 1), range(1, 2), range(1, 3)];
    const edits = buildPasteEdits(data.text, ranges);
    expect(edits).toEqual(
      nativeEdits(target, data.text, ranges, false, data.multicursorText),
    );
    expect(edits.every((edit) => edit.text === data.text)).toBe(true);
    assertNonOverlapping(edits);
    target.applyEdits(edits);
    expect(target.getValue()).toBe("a\nbaa\nbba\nbc");
  });

  it("keeps both blank-line payloads for mixed selections", () => {
    const data = buildClipboardData(model(""), [range(1, 1)]);
    if (!data) throw new Error("missing blank-line copy");
    rememberClipboardData(data);
    const target = model("abc\ndef");
    const ranges = [range(1, 2, 2, 2), range(2, 4)];
    const edits = buildPasteEdits(data.text, ranges);
    expect(edits).toEqual(nativeEdits(target, data.text, ranges, true));
    target.applyEdits(edits);
    expect(target.getValue()).toBe("a\nef\n");
  });

  it("applies every valid pair of ranges without introducing overlaps or losing text", () => {
    const text = copyLine();
    const source = "abc\ndef";
    const target = model(source);
    const oracle = model(source);
    const positions = Array.from({ length: source.length + 1 }, (_, offset) =>
      offset <= 3
        ? { line: 1, column: offset + 1 }
        : { line: 2, column: offset - 3 },
    );
    for (let a = 0; a < positions.length; a++) {
      for (let b = a; b < positions.length; b++) {
        for (let c = b; c < positions.length; c++) {
          for (let d = c; d < positions.length; d++) {
            const ranges = [
              range(
                positions[a].line,
                positions[a].column,
                positions[b].line,
                positions[b].column,
              ),
              range(
                positions[c].line,
                positions[c].column,
                positions[d].line,
                positions[d].column,
              ),
            ];
            for (const input of [ranges, [...ranges].reverse()]) {
              target.setValue(source);
              oracle.setValue(source);
              const edits = buildPasteEdits(text, input);
              expect(edits).toHaveLength(2);
              expect(edits.every((edit) => edit.text === text)).toBe(true);
              assertNonOverlapping(edits);
              target.applyEdits(edits);
              // All-empty batches retain whole-line semantics; every mixed
              // batch uses Monaco's ordinary replacement commands as a unit.
              oracle.applyEdits(
                nativeEdits(oracle, text, input, a === b && c === d),
              );
              expect(target.getValue()).toBe(oracle.getValue());
            }
          }
        }
      }
    }
  });

  it("records the mixed paste as one real-model undo/redo action", async () => {
    const text = copyLine();
    const target = model("abc\ndef");
    const edits = buildPasteEdits(text, [range(1, 2, 2, 2), range(2, 4)]);
    target.pushStackElement();
    target.pushEditOperations(null, edits, () => null);
    target.pushStackElement();
    expect(target.getValue()).toBe("ax\nefx\n");
    await target.undo();
    expect(target.getValue()).toBe("abc\ndef");
    expect(target.canUndo()).toBe(false);
    await target.redo();
    expect(target.getValue()).toBe("ax\nefx\n");
  });
});
