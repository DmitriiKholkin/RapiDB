import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { runInThisContext } from "node:vm";
import { act, render } from "@testing-library/react";
import type * as Monaco from "monaco-editor";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const runtime = vi.hoisted(() => ({
  editor: null as Monaco.editor.IStandaloneCodeEditor | null,
  models: [] as Monaco.editor.ITextModel[],
  inverseRanges: [] as Monaco.IRange[],
  inverseIdentifiers: [] as Array<{ major: number; minor: number } | null>,
}));

// Bundle the installed runtime in memory, bypassing the suite's Monaco alias.
// Only CSS/font loading and browser layout are stubbed: edits, selections and
// undo/redo run through the real 0.53 editor, cursor controller and text model.
vi.mock("monaco-editor", () => {
  // esbuild must run in Node's realm (jsdom's Uint8Array/TextEncoder differ).
  const bundle = execFileSync(
    process.execPath,
    [
      "-e",
      `const { buildSync } = require("esbuild");
       const result = buildSync({ entryPoints: [process.argv[1]], bundle: true,
         format: "cjs", platform: "browser", write: false,
         loader: { ".css": "empty", ".ttf": "empty" } });
       process.stdout.write(result.outputFiles[0].text);`,
      resolve("node_modules/monaco-editor/esm/vs/editor/editor.api.js"),
    ],
    {
      cwd: process.cwd(),
      encoding: "utf8",
      maxBuffer: 20 * 1024 * 1024,
    },
  );
  const module = { exports: {} as typeof Monaco };
  const load = runInThisContext(
    `(function(module, exports) { ${bundle}\n})`,
  ) as (module: { exports: typeof Monaco }, exports: typeof Monaco) => void;
  load(module, module.exports);
  const actual = module.exports;
  return {
    ...actual,
    editor: {
      ...actual.editor,
      create: (
        container: HTMLElement,
        options: Monaco.editor.IStandaloneEditorConstructionOptions,
      ) => {
        const editor = actual.editor.create(container, {
          ...options,
          automaticLayout: false,
          dimension: { width: 800, height: 300 },
        });
        runtime.editor = editor;
        const model = editor.getModel();
        if (model) runtime.models.push(model);
        const executeEdits = editor.executeEdits.bind(editor);
        vi.spyOn(editor, "executeEdits").mockImplementation(
          (source, edits, cursor) =>
            executeEdits(source, edits, (inverse) => {
              runtime.inverseRanges = inverse.map(({ range }) => range);
              runtime.inverseIdentifiers = inverse.map((edit) => {
                const identified = edit as typeof edit & {
                  identifier?: { major: number; minor: number } | null;
                };
                return identified.identifier ?? null;
              });
              return typeof cursor === "function"
                ? cursor(inverse)
                : (cursor ?? null);
            }),
        );
        return editor;
      },
    },
  };
});

import { editor as monacoEditor, Selection } from "monaco-editor";
import { MonacoEditor } from "../../src/webview/components/MonacoEditor";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getPostedMessages,
} from "./testUtils";

function editor() {
  if (!runtime.editor) throw new Error("Monaco editor was not created");
  return runtime.editor;
}

async function action(id: string) {
  await act(async () => {
    const action = editor().getAction(id);
    if (!action) throw new Error(`Missing Monaco action: ${id}`);
    await action.run();
  });
}

function pasteRequest() {
  const request = getPostedMessages()
    .filter((message) => message.type === "readClipboard")
    .at(-1)?.payload as { requestId: string; recipient: string } | undefined;
  if (!request) throw new Error("Missing clipboard request");
  return request;
}

function answerPaste(text: string) {
  const request = pasteRequest();
  act(() => dispatchIncomingMessage("clipboardText", { ...request, text }));
}

function positions() {
  return editor()
    .getSelections()
    ?.map((selection) => [
      selection.positionLineNumber,
      selection.positionColumn,
    ]);
}

describe("Monaco primary cursor paste (installed runtime)", () => {
  beforeEach(() => {
    clearPostedMessages();
    runtime.editor = null;
    runtime.models = [];
    runtime.inverseRanges = [];
    runtime.inverseIdentifiers = [];
    vi.stubGlobal("matchMedia", () => ({
      matches: false,
      addEventListener: () => {},
      removeEventListener: () => {},
    }));
    vi.stubGlobal("CSS", {
      ...globalThis.CSS,
      escape: (value: string) => value.replace(/[^a-zA-Z0-9_-]/g, "\\$&"),
    });
  });

  afterEach(() => {
    // MonacoEditor disposes the editor; its separately owned model needs this.
    for (const model of runtime.models) model.dispose();
  });

  it("keeps the right-hand touching selection primary, including subsequent typing and undo/redo", async () => {
    render(<MonacoEditor initialValue="aaaa" />);
    const original = [new Selection(1, 3, 1, 5), new Selection(1, 1, 1, 3)];
    editor().setSelections(original);
    await action("rapidb.paste");
    answerPaste("Z");

    expect(editor().getValue()).toBe("ZZ");
    // Runtime proof: touching ranges' inverse edits are in document order,
    // opposite to the primary-first input above. Never hand-feed these ranges.
    expect(runtime.inverseRanges.map((range) => range.endColumn)).toEqual([
      2, 3,
    ]);
    expect(positions()).toEqual([
      [1, 3],
      [1, 2],
    ]);
    expect(runtime.inverseIdentifiers).toEqual([
      { major: 1, minor: 0 },
      { major: 0, minor: 0 },
    ]);

    await act(async () => editor().getModel()?.undo());
    expect(editor().getValue()).toBe("aaaa");
    expect(editor().getSelections()).toEqual(original);
    await act(async () => editor().getModel()?.redo());
    expect(editor().getValue()).toBe("ZZ");
    expect(positions()).toEqual([
      [1, 3],
      [1, 2],
    ]);

    act(() => {
      editor().setSelection(editor().getSelection() as Selection);
      editor().trigger("keyboard", "type", { text: "!" });
    });
    expect(editor().getValue()).toBe("ZZ!");
    expect(positions()).toEqual([[1, 4]]);
  });

  it.each([
    "\n",
    "\r\n",
  ])("normalizes multiline EOL at touching ranges and EOF (%j)", async (eol) => {
    render(<MonacoEditor initialValue={`aa${eol}bb`} />);
    const original = [new Selection(2, 3, 2, 1), new Selection(1, 1, 2, 1)];
    editor().setSelections(original);
    await action("rapidb.paste");
    answerPaste("Q\rR\nS");

    expect(editor().getValue()).toBe(`Q${eol}R${eol}SQ${eol}R${eol}S`);
    expect(positions()).toEqual([
      [5, 2],
      [3, 2],
    ]);
    await act(async () => editor().getModel()?.undo());
    expect(editor().getValue()).toBe(`aa${eol}bb`);
    expect(editor().getSelections()).toEqual(original);
    await act(async () => editor().getModel()?.redo());
    expect(positions()).toEqual([
      [5, 2],
      [3, 2],
    ]);
  });

  it("retains primary-first order with different distributed lengths at adjacent ranges", async () => {
    render(<MonacoEditor initialValue="aaaa" />);
    editor().setSelections([
      new Selection(1, 3, 1, 5),
      new Selection(1, 1, 1, 3),
    ]);
    await action("rapidb.paste");
    answerPaste("X\nLONG");

    expect(editor().getValue()).toBe("XLONG");
    expect(runtime.inverseRanges.map((range) => range.endColumn)).toEqual([
      2, 6,
    ]);
    expect(positions()).toEqual([
      [1, 6],
      [1, 2],
    ]);
  });

  it("preserves all three input identities when non-touching inverses use input order", async () => {
    render(<MonacoEditor initialValue="aa--bb--cc" />);
    editor().setSelections([
      new Selection(1, 9, 1, 11),
      new Selection(1, 1, 1, 3),
      new Selection(1, 5, 1, 7),
    ]);
    await action("rapidb.paste");
    answerPaste("X\nMID\nLONG");

    expect(editor().getValue()).toBe("X--MID--LONG");
    expect(runtime.inverseRanges.map((range) => range.endColumn)).toEqual([
      13, 2, 7,
    ]);
    expect(positions()).toEqual([
      [1, 13],
      [1, 2],
      [1, 7],
    ]);
  });

  it("accounts for an empty distributed paste deleting the preceding selection", async () => {
    render(<MonacoEditor initialValue="aaaa" />);
    editor().setSelections([
      new Selection(1, 3, 1, 5),
      new Selection(1, 1, 1, 3),
    ]);
    await action("rapidb.paste");
    answerPaste("\nLONG");

    expect(editor().getValue()).toBe("LONG");
    expect(positions()).toEqual([
      [1, 5],
      [1, 1],
    ]);
  });

  it("handles an empty EOF edit touching a replacement without swapping identities", async () => {
    render(<MonacoEditor initialValue="aaaa" />);
    // Default Monaco merges an empty cursor touching a selection before paste.
    // Disable that option to exercise the model's supported touching-empty API.
    editor().updateOptions({ multiCursorMergeOverlapping: false });
    editor().setSelections([
      new Selection(1, 5, 1, 5),
      new Selection(1, 1, 1, 5),
    ]);
    expect(editor().getSelections()).toHaveLength(2);
    await action("rapidb.paste");
    answerPaste("Z");

    expect(editor().getValue()).toBe("ZZ");
    expect(positions()).toEqual([
      [1, 3],
      [1, 2],
    ]);
  });

  it.each([
    "\n",
    "\r\n",
  ])("preserves an empty primary at EOF and a secondary on another line (%j)", async (eol) => {
    render(<MonacoEditor initialValue={`ab${eol}cd`} />);
    editor().setSelections([
      new Selection(2, 3, 2, 3),
      new Selection(1, 2, 1, 2),
    ]);
    await action("rapidb.paste");
    answerPaste("X\nLONG");

    expect(editor().getValue()).toBe(`aXb${eol}cdLONG`);
    expect(positions()).toEqual([
      [2, 7],
      [1, 3],
    ]);
  });

  it.each([
    "\n",
    "\r\n",
  ])("keeps the column when pasting a copied whole line above EOF (%j)", async (eol) => {
    render(<MonacoEditor initialValue={`first${eol}last`} />);
    editor().setSelection(new Selection(1, 3, 1, 3));
    await action("rapidb.copy");
    editor().setSelection(new Selection(2, 4, 2, 4));
    await action("rapidb.paste");
    answerPaste(`first${eol}`);

    expect(editor().getValue()).toBe(`first${eol}first${eol}last`);
    expect(positions()).toEqual([[3, 4]]);
  });

  it("tracks 1000 edits so Monaco cannot reduce away their per-cursor identifiers", async () => {
    const original = Array.from(
      { length: 1000 },
      (_, index) => new Selection(1, 1001 - index, 1, 1001 - index),
    );
    const untracked = monacoEditor.createModel("a".repeat(1000));
    runtime.models.push(untracked);
    expect(
      untracked.applyEdits(
        original.map((range) => ({ range, text: "Z" })),
        true,
      ),
    ).toHaveLength(1);

    render(<MonacoEditor initialValue={"a".repeat(1000)} />);
    editor().setSelections(original);
    await action("rapidb.paste");
    answerPaste("Z");

    expect(editor().getValue()).toBe("aZ".repeat(1000));
    expect(runtime.inverseRanges).toHaveLength(1000);
    expect(runtime.inverseIdentifiers).toEqual(
      Array.from({ length: 1000 }, (_, major) => ({ major, minor: 0 })),
    );
    expect(positions()).toEqual(
      Array.from({ length: 1000 }, (_, index) => [1, 2001 - 2 * index]),
    );
  });

  it("matches touching edits by identifier when Monaco appends an unlabelled auto-whitespace edit", async () => {
    render(<MonacoEditor initialValue={"\naaaa"} />);
    const autoIndent = {
      range: new Selection(1, 1, 1, 1),
      text: "  ",
      isAutoWhitespaceEdit: true,
    };
    act(() => {
      editor()
        .getModel()
        ?.pushEditOperations(null, [autoIndent], () => null);
      editor().pushUndoStop();
    });
    expect(editor().getValue()).toBe("  \naaaa");
    const original = [new Selection(2, 3, 2, 5), new Selection(2, 1, 2, 3)];
    editor().setSelections(original);
    await action("rapidb.paste");
    answerPaste("Z");

    expect(editor().getValue()).toBe("\nZZ");
    expect(runtime.inverseIdentifiers).toEqual([
      null,
      { major: 1, minor: 0 },
      { major: 0, minor: 0 },
    ]);
    expect(positions()).toEqual([
      [2, 3],
      [2, 2],
    ]);
    await act(async () => editor().getModel()?.undo());
    expect(editor().getValue()).toBe("  \naaaa");
    expect(editor().getSelections()).toEqual(original);
    await act(async () => editor().getModel()?.redo());
    expect(positions()).toEqual([
      [2, 3],
      [2, 2],
    ]);
  });

  it("uses request-time identities but rejects changed model versions and replacement models", async () => {
    render(<MonacoEditor initialValue="aaaa" />);
    editor().setSelections([
      new Selection(1, 3, 1, 5),
      new Selection(1, 1, 1, 3),
    ]);
    await action("rapidb.paste");
    editor().setSelection(new Selection(1, 2, 1, 2));
    answerPaste("Z");
    expect(positions()).toEqual([
      [1, 3],
      [1, 2],
    ]);

    await action("rapidb.paste");
    act(() => editor().getModel()?.setValue("changed"));
    answerPaste("ignored");
    expect(editor().getValue()).toBe("changed");

    await action("rapidb.paste");
    const replacement = monacoEditor.createModel("replacement");
    runtime.models.push(replacement);
    act(() => editor().setModel(replacement));
    answerPaste("ignored");
    expect(editor().getValue()).toBe("replacement");
  });

  it("rejects another window's response, read-only transitions and responses after unmount", async () => {
    const view = render(<MonacoEditor initialValue="aaaa" />);
    editor().setSelections([
      new Selection(1, 3, 1, 5),
      new Selection(1, 1, 1, 3),
    ]);
    await action("rapidb.paste");
    const request = pasteRequest();
    act(() =>
      dispatchIncomingMessage("clipboardText", {
        ...request,
        recipient: "another-window",
        text: "Z",
      }),
    );
    expect(editor().getValue()).toBe("aaaa");
    answerPaste("Z");
    expect(positions()).toEqual([
      [1, 3],
      [1, 2],
    ]);

    await action("rapidb.paste");
    view.rerender(<MonacoEditor initialValue="aaaa" readOnly />);
    answerPaste("ignored");
    expect(editor().executeEdits).toHaveBeenCalledTimes(1);

    view.rerender(<MonacoEditor initialValue="aaaa" />);
    await action("rapidb.paste");
    const executeEdits = editor().executeEdits;
    view.unmount();
    answerPaste("ignored");
    expect(executeEdits).toHaveBeenCalledTimes(1);
  });
});
