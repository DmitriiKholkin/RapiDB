import { act, fireEvent, render, screen } from "@testing-library/react";
import { createRef, useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

interface ActionDescriptor {
  id: string;
  run: () => void;
}

interface TrackedEditor {
  actions: ActionDescriptor[];
  getValue(): string;
}

const monacoState = vi.hoisted(() => ({
  editors: [] as TrackedEditor[],
}));

vi.mock("monaco-editor", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../mocks/monaco-editor")>();
  return {
    ...actual,
    editor: {
      ...actual.editor,
      create: (container: HTMLElement, options: { value?: string }) => {
        const base = actual.editor.create(container, options);
        const actions: ActionDescriptor[] = [];
        const instance = {
          ...base,
          // Real Monaco emits a content change for executeEdits; the shared
          // mock does not, so route edits through setValue to notify onChange.
          executeEdits: (_source: string, edits: Array<{ text: string }>) => {
            const text = edits[0]?.text;
            if (text !== undefined) base.setValue(text);
          },
          addAction: (descriptor: ActionDescriptor) => {
            actions.push(descriptor);
            return { dispose: () => {} };
          },
        };
        monacoState.editors.push({ actions, getValue: instance.getValue });
        return instance;
      },
    },
  };
});

import {
  formatJSONOrError,
  MonacoEditor,
  type MonacoEditorHandle,
} from "../../src/webview/components/MonacoEditor";
import { getStructuredCellDialogValue } from "../../src/webview/components/table/structuredCellDialog";
import { LargeMonacoDialog } from "../../src/webview/components/table/TableDialogs";
import { formatJsonPreservingTokens } from "../../src/webview/utils/jsonFormatting";
import { __resetMockMonacoState } from "../mocks/monaco-editor";

afterEach(() => {
  monacoState.editors.length = 0;
  __resetMockMonacoState();
});

const LOSSY_JSON =
  '{"id":9007199254740993,"price":0.10000000000000000555,"big":1e400,"k":1,"k":2,"s":"\\u0041\\/","n":-0}';
const LOSSY_JSON_FORMATTED = [
  "{",
  '  "id": 9007199254740993,',
  '  "price": 0.10000000000000000555,',
  '  "big": 1e400,',
  '  "k": 1,',
  '  "k": 2,',
  '  "s": "\\u0041\\/",',
  '  "n": -0',
  "}",
].join("\n");

function runFormatAction(editor: TrackedEditor) {
  const action = editor.actions.find(({ id }) => id === "rapidb.format");
  expect(action).toBeDefined();
  act(() => action?.run());
}

describe("lossless JSON formatting helper", () => {
  it("jsonFormatLossless keeps unsafe integers, decimal literals, duplicate keys and escapes", () => {
    expect(formatJsonPreservingTokens(LOSSY_JSON)).toEqual({
      result: LOSSY_JSON_FORMATTED,
    });
    expect(formatJSONOrError(LOSSY_JSON)).toEqual({
      result: LOSSY_JSON_FORMATTED,
    });
  });

  it("jsonFormatLossless formats nested and empty containers like JSON.stringify", () => {
    const input = ' { "a" : [ 1 , { } , [ ] , { "b" : null } ] , "c" : true } ';
    expect(formatJsonPreservingTokens(input)).toEqual({
      result: JSON.stringify(JSON.parse(input), null, 2),
    });
    expect(formatJsonPreservingTokens('"x"')).toEqual({ result: '"x"' });
    expect(formatJsonPreservingTokens("  \n")).toEqual({ result: "  \n" });
  });

  it("jsonFormatLossless handles deep nesting without recursion", () => {
    const depth = 1_000;
    const input = `${"[".repeat(depth)}1${"]".repeat(depth)}`;
    const out = formatJsonPreservingTokens(input);
    expect("result" in out && out.result.replace(/\s/g, "")).toBe(input);
    expect(formatJsonPreservingTokens("[".repeat(200_000))).toEqual({
      error: "Unexpected end of JSON input",
    });
  });

  it.each([
    "{",
    '{"a":1,}',
    "[1,]",
    "[,1]",
    "{,}",
    '{"a" 1}',
    "{a:1}",
    "{'a':1}",
    "[01]",
    "[1.]",
    "[.5]",
    "[+1]",
    "[1e]",
    "[NaN]",
    "[Infinity]",
    "[truex]",
    "[True]",
    '["\\x41"]',
    '["unterminated]',
    '["tab\there"]',
    "[1 2]",
    "1 2",
    "{}{}",
    '{"a":1}}',
    "\ufeff{}",
  ])("jsonFormatLossless rejects invalid JSON %j", (input) => {
    expect(formatJsonPreservingTokens(input)).toEqual({
      error: expect.any(String),
    });
    expect(() => JSON.parse(input)).toThrow();
  });
});

describe("MonacoEditor JSON formatting", () => {
  it("reports excessive pretty-print expansion without throwing or changing the editor", () => {
    const input = "[".repeat(20_000) + "]".repeat(20_000);
    const ref = createRef<MonacoEditorHandle>();
    render(<MonacoEditor ref={ref} initialValue={input} language="json" />);
    let error: string | null | undefined;
    act(() => {
      error = ref.current?.format();
    });
    expect(error).toMatch(/safe output size limit/);
    runFormatAction(monacoState.editors[0]);
    expect(monacoState.editors[0].getValue() === input).toBe(true);
  });

  it("jsonFormatLossless Shift+Alt+F action keeps precision and duplicate keys", () => {
    const ref = createRef<MonacoEditorHandle>();
    render(
      <MonacoEditor ref={ref} initialValue={LOSSY_JSON} language="json" />,
    );
    const [editor] = monacoState.editors;

    runFormatAction(editor);

    expect(editor.getValue()).toBe(LOSSY_JSON_FORMATTED);
  });

  it("jsonFormatLossless imperative format reports invalid JSON without editing", () => {
    const ref = createRef<MonacoEditorHandle>();
    render(<MonacoEditor ref={ref} initialValue='{"a":1,}' language="json" />);
    const [editor] = monacoState.editors;

    let error: string | null | undefined;
    act(() => {
      error = ref.current?.format();
    });
    expect(error).toMatch(/Unexpected token/);
    runFormatAction(editor);
    expect(editor.getValue()).toBe('{"a":1,}');
  });

  it("jsonFormatDisabled ignores Shift+Alt+F and ref.format when allowFormatting is false", () => {
    const ref = createRef<MonacoEditorHandle>();
    const { rerender } = render(
      <MonacoEditor
        ref={ref}
        initialValue={'{"a":1}'}
        language="json"
        allowFormatting={false}
      />,
    );
    const [editor] = monacoState.editors;

    runFormatAction(editor);
    let error: string | null | undefined;
    act(() => {
      error = ref.current?.format();
    });
    expect(error).toBeNull();
    expect(editor.getValue()).toBe('{"a":1}');

    rerender(
      <MonacoEditor ref={ref} initialValue={'{"a":1}'} language="json" />,
    );
    runFormatAction(editor);
    expect(editor.getValue()).toBe('{\n  "a": 1\n}');
  });

  it("jsonFormatDisabled also blocks SQL formatting through the keyboard action", () => {
    render(<MonacoEditor initialValue="select 1" allowFormatting={false} />);
    const [editor] = monacoState.editors;

    runFormatAction(editor);

    expect(editor.getValue()).toBe("select 1");
  });
});

describe("structured JSON cell formatting", () => {
  it("jsonFormatLossless structured cell dialog keeps raw JSON text tokens", () => {
    expect(
      getStructuredCellDialogValue(LOSSY_JSON, {
        category: "json",
        nativeType: "jsonb",
      }),
    ).toEqual({
      kind: "json",
      language: "json",
      formattedText: LOSSY_JSON_FORMATTED,
    });
  });

  it("jsonFormatLossless structured cell dialog leaves non-JSON text unformatted", () => {
    expect(
      getStructuredCellDialogValue("[01, 1]", {
        category: "json",
        nativeType: "json",
      }),
    ).toEqual({ kind: "json", language: "json", formattedText: "[01, 1]" });
  });

  it("jsonFormatLossless Shift+Alt+F then Apply in an editable JSON cell commits unchanged values", () => {
    const onConfirm = vi.fn<(value: string) => void>();
    function Harness() {
      const [draft, setDraft] = useState(LOSSY_JSON);
      return (
        <LargeMonacoDialog
          title="Cell"
          description="payload"
          value={draft}
          language="json"
          ariaLabel="Cell data"
          confirmLabel="Apply"
          onChange={setDraft}
          onCancel={() => {}}
          onConfirm={() => onConfirm(draft)}
        />
      );
    }
    render(<Harness />);
    const [editor] = monacoState.editors;

    runFormatAction(editor);
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));

    expect(onConfirm).toHaveBeenCalledWith(LOSSY_JSON_FORMATTED);
    const committed = onConfirm.mock.calls[0][0];
    expect(committed).toContain("9007199254740993");
    expect(committed).toContain("0.10000000000000000555");
    expect(committed.match(/"k"/g)).toHaveLength(2);
  });
});
