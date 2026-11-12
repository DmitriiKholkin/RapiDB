import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import type * as Monaco from "monaco-editor";

interface NativePasteCommand {
  getEditOperations(
    model: Monaco.editor.ITextModel,
    builder: {
      addTrackedEditOperation(range: Monaco.IRange, text: string): void;
      trackSelection(selection: Monaco.Selection): string;
    },
  ): void;
}

export interface RealMonacoRuntime {
  editor: typeof Monaco.editor;
  Selection: typeof Monaco.Selection;
  PasteOperation: {
    getEdits(
      config: { multiCursorPaste: "spread" },
      model: Monaco.editor.ITextModel,
      selections: Monaco.Selection[],
      text: string,
      pasteOnNewLine: boolean,
      multicursorText: string[] | null,
    ): { commands: NativePasteCommand[] };
  };
}

// Bundle the installed Monaco directly, outside Vitest's monaco-editor mock
// alias. Only CSS is omitted: models, paste commands and overlap validation
// execute the unmodified upstream implementation, in the jsdom environment.
export function loadRealMonaco(): RealMonacoRuntime {
  const require = createRequire(import.meta.url);
  const packageRoot = dirname(require.resolve("monaco-editor/package.json"));
  const api = resolve(packageRoot, "esm/vs/editor/editor.api.js");
  const paste = resolve(
    packageRoot,
    "esm/vs/editor/common/cursor/cursorTypeEditOperations.js",
  );
  const options = {
    stdin: {
      contents: `export { editor, Selection } from ${JSON.stringify(api)};
export { PasteOperation } from ${JSON.stringify(paste)};`,
      resolveDir: packageRoot,
    },
    bundle: true,
    write: false,
    format: "cjs",
    platform: "browser",
    loader: { ".css": "empty", ".ttf": "dataurl" },
    logLevel: "silent",
  };
  // esbuild must run in Node, not jsdom's cross-realm Uint8Array globals.
  const bundle = execFileSync(
    process.execPath,
    [
      "-e",
      `process.stdout.write(require(${JSON.stringify(require.resolve("esbuild"))}).buildSync(${JSON.stringify(options)}).outputFiles[0].text)`,
    ],
    { cwd: packageRoot, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 },
  );
  const output = { exports: {} };
  const evaluate = new Function("module", "exports", bundle);
  evaluate(output, output.exports);
  return output.exports as RealMonacoRuntime;
}
