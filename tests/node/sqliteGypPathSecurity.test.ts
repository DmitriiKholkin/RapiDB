import { execFile } from "node:child_process";
import {
  accessSync,
  constants,
  existsSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { delimiter, dirname, isAbsolute, join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";
import {
  pinSQLiteSourceNodeActions,
  resolveSQLiteBuildTools,
} from "../../src/extension/utils/sqliteBuildTools";
import { sqliteNodeGypArguments } from "../../src/extension/utils/sqliteInstaller";
import { createProjectTempDir } from "../runtime/tempDirectories";

const execFileAsync = promisify(execFile);
const require = createRequire(import.meta.url);
const repository = fileURLToPath(new URL("../../", import.meta.url));
const gypLibrary = join(
  dirname(require.resolve("node-gyp/package.json")),
  "gyp/pylib",
);

function trustedExecutable(
  name: string,
  env: NodeJS.ProcessEnv,
  root: string,
): string {
  for (const directory of (env.PATH ?? "").split(delimiter)) {
    if (!isAbsolute(directory)) continue;
    try {
      const executable = realpathSync(join(directory, name));
      if (
        [repository, root].some((excluded) => {
          const rel = relative(excluded, executable);
          return (
            rel === "" ||
            (rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel))
          );
        })
      )
        continue;
      accessSync(executable, constants.X_OK);
      return executable;
    } catch {}
  }
  throw new Error(
    `A trusted external ${name} is required for the real SQLite GYP regression.`,
  );
}

async function fixture() {
  const root = await createProjectTempDir("sqlite-real-gyp");
  const source = join(root, "source");
  const deps = join(source, "deps");
  mkdirSync(join(deps, "sqlite3"), { recursive: true });
  writeFileSync(
    join(deps, "copy.js"),
    readFileSync(
      new URL(
        "../../node_modules/better-sqlite3/deps/copy.js",
        import.meta.url,
      ),
    ),
  );
  for (const file of ["sqlite3.c", "sqlite3.h", "sqlite3ext.h"])
    writeFileSync(join(deps, "sqlite3", file), `actual ${file}`);
  // Complete GYP input, not a regex/JSON-only parser fixture. Both actions use
  // the pinned source's real copy.js with the builtin and custom argument forms.
  const gyp = `{
    'targets': [{
      'target_name': 'copy_fixture', 'type': 'none',
      'actions': [
        {'action_name': 'builtin', 'inputs': ['copy.js', 'sqlite3/sqlite3.c'],
         'outputs': ['out/builtin/sqlite3.c'],
         'action': ['node', 'copy.js', 'out/builtin', '']},
        {'action_name': 'custom', 'inputs': ['copy.js', 'sqlite3/sqlite3.c'],
         'outputs': ['out/custom/sqlite3.c'],
         'action': ['node', 'copy.js', 'out/custom', ${JSON.stringify(join(deps, "sqlite3"))}]}
      ]
    }]
  }\n`;
  const gypPath = join(deps, "sqlite3.gyp");
  writeFileSync(gypPath, gyp);
  const tools = resolveSQLiteBuildTools(repository, root, process.env, [
    repository,
  ]);
  const python = trustedExecutable("python3", tools.env, root);
  const make = trustedExecutable("make", tools.env, root);
  const started = join(root, "gyp-started");
  const script = `
import sys
sys.path.insert(0, ${JSON.stringify(gypLibrary)})
import gyp
with open(${JSON.stringify(started)}, 'w') as marker:
    marker.write('real GYP parser/generator entered')
sys.exit(gyp.main(['--depth=.', '-f', 'make', '-Gstandalone', 'sqlite3.gyp']))
`;
  // Admission is the same production boundary used before npm/node-gyp. The
  // execFile below is real and uses an absolute trusted Python with isolated
  // startup (-I), no inherited interpreter injection, and explicitly bundled GYP.
  async function admitAndGenerate(
    node: string,
    platform: NodeJS.Platform = process.platform,
    buildPaths?: { toolDir: string; headerDir: string },
  ) {
    if (buildPaths) {
      sqliteNodeGypArguments(
        buildPaths.toolDir,
        buildPaths.headerDir,
        "42.0.0",
        platform,
        process.arch,
      );
    }
    pinSQLiteSourceNodeActions(source, node, platform);
    return await execFileAsync(python, ["-I", "-c", script], {
      cwd: deps,
      env: tools.env,
      timeout: 30_000,
    });
  }
  return {
    root,
    source,
    deps,
    gyp,
    gypPath,
    started,
    tools,
    make,
    admitAndGenerate,
  };
}

describe.skipIf(process.platform === "win32")(
  "SQLite paths through the real GYP parser and make generator",
  () => {
    it.each([
      "toolDir",
      "headerDir",
    ] as const)("rejects storage-derived %s expansion before entering real GYP", async (field) => {
      const f = await fixture();
      for (const component of [
        "<!(echo STORAGE_GYP_EXPANDED)",
        "<(node_root_dir)",
        "$(echo STORAGE_MAKE_EXPANDED)",
        "%PATH%",
      ]) {
        const paths = {
          toolDir: join(f.root, "tools"),
          headerDir: join(f.root, "headers"),
          [field]: join(f.root, component, "private-build-path"),
        };
        await expect(
          f.admitAndGenerate(f.tools.node, process.platform, paths),
        ).rejects.toThrow("unsupported GYP/build expansion");
        expect(existsSync(f.started)).toBe(false);
        expect(readFileSync(f.gypPath, "utf8")).toBe(f.gyp);
      }
    });

    it("rejects a safe header alias targeting an expansion-bearing storage path", async () => {
      const f = await fixture();
      const target = join(f.root, "<!(echo STORAGE_GYP_EXPANDED)");
      mkdirSync(target);
      const alias = join(f.root, "safe-headers");
      symlinkSync(target, alias, "dir");
      await expect(
        f.admitAndGenerate(f.tools.node, process.platform, {
          toolDir: join(f.root, "tools"),
          headerDir: alias,
        }),
      ).rejects.toThrow(
        "canonical path contains unsupported GYP/build expansion",
      );
      expect(existsSync(f.started)).toBe(false);
    });

    it.each([
      "/opt/trusted/<!(echo C1_GYP_INTERPOLATED)/node",
      "/opt/trusted/<(node_root_dir)/node",
      "/opt/trusted/>(node_root_dir)/node",
      "/opt/trusted/$(echo C1_MAKE_INTERPOLATED)/node",
      // biome-ignore lint/suspicious/noTemplateCurlyInString: Deliberate literal generator/shell expansion token, not JavaScript interpolation.
      "/opt/trusted/${HOME}/node",
      "/opt/trusted/`echo C1_SHELL_INTERPOLATED`/node",
      "/opt/trusted/;echo C1_SHELL_INTERPOLATED/node",
      "/opt/trusted/&echo C1_SHELL_INTERPOLATED/node",
      "/opt/trusted/pipe|node",
      "/opt/trusted/hash#node",
      '/opt/trusted/quote"node',
      "/opt/trusted/glob*node",
      "/opt/trusted/glob?node",
      "/opt/trusted/[node]",
      "/opt/trusted/{node}",
      "/opt/trusted/!node",
      "/opt/trusted/=node",
      "/opt/trusted/~node",
      "/opt/trusted/tab\tnode",
      "/opt/trusted/line\nnode",
      "/opt/trusted/line\rnode",
      "/opt/trusted/nul\0node",
      "C:\\trusted\\%PATH%\\node.exe",
      "C:\\trusted\\$(MSBuildProjectDirectory)\\node.exe",
      "C:\\trusted\\<!(echo C1_GYP_INTERPOLATED)\\node.exe",
    ])("rejects expansion/metacharacter path before invoking GYP: %s", async (node) => {
      const f = await fixture();
      await expect(
        f.admitAndGenerate(
          node,
          node.startsWith("C:") ? "win32" : process.platform,
        ),
      ).rejects.toThrow(
        /unsupported GYP\/build expansion or shell characters|absolute selected Node/,
      );
      expect(readFileSync(f.gypPath, "utf8")).toBe(f.gyp);
      // No Python/GYP start, no generated file, and no input containing an
      // injected command ever reaches a parser or shell.
      expect(existsSync(f.started)).toBe(false);
      expect(existsSync(join(f.deps, "Makefile"))).toBe(false);
      expect(existsSync(join(f.deps, "out"))).toBe(false);
    });

    it("generates and executes both real copy actions with spaces/apostrophes in the absolute Node path", async () => {
      const f = await fixture();
      const installation = join(f.root, "trusted Node's installation (local)");
      mkdirSync(installation);
      const node = join(installation, "selected Node's executable");
      // Invoke the real interpreter via a path with the tested punctuation,
      // without a fake launcher or relocating its shared-library installation.
      symlinkSync(f.tools.node, node);
      await f.admitAndGenerate(node);
      expect(existsSync(f.started)).toBe(true);
      expect(existsSync(join(f.deps, "Makefile"))).toBe(true);
      await execFileAsync(f.make, ["-f", "Makefile", "copy_fixture"], {
        cwd: f.deps,
        env: f.tools.env,
        timeout: 30_000,
      });
      for (const action of ["builtin", "custom"]) {
        expect(
          readFileSync(join(f.deps, "out", action, "sqlite3.c"), "utf8"),
        ).toBe("actual sqlite3.c");
        expect(
          readFileSync(join(f.deps, "out", action, "sqlite3.h"), "utf8"),
        ).toBe("actual sqlite3.h");
      }
    });

    it("parses and generates ordinary Windows drive/backslash/space/apostrophe paths without expansion", async () => {
      const f = await fixture();
      const node = "C:\\Program Files (x86)\\Node's installation\\node.exe";
      await f.admitAndGenerate(node, "win32");
      expect(existsSync(f.started)).toBe(true);
      expect(
        readFileSync(join(f.deps, "copy_fixture.target.mk"), "utf8"),
      ).toContain("node.exe");
    });
  },
);
