import {
  chmodSync,
  existsSync,
  mkdirSync,
  realpathSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { delimiter, join, relative } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resolveSQLiteBuildTools } from "../../src/extension/utils/sqliteBuildTools";
import { createProjectTempDir } from "../runtime/tempDirectories";

// Discovery-only fixtures: no fixture node/npm/shim is ever executed.
function installation(root: string) {
  const bin = join(root, "bin");
  const npmCli = join(root, "lib/node_modules/npm/bin/npm-cli.js");
  mkdirSync(bin, { recursive: true });
  mkdirSync(join(npmCli, ".."), { recursive: true });
  const node = join(bin, "node");
  writeFileSync(node, "fixture interpreter, never executed");
  chmodSync(node, 0o700);
  writeFileSync(npmCli, "fixture npm, never executed");
  symlinkSync(relative(bin, npmCli), join(bin, "npm"));
  return { bin, node, npmCli };
}

async function fixture() {
  const root = await createProjectTempDir("sqlite-build-boundaries");
  const packageRoot = join(root, "extension");
  const runtimeRoot = join(root, "runtime");
  const workspaceRoots = [
    join(root, "workspace-one"),
    join(root, "workspace-two"),
  ];
  for (const path of [packageRoot, runtimeRoot, ...workspaceRoots])
    mkdirSync(path);
  return {
    root,
    packageRoot,
    runtimeRoot,
    workspaceRoots,
    resolve(path: string[], roots: readonly string[] = workspaceRoots) {
      return resolveSQLiteBuildTools(
        packageRoot,
        runtimeRoot,
        { PATH: path.join(delimiter) },
        roots,
      );
    },
  };
}

afterEach(() => vi.restoreAllMocks());

describe.skipIf(process.platform === "win32")(
  "SQLite actual workspace trust boundary",
  () => {
    it.each([
      0, 1,
    ])("rejects conventional tools in workspace root %s even when cwd differs", async (index) => {
      const f = await fixture();
      vi.spyOn(process, "cwd").mockReturnValue(
        join(f.root, "unrelated-extension-host"),
      );
      const tools = installation(join(f.workspaceRoots[index], "tools"));
      expect(() => f.resolve([tools.bin])).toThrow("all workspace roots");
    });

    it("excludes every workspace root from selected and child PATH", async () => {
      const f = await fixture();
      const workspaceTools = f.workspaceRoots.map((root) =>
        installation(join(root, "tools")),
      );
      const trusted = installation(join(f.root, "external"));
      const tools = f.resolve([
        ...workspaceTools.map((tool) => tool.bin),
        trusted.bin,
      ]);
      expect(tools.node).toBe(realpathSync(trusted.node));
      expect(tools.env.PATH).toBe(trusted.bin);
    });

    it("rejects case aliases with conventional relative npm symlinks", async ({
      skip,
    }) => {
      const f = await fixture();
      installation(join(f.workspaceRoots[0], "tools"));
      const alias = join(f.root, "WORKSPACE-ONE", "tools", "bin");
      if (!existsSync(alias)) return skip(); // Case-sensitive filesystem.
      expect(() => f.resolve([alias])).toThrow("trusted Node.js/npm");
      const trusted = installation(join(f.root, "external"));
      const tools = f.resolve([alias, trusted.bin]);
      expect(tools.node).toBe(realpathSync(trusted.node));
      expect(tools.env.PATH).toBe(trusted.bin);
    });

    it("does not reject distinct case-sensitive sibling directories", async ({
      skip,
    }) => {
      const f = await fixture();
      const sibling = join(f.root, "WORKSPACE-ONE");
      if (existsSync(sibling)) return skip(); // Case-insensitive filesystem.
      const trusted = installation(join(sibling, "tools"));
      expect(f.resolve([trusted.bin]).node).toBe(realpathSync(trusted.node));
    });

    it("rejects lexical workspace tools even when the directory symlinks outside", async () => {
      const f = await fixture();
      const external = installation(join(f.root, "external"));
      const alias = join(f.workspaceRoots[0], "external-bin");
      symlinkSync(external.bin, alias, "dir");
      expect(() => f.resolve([alias])).toThrow("trusted Node.js/npm");
    });

    it("rejects an external PATH directory symlink into a workspace", async () => {
      const f = await fixture();
      const internal = installation(join(f.workspaceRoots[1], "tools"));
      const alias = join(f.root, "external-bin");
      symlinkSync(internal.bin, alias, "dir");
      expect(() => f.resolve([alias])).toThrow("trusted Node.js/npm");
    });

    it("excludes both lexical and canonical spellings of a symlinked workspace root", async () => {
      const f = await fixture();
      const internal = installation(join(f.workspaceRoots[1], "tools"));
      const alias = join(f.root, "workspace-alias");
      symlinkSync(f.workspaceRoots[1], alias, "dir");
      for (const bin of [internal.bin, join(alias, "tools/bin")])
        expect(() => f.resolve([bin], [f.workspaceRoots[0], alias])).toThrow(
          "trusted Node.js/npm",
        );
    });

    it.each([
      "node",
      "npm",
    ])("rejects an external %s symlink whose canonical target is workspace-controlled", async (name) => {
      const f = await fixture();
      const internal = installation(join(f.workspaceRoots[1], "tools"));
      const external = join(f.root, "external-bin");
      mkdirSync(external);
      const trusted = installation(join(f.root, "trusted"));
      symlinkSync(
        name === "node" ? internal.node : trusted.node,
        join(external, "node"),
      );
      symlinkSync(
        name === "npm" ? internal.npmCli : trusted.npmCli,
        join(external, "npm"),
      );
      expect(() => f.resolve([external])).toThrow("trusted Node.js/npm");
      const tools = f.resolve([external, trusted.bin]);
      expect(tools.node).toBe(realpathSync(trusted.node));
      // A rejected workspace interpreter must not survive in nested-action PATH.
      if (name === "node") expect(tools.env.PATH).toBe(trusted.bin);
    });

    it.each([
      "extension",
      "runtime",
    ])("retains lexical/canonical %s exclusions", async (name) => {
      const f = await fixture();
      const internal = installation(
        join(name === "extension" ? f.packageRoot : f.runtimeRoot, "tools"),
      );
      const alias = join(f.root, "external-bin");
      symlinkSync(internal.bin, alias, "dir");
      for (const bin of [internal.bin, alias])
        expect(() => f.resolve([bin])).toThrow("trusted Node.js/npm");
    });

    it("accepts an installation under HOME cwd when it is outside all actual workspace roots", async () => {
      const f = await fixture();
      const home = join(f.root, "home");
      const trusted = installation(join(home, ".nvm/versions/node/v24.0.0"));
      vi.spyOn(process, "cwd").mockReturnValue(home);
      const tools = f.resolve([trusted.bin]);
      expect(tools.node).toBe(realpathSync(trusted.node));
      expect(tools.npmCli).toBe(realpathSync(trusted.npmCli));
      expect(tools.env.PATH).toBe(trusted.bin);
    });

    it("does not treat cwd as a workspace when no folders are open", async () => {
      const f = await fixture();
      const trusted = installation(join(f.root, "home/tools"));
      vi.spyOn(process, "cwd").mockReturnValue(join(f.root, "home"));
      expect(f.resolve([trusted.bin], []).node).toBe(
        realpathSync(trusted.node),
      );
    });

    it("keeps absolute-PATH-only discovery", async () => {
      const f = await fixture();
      expect(() => f.resolve(["", ".", "relative/bin"])).toThrow(
        "absolute PATH",
      );
    });
  },
);

describe.skipIf(process.platform === "win32")(
  "SQLite conventional layouts and shim-only compatibility gap",
  () => {
    it.each([
      ".nvm/versions/node/v24.0.0",
      ".asdf/installs/nodejs/24.0.0",
      ".nodenv/versions/24.0.0",
    ])("accepts conventional installation layout %s without asserting manager integration", async (layout) => {
      const f = await fixture();
      const trusted = installation(join(f.root, "home", layout));
      expect(f.resolve([trusted.bin]).npmCli).toBe(
        realpathSync(trusted.npmCli),
      );
    });

    it("accepts bin-directory symlinks and independently canonical node/npm targets", async () => {
      const f = await fixture();
      const nodeInstallation = installation(join(f.root, "node-installation"));
      const npmInstallation = installation(join(f.root, "npm-installation"));
      const bin = join(f.root, "symlink-bin");
      const alias = join(f.root, "bin-alias");
      mkdirSync(bin);
      symlinkSync(nodeInstallation.node, join(bin, "node"));
      symlinkSync(npmInstallation.npmCli, join(bin, "npm"));
      symlinkSync(bin, alias, "dir");
      const tools = f.resolve([alias]);
      expect(tools.node).toBe(realpathSync(nodeInstallation.node));
      expect(tools.npmCli).toBe(realpathSync(npmInstallation.npmCli));
    });

    it("diagnoses shim-only PATH safely, with no workspace npx fallback", async () => {
      const f = await fixture();
      const bin = join(f.root, "manager-shims");
      mkdirSync(bin);
      for (const name of ["node", "npm", "npx"]) {
        writeFileSync(join(bin, name), "fixture manager shim, never executed");
        chmodSync(join(bin, name), 0o700);
      }
      const workspaceBin = join(f.workspaceRoots[0], "node_modules/.bin");
      mkdirSync(workspaceBin, { recursive: true });
      writeFileSync(
        join(workspaceBin, "npx"),
        "fixture workspace npx, never executed",
      );
      expect(() => f.resolve([bin, workspaceBin])).toThrow(
        "shim-only manager PATH entries are not resolved or executed",
      );
      expect(() => f.resolve([bin])).toThrow(
        "actual Node/npm installation bin directory",
      );
      const trusted = installation(join(f.root, "actual-installation"));
      const tools = f.resolve([bin, trusted.bin, workspaceBin]);
      expect(tools.node).toBe(realpathSync(trusted.node));
      expect(tools.npmCli).toBe(realpathSync(trusted.npmCli));
      expect(tools.env.PATH?.split(delimiter)).not.toContain(workspaceBin);
    });
  },
);
