import {
  accessSync,
  constants,
  readFileSync,
  realpathSync,
  type Stats,
  statSync,
  writeFileSync,
} from "node:fs";
import {
  delimiter,
  dirname,
  isAbsolute,
  join,
  relative,
  resolve,
  sep,
  win32,
} from "node:path";

function inside(path: string, root: string): boolean {
  const rel = relative(root, path);
  if (
    rel === "" ||
    (!rel.startsWith(`..${sep}`) && rel !== ".." && !isAbsolute(rel))
  )
    return true;
  let rootStat: Stats;
  try {
    rootStat = statSync(root);
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ENOENT";
  }
  for (let ancestor = path; ; ancestor = dirname(ancestor)) {
    try {
      const stat = statSync(ancestor);
      if (stat.dev === rootStat.dev && stat.ino === rootStat.ino) return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") return true;
    }
    if (dirname(ancestor) === ancestor) return false;
  }
}

export function resolveSQLiteBuildTools(
  packageRoot: string,
  runtimeRoot: string,
  environment: NodeJS.ProcessEnv = process.env,
  workspaceRoots: readonly string[] = [],
): { node: string; npmCli: string; env: NodeJS.ProcessEnv } {
  const excludedRoots = [packageRoot, runtimeRoot, ...workspaceRoots].flatMap(
    (path) => {
      const absolute = resolve(path);
      try {
        return [absolute, realpathSync(absolute)];
      } catch {
        return [absolute];
      }
    },
  );
  const permitted = (path: string): boolean =>
    !excludedRoots.some((root) => inside(path, root));
  const pathKey =
    Object.keys(environment).find((key) => key.toLowerCase() === "path") ??
    "PATH";
  const directories = (environment[pathKey] ?? "")
    .split(delimiter)
    .filter((directory) => {
      if (!isAbsolute(directory) || !permitted(resolve(directory)))
        return false;
      try {
        return permitted(realpathSync(directory));
      } catch {
        return false;
      }
    });
  for (const directory of directories) {
    try {
      const node = realpathSync(
        join(directory, process.platform === "win32" ? "node.exe" : "node"),
      );
      const npm =
        process.platform === "win32"
          ? join(directory, "node_modules", "npm", "bin", "npm-cli.js")
          : realpathSync(join(directory, "npm"));
      const npmCli = realpathSync(npm);
      if (
        !permitted(node) ||
        !permitted(npmCli) ||
        !npmCli.endsWith(`${sep}npm${sep}bin${sep}npm-cli.js`)
      )
        continue;
      accessSync(
        node,
        process.platform === "win32" ? constants.F_OK : constants.X_OK,
      );
      const env = sqliteBuildEnvironment(
        environment,
        [
          directory,
          ...directories.filter((candidateDirectory) => {
            if (candidateDirectory === directory) return false;
            try {
              return permitted(
                realpathSync(
                  join(
                    candidateDirectory,
                    process.platform === "win32" ? "node.exe" : "node",
                  ),
                ),
              );
            } catch (error) {
              return (error as NodeJS.ErrnoException).code === "ENOENT";
            }
          }),
        ].join(delimiter),
        runtimeRoot,
      );
      return { node, npmCli, env };
    } catch {}
  }
  throw new Error(
    "A trusted Node.js/npm installation is required for the verified SQLite source build. No node and npm entries in the same absolute PATH directory resolved outside the extension package, runtime and all workspace roots to an executable Node and npm/bin/npm-cli.js. Conventional installation/symlink layouts are supported; shim-only manager PATH entries are not resolved or executed. Add the actual Node/npm installation bin directory to the extension host PATH, or ask the publisher to provide pinned patched prebuilts. Python/C++ build tools are also required; workspace npx is never used.",
  );
}

export function pinSQLiteSourceNodeActions(
  sourceDir: string,
  node: string,
  platform: NodeJS.Platform = process.platform,
): void {
  if (
    !(platform === "win32" ? win32.isAbsolute(node) : isAbsolute(node)) ||
    /[\r\n\0]/.test(node)
  ) {
    throw new Error(
      "SQLite source actions require an absolute selected Node interpreter.",
    );
  }
  assertSQLiteBuildPath(node, "Node", platform);
  const path = join(sourceDir, "deps", "sqlite3.gyp");
  const gyp = readFileSync(path, "utf8");
  const action = /('action'\s*:\s*\[\s*)'node'(?=\s*,\s*'copy\.js')/g;
  if ([...gyp.matchAll(action)].length !== 2) {
    throw new Error(
      "Pinned SQLite source copy.js actions changed. Review the source pin and absolute Node action binding before building.",
    );
  }
  const patched = gyp.replace(
    action,
    (_match, prefix: string) => `${prefix}${JSON.stringify(node)}`,
  );
  writeFileSync(path, patched, "utf8");
}

export function assertSQLiteBuildPath(
  path: string,
  label: string,
  platform: NodeJS.Platform = process.platform,
): void {
  if (
    !(platform === "win32" ? win32.isAbsolute(path) : isAbsolute(path)) ||
    !/^[\p{L}\p{N} /\\._:'()+-]+$/u.test(path)
  ) {
    throw new Error(
      `SQLite source ${label} path contains unsupported GYP/build expansion or shell characters or is not absolute. Use conventional paths with letters, numbers, spaces, apostrophes, separators or . _ ( ) + -. No GYP input was modified or executed.`,
    );
  }
  let canonical: string;
  try {
    canonical = realpathSync(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  if (canonical !== path) {
    if (!/^[\p{L}\p{N} /\\._:'()+-]+$/u.test(canonical)) {
      throw new Error(
        `SQLite source ${label} canonical path contains unsupported GYP/build expansion or shell characters.`,
      );
    }
  }
}

export function sqliteBuildEnvironment(
  environment: NodeJS.ProcessEnv,
  path: string,
  runtimeRoot: string,
): NodeJS.ProcessEnv {
  const allowed = new Set([
    "SYSTEMROOT",
    "WINDIR",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "PROGRAMW6432",
    "SYSTEMDRIVE",
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_ARCHITEW6432",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NO_PROXY",
  ]);
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(environment)) {
    const canonical = key.toUpperCase();
    if (allowed.has(canonical) && value !== undefined) env[canonical] = value;
  }
  const home = join(runtimeRoot, "build-home");
  const temp = join(home, "tmp");
  const windowsRoot = env.SYSTEMROOT ?? env.WINDIR;
  if (windowsRoot) {
    env.COMSPEC = join(windowsRoot, "System32", "cmd.exe");
    env.PATHEXT = ".COM;.EXE;.BAT;.CMD";
  }
  return {
    ...env,
    PATH: path,
    HOME: home,
    USERPROFILE: home,
    APPDATA: join(home, "appdata"),
    LOCALAPPDATA: join(home, "localappdata"),
    TMP: temp,
    TEMP: temp,
    TMPDIR: temp,
    PYTHONNOUSERSITE: "1",
    PYTHONSAFEPATH: "1",
    ELECTRON_RUN_AS_NODE: "1",
  };
}

interface LockedPackage {
  version: string;
  resolved: string;
  integrity: string;
  dependencies?: Record<string, string>;
  optionalDependencies?: Record<string, string>;
  dev?: boolean;
  devOptional?: boolean;
}

export function createSQLiteBuildToolLock() {
  const repositoryLock = require("../../../package-lock.json") as {
    packages: Record<string, LockedPackage>;
  };
  const tool = repositoryLock.packages["node_modules/node-gyp"];
  const manifest = {
    name: "rapidb-sqlite-build-tools",
    version: "1.0.0",
    private: true,
    dependencies: { "node-gyp": tool.version },
  };
  const packages: Record<string, unknown> = { "": manifest };
  const visit = (path: string): void => {
    if (packages[path]) return;
    const locked = repositoryLock.packages[path];
    if (
      !locked?.resolved.startsWith("https://registry.npmjs.org/") ||
      !/^sha512-[A-Za-z0-9+/]+={0,2}$/.test(locked.integrity)
    ) {
      throw new Error(
        `Missing trusted lockfile integrity for SQLite build dependency ${path}.`,
      );
    }
    const { dev: _dev, devOptional: _devOptional, ...entry } = locked;
    packages[path] = entry;
    for (const dependency of Object.keys({
      ...locked.dependencies,
      ...locked.optionalDependencies,
    })) {
      let parent = path;
      while (true) {
        const candidate = `${parent ? `${parent}/` : ""}node_modules/${dependency}`;
        if (repositoryLock.packages[candidate]) {
          visit(candidate);
          break;
        }
        if (!parent)
          throw new Error(
            `Missing locked SQLite build dependency ${dependency}.`,
          );
        const index = parent.lastIndexOf("/node_modules/");
        parent = index < 0 ? "" : parent.slice(0, index);
      }
    }
  };
  visit("node_modules/node-gyp");
  return {
    manifest,
    lock: {
      name: manifest.name,
      version: manifest.version,
      lockfileVersion: 3,
      requires: true,
      packages,
    },
  };
}
