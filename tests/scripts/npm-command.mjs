import { spawn } from "node:child_process";
import path from "node:path";

const SHELL_SAFE_ARG = /^[\w.\-/\\:=@+,]+$/;

// Windows refuses to spawn .cmd/.bat files without a shell (CVE-2024-27980),
// so prefer running npm's JS entry point with the current Node binary.
export function spawnNpm(args, options) {
  const npmExecPath = process.env.npm_execpath;
  if (npmExecPath && /^npm-cli\.c?js$/i.test(path.basename(npmExecPath))) {
    const nodePath = process.env.npm_node_execpath || process.execPath;
    return spawn(nodePath, [npmExecPath, ...args], options);
  }

  if (process.platform === "win32") {
    // cmd concatenates arguments unescaped, so refuse anything it could
    // reinterpret instead of attempting cmd quoting.
    const unsafeArg = args.find((arg) => !SHELL_SAFE_ARG.test(arg));
    if (unsafeArg !== undefined) {
      throw new Error(
        `[RapiDB:test] Refusing to pass ${JSON.stringify(unsafeArg)} through cmd.exe; run this script via npm so it can avoid the shell.`,
      );
    }
    return spawn("npm.cmd", args, { ...options, shell: true });
  }

  return spawn("npm", args, options);
}
