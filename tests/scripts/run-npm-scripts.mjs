import { spawn } from "node:child_process";

const rawArgs = process.argv.slice(2);
const mainScripts = [];
const finallyScripts = [];
let targetScripts = mainScripts;
let skipFinallyIfEnvNot = null;

for (let index = 0; index < rawArgs.length; index += 1) {
  const arg = rawArgs[index];
  if (arg === "--finally") {
    targetScripts = finallyScripts;
    continue;
  }

  if (arg === "--finally-if-env-not") {
    skipFinallyIfEnvNot = rawArgs[index + 1] ?? null;
    index += 1;
    continue;
  }

  targetScripts.push(arg);
}

if (mainScripts.length === 0) {
  console.error("[RapiDB:test] No npm scripts were provided.");
  process.exit(1);
}

const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";

function isTruthyEnv(value) {
  if (typeof value !== "string") {
    return false;
  }

  return !["", "0", "false", "no", "off"].includes(value.toLowerCase());
}

function runScript(scriptName) {
  return new Promise((resolve) => {
    const child = spawn(npmCommand, ["run", scriptName], {
      stdio: "inherit",
      env: process.env,
    });

    child.on("close", (code) => {
      resolve(code ?? 1);
    });
    child.on("error", () => {
      resolve(1);
    });
  });
}

let exitCode = 0;
for (const scriptName of mainScripts) {
  const code = await runScript(scriptName);
  if (code !== 0) {
    exitCode = code;
    break;
  }
}

const shouldRunFinally =
  !skipFinallyIfEnvNot || !isTruthyEnv(process.env[skipFinallyIfEnvNot]);

if (shouldRunFinally) {
  for (const scriptName of finallyScripts) {
    const code = await runScript(scriptName);
    if (exitCode === 0 && code !== 0) {
      exitCode = code;
    }
  }
}

process.exit(exitCode);
