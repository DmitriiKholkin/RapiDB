import { spawnSync } from "node:child_process";

function run(command, args) {
  return spawnSync(command, args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function fail(message, detail) {
  if (detail) {
    console.error(`${message}\n${detail.trim()}`);
  } else {
    console.error(message);
  }
  process.exit(1);
}

const dockerVersion = run("docker", ["--version"]);
if (dockerVersion.error) {
  fail(
    "[RapiDB:testdb] Docker CLI is not available. Install Docker Desktop or make `docker` available in PATH.",
    dockerVersion.error.message,
  );
}

if (dockerVersion.status !== 0) {
  fail(
    "[RapiDB:testdb] Docker CLI is installed but did not respond correctly.",
    dockerVersion.stderr || dockerVersion.stdout,
  );
}

const dockerInfo = run("docker", ["info", "--format", "{{.ServerVersion}}"]);
if (dockerInfo.error) {
  fail(
    "[RapiDB:testdb] Docker daemon is not reachable. Start Docker Desktop before running compose-backed tests.",
    dockerInfo.error.message,
  );
}

if (dockerInfo.status !== 0) {
  fail(
    "[RapiDB:testdb] Docker daemon is not reachable. Start Docker Desktop before running compose-backed tests.",
    dockerInfo.stderr || dockerInfo.stdout,
  );
}

const composeVersion = run("docker", ["compose", "version"]);
if (composeVersion.error) {
  fail(
    "[RapiDB:testdb] Docker Compose is not available. Install a Docker version with the compose plugin.",
    composeVersion.error.message,
  );
}

if (composeVersion.status !== 0) {
  fail(
    "[RapiDB:testdb] Docker Compose is not available. Install a Docker version with the compose plugin.",
    composeVersion.stderr || composeVersion.stdout,
  );
}

console.log("[RapiDB:testdb] Docker preflight check passed.");
