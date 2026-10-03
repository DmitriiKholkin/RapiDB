import { execFile } from "node:child_process";
import { generateKeyPairSync } from "node:crypto";
import { cp, mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import type { Duplex } from "node:stream";
import { promisify } from "node:util";
import { listFiles, PackageManager } from "@vscode/vsce";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";
import { extensionConfig } from "../../esbuild.config.mjs";
import { createProjectTempDir } from "../runtime/tempDirectories";

const execFileAsync = promisify(execFile);
const projectRoot = resolve(__dirname, "../..");
const runner = `
const assert = require('node:assert/strict');
const Module = require('node:module');
const path = require('node:path');
const net = require('node:net');
const root = process.cwd();
const originalLoad = Module._load;
const originalResolve = Module._resolveFilename;
// Only VS Code's host API is mocked. SSH and all its dependencies must resolve
// from the packaged files, never the checkout or a global NODE_PATH.
Module._load = function(name, ...args) {
  if (name === 'vscode') return {
    TreeItem: class {}, EventEmitter: class {}, ThemeColor: class {}, ThemeIcon: class {}
  };
  return originalLoad.call(this, name, ...args);
};
Module._resolveFilename = function(name, ...args) {
  const result = originalResolve.call(this, name, ...args);
  if (!Module.isBuiltin(result)) {
    assert.ok(result.startsWith(root + path.sep), 'Non-packaged dependency: ' + result);
  }
  return result;
};
(async () => {
  const { __packagedCreateSshRuntime: createSshRuntime } = require('./dist/extension.js');
  const runtime = await createSshRuntime({
    host: '127.0.0.1', port: Number(process.argv[2]), username: 'package-test',
    hostVerificationMode: 'trustOnFirstUse', auth: { kind: 'password', password: 'local-only' }
  }, { kind: 'tcpForward', remoteHost: 'echo.internal', remotePort: 4321 });
  try {
    assert.match(runtime.verifiedFingerprintSha256, /^SHA256:/);
    await new Promise((resolve, reject) => {
      const socket = net.connect(runtime.transport.localPort, runtime.transport.localHost);
      socket.on('error', reject);
      socket.on('connect', () => socket.write('packaged-ssh-roundtrip'));
      socket.on('data', data => {
        try { assert.equal(data.toString(), 'packaged-ssh-roundtrip'); resolve(); }
        catch (error) { reject(error); }
        socket.destroy();
      });
    });
    assert.equal(require('./node_modules/ssh2/lib/protocol/crypto.js').bindingAvailable, false);
    assert.throws(() => require.resolve('cpu-features'), { code: 'MODULE_NOT_FOUND' });
    console.log('packaged SSH handshake + forward roundtrip; native-free fallback');
  } finally { await runtime.dispose(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
`;

interface LocalSshClient {
  on(event: "error" | "close", listener: () => void): void;
  on(
    event: "authentication",
    listener: (context: {
      method: string;
      username: string;
      password?: string;
      accept(): void;
      reject(): void;
    }) => void,
  ): void;
  on(
    event: "tcpip",
    listener: (
      accept: () => Duplex,
      reject: () => void,
      info: { destIP: string; destPort: number },
    ) => void,
  ): void;
  end(): void;
}
interface LocalSshServer {
  listen(port: number, host: string, callback: () => void): void;
  address(): { port: number };
  close(callback: () => void): void;
  on(event: "connection", listener: (client: LocalSshClient) => void): void;
}

describe("SSH installed extension packaging", () => {
  it("includes the required dependency closure and rejects native/build assets even when present", async () => {
    const files = await listFiles({
      cwd: projectRoot,
      packageManager: PackageManager.Npm,
    });
    const pending = [join(projectRoot, "node_modules/ssh2/package.json")];
    const visited = new Set<string>();
    while (pending.length) {
      const manifestPath = pending.pop();
      if (!manifestPath || visited.has(manifestPath)) continue;
      visited.add(manifestPath);
      const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as {
        dependencies?: Record<string, string>;
      };
      expect(files).toContain(manifestPath.slice(projectRoot.length + 1));
      const require = createRequire(manifestPath);
      expect(files).toContain(
        require.resolve(dirname(manifestPath)).slice(projectRoot.length + 1),
      );
      for (const name of Object.keys(manifest.dependencies ?? {})) {
        pending.push(require.resolve(`${name}/package.json`));
      }
    }
    expect(visited.size).toBe(5);

    // npm reports canonical paths (not macOS's /var symlink spelling).
    const root = await realpath(await createProjectTempDir("ssh-ignore"));
    await cp(join(projectRoot, ".vscodeignore"), join(root, ".vscodeignore"));
    await writeFile(
      join(root, "package.json"),
      JSON.stringify({
        name: "ssh-ignore-fixture",
        publisher: "rapidb",
        version: "1.0.0",
        engines: { vscode: "^1.101.0" },
        dependencies: { ssh2: "1.0.0", "cpu-features": "1.0.0", nan: "1.0.0" },
      }),
    );
    const candidates = [
      "node_modules/ssh2/lib/index.js",
      "node_modules/ssh2/lib/protocol/crypto/poly1305.js",
      "node_modules/ssh2/lib/protocol/crypto/build/Release/sshcrypto.node",
      "node_modules/ssh2/lib/protocol/crypto/build/config.js",
      "node_modules/ssh2/lib/protocol/crypto/src/binding.cc",
      "node_modules/ssh2/lib/protocol/crypto/binding.gyp",
      "node_modules/ssh2/install.js",
      "node_modules/cpu-features/lib/index.js",
      "node_modules/cpu-features/build/Release/cpufeatures.node",
      "node_modules/nan/nan.h",
    ];
    for (const file of candidates) {
      await mkdir(dirname(join(root, file)), { recursive: true });
      await writeFile(join(root, file), "packaging fixture");
    }
    for (const name of ["ssh2", "cpu-features", "nan"]) {
      await writeFile(
        join(root, "node_modules", name, "package.json"),
        JSON.stringify({ name, version: "1.0.0" }),
      );
    }
    const stagedFiles = await listFiles({
      cwd: root,
      packageManager: PackageManager.Npm,
    });
    expect(
      stagedFiles.filter((file) => file.startsWith("node_modules/")).sort(),
    ).toEqual([...candidates.slice(0, 2), "node_modules/ssh2/package.json"]);
  }, 30000);

  it.each([
    { mode: "development", cipher: "aes128-gcm@openssh.com" },
    { mode: "production", cipher: "chacha20-poly1305@openssh.com" },
  ])("loads real SSH from the $mode CJS bundle and forwards using $cipher", async ({
    mode,
    cipher,
  }) => {
    const root = await createProjectTempDir("ssh-package");
    // Use vsce's actual npm dependency collection and the checkout's ignore
    // rules, then copy ONLY files that would be installed from the VSIX.
    const files = await listFiles({
      cwd: projectRoot,
      packageManager: PackageManager.Npm,
    });
    for (const file of files) {
      const target = join(root, file);
      await mkdir(dirname(target), { recursive: true });
      await cp(join(projectRoot, file), target);
    }
    await mkdir(join(root, "dist"), { recursive: true });
    const result = await build({
      ...extensionConfig,
      absWorkingDir: projectRoot,
      outfile: join(root, "dist/extension.js"),
      minify: mode === "production",
      sourcemap: false,
      logLevel: "silent",
      metafile: true,
      // Expose the real internal function, without replacing its loader or
      // changing the production entry point/dependency graph.
      plugins: [
        {
          name: "ssh-package-probe",
          setup(builder) {
            builder.onLoad(
              { filter: /src\/extension\/extension\.ts$/ },
              async (args) => ({
                contents: `${await readFile(args.path, "utf8")}\nexport { createSshRuntime as __packagedCreateSshRuntime } from './services/sshRuntime';`,
                loader: "ts",
              }),
            );
          },
        },
      ],
    });
    expect(extensionConfig.target).toBe("node20");
    expect(extensionConfig.format).toBe("cjs");
    expect(
      Object.keys(result.metafile?.inputs ?? {}).filter((file) =>
        file.includes("node_modules/ssh2/"),
      ),
    ).toEqual([]);
    expect(
      Object.values(result.metafile?.outputs ?? {}).flatMap(
        (output) => output.imports,
      ),
    ).toContainEqual({
      path: "ssh2",
      kind: "require-call",
      external: true,
    });
    const require = createRequire(join(projectRoot, "package.json"));
    const { Server } = require("ssh2") as {
      Server: new (options: unknown) => LocalSshServer;
    };
    const hostKey = generateKeyPairSync("rsa", {
      modulusLength: 2048,
    }).privateKey.export({ type: "pkcs1", format: "pem" });
    const server = new Server({
      hostKeys: [hostKey],
      algorithms: { cipher: [cipher] },
    });
    const clients = new Set<LocalSshClient>();
    server.on("connection", (client) => {
      clients.add(client);
      client.on("error", () => {});
      client.on("close", () => clients.delete(client));
      client.on("authentication", (context) => {
        if (
          context.method === "password" &&
          context.username === "package-test" &&
          context.password === "local-only"
        )
          context.accept();
        else context.reject();
      });
      client.on("tcpip", (accept, reject, info) => {
        if (info.destIP !== "echo.internal" || info.destPort !== 4321)
          return reject();
        const stream = accept();
        stream.on("error", () => {});
        stream.on("data", (data: Buffer) => stream.write(data));
      });
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const runnerFile = join(root, "ssh-package-probe.cjs");
    await writeFile(runnerFile, runner);
    try {
      const result = await execFileAsync(
        process.execPath,
        [runnerFile, String(server.address().port)],
        {
          cwd: root,
          env: { ...process.env, NODE_PATH: "", NODE_OPTIONS: "" },
          timeout: 15000,
        },
      );
      expect(result.stdout).toContain(
        "packaged SSH handshake + forward roundtrip; native-free fallback",
      );
      expect(files).toContain("node_modules/ssh2/lib/index.js");
      expect(files).toContain(
        "node_modules/ssh2/lib/protocol/crypto/poly1305.js",
      );
      expect(files).not.toContain("node_modules/ssh2/install.js");
      expect(
        files.filter((file) =>
          /node_modules\/(?:cpu-features|nan)\/|node_modules\/ssh2\/.*(?:\/build\/|\.node$|\.cc$|\.gyp$)/.test(
            file,
          ),
        ),
      ).toEqual([]);
    } finally {
      for (const client of clients) client.end();
      await new Promise<void>((resolve) => server.close(resolve));
    }
  }, 60000);
});
