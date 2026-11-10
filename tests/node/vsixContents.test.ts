import { execFile } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";
import { createProjectTempDir } from "../runtime/tempDirectories";

const execFileAsync = promisify(execFile);
const projectRoot = resolve(__dirname, "../..");

describe("VSIX contents", () => {
  it("rejects local secrets while retaining application assets and the runtime closure in real vsce ls", async () => {
    const root = await createProjectTempDir("vsix-contents");
    const put = async (
      file: string,
      contents = "DUMMY packaging fixture\n",
    ) => {
      await mkdir(dirname(join(root, file)), { recursive: true });
      await writeFile(join(root, file), contents);
    };
    await put(
      ".vscodeignore",
      await readFile(join(projectRoot, ".vscodeignore"), "utf8"),
    );
    const manifest = JSON.parse(
      await readFile(join(projectRoot, "package.json"), "utf8"),
    );
    await put(
      "package.json",
      JSON.stringify({
        ...manifest,
        scripts: {},
        devDependencies: {},
        dependencies: { oracledb: "1.0.0", ssh2: "1.0.0" },
      }),
    );
    const dependencies: Record<string, Record<string, string>> = {
      oracledb: {},
      ssh2: { asn1: "1.0.0", "bcrypt-pbkdf": "1.0.0" },
      asn1: { "safer-buffer": "1.0.0" },
      "safer-buffer": {},
      "bcrypt-pbkdf": { tweetnacl: "1.0.0" },
      tweetnacl: {},
    };
    const required = [
      "package.json",
      "README.md",
      "LICENSE.md",
      "dist/extension.js",
      "dist/sqliteWorker.js",
      "dist/webview.js",
      "dist/webview.css",
      "media/icon-app.png",
      "media/icon-market.png",
      "node_modules/oracledb/index.js",
      "node_modules/oracledb/lib/thin/index.js",
      "node_modules/oracledb/plugins/configProviders/base.js",
      "node_modules/oracledb/LICENSE.txt",
      "node_modules/oracledb/NOTICE.txt",
      "node_modules/oracledb/THIRD_PARTY_LICENSES.txt",
      "node_modules/ssh2/LICENSE",
      "node_modules/ssh2/lib/index.js",
      "node_modules/ssh2/lib/protocol/crypto.js",
      "node_modules/ssh2/lib/protocol/crypto/poly1305.js",
      "node_modules/asn1/LICENSE",
      "node_modules/asn1/lib/ber/index.js",
      "node_modules/safer-buffer/LICENSE",
      "node_modules/safer-buffer/safer.js",
      "node_modules/bcrypt-pbkdf/LICENSE",
      "node_modules/bcrypt-pbkdf/index.js",
      "node_modules/tweetnacl/LICENSE",
      "node_modules/tweetnacl/nacl-fast.js",
    ];
    for (const [name, deps] of Object.entries(dependencies)) {
      const file = `node_modules/${name}/package.json`;
      await put(
        file,
        JSON.stringify({ name, version: "1.0.0", dependencies: deps }),
      );
      required.push(file);
    }
    for (const [name, entry, license] of [
      ["better-sqlite3", "lib/index.js", "LICENSE"],
      ["bindings", "bindings.js", "LICENSE.md"],
      ["file-uri-to-path", "index.js", "LICENSE"],
    ]) {
      for (const file of ["package.json", entry, license]) {
        const runtimeFile = `.rapidb-runtime/node_modules/${name}/${file}`;
        required.push(runtimeFile);
        if (file === "package.json") {
          await put(runtimeFile, JSON.stringify({ name, version: "1.0.0" }));
        }
      }
    }
    required.push(
      "node_modules/oracledb/lib/certs/ca.pem",
      "node_modules/oracledb/lib/certs/package.key",
      ".rapidb-runtime/node_modules/better-sqlite3/lib/certs/ca.crt",
    );
    for (const file of required.filter(
      (file) => !file.endsWith("package.json"),
    )) {
      await put(file);
    }

    const forbidden = [
      ".changelog.md",
      "unexpected-local-file.json",
      "src/local.ts",
      "scripts/local.mjs",
      "scripts/demo/postgres/compose.yaml",
      "README.preview.md",
      "docs/usage-notes.md",
      "media/screenshots/workspace.png",
      "dist/webview.js.map",
      "node_modules/ssh2/install.js",
      "node_modules/ssh2/lib/protocol/crypto/build/Release/sshcrypto.node",
      "node_modules/ssh2/lib/protocol/crypto/src/binding.cc",
    ];
    for (const prefix of [
      "",
      "local/",
      "dist/",
      "media/",
      ".rapidb-runtime/",
    ]) {
      for (const file of [
        ".env",
        ".env.local",
        ".env.production",
        ".env.production.local",
        ".env.backup.json",
        ".env.d/credentials",
        "id_rsa",
        "id_ed25519",
        ".ssh/id_rsa",
        ".ssh/id_ed25519",
        ".aws/credentials",
        "tls/client.key",
        "tls/client.pem",
        "tls/client.p12",
        "tls/client.pfx",
        "tls/client.crt",
        "ssh-private.ppk",
        "credentials.json",
      ]) {
        forbidden.push(prefix + file);
      }
    }
    for (const file of forbidden) await put(file);

    const result = await execFileAsync(
      process.execPath,
      [join(projectRoot, "node_modules/@vscode/vsce/vsce"), "ls", "--no-yarn"],
      { cwd: root, timeout: 30000, maxBuffer: 1024 * 1024 },
    );
    const files = result.stdout.trim().split(/\r?\n/).sort();
    expect(files.filter((file) => forbidden.includes(file))).toEqual([]);
    expect(files).toEqual([...required].sort());
    expect(await readFile(join(root, ".env"), "utf8")).toBe(
      "DUMMY packaging fixture\n",
    );
  }, 40000);
});
