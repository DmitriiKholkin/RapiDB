import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { createRequire } from "node:module";
import { Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import { vi } from "vitest";
import {
  SQLITE_ARTIFACTS,
  sqliteTarballUrl,
} from "../../src/extension/utils/sqliteArtifactPins";
import type {
  SQLiteHostProbe,
  SQLiteNativeTarget,
} from "../../src/extension/utils/sqliteNativeTarget";

const require = createRequire(import.meta.url);
export const https = require("node:https") as typeof import("node:https");
const REVIEWED_ARTIFACTS = JSON.stringify(SQLITE_ARTIFACTS);

export const FIXTURE_TARGET: SQLiteNativeTarget = "linux-x64";
export const TARBALL_URL = sqliteTarballUrl(
  SQLITE_ARTIFACTS.betterSqlite3Version,
);

export function restoreSQLiteArtifacts(): void {
  const reviewed = JSON.parse(REVIEWED_ARTIFACTS);
  for (const key of Object.keys(SQLITE_ARTIFACTS)) {
    delete (SQLITE_ARTIFACTS as unknown as Record<string, unknown>)[key];
  }
  Object.assign(SQLITE_ARTIFACTS, reviewed);
}

export type TarEntry = {
  name: string;
  data?: string | Buffer;
  type?: string;
  linkname?: string;
  size?: number;
};

export function tar(entries: TarEntry[]): Buffer {
  const chunks: Buffer[] = [];
  for (const entry of entries) {
    const header = Buffer.alloc(512);
    const data = Buffer.from(entry.data ?? "");
    header.write(entry.name, 0, 100);
    header.write("0000644\0", 100);
    header.write("0000000\0", 108);
    header.write("0000000\0", 116);
    header.write(
      `${(entry.size ?? data.length).toString(8).padStart(11, "0")}\0`,
      124,
    );
    header.write("00000000000\0", 136);
    header.fill(32, 148, 156);
    header.write(entry.type ?? "0", 156);
    if (entry.linkname) header.write(entry.linkname, 157, 100);
    header.write("ustar\0", 257);
    header.write("00", 263);
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148);
    chunks.push(header, data, Buffer.alloc((512 - (data.length % 512)) % 512));
  }
  return Buffer.concat([...chunks, Buffer.alloc(1024)]);
}

export function gzipTar(entries: TarEntry[]): Buffer {
  return gzipSync(tar(entries));
}

export function sha256Hex(data: string | Buffer): string {
  return createHash("sha256").update(data).digest("hex");
}

export function sha512Integrity(data: Buffer): string {
  return `sha512-${createHash("sha512").update(data).digest("base64")}`;
}

export function hostProbe(
  target: string,
  libc: "glibc" | "musl" = target.startsWith("linuxmusl") ? "musl" : "glibc",
): SQLiteHostProbe {
  const [platform, arch] = target.replace("linuxmusl", "linux").split("-");
  return {
    platform: platform as NodeJS.Platform,
    arch,
    readProcessMaps: () =>
      libc === "musl"
        ? "7f00-7f01 r-xp 0 0:0 0 /lib/ld-musl-x86_64.so.1\n"
        : "7f00-7f01 r-xp 0 0:0 0 /usr/lib/x86_64-linux-gnu/libc.so.6\n",
    glibcVersionRuntime: () => (libc === "musl" ? null : "2.39"),
  };
}

export interface FixturePackage {
  data: Buffer;
  binary: Buffer;
  files: Record<string, Buffer>;
}

export function fixturePackageFiles(
  binary: string | Buffer = "fixture addon",
  target: SQLiteNativeTarget = FIXTURE_TARGET,
): Record<string, Buffer> {
  return {
    "package.json": Buffer.from(
      JSON.stringify({
        name: "better-sqlite3",
        version: SQLITE_ARTIFACTS.betterSqlite3Version,
        main: "lib/index.js",
      }),
    ),
    LICENSE: Buffer.from("MIT fixture license\n"),
    "lib/index.js": Buffer.from("module.exports = 'fixture';\n"),
    [`prebuilds/${target}.node`]: Buffer.from(binary),
  };
}

// Pin a synthetic better-sqlite3 tarball for FIXTURE_TARGET. Only the live
// manifest is changed; the reviewed policy (and thus the cache directory) is
// unchanged, as it would be for a substituted download.
export function pinFixturePackage(
  options: {
    binary?: string | Buffer;
    files?: Record<string, Buffer>;
    extraEntries?: TarEntry[];
    target?: SQLiteNativeTarget;
  } = {},
): FixturePackage {
  const target = options.target ?? FIXTURE_TARGET;
  const files = options.files ?? fixturePackageFiles(options.binary, target);
  const binaryPath = `prebuilds/${target}.node`;
  const data = gzipTar([
    ...Object.entries(files).map(([path, contents]) => ({
      name: `package/${path}`,
      data: contents,
    })),
    ...(options.extraEntries ?? []),
  ]);
  SQLITE_ARTIFACTS.files = Object.fromEntries(
    Object.entries(files)
      .filter(([path]) => !path.startsWith("prebuilds/"))
      .map(([path, contents]) => [
        path,
        { sha256: sha256Hex(contents), size: contents.length },
      ]),
  );
  const { minGlibc, minGlibcxx } = SQLITE_ARTIFACTS.prebuilds[target] ?? {};
  SQLITE_ARTIFACTS.prebuilds[target] = {
    ...(minGlibc ? { minGlibc } : {}),
    ...(minGlibcxx ? { minGlibcxx } : {}),
    path: binaryPath,
    sha256: sha256Hex(files[binaryPath]),
    size: files[binaryPath].length,
  };
  pinTarball(data);
  return { data, binary: files[binaryPath], files };
}

export function pinTarball(data: Buffer): void {
  SQLITE_ARTIFACTS.tarball = {
    url: TARBALL_URL,
    integrity: sha512Integrity(data),
    size: data.length,
  };
}

export interface RegistryReply {
  data?: Buffer;
  status?: number;
  location?: string;
  headers?: Record<string, string>;
}

export function mockRegistry(
  respond: (url: string) => RegistryReply,
  delayMs = 0,
): string[] {
  const urls: string[] = [];
  vi.spyOn(https, "get").mockImplementation(((
    url: string,
    callback: (response: Readable) => void,
  ) => {
    urls.push(String(url));
    const request = Object.assign(new EventEmitter(), {
      setTimeout: () => request,
      destroy: () => request,
    });
    const deliver = () => {
      let reply: RegistryReply;
      try {
        reply = respond(String(url));
      } catch (error) {
        request.emit("error", error);
        return;
      }
      const response = Object.assign(
        Readable.from(reply.data ? [reply.data] : []),
        {
          statusCode: reply.status ?? 200,
          headers: {
            ...(reply.location ? { location: reply.location } : {}),
            ...(reply.headers ?? {}),
          },
        },
      );
      callback(response);
    };
    if (delayMs > 0) setTimeout(deliver, delayMs);
    else queueMicrotask(deliver);
    return request;
  }) as unknown as typeof https.get);
  return urls;
}

export function unexpectedDownload(): never {
  throw new Error("Unexpected SQLite download");
}
