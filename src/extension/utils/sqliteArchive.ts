import { createHash, timingSafeEqual } from "node:crypto";
import {
  closeSync,
  constants,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import type { Readable, Writable } from "node:stream";
import { promisify } from "node:util";
import { gunzip } from "node:zlib";

// Copied from package-lock.json, not fetched alongside the archive. A regression
// test requires these pins to be updated together with the locked dependency.
export const SQLITE_SOURCE = {
  version: "12.10.0",
  url: "https://registry.npmjs.org/better-sqlite3/-/better-sqlite3-12.10.0.tgz",
  integrity:
    "sha512-CyzaZRQKyHkB2ZInfTTl2nvT33EbDpjkLEbE8/Zck3Ll6O0qqvuGdrJ45HgtH+HykRg88ITY3AdreBGN70aBSQ==",
} as const;

export const SQLITE_ARCHIVE_LIMITS = {
  compressedBytes: 32 * 1024 * 1024,
  expandedBytes: 128 * 1024 * 1024,
  fileBytes: 64 * 1024 * 1024,
  entries: 10_000,
} as const;

const gunzipAsync = promisify(gunzip);
const BINARY_PATH = "build/Release/better_sqlite3.node";

export function verifySQLiteIntegrity(data: Buffer, integrity: string): void {
  const match = /^(sha256|sha512)-([A-Za-z0-9+/]+={0,2})$/.exec(integrity);
  if (!match) throw new Error("Invalid pinned SQLite archive integrity.");
  const expected = Buffer.from(match[2], "base64");
  const actual = createHash(match[1]).update(data).digest();
  if (expected.length !== actual.length || !timingSafeEqual(expected, actual)) {
    throw new Error("SQLite archive integrity mismatch; refusing to extract.");
  }
}

function safeArchivePath(name: string): string {
  if (
    name.includes("\\") ||
    name.includes(":") ||
    name.startsWith("/") ||
    [...name].some(
      (character) =>
        character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
    )
  ) {
    throw new Error(`Unsafe SQLite archive path: ${name}`);
  }
  const parts = name.split("/");
  if (parts.includes(".."))
    throw new Error(`Unsafe SQLite archive path: ${name}`);
  if (
    parts.some(
      (part) =>
        part !== "." &&
        (/^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(part) ||
          /[. ]$/.test(part)),
    )
  ) {
    throw new Error(`Unsafe SQLite archive path: ${name}`);
  }
  return parts.filter((part) => part !== "" && part !== ".").join("/");
}

type ArchiveHeader = {
  name: string;
  type: string;
  size: number;
  linkname?: string;
};
type Extract = Writable & {
  on(
    event: "entry",
    listener: (
      header: ArchiveHeader,
      stream: Readable,
      next: (error?: Error) => void,
    ) => void,
  ): Extract;
};

/** Parse and validate the entire bounded archive before making any filesystem writes.
 * tar-stream only parses headers/data: unlike tar-fs or a PATH tar executable it
 * cannot create links, devices, or files behind our validation's back.
 */
export async function extractSQLiteArchive(
  archivePath: string,
  destination: string,
  kind: "source" | "prebuilt" | "headers",
  integrity: string,
  limits: {
    compressedBytes: number;
    expandedBytes: number;
    fileBytes: number;
    entries: number;
  } = SQLITE_ARCHIVE_LIMITS,
  archiveRoot?: string,
): Promise<void> {
  if (!integrity) {
    throw new Error(
      "Trusted static integrity is required for every SQLite archive.",
    );
  }
  if (statSync(archivePath).size > limits.compressedBytes) {
    throw new Error("SQLite archive exceeds compressed size limit.");
  }
  const compressed = readFileSync(archivePath);
  verifySQLiteIntegrity(compressed, integrity);
  const tar = await gunzipAsync(compressed, {
    maxOutputLength: limits.expandedBytes,
  });
  const tarStream = require("tar-stream") as { extract(): Extract };
  const files = new Map<string, Buffer>();
  const paths = new Set<string>();
  let entries = 0;
  await new Promise<void>((resolvePromise, rejectPromise) => {
    const extract = tarStream.extract();
    extract.on("error", rejectPromise);
    extract.on("finish", resolvePromise);
    extract.on("entry", (header, stream, next) => {
      try {
        if (++entries > limits.entries)
          throw new Error("SQLite archive exceeds entry limit.");
        if (header.type !== "file" && header.type !== "directory") {
          throw new Error(
            `Unsupported SQLite archive entry type: ${header.type}`,
          );
        }
        if (header.linkname)
          throw new Error("SQLite archive links are not allowed.");
        if (
          !Number.isSafeInteger(header.size) ||
          header.size < 0 ||
          header.size > limits.fileBytes
        ) {
          throw new Error("SQLite archive exceeds file size limit.");
        }
        let path = safeArchivePath(header.name);
        if (kind === "source" || kind === "headers") {
          const root = kind === "source" ? "package" : archiveRoot;
          if (
            !root ||
            safeArchivePath(root) !== root ||
            root.includes("/") ||
            (path !== root && !path.startsWith(`${root}/`))
          ) {
            throw new Error(
              `SQLite ${kind} archive must contain only ${root}/ entries.`,
            );
          }
          path = path.slice(root.length).replace(/^\//, "");
          if (
            kind === "headers" &&
            !path.startsWith("include/node/") &&
            !(
              header.type === "directory" &&
              ["", "include", "include/node"].includes(path)
            )
          ) {
            throw new Error(`Unexpected SQLite header archive entry: ${path}`);
          }
        } else if (
          path !== BINARY_PATH &&
          !(
            header.type === "directory" &&
            ["", "build", "build/Release"].includes(path)
          )
        ) {
          throw new Error(`Unexpected SQLite prebuilt archive entry: ${path}`);
        }
        if ((!path && header.type !== "directory") || paths.has(path)) {
          throw new Error(`Duplicate or empty SQLite archive path: ${path}`);
        }
        paths.add(path);
        if (header.type === "directory" && header.size !== 0) {
          throw new Error("SQLite archive directory has nonzero size.");
        }
        const chunks: Buffer[] = [];
        let bytes = 0;
        stream.on("data", (chunk: Buffer) => {
          bytes += chunk.length;
          if (bytes > limits.fileBytes || bytes > header.size) {
            extract.destroy(
              new Error("SQLite archive exceeds file size limit."),
            );
          } else chunks.push(chunk);
        });
        stream.on("error", rejectPromise);
        stream.on("end", () => {
          if (bytes !== header.size)
            return next(new Error("Truncated SQLite archive entry."));
          if (header.type === "file") files.set(path, Buffer.concat(chunks));
          next();
        });
      } catch (error) {
        extract.destroy(error as Error);
      }
    });
    extract.end(tar);
  });
  if (kind === "prebuilt" && !files.get(BINARY_PATH)?.length) {
    throw new Error(
      "SQLite prebuilt archive does not contain a nonempty better_sqlite3.node.",
    );
  }
  if (
    kind === "headers" &&
    [
      "include/node/common.gypi",
      "include/node/config.gypi",
      "include/node/node_version.h",
    ].some((path) => !files.get(path)?.length)
  ) {
    throw new Error(
      "SQLite header archive is missing required pinned build inputs.",
    );
  }
  // Validate file/directory conflicts before writing, including case-insensitive
  // filesystems. Native binary archives may not overwrite any scaffold JS.
  const foldedPaths = new Set<string>();
  for (const path of paths) {
    const folded = path.toLowerCase();
    if (foldedPaths.has(folded))
      throw new Error("Conflicting SQLite archive paths.");
    foldedPaths.add(folded);
  }
  const foldedFiles = new Set(
    [...files.keys()].map((path) => path.toLowerCase()),
  );
  for (const path of paths) {
    const parts = path.toLowerCase().split("/");
    while (parts.pop() && parts.length) {
      if (foldedFiles.has(parts.join("/")))
        throw new Error("Conflicting SQLite archive paths.");
    }
  }
  // Never follow a preexisting link, even if the archive itself has no links.
  const ensureDirectory = (path: string): void => {
    try {
      if (!lstatSync(path).isDirectory())
        throw new Error(`Unsafe SQLite extraction directory: ${path}`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      mkdirSync(path);
    }
  };
  ensureDirectory(destination);
  for (const [path, data] of files) {
    let parent = destination;
    for (const component of path.split("/").slice(0, -1)) {
      parent = join(parent, component);
      ensureDirectory(parent);
    }
    const fd = openSync(
      join(destination, path),
      constants.O_WRONLY |
        constants.O_CREAT |
        constants.O_EXCL |
        (constants.O_NOFOLLOW || 0),
      0o600,
    );
    try {
      writeFileSync(fd, data);
    } finally {
      closeSync(fd);
    }
  }
}
