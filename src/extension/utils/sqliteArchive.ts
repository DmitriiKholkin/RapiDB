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
import {
  SQLITE_PACKAGE_ROOT,
  type SQLiteInstallFile,
} from "./sqliteArtifactPins";

export const SQLITE_ARCHIVE_LIMITS = {
  compressedBytes: 32 * 1024 * 1024,
  expandedBytes: 128 * 1024 * 1024,
  fileBytes: 64 * 1024 * 1024,
  entries: 10_000,
} as const;

export type SQLiteArchiveLimits = {
  compressedBytes: number;
  expandedBytes: number;
  fileBytes: number;
  entries: number;
};

const gunzipAsync = promisify(gunzip);

export function verifySQLiteIntegrity(data: Buffer, integrity: string): void {
  const match = /^(sha256|sha512)-([A-Za-z0-9+/]+={0,2})$/.exec(integrity);
  if (!match) throw new Error("Invalid pinned SQLite archive integrity.");
  const expected = Buffer.from(match[2], "base64");
  const actual = createHash(match[1]).update(data).digest();
  if (expected.length !== actual.length || !timingSafeEqual(expected, actual)) {
    throw new Error("SQLite archive integrity mismatch; refusing to extract.");
  }
}

export function sqliteSha256Matches(data: Buffer, sha256: string): boolean {
  if (!/^[0-9a-f]{64}$/.test(sha256)) return false;
  const expected = Buffer.from(sha256, "hex");
  const actual = createHash("sha256").update(data).digest();
  return timingSafeEqual(expected, actual);
}

// Windows device names, with or without an extension and in any case,
// including COM/LPT with superscript digits and the console devices.
const WINDOWS_RESERVED_NAME =
  /^(?:con|prn|aux|nul|conin\$|conout\$|com[0-9¹²³]|lpt[0-9¹²³])(?:[ .]|$)/i;

export function safeSQLiteArchivePath(name: string): string {
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
        (WINDOWS_RESERVED_NAME.test(part) || /[. ]$/.test(part)),
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

export interface SQLitePackageExtraction {
  archivePath: string;
  destination: string;
  integrity: string;
  files: readonly SQLiteInstallFile[];
  limits?: SQLiteArchiveLimits;
}

// Extract only the reviewed files of the integrity-pinned npm tarball. Every
// entry is validated (type, path, size, uniqueness) even when it is skipped,
// and each extracted file must match its own SHA-256 pin before anything is
// written. Destination paths come from the pins, never from the archive.
export async function extractSQLitePackage({
  archivePath,
  destination,
  integrity,
  files,
  limits = SQLITE_ARCHIVE_LIMITS,
}: SQLitePackageExtraction): Promise<void> {
  if (!integrity) {
    throw new Error(
      "Trusted static integrity is required for every SQLite archive.",
    );
  }
  if (files.length === 0) {
    throw new Error("No reviewed SQLite package files were requested.");
  }
  const wanted = new Map<string, SQLiteInstallFile>();
  const installPaths = new Set<string>();
  for (const file of files) {
    if (
      safeSQLiteArchivePath(file.archivePath) !== file.archivePath ||
      safeSQLiteArchivePath(file.installPath) !== file.installPath ||
      wanted.has(file.archivePath) ||
      installPaths.has(file.installPath.toLowerCase())
    ) {
      throw new Error(
        `Invalid reviewed SQLite package file: ${file.archivePath}`,
      );
    }
    wanted.set(file.archivePath, file);
    installPaths.add(file.installPath.toLowerCase());
  }
  if (statSync(archivePath).size > limits.compressedBytes) {
    throw new Error("SQLite archive exceeds compressed size limit.");
  }
  const compressed = readFileSync(archivePath);
  if (compressed.length > limits.compressedBytes) {
    throw new Error("SQLite archive exceeds compressed size limit.");
  }
  verifySQLiteIntegrity(compressed, integrity);
  const tar = await gunzipAsync(compressed, {
    maxOutputLength: limits.expandedBytes,
  });
  const tarStream = require("tar-stream") as { extract(): Extract };
  const contents = new Map<string, Buffer>();
  const paths = new Set<string>();
  const foldedPaths = new Set<string>();
  const foldedFiles = new Set<string>();
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
        const fullPath = safeSQLiteArchivePath(header.name);
        if (
          fullPath !== SQLITE_PACKAGE_ROOT &&
          !fullPath.startsWith(`${SQLITE_PACKAGE_ROOT}/`)
        ) {
          throw new Error(
            `SQLite package archive must contain only ${SQLITE_PACKAGE_ROOT}/ entries: ${fullPath}`,
          );
        }
        const path = fullPath.slice(SQLITE_PACKAGE_ROOT.length + 1);
        if (!path && header.type !== "directory") {
          throw new Error("Empty SQLite archive file path.");
        }
        const folded = path.toLowerCase();
        if (paths.has(path) || foldedPaths.has(folded)) {
          throw new Error(
            `Duplicate or conflicting SQLite archive path: ${path}`,
          );
        }
        paths.add(path);
        foldedPaths.add(folded);
        if (header.type === "directory" && header.size !== 0) {
          throw new Error("SQLite archive directory has nonzero size.");
        }
        if (header.type === "file") foldedFiles.add(folded);
        const keep = header.type === "file" && wanted.has(path);
        const chunks: Buffer[] = [];
        let bytes = 0;
        stream.on("data", (chunk: Buffer) => {
          bytes += chunk.length;
          if (bytes > limits.fileBytes || bytes > header.size) {
            extract.destroy(
              new Error("SQLite archive exceeds file size limit."),
            );
          } else if (keep) chunks.push(chunk);
        });
        stream.on("error", rejectPromise);
        stream.on("end", () => {
          if (bytes !== header.size)
            return next(new Error("Truncated SQLite archive entry."));
          if (keep) contents.set(path, Buffer.concat(chunks));
          next();
        });
      } catch (error) {
        extract.destroy(error as Error);
      }
    });
    extract.end(tar);
  });
  for (const path of paths) {
    const parts = path.toLowerCase().split("/");
    while (parts.pop() && parts.length) {
      if (foldedFiles.has(parts.join("/")))
        throw new Error("Conflicting SQLite archive paths.");
    }
  }
  for (const file of files) {
    const data = contents.get(file.archivePath);
    if (!data) {
      throw new Error(
        `SQLite package archive is missing reviewed file ${file.archivePath}.`,
      );
    }
    if (data.length !== file.size || !sqliteSha256Matches(data, file.sha256)) {
      throw new Error(
        `SQLite package file ${file.archivePath} does not match its reviewed SHA-256 pin; refusing to install.`,
      );
    }
  }

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
  for (const file of files) {
    let parent = destination;
    for (const component of file.installPath.split("/").slice(0, -1)) {
      parent = join(parent, component);
      ensureDirectory(parent);
    }
    const fd = openSync(
      join(destination, file.installPath),
      constants.O_WRONLY |
        constants.O_CREAT |
        constants.O_EXCL |
        (constants.O_NOFOLLOW || 0),
      0o600,
    );
    try {
      writeFileSync(fd, contents.get(file.archivePath) as Buffer);
    } finally {
      closeSync(fd);
    }
  }
}
