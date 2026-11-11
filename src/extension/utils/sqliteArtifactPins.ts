import { createHash } from "node:crypto";

export interface SQLiteArtifactPin {
  url: string;
  integrity: string;
  size: number;
}

export interface SQLiteHeaderPins {
  version: string;
  abi: string;
  archiveRoot: string;
  releaseId: number;
  releaseApiUrl: string;
  checksumUrl: string;
  checksumIntegrity: string;
  archive: SQLiteArtifactPin;
  windowsLibraries: Record<string, SQLiteArtifactPin>;
}

export interface SQLiteArtifactManifest {
  schemaVersion: number;
  betterSqlite3Version: string;
  prebuilts: Record<
    string,
    SQLiteArtifactPin & {
      releaseId: number;
      assetId: number;
      apiDigest: string;
    }
  >;
  headers: Record<string, SQLiteHeaderPins>;
}

export interface SQLiteArtifactTarget {
  betterSqlite3Version: string;
  runtime: "electron" | "node";
  abi: string;
  platform: NodeJS.Platform;
  arch: NodeJS.Architecture;
  libc: string;
  electronVersion?: string;
}

export const SQLITE_ARTIFACTS =
  require("./sqliteArtifacts.json") as SQLiteArtifactManifest;
const REVIEWED_SQLITE_ARTIFACTS = JSON.parse(
  JSON.stringify(SQLITE_ARTIFACTS),
) as SQLiteArtifactManifest;

export function sqlitePrebuiltFileName(
  kind: "official" | "patched",
  target: SQLiteArtifactTarget,
): string {
  const { betterSqlite3Version: version, abi, platform, arch } = target;
  return kind === "official"
    ? `better-sqlite3-v${version}-${target.runtime}-v${abi}-${platform}${target.libc}-${arch}.tar.gz`
    : `better-sqlite3-v${version}-electron-v${abi}-${platform}-${arch}.tar.gz`;
}

function sqliteHeaderPinKey(
  electronVersion: string,
  abi: string,
): string | null {
  const major = /^([0-9]+)\.[0-9]+\.[0-9]+$/.exec(electronVersion)?.[1];
  return major ? `${major}-${abi}` : null;
}

export function sqliteArtifactPolicyId(
  target: SQLiteArtifactTarget,
  manifest: SQLiteArtifactManifest = REVIEWED_SQLITE_ARTIFACTS,
): string {
  const electron = target.runtime === "electron";
  const headerKey = electron
    ? sqliteHeaderPinKey(target.electronVersion ?? "", target.abi)
    : null;
  const headers = headerKey ? manifest.headers[headerKey] : undefined;
  const relevant = {
    schemaVersion: manifest.schemaVersion,
    official:
      manifest.prebuilts[
        `official/${sqlitePrebuiltFileName("official", target)}`
      ] ?? null,
    patched: electron
      ? (manifest.prebuilts[
          `patched/${sqlitePrebuiltFileName("patched", target)}`
        ] ?? null)
      : null,
    headers: headers
      ? {
          ...headers,
          windowsLibraries:
            target.platform === "win32"
              ? (headers.windowsLibraries[target.arch] ?? null)
              : null,
        }
      : null,
  };
  return `pins-v5-${createHash("sha256").update(JSON.stringify(relevant)).digest("hex").slice(0, 16)}`;
}

export function sqlitePrebuiltPin(
  kind: "official" | "patched",
  fileName: string,
): SQLiteArtifactPin {
  const pin = SQLITE_ARTIFACTS.prebuilts[`${kind}/${fileName}`];
  if (!pin) {
    throw new Error(
      `Unsupported SQLite target: ${kind} ${fileName} has no reviewed static integrity pin. Update RapiDB to a release with pins for this runtime/platform, or use a supported VS Code runtime. No unverified binary was downloaded.`,
    );
  }
  return pin;
}

export function sqliteHeaderPins(
  electronVersion: string,
  abi: string,
  platform: NodeJS.Platform,
  arch: NodeJS.Architecture,
): SQLiteHeaderPins {
  const key = sqliteHeaderPinKey(electronVersion, abi);
  const pins = key ? SQLITE_ARTIFACTS.headers[key] : undefined;
  if (!pins || (platform === "win32" && !pins.windowsLibraries[arch])) {
    throw new Error(
      `Unsupported SQLite source-build target: Electron ${electronVersion}, ABI ${abi}, ${platform}-${arch} has no reviewed header/import-library pins. Update RapiDB or use a supported VS Code runtime; the publisher must review and refresh the static SQLite artifact manifest. No unverified headers were downloaded.`,
    );
  }
  return pins;
}
