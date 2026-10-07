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

// esbuild embeds this reviewed snapshot. Never fetch a replacement at runtime.
export const SQLITE_ARTIFACTS =
  require("./sqliteArtifacts.json") as SQLiteArtifactManifest;
export const SQLITE_ARTIFACT_POLICY_ID = `pins-v4-${createHash("sha256").update(JSON.stringify(SQLITE_ARTIFACTS)).digest("hex").slice(0, 16)}`;

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
  const major = /^([0-9]+)\.[0-9]+\.[0-9]+$/.exec(electronVersion)?.[1];
  const pins = major ? SQLITE_ARTIFACTS.headers[`${major}-${abi}`] : undefined;
  if (!pins || (platform === "win32" && !pins.windowsLibraries[arch])) {
    throw new Error(
      `Unsupported SQLite source-build target: Electron ${electronVersion}, ABI ${abi}, ${platform}-${arch} has no reviewed header/import-library pins. Update RapiDB or use a supported VS Code runtime; the publisher must review and refresh the static SQLite artifact manifest. No unverified headers were downloaded.`,
    );
  }
  return pins;
}
