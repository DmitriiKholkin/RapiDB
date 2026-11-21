import { createHash } from "node:crypto";
import {
  SQLITE_NATIVE_TARGETS,
  type SQLiteLinuxLibraryRequirements,
  type SQLiteNativeTarget,
} from "./sqliteNativeTarget";

export const SQLITE_REGISTRY_HOST = "registry.npmjs.org";
export const SQLITE_PACKAGE_ROOT = "package";

export interface SQLiteFilePin {
  sha256: string;
  size: number;
}

export interface SQLitePrebuildPin extends SQLiteFilePin {
  path: string;
  // Linux glibc prebuilds only: the highest GLIBC_x.y and GLIBCXX_x.y.z symbol
  // versions the binary needs, read from its ELF .gnu.version_r section by
  // scripts/refresh-sqlite-artifact-pins.mjs.
  minGlibc?: string;
  minGlibcxx?: string;
}

export interface SQLiteTarballPin {
  url: string;
  integrity: string;
  size: number;
}

export interface SQLiteArtifactManifest {
  schemaVersion: number;
  betterSqlite3Version: string;
  nodeApiVersion: number;
  registryMetadataUrl: string;
  tarball: SQLiteTarballPin;
  files: Record<string, SQLiteFilePin>;
  prebuilds: Record<string, SQLitePrebuildPin>;
}

// Where the selected prebuild is installed inside the managed package. The
// runtime always passes this exact path to better-sqlite3 as nativeBinding.
export const SQLITE_INSTALLED_BINARY_PATH = "build/Release/better_sqlite3.node";

export interface SQLiteInstallFile {
  archivePath: string;
  installPath: string;
  sha256: string;
  size: number;
}

export interface SQLitePackagePlan {
  version: string;
  target: SQLiteNativeTarget;
  nodeApiVersion: number;
  tarball: SQLiteTarballPin;
  files: readonly SQLiteInstallFile[];
  libraryRequirements: SQLiteLinuxLibraryRequirements;
}

export const SQLITE_ARTIFACTS =
  require("./sqliteArtifacts.json") as SQLiteArtifactManifest;
const REVIEWED_SQLITE_ARTIFACTS = JSON.parse(
  JSON.stringify(SQLITE_ARTIFACTS),
) as SQLiteArtifactManifest;

const VERSION_PATTERN = /^\d+\.\d+\.\d+$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const SHA512_INTEGRITY_PATTERN = /^sha512-[A-Za-z0-9+/]{86}==$/;
const INSTALLABLE_FILE_PATTERN =
  /^(?:package\.json|LICENSE|lib\/(?:[a-z0-9-]+\/)?[a-z0-9-]+\.js)$/;
const GLIBC_TARGETS: readonly SQLiteNativeTarget[] = [
  "linux-x64",
  "linux-arm64",
];
const MIN_GLIBC_PATTERN = /^2\.\d{1,3}$/;
const MIN_GLIBCXX_PATTERN = /^3\.4\.\d{1,3}$/;
// Bounds for individual pinned files; the archive limits bound the rest.
const MAX_PINNED_FILE_BYTES = 16 * 1024 * 1024;

export function sqliteTarballUrl(version: string): string {
  return `https://${SQLITE_REGISTRY_HOST}/better-sqlite3/-/better-sqlite3-${version}.tgz`;
}

function invalidManifest(detail: string): Error {
  return new Error(
    `The reviewed SQLite artifact manifest is invalid (${detail}). No SQLite runtime was downloaded or loaded.`,
  );
}

function checkFilePin(path: string, pin: SQLiteFilePin | undefined): void {
  if (
    !pin ||
    typeof pin.sha256 !== "string" ||
    !SHA256_PATTERN.test(pin.sha256) ||
    !Number.isSafeInteger(pin.size) ||
    pin.size <= 0 ||
    pin.size > MAX_PINNED_FILE_BYTES
  ) {
    throw invalidManifest(`missing or malformed pin for ${path}`);
  }
}

export function sqlitePackagePlan(
  target: SQLiteNativeTarget,
  manifest: SQLiteArtifactManifest = SQLITE_ARTIFACTS,
): SQLitePackagePlan {
  const version = manifest.betterSqlite3Version;
  if (manifest.schemaVersion !== 3)
    throw invalidManifest("unsupported schema version");
  if (typeof version !== "string" || !VERSION_PATTERN.test(version))
    throw invalidManifest("invalid better-sqlite3 version");
  if (
    !Number.isSafeInteger(manifest.nodeApiVersion) ||
    manifest.nodeApiVersion < 1
  )
    throw invalidManifest("invalid Node-API version");
  const { tarball } = manifest;
  if (
    !tarball ||
    tarball.url !== sqliteTarballUrl(version) ||
    typeof tarball.integrity !== "string" ||
    !SHA512_INTEGRITY_PATTERN.test(tarball.integrity) ||
    !Number.isSafeInteger(tarball.size) ||
    tarball.size <= 0
  )
    throw invalidManifest("invalid tarball pin");
  if (!SQLITE_NATIVE_TARGETS.includes(target))
    throw invalidManifest(`unknown target ${target}`);

  const files: SQLiteInstallFile[] = [];
  const entries = Object.entries(manifest.files ?? {});
  if (
    !entries.some(([path]) => path === "package.json") ||
    !entries.some(([path]) => path === "lib/index.js")
  )
    throw invalidManifest("package.json or lib/index.js is not pinned");
  for (const [path, pin] of entries) {
    if (!INSTALLABLE_FILE_PATTERN.test(path))
      throw invalidManifest(`unexpected pinned file ${path}`);
    checkFilePin(path, pin);
    files.push({
      archivePath: path,
      installPath: path,
      sha256: pin.sha256,
      size: pin.size,
    });
  }
  const prebuild = manifest.prebuilds?.[target];
  if (!prebuild)
    throw new Error(
      `better-sqlite3 ${version} has no reviewed prebuild pin for ${target}. No SQLite binary was downloaded.`,
    );
  if (prebuild.path !== `prebuilds/${target}.node`)
    throw invalidManifest(`unexpected prebuild path for ${target}`);
  checkFilePin(prebuild.path, prebuild);
  const libraryRequirements: SQLiteLinuxLibraryRequirements = {};
  if (GLIBC_TARGETS.includes(target)) {
    if (
      typeof prebuild.minGlibc !== "string" ||
      !MIN_GLIBC_PATTERN.test(prebuild.minGlibc) ||
      typeof prebuild.minGlibcxx !== "string" ||
      !MIN_GLIBCXX_PATTERN.test(prebuild.minGlibcxx)
    )
      throw invalidManifest(
        `missing glibc/libstdc++ requirement for ${target}`,
      );
    libraryRequirements.minGlibc = prebuild.minGlibc;
    libraryRequirements.minGlibcxx = prebuild.minGlibcxx;
  } else if (
    prebuild.minGlibc !== undefined ||
    prebuild.minGlibcxx !== undefined
  ) {
    throw invalidManifest(`unexpected glibc requirement for ${target}`);
  }
  files.push({
    archivePath: prebuild.path,
    installPath: SQLITE_INSTALLED_BINARY_PATH,
    sha256: prebuild.sha256,
    size: prebuild.size,
  });
  return {
    version,
    target,
    nodeApiVersion: manifest.nodeApiVersion,
    tarball,
    files,
    libraryRequirements,
  };
}

// The cache directory is keyed by version, target and the reviewed pins that
// apply to the target, never by the host's ABI: the prebuilds use Node-API.
export function sqliteArtifactPolicyId(
  target: SQLiteNativeTarget,
  manifest: SQLiteArtifactManifest = REVIEWED_SQLITE_ARTIFACTS,
): string {
  const relevant = {
    schemaVersion: manifest.schemaVersion,
    betterSqlite3Version: manifest.betterSqlite3Version,
    nodeApiVersion: manifest.nodeApiVersion,
    tarball: manifest.tarball,
    files: Object.fromEntries(
      Object.entries(manifest.files ?? {}).sort(([a], [b]) =>
        a.localeCompare(b),
      ),
    ),
    prebuild: manifest.prebuilds?.[target] ?? null,
  };
  return `pins-v6-${createHash("sha256").update(JSON.stringify(relevant)).digest("hex").slice(0, 16)}`;
}

export function reviewedSQLiteVersion(): string {
  return REVIEWED_SQLITE_ARTIFACTS.betterSqlite3Version;
}
