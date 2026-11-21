import { readFileSync } from "node:fs";

// Targets for which better-sqlite3 13.x publishes Node-API prebuilds in its
// npm tarball. Each has a reviewed SHA-256 pin in sqliteArtifacts.json.
export const SQLITE_NATIVE_TARGETS = [
  "darwin-arm64",
  "darwin-x64",
  "linux-arm64",
  "linux-x64",
  "linuxmusl-arm64",
  "linuxmusl-x64",
  "win32-arm64",
  "win32-x64",
] as const;

export type SQLiteNativeTarget = (typeof SQLITE_NATIVE_TARGETS)[number];

export type SQLiteLinuxLibc = "glibc" | "musl";

export interface SQLiteHostProbe {
  platform: NodeJS.Platform;
  arch: NodeJS.Architecture | string;
  // Contents of /proc/self/maps; throws or returns null when unavailable.
  readProcessMaps?: () => string | null;
  // process.report header.glibcVersionRuntime: a version string on glibc,
  // null when a report is available but names no glibc, and undefined when no
  // report is available.
  glibcVersionRuntime?: () => string | null | undefined;
}

export class SQLiteUnsupportedTargetError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SQLiteUnsupportedTargetError";
  }
}

const MUSL_LOADER = /\/ld-musl-[^/\s]*\.so/;
const GLIBC_LOADER = /\/(?:ld-linux[^/\s]*|libc)\.so\.\d/;

function readCurrentProcessMaps(): string | null {
  return readFileSync("/proc/self/maps", "latin1");
}

function currentGlibcVersionRuntime(): string | null | undefined {
  const report = process.report as
    | (NodeJS.ProcessReport & { excludeNetwork?: boolean })
    | undefined;
  if (!report || typeof report.getReport !== "function") return undefined;
  const previous = report.excludeNetwork;
  try {
    report.excludeNetwork = true;
    const header = (
      report.getReport() as { header?: { glibcVersionRuntime?: unknown } }
    ).header;
    if (!header || typeof header !== "object") return undefined;
    const version = header.glibcVersionRuntime;
    return typeof version === "string" && version.length > 0 ? version : null;
  } catch {
    return undefined;
  } finally {
    try {
      report.excludeNetwork = previous;
    } catch {}
  }
}

export function currentSQLiteHostProbe(): SQLiteHostProbe {
  return {
    platform: process.platform,
    arch: process.arch,
    readProcessMaps: readCurrentProcessMaps,
    glibcVersionRuntime: currentGlibcVersionRuntime,
  };
}

interface LinuxLibcSignals {
  libc: SQLiteLinuxLibc | null;
  maps: string | null;
  glibcVersionRuntime: string | null | undefined;
}

function inspectLinuxLibc(probe: SQLiteHostProbe): LinuxLibcSignals {
  let maps: string | null = null;
  try {
    maps = probe.readProcessMaps?.() ?? null;
  } catch {
    maps = null;
  }
  const mapsMusl = maps !== null && MUSL_LOADER.test(maps);
  const mapsGlibc = maps !== null && GLIBC_LOADER.test(maps);
  let report: string | null | undefined;
  try {
    report = probe.glibcVersionRuntime?.();
  } catch {
    report = undefined;
  }
  const reportGlibc = typeof report === "string" && report.length > 0;
  const reportNonGlibc = report === null;

  let libc: SQLiteLinuxLibc | null;
  if (mapsMusl && mapsGlibc) libc = null;
  else if (mapsMusl) libc = reportGlibc ? null : "musl";
  else if (mapsGlibc) libc = reportNonGlibc ? null : "glibc";
  else libc = reportGlibc ? "glibc" : null;
  return { libc, maps, glibcVersionRuntime: report };
}

// Determine the C library from independent signals and refuse to guess when
// they are missing or disagree: loading a glibc addon into a musl process (or
// the reverse) fails at best and is undefined behavior at worst.
export function detectSQLiteLinuxLibc(
  probe: SQLiteHostProbe,
): SQLiteLinuxLibc | null {
  return inspectLinuxLibc(probe).libc;
}

export interface SQLiteNativeHost {
  target: SQLiteNativeTarget;
  // glibc hosts only: process.report header.glibcVersionRuntime as reported
  // (null when no report was available).
  glibcVersionRuntime: string | null;
  // GLIBCXX version of a libstdc++.so.6.0.N already mapped into this process
  // ("3.4.N"), or null when none is mapped or its version is not encoded in
  // the file name.
  mappedLibstdcxxVersion: string | null;
}

const MAPPED_LIBSTDCXX = /\/libstdc\+\+\.so\.6\.0\.(\d+)(?=\s|$)/gm;

function mappedLibstdcxxVersion(maps: string | null): string | null {
  if (!maps) return null;
  const versions = new Set(
    [...maps.matchAll(MAPPED_LIBSTDCXX)].map((match) => Number(match[1])),
  );
  // Several different libstdc++ builds in one process: nothing reliable.
  if (versions.size !== 1) return null;
  return `3.4.${[...versions][0]}`;
}

export function resolveSQLiteNativeHost(
  probe: SQLiteHostProbe = currentSQLiteHostProbe(),
): SQLiteNativeHost {
  const { platform, arch } = probe;
  if (arch !== "x64" && arch !== "arm64") {
    throw new SQLiteUnsupportedTargetError(
      `SQLite is not available on ${platform}-${arch}: better-sqlite3 publishes prebuilt Node-API binaries only for x64 and arm64 on macOS, Windows and Linux (glibc or musl). No binary was downloaded.`,
    );
  }
  if (platform === "darwin" || platform === "win32") {
    return {
      target: `${platform}-${arch}`,
      glibcVersionRuntime: null,
      mappedLibstdcxxVersion: null,
    };
  }
  if (platform === "linux") {
    const signals = inspectLinuxLibc(probe);
    if (!signals.libc) {
      throw new SQLiteUnsupportedTargetError(
        `SQLite is not available on linux-${arch}: RapiDB could not reliably determine whether this process uses glibc or musl, so it did not choose a native SQLite binary. No binary was downloaded.`,
      );
    }
    return {
      target: signals.libc === "musl" ? `linuxmusl-${arch}` : `linux-${arch}`,
      glibcVersionRuntime:
        signals.libc === "glibc" &&
        typeof signals.glibcVersionRuntime === "string"
          ? signals.glibcVersionRuntime
          : null,
      mappedLibstdcxxVersion: mappedLibstdcxxVersion(signals.maps),
    };
  }
  throw new SQLiteUnsupportedTargetError(
    `SQLite is not available on ${platform}-${arch}: better-sqlite3 publishes prebuilt Node-API binaries only for macOS, Windows and Linux (x64/arm64). No binary was downloaded.`,
  );
}

export function resolveSQLiteNativeTarget(
  probe: SQLiteHostProbe = currentSQLiteHostProbe(),
): SQLiteNativeTarget {
  return resolveSQLiteNativeHost(probe).target;
}

export interface SQLiteLinuxLibraryRequirements {
  minGlibc?: string;
  minGlibcxx?: string;
}

const GLIBC_RUNTIME_VERSION = /^(\d+)\.(\d+)(?:\.(\d+))?$/;

function compareVersions(left: string, right: string): number {
  const a = left.split(".").map(Number);
  const b = right.split(".").map(Number);
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    const difference = (a[index] ?? 0) - (b[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return 0;
}

// Linux distributions supported by VS Code whose glibc or libstdc++ is older
// than the better-sqlite3 13.x glibc prebuilds require (glibc 2.34,
// GLIBCXX_3.4.29 from GCC 11).
export const SQLITE_UNSUPPORTED_GLIBC_DISTRIBUTIONS =
  "RHEL, CentOS Stream, Rocky Linux, AlmaLinux and Oracle Linux 8 (glibc 2.28), Ubuntu 20.04 (glibc 2.31), Debian 10 and 11 (glibc 2.28 and 2.31) and SUSE Linux Enterprise / openSUSE Leap 15 releases with glibc 2.31";

export function sqliteLinuxRuntimeRequirementHint(
  requirements: SQLiteLinuxLibraryRequirements,
): string {
  const glibc = requirements.minGlibc ?? "2.34";
  const glibcxx = requirements.minGlibcxx ?? "3.4.29";
  return `The prebuilt SQLite binary for Linux (glibc) requires glibc ${glibc} or newer and libstdc++ with GLIBCXX_${glibcxx} (GCC 11 or newer). It cannot run on older distributions such as ${SQLITE_UNSUPPORTED_GLIBC_DISTRIBUTIONS}; use for example RHEL 9, Ubuntu 22.04 or Debian 12 or newer.`;
}

// Refuses, before anything is downloaded, a glibc host whose C or C++ runtime
// is known to be older than the pinned prebuild needs. An unknown glibc
// version fails closed; libstdc++ is only checked when this process already
// mapped one (the dynamic loader would reuse it for the addon), otherwise the
// load error explains the requirement.
export function sqliteHostLibraryRequirementError(
  host: SQLiteNativeHost,
  requirements: SQLiteLinuxLibraryRequirements,
): Error | null {
  if (!requirements.minGlibc && !requirements.minGlibcxx) return null;
  const hint = sqliteLinuxRuntimeRequirementHint(requirements);
  if (requirements.minGlibc) {
    const found = host.glibcVersionRuntime;
    if (found === null || !GLIBC_RUNTIME_VERSION.test(found.trim())) {
      return new SQLiteUnsupportedTargetError(
        `SQLite runtime requires glibc ${requirements.minGlibc} or newer, but RapiDB could not determine the glibc version of this system (reported: ${found === null ? "none" : JSON.stringify(found)}). ${hint} No binary was downloaded.`,
      );
    }
    if (compareVersions(found.trim(), requirements.minGlibc) < 0) {
      return new SQLiteUnsupportedTargetError(
        `SQLite runtime requires glibc ${requirements.minGlibc} or newer (found ${found.trim()}). ${hint} No binary was downloaded.`,
      );
    }
  }
  if (
    requirements.minGlibcxx &&
    host.mappedLibstdcxxVersion &&
    compareVersions(host.mappedLibstdcxxVersion, requirements.minGlibcxx) < 0
  ) {
    return new SQLiteUnsupportedTargetError(
      `SQLite runtime requires libstdc++ with GLIBCXX_${requirements.minGlibcxx} or newer (found GLIBCXX_${host.mappedLibstdcxxVersion}). ${hint} No binary was downloaded.`,
    );
  }
  return null;
}
