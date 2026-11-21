import { createHash, randomBytes, randomUUID } from "node:crypto";
import {
  mkdir,
  readdir,
  readFile,
  rename,
  rmdir,
  unlink,
  writeFile,
} from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { logger } from "./utils/logger";

const STALE_SETTINGS =
  "Connection settings are stale or the last write could not be confirmed. Reload the window before retrying. If this persists, see the RapiDB settings recovery notes.";
const MUTEX_DIRECTORY = "write.lock";
const OWNER_FILE = /^owner-([a-f0-9]{32})\.json$/;
const lockToken = () => randomBytes(16).toString("hex");
const DIGEST = /^[a-f0-9]{64}$/;
// Exact digests kept in recent order. Older digests are moved into a Bloom
// filter: membership can only produce false "stale" positives (fail closed),
// never forget a digest, while the file size stays bounded.
export const MAX_EXACT_FENCED_DIGESTS = 512;
const BLOOM_BYTES = 16_384;
const BLOOM_HASHES = 6;

interface FenceState {
  latest?: { revision: string; owner?: string };
  fenced: string[];
  evicted?: string;
}

interface LockOwner {
  token: string;
  pid: number;
  host: string;
  acquiredAt: number;
}

function hasCode(error: unknown, ...codes: string[]): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code;
  return code !== undefined && codes.includes(code);
}

function bloomPositions(digest: string): number[] {
  const bits = BLOOM_BYTES * 8;
  const hash = createHash("sha256").update(digest, "hex").digest();
  return Array.from(
    { length: BLOOM_HASHES },
    (_, index) => hash.readUInt32BE(index * 4) % bits,
  );
}

function bloomHas(filter: Buffer | undefined, digest: string): boolean {
  return (
    filter !== undefined &&
    bloomPositions(digest).every(
      (bit) => (filter[bit >> 3] & (1 << (bit & 7))) !== 0,
    )
  );
}

function bloomAdd(filter: Buffer, digest: string): void {
  for (const bit of bloomPositions(digest)) filter[bit >> 3] |= 1 << (bit & 7);
}

function decodeBloom(value: string | undefined): Buffer | undefined {
  if (value === undefined) return undefined;
  const filter = Buffer.from(value, "base64");
  if (filter.length !== BLOOM_BYTES || filter.toString("base64") !== value) {
    throw new Error(
      "RapiDB's connection settings coordination state is invalid.",
    );
  }
  return filter;
}

// Configuration snapshots in another extension host can lag behind the disk
// write even after the writer's update() resolves. Keep the digests that are
// unsafe to use as a baseline; a mutex alone would not reject these snapshots.
//
// `owner` must identify the settings file as seen by this participant and be
// stable across reloads (a canonical path or a per-profile token), never a
// per-process value: it is persisted when a write restores an earlier digest.
export class ConnectionSettingsFence {
  private constructor(
    private readonly path: string,
    private state: FenceState,
    private readonly owner: string,
    private readonly mayCoverMultipleFiles: boolean,
  ) {}

  static async open(
    directory: string,
    owner: string,
    mayCoverMultipleFiles: boolean,
  ): Promise<ConnectionSettingsFence> {
    const path = join(directory, "revisions.json");
    let raw: string;
    try {
      raw = await readFile(path, "utf8");
    } catch (error) {
      if (!hasCode(error, "ENOENT")) throw error;
      return new ConnectionSettingsFence(
        path,
        { fenced: [] },
        owner,
        mayCoverMultipleFiles,
      );
    }
    const state: Partial<FenceState> | null = JSON.parse(raw);
    if (
      !state ||
      !Array.isArray(state.fenced) ||
      !state.fenced.every(
        (item) => typeof item === "string" && DIGEST.test(item),
      ) ||
      (state.evicted !== undefined && typeof state.evicted !== "string") ||
      (state.latest !== undefined &&
        (!state.latest ||
          typeof state.latest.revision !== "string" ||
          !state.fenced.includes(state.latest.revision) ||
          (state.latest.owner !== undefined &&
            typeof state.latest.owner !== "string")))
    ) {
      throw new Error(
        "RapiDB's connection settings coordination state is invalid.",
      );
    }
    decodeBloom(state.evicted);
    return new ConnectionSettingsFence(
      path,
      state as FenceState,
      owner,
      mayCoverMultipleFiles,
    );
  }

  private isFenced(revision: string, state = this.state): boolean {
    return (
      state.fenced.includes(revision) ||
      bloomHas(decodeBloom(state.evicted), revision)
    );
  }

  assertFresh(revision: string): void {
    const { latest } = this.state;
    if (
      this.isFenced(revision) &&
      !(
        revision === latest?.revision &&
        (latest.owner === undefined || latest.owner === this.owner)
      )
    ) {
      throw new Error(STALE_SETTINGS);
    }
  }

  // If VS Code rejects without confirming the actual outcome, neither the old
  // value nor the attempted value is safe: the latter can itself be historical.
  async beginWrite(
    before: string,
    after: string,
  ): Promise<(applied: boolean) => Promise<void>> {
    this.assertFresh(before);
    const previous = this.state;
    const restoresEarlierDigest = this.isFenced(after, previous);
    const recent = [
      ...previous.fenced.filter((item) => item !== before && item !== after),
      before,
      after,
    ];
    let evicted = previous.evicted;
    if (recent.length > MAX_EXACT_FENCED_DIGESTS) {
      const filter =
        decodeBloom(previous.evicted) ?? Buffer.alloc(BLOOM_BYTES, 0);
      for (const digest of recent.splice(
        0,
        recent.length - MAX_EXACT_FENCED_DIGESTS,
      )) {
        bloomAdd(filter, digest);
      }
      evicted = filter.toString("base64");
    }
    await this.replace({ fenced: recent, evicted });
    return (applied) =>
      this.replace(
        applied
          ? {
              ...this.state,
              latest: {
                revision: after,
                // Global can stand for different User/profile files behind the
                // same lock. A restored digest is only trusted for the same
                // settings file (stable owner), not for another profile that
                // may hold an older copy with identical content.
                owner:
                  this.mayCoverMultipleFiles && restoresEarlierDigest
                    ? this.owner
                    : undefined,
              },
            }
          : previous,
      );
  }

  private async replace(state: FenceState): Promise<void> {
    const temp = `${this.path}.${randomUUID()}.tmp`;
    const persisted: FenceState = { fenced: state.fenced };
    if (state.latest) persisted.latest = state.latest;
    if (state.evicted !== undefined) persisted.evicted = state.evicted;
    try {
      await writeFile(temp, JSON.stringify(persisted), {
        flag: "wx",
        mode: 0o600,
      });
      await rename(temp, this.path);
      this.state = persisted;
    } catch (error) {
      await unlink(temp).catch(() => {});
      throw error;
    }
  }
}

const STORAGE_OWNER_FILE = "owner.json";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

// Returns the identity token of the storage behind a lock directory, creating
// it on first use. Call only while holding that directory's lock: creation is
// then serialized between windows, so every participant of the directory
// reads one token immediately, unlike globalState, which other windows
// observe asynchronously and which is last-writer-wins per extension.
// `preferred` seeds a token that does not exist yet.
export async function readOrCreateStorageOwnerToken(
  directory: string,
  preferred?: string,
): Promise<string> {
  const path = join(directory, STORAGE_OWNER_FILE);
  try {
    const value = JSON.parse(await readFile(path, "utf8")) as {
      token?: unknown;
    } | null;
    if (typeof value?.token === "string" && UUID.test(value.token)) {
      return value.token;
    }
    throw new Error(
      "RapiDB's connection settings coordination state is invalid.",
    );
  } catch (error) {
    if (!hasCode(error, "ENOENT")) {
      if (error instanceof SyntaxError) {
        throw new Error(
          "RapiDB's connection settings coordination state is invalid.",
        );
      }
      throw error;
    }
  }
  const token =
    preferred !== undefined && UUID.test(preferred) ? preferred : randomUUID();
  const temp = `${path}.${randomUUID()}.tmp`;
  try {
    await writeFile(temp, JSON.stringify({ token }), {
      flag: "wx",
      mode: 0o600,
    });
    await rename(temp, path);
  } catch (error) {
    await unlink(temp).catch(() => {});
    throw error;
  }
  return token;
}

function processIsGone(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    // EPERM means the process exists but belongs to someone else.
    return hasCode(error, "ESRCH");
  }
}

// A lock is only taken over when its owner is provably gone on this machine:
// same host name and the PID no longer exists. Locks from other machines,
// suspended or reused PIDs, and unreadable owner records are never taken over
// (manual recovery is documented). A crashed writer's in-flight configuration RPC is still
// covered by the fence, because beginWrite() fenced both digests beforehand.
export function isAbandonedLockOwner(
  owner: LockOwner,
  local: { host: string; pid: number } = {
    host: hostname(),
    pid: process.pid,
  },
  isGone: (pid: number) => boolean = processIsGone,
): boolean {
  return (
    owner.host === local.host && owner.pid !== local.pid && isGone(owner.pid)
  );
}

function parseOwner(raw: string, token: string): LockOwner | undefined {
  try {
    const value = JSON.parse(raw) as Partial<LockOwner>;
    if (
      value.token === token &&
      Number.isSafeInteger(value.pid) &&
      (value.pid as number) > 0 &&
      typeof value.host === "string" &&
      Number.isFinite(value.acquiredAt)
    ) {
      return value as LockOwner;
    }
  } catch {}
  return undefined;
}

async function tryAcquire(lock: string, owner: LockOwner): Promise<boolean> {
  // Populate the lock before it becomes visible: rename() publishes the owner
  // record atomically, so a lock directory without a record is never held.
  const staging = `${lock}.${lockToken()}.acquire`;
  const record = join(staging, `owner-${owner.token}.json`);
  await mkdir(staging, { mode: 0o700 });
  try {
    await writeFile(record, JSON.stringify(owner), { flag: "wx", mode: 0o600 });
    await rename(staging, lock);
    return true;
  } catch (error) {
    await unlink(record).catch(() => {});
    await rmdir(staging).catch(() => {});
    if (hasCode(error, "EEXIST", "ENOTEMPTY", "EPERM", "EACCES", "EISDIR")) {
      return false;
    }
    throw error;
  }
}

async function tryRecoverAbandonedLock(
  lock: string,
  isAbandoned: (owner: LockOwner) => boolean,
): Promise<void> {
  let entries: string[];
  try {
    entries = await readdir(lock);
  } catch (error) {
    if (hasCode(error, "ENOENT", "ENOTDIR")) return;
    throw error;
  }
  const records = entries.filter((entry) => OWNER_FILE.test(entry));
  if (records.length === 0) {
    // Published locks always contain their owner record. An empty directory is
    // a takeover or release that stopped half-way; rmdir() fails if someone
    // has meanwhile published a new (non-empty) lock in its place.
    if (entries.length === 0) {
      await rmdir(lock).catch((error) => {
        if (!hasCode(error, "ENOENT", "ENOTEMPTY", "EEXIST")) throw error;
      });
    }
    return;
  }
  if (records.length !== 1) return;
  const [record] = records;
  const token = OWNER_FILE.exec(record)?.[1] as string;
  let owner: LockOwner | undefined;
  try {
    owner = parseOwner(await readFile(join(lock, record), "utf8"), token);
  } catch (error) {
    if (hasCode(error, "ENOENT")) return;
    throw error;
  }
  if (!owner || !isAbandoned(owner)) return;
  // Only one contender can unlink this exact record. The lock cannot be
  // re-acquired while the directory is non-empty; once it is empty, a new
  // owner may replace it, in which case rmdir() fails and leaves it alone.
  try {
    await unlink(join(lock, record));
  } catch (error) {
    if (hasCode(error, "ENOENT")) return;
    throw error;
  }
  logger.warn(
    `Recovered a connection settings lock abandoned by process ${owner.pid}`,
  );
  await rmdir(lock).catch((error) => {
    if (!hasCode(error, "ENOENT", "ENOTEMPTY", "EEXIST")) throw error;
  });
}

async function release(lock: string, token: string): Promise<void> {
  const record = join(lock, `owner-${token}.json`);
  try {
    await unlink(record);
  } catch (error) {
    // Someone treated this lock as abandoned. Leave whatever is there now.
    if (hasCode(error, "ENOENT")) return;
    throw error;
  }
  await rmdir(lock).catch((error) => {
    if (!hasCode(error, "ENOENT", "ENOTEMPTY", "EEXIST")) throw error;
  });
}

export async function withConnectionSettingsLock<TResult>(
  directory: string,
  owner: string | (() => Promise<string>),
  mayCoverMultipleFiles: boolean,
  operation: (fence: ConnectionSettingsFence) => Promise<TResult>,
  waitMs = 2_000,
  isAbandoned: (owner: LockOwner) => boolean = (record) =>
    isAbandonedLockOwner(record),
): Promise<TResult> {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const lock = join(directory, MUTEX_DIRECTORY);
  const deadline = performance.now() + waitMs;
  const holder: LockOwner = {
    token: lockToken(),
    pid: process.pid,
    host: hostname(),
    acquiredAt: Date.now(),
  };
  for (;;) {
    if (await tryAcquire(lock, holder)) break;
    await tryRecoverAbandonedLock(lock, isAbandoned);
    const remaining = deadline - performance.now();
    if (remaining <= 0) {
      throw new Error(
        "RapiDB connection settings are busy in another operation or window. Retry after it finishes. If the lock remains after a crash, see the RapiDB settings recovery notes.",
      );
    }
    await delay(Math.min(25, remaining));
  }
  try {
    return await operation(
      await ConnectionSettingsFence.open(
        directory,
        typeof owner === "string" ? owner : await owner(),
        mayCoverMultipleFiles,
      ),
    );
  } finally {
    try {
      await release(lock, holder.token);
    } catch (error) {
      // Retain a lock that couldn't be cleaned up rather than hiding the
      // operation's outcome or trying to force-remove it.
      logger.error("Could not release the connection settings lock", error);
    }
  }
}
