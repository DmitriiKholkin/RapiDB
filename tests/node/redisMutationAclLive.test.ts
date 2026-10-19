import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { createClient } from "redis";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { logger } from "../../src/extension/utils/logger";

const exec = promisify(execFile);
// Opt in explicitly: each suite owns a disposable container, a random loopback
// port and no mounts. It never touches the compose services or existing data.
const live = process.env.RAPIDB_REDIS_ACL_LIVE === "1";

describe.runIf(live).each(["redis:7", "redis:6-alpine"])(
  "Redis mutation ACL safety (%s)",
  (image) => {
    const container = `rapidb-redis-acl-${randomUUID()}`;
    const drivers: RedisDriver[] = [];
    let port: number;
    let client: ReturnType<typeof createClient>;
    let started = false;

    beforeAll(async () => {
      await exec("docker", [
        "run",
        "--detach",
        "--name",
        container,
        "--publish",
        "127.0.0.1::6379",
        image,
        "redis-server",
        "--save",
        "",
        "--appendonly",
        "no",
      ]);
      started = true;
      const mapping = await exec("docker", ["port", container, "6379/tcp"]);
      port = Number(mapping.stdout.trim().split(":").at(-1));
      client = createClient({
        url: `redis://127.0.0.1:${port}`,
        socket: { reconnectStrategy: false },
      });
      client.on("error", () => undefined);
      for (let attempt = 0; ; attempt++) {
        try {
          await client.connect();
          break;
        } catch (error) {
          if (attempt >= 50) throw error;
          await delay(100);
        }
      }
    }, 30000);

    afterEach(async () => {
      // The common setup restores spies before suite teardown hooks run.
      vi.spyOn(logger, "error").mockReturnValue(new Error("logged"));
      for (const driver of drivers.splice(0)) await driver.disconnect();
      await delay(0);
    });

    afterAll(async () => {
      try {
        if (client?.isOpen) await client.close();
      } finally {
        if (started) await exec("docker", ["rm", "--force", container]);
      }
    }, 30000);

    async function restricted(...rules: string[]) {
      const username = `edit-${randomUUID()}`;
      await client.sendCommand([
        "ACL",
        "SETUSER",
        username,
        "on",
        ">test-password",
        "~*",
        "+@all",
        ...rules,
      ]);
      const driver = new RedisDriver({
        id: username,
        name: username,
        type: "redis",
        connectionUri: `redis://${username}:test-password@127.0.0.1:${port}/0`,
      });
      drivers.push(driver);
      await driver.connect();
      return driver;
    }

    function update(
      driver: RedisDriver,
      source: string,
      changes: Record<string, unknown>,
    ) {
      return driver.updateRows({
        database: "db0",
        schema: "",
        table: "audit",
        updates: [
          {
            primaryKeys: { key: source },
            changes,
            originalValues: { key: source, ttl: null },
          },
        ],
      });
    }

    async function unchanged(source: string, target: string, expiring = false) {
      expect(await client.get(source)).toBe("data");
      expect(await client.exists(target)).toBe(0);
      const ttl = await client.pTTL(source);
      if (expiring) {
        expect(ttl).toBeGreaterThan(110000);
        expect(ttl).toBeLessThanOrEqual(120000);
      } else {
        expect(ttl).toBe(-1);
      }
      expect(await client.keys(`${source}:rapidb-edit:*`)).toEqual([]);
    }

    it("rejects rename + denied PEXPIRE without moving the persistent source", async () => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      await client.set(source, "data");
      const driver = await restricted("-pexpire");
      await expect(
        update(driver, source, { key: target, ttl: 60 }),
      ).rejects.toThrow();
      await unchanged(source, target);
    });

    it("rejects rename + denied PERSIST without changing the source TTL", async () => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      await client.set(source, "data", { PX: 120000 });
      const driver = await restricted("-persist");
      await expect(
        update(driver, source, { key: target, ttl: null }),
      ).rejects.toThrow();
      await unchanged(source, target, true);
    });

    it("rejects a denied RENAME before applying TTL changes", async () => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      await client.set(source, "data");
      const driver = await restricted("-rename");
      await expect(
        update(driver, source, { key: target, ttl: 60 }),
      ).rejects.toThrow();
      await unchanged(source, target);
    });

    it.each([
      false,
      true,
    ])("rename-only preserves expiration without PEXPIRE/PERSIST (expiring=%s)", async (expiring) => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      if (expiring) await client.set(source, "data", { PX: 120000 });
      else await client.set(source, "data");
      const driver = await restricted("-pexpire", "-persist");
      expect(await update(driver, source, { key: target })).toEqual({
        affectedRows: 1,
      });
      expect(await client.exists(source)).toBe(0);
      expect(await client.get(target)).toBe("data");
      if (expiring) {
        expect(await client.pTTL(target)).toBeGreaterThan(110000);
        expect(await client.pTTL(target)).toBeLessThanOrEqual(120000);
      } else expect(await client.pTTL(target)).toBe(-1);
    });

    it("supports single-command TTL set and persist on both server versions", async () => {
      const source = `audit:${randomUUID()}:source`;
      await client.set(source, "data");
      const driver = await restricted();
      expect(await update(driver, source, { ttl: 60 })).toEqual({
        affectedRows: 1,
      });
      expect(await client.pTTL(source)).toBeGreaterThan(59000);
      expect(await update(driver, source, { ttl: null })).toEqual({
        affectedRows: 1,
      });
      expect(await client.pTTL(source)).toBe(-1);
      expect(await client.get(source)).toBe("data");
    });

    it.each([
      60,
      null,
    ])("combined rename and TTL %s succeeds on Redis 7 and fails closed on Redis 6", async (ttl) => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      await client.set(source, "data", { PX: 120000 });
      const driver = await restricted();
      const result = update(driver, source, { key: target, ttl });
      if (image === "redis:6-alpine") {
        await expect(result).rejects.toThrow("require Redis 7 or newer");
        await unchanged(source, target, true);
      } else {
        expect(await result).toEqual({ affectedRows: 1 });
        expect(await client.exists(source)).toBe(0);
        expect(await client.get(target)).toBe("data");
        if (ttl === null) expect(await client.pTTL(target)).toBe(-1);
        else expect(await client.pTTL(target)).toBeGreaterThan(59000);
      }
    });

    it.each([
      Number.MAX_SAFE_INTEGER,
      Number.MAX_SAFE_INTEGER + 1,
      1e20,
      "9223372036854776",
      9007199254741,
      "9007199254741",
    ])("rejects out-of-range TTL %s before any writes", async (ttl) => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      await client.set(source, "data");
      const driver = await restricted();
      await expect(
        update(driver, source, { key: target, ttl }),
      ).rejects.toThrow("TTL");
      await unchanged(source, target);
      await expect(
        update(driver, source, { key: target, value: "new", ttl }),
      ).rejects.toThrow("TTL");
      await unchanged(source, target);
    });

    it("accepts the largest supported TTL using an exact decimal PEXPIRE argument", async () => {
      const source = `audit:${randomUUID()}:source`;
      await client.set(source, "data");
      const driver = await restricted();
      expect(await update(driver, source, { ttl: 9007199254740 })).toEqual({
        affectedRows: 1,
      });
      expect(await client.pTTL(source)).toBeGreaterThan(
        9007199254740000 - 10000,
      );
    });

    it.each([
      ["SET", "string", "new"],
      ["HSET", "hash", { field: "new" }],
      ["RPUSH", "list", ["new"]],
      ["SADD", "set", ["new"]],
      ["ZADD", "zset", [{ score: 1, value: "new" }]],
    ] as const)("preflights %s replacement and its final commit", async (command, kind, value) => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      if (kind === "string") await client.set(source, "data");
      else if (kind === "hash") await client.hSet(source, "field", "data");
      else if (kind === "list") await client.rPush(source, "data");
      else if (kind === "set") await client.sAdd(source, "data");
      else await client.zAdd(source, { score: 1, value: "data" });
      const before = await client.dump(source);
      const driver = await restricted(`-${command.toLowerCase()}`);
      await expect(
        update(driver, source, { key: target, value, ttl: 60 }),
      ).rejects.toThrow();
      expect(await client.dump(source)).toEqual(before);
      expect(await client.exists(target)).toBe(0);
      expect(await client.keys(`${source}:rapidb-edit:*`)).toEqual([]);
    });

    it.each([
      "pexpire",
      "rename",
      "del",
    ])("preflights replacement final %s permission before writes", async (command) => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      await client.set(source, "data");
      const driver = await restricted(`-${command}`);
      await expect(
        update(driver, source, { key: target, value: "new", ttl: 60 }),
      ).rejects.toThrow();
      await unchanged(source, target);
    });

    it.each([
      undefined,
      null,
      60,
    ])("staged value + rename with TTL %s succeeds or safely rejects unsupported Redis 6", async (ttl) => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      await client.set(source, "data", { PX: 120000 });
      const driver = await restricted("-persist");
      const result = update(driver, source, {
        key: target,
        value: "new",
        ...(ttl === undefined ? {} : { ttl }),
      });
      if (image === "redis:6-alpine") {
        await expect(result).rejects.toThrow("require Redis 7 or newer");
        await unchanged(source, target, true);
      } else {
        expect(await result).toEqual({ affectedRows: 1 });
        expect(await client.exists(source)).toBe(0);
        expect(await client.get(target)).toBe("new");
        if (ttl === null) expect(await client.pTTL(target)).toBe(-1);
        else if (ttl === undefined)
          expect(await client.pTTL(target)).toBeGreaterThan(110000);
        else expect(await client.pTTL(target)).toBeGreaterThan(59000);
        expect(await client.keys(`${source}:rapidb-edit:*`)).toEqual([]);
      }
    });

    it.each([
      ["hash", { field: "new" }],
      ["list", ["new"]],
      ["set", ["new"]],
      ["zset", [{ score: 2, value: "new" }]],
    ] as const)("builds a valid %s replacement or rejects unsupported Redis 6", async (kind, value) => {
      const source = `audit:${randomUUID()}:source`;
      if (kind === "hash") await client.hSet(source, "field", "data");
      else if (kind === "list") await client.rPush(source, "data");
      else if (kind === "set") await client.sAdd(source, "data");
      else await client.zAdd(source, { score: 1, value: "data" });
      const before = await client.dump(source);
      const driver = await restricted();
      const result = update(driver, source, { value, ttl: 60 });
      if (image === "redis:6-alpine") {
        await expect(result).rejects.toThrow("require Redis 7 or newer");
        expect(await client.dump(source)).toEqual(before);
      } else {
        expect(await result).toEqual({ affectedRows: 1 });
        if (kind === "hash")
          expect(await client.hGetAll(source)).toEqual(value);
        else if (kind === "list")
          expect(await client.lRange(source, 0, -1)).toEqual(value);
        else if (kind === "set")
          expect(await client.sMembers(source)).toEqual(value);
        else
          expect(await client.zRangeWithScores(source, 0, -1)).toEqual(value);
        expect(await client.pTTL(source)).toBeGreaterThan(59000);
      }
      expect(await client.keys(`${source}:rapidb-edit:*`)).toEqual([]);
    });

    it.each([
      "hash",
      "list",
      "set",
      "zset",
    ])("empty %s replacements remain single-command deletions", async (kind) => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      if (kind === "hash") await client.hSet(source, "field", "data");
      else if (kind === "list") await client.rPush(source, "data");
      else if (kind === "set") await client.sAdd(source, "data");
      else await client.zAdd(source, { score: 1, value: "data" });
      const driver = await restricted("-rename", "-pexpire");
      expect(
        await update(driver, source, {
          key: target,
          value: kind === "hash" ? {} : [],
          ttl: 60,
        }),
      ).toEqual({ affectedRows: 1 });
      expect(await client.exists(source)).toBe(0);
      expect(await client.exists(target)).toBe(0);
      expect(await client.keys(`${source}:rapidb-edit:*`)).toEqual([]);
    });

    it("keeps an oversized existing TTL on rename-only edits without re-expiring", async () => {
      const source = `audit:${randomUUID()}:source`;
      const target = `${source}:target`;
      await client.set(source, "data");
      await client.sendCommand(["PEXPIRE", source, "10000000000000000"]);
      const driver = await restricted("-pexpire");
      expect(await update(driver, source, { key: target })).toEqual({
        affectedRows: 1,
      });
      expect(await client.get(target)).toBe("data");
      expect(await client.pTTL(target)).toBeGreaterThan(9e15);
      expect(await client.exists(source)).toBe(0);
    });

    it.runIf(image === "redis:7")(
      "rejects an existing TTL too large to copy to staging before writes",
      async () => {
        const source = `audit:${randomUUID()}:source`;
        const target = `${source}:target`;
        await client.set(source, "data");
        await client.sendCommand(["PEXPIRE", source, "10000000000000000"]);
        const driver = await restricted();
        await expect(
          update(driver, source, { key: target, value: "new" }),
        ).rejects.toThrow("too large to preserve");
        expect(await client.get(source)).toBe("data");
        expect(await client.pTTL(source)).toBeGreaterThan(9e15);
        expect(await client.exists(target)).toBe(0);
        expect(await client.keys(`${source}:rapidb-edit:*`)).toEqual([]);
      },
    );

    it.runIf(image === "redis:7")(
      "preflights selector-restricted source deletion before replacement commit",
      async () => {
        const source = `audit:${randomUUID()}:source`;
        const target = `${source}:target`;
        await client.set(source, "data");
        const driver = await restricted(
          "resetkeys",
          "-@all",
          "(+@all -del ~*)",
          `(+del ~${source}:rapidb-edit:*)`,
        );
        await expect(
          update(driver, source, { key: target, value: "new" }),
        ).rejects.toThrow("DEL permission");
        await unchanged(source, target);
      },
    );

    it.runIf(image === "redis:7")(
      "checks the target key's TTL permission with Redis 7 ACL selectors",
      async () => {
        const source = `audit:${randomUUID()}:source`;
        const target = `${source}:target`;
        await client.set(source, "data");
        const driver = await restricted(
          "resetkeys",
          "-@all",
          "(+@all -pexpire ~*)",
          "(+pexpire ~other:*)",
        );
        await expect(
          update(driver, source, { key: target, ttl: 60 }),
        ).rejects.toThrow("PEXPIRE permission");
        await unchanged(source, target);
      },
    );
  },
);
