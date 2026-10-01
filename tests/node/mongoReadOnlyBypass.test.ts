import { describe, expect, it } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

function guard() {
  const driver = new MongoDBDriver({
    id: "mongo",
    name: "mongo",
    type: "mongodb",
    host: "localhost",
  } as ConnectionConfig);
  const g = driver.getCapabilities().readOnlyQueryGuard;
  if (!g) throw new Error("missing guard");
  return g;
}

describe("mongo readonly bypass regressions (stage 2)", () => {
  it("allows plain reads", () => {
    const g = guard();
    expect(g("db.users.find({ active: true })")).toEqual({ allowed: true });
    expect(g("db.users.find({}).limit(5)")).toEqual({ allowed: true });
    expect(g("db.users.find({}).skip(2).limit(5).sort({ name: 1 })")).toEqual({
      allowed: true,
    });
    expect(g("db.users.aggregate([{ $match: { active: true } }])")).toEqual({
      allowed: true,
    });
  });

  it("allows read-only cursor modifiers", () => {
    const g = guard();
    expect(g("db.users.find({}).batchSize(10)")).toEqual({ allowed: true });
    expect(g("db.users.find({}).maxTimeMS(100)")).toEqual({ allowed: true });
    expect(g("db.users.find({}).hint({ name: 1 })")).toEqual({ allowed: true });
    expect(g("db.users.find({}).pretty()")).toEqual({ allowed: true });
    expect(g('db.users.find({}).comment("hi")')).toEqual({ allowed: true });
    expect(g("db.users.find({}).explain()")).toEqual({ allowed: true });
  });

  it("does not flag $out/$merge in values or similar operator names", () => {
    const g = guard();
    expect(g('db.users.aggregate([{ $match: { status: "$out" } }])')).toEqual({
      allowed: true,
    });
    expect(g('db.users.aggregate([{ $group: { _id: "$out" } }])')).toEqual({
      allowed: true,
    });
    expect(
      g(
        'db.users.aggregate([{ $project: { x: { $mergeObjects: ["$a", "$b"] } } }])',
      ),
    ).toEqual({ allowed: true });
    // Stage names are case-sensitive; $OUT is unknown to the server.
    expect(g('db.users.aggregate([{ $OUT: "x" }])')).toEqual({ allowed: true });
    expect(
      g(
        'db.users.aggregate([{ $project: { payload: { $literal: { $out: "preview" } } } }])',
      ),
    ).toEqual({ allowed: true });
    expect(
      g(
        'db.users.aggregate([{ $match: { metadata: { $out: "literal-data" } } }])',
      ),
    ).toEqual({ allowed: true });
  });

  it("denies top-level $out/$merge", () => {
    const g = guard();
    expect(
      g('db.users.aggregate([{ $match: {} }, { $out: "archive" }])').allowed,
    ).toBe(false);
    expect(
      g('db.users.aggregate([{ $merge: { into: "archive" } }])').allowed,
    ).toBe(false);
  });

  it("denies nested $out/$merge in facet/unionWith/lookup pipelines", () => {
    const g = guard();
    expect(
      g('db.users.aggregate([{ $facet: { a: [{ $out: "x" }] } }])').allowed,
    ).toBe(false);
    expect(
      g(
        'db.users.aggregate([{ $unionWith: { coll: "other", pipeline: [{ $merge: { into: "x" } }] } }])',
      ).allowed,
    ).toBe(false);
    expect(
      g(
        'db.users.aggregate([{ $lookup: { from: "o", pipeline: [{ $out: "x" }], as: "j" } }])',
      ).allowed,
    ).toBe(false);
    expect(
      g(
        'db.users.aggregate([{ $lookup: { from: "o", pipeline: [{ $project: { data: { $literal: { $merge: "text" } } } }, { $out: "x" }], as: "j" } }])',
      ).allowed,
    ).toBe(false);
  });

  it("denies write chainOps on read ops", () => {
    const g = guard();
    expect(g("db.users.find({}).deleteMany()").allowed).toBe(false);
    expect(g("db.users.find({}).updateOne({ $set: { a: 1 } })").allowed).toBe(
      false,
    );
    expect(g("db.users.find({}).remove()").allowed).toBe(false);
  });

  it("denies mutations and runCommand", () => {
    const g = guard();
    expect(g("db.users.deleteMany({})").allowed).toBe(false);
    expect(g("db.runCommand({ ping: 1 })").allowed).toBe(false);
  });
});
