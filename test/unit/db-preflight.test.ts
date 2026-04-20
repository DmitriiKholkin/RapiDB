import { describe, expect, it } from "vitest";
import { getRequestedNetworkKinds, parseRequestedKinds } from "../db-preflight";

describe("db preflight selection", () => {
  it("parses db selectors from argv and environment", () => {
    const kinds = parseRequestedKinds(
      ["--db=pg", "db=mysql", "sqlite"],
      "oracle",
    );

    expect(Array.from(kinds)).toEqual(["pg", "mysql", "sqlite", "oracle"]);
  });

  it("ignores sqlite when deriving Docker-backed services", () => {
    expect(getRequestedNetworkKinds(new Set(["sqlite"]))).toEqual([]);
    expect(
      getRequestedNetworkKinds(new Set(["pg", "sqlite", "oracle"])),
    ).toEqual(["pg", "oracle"]);
  });

  it("defaults to all networked services when no selector is provided", () => {
    expect(getRequestedNetworkKinds(new Set())).toEqual([
      "pg",
      "mysql",
      "mssql",
      "oracle",
    ]);
  });
});
