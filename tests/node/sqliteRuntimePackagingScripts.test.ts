import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

type PackageScripts = Record<string, string>;

type PackageManifest = {
  activationEvents?: string[];
  scripts?: PackageScripts;
};

function readManifest(): PackageManifest {
  return JSON.parse(
    readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
  ) as PackageManifest;
}

describe("SQLite VS Code runtime packaging scripts", () => {
  it("keeps vscode:prepublish focused on package preparation and production build", () => {
    const scripts = readManifest().scripts ?? {};
    const prepublish = scripts["vscode:prepublish"];

    expect(prepublish).toBe(
      "npm run package:prepare && node esbuild.config.mjs --production",
    );
    expect(prepublish).not.toContain("native:sqlite:vscode");
  });

  it("removes legacy staged sqlite packaging scripts", () => {
    const scripts = readManifest().scripts ?? {};

    expect(scripts.postinstall).toBeUndefined();
    expect(scripts["native:sqlite:vscode"]).toBeUndefined();
    expect(scripts["native:sqlite:vscode:all"]).toBeUndefined();
    expect(scripts["native:sqlite:vscode:verify"]).toBeUndefined();
  });

  it("warms the SQLite runtime automatically after startup", () => {
    const activationEvents = readManifest().activationEvents ?? [];

    expect(activationEvents).toContain("onStartupFinished");
  });
});
