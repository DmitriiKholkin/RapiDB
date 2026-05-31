import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

type PackageScripts = Record<string, string>;

function readScripts(): PackageScripts {
  const manifest = JSON.parse(
    readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
  ) as {
    scripts?: PackageScripts;
  };

  return manifest.scripts ?? {};
}

describe("SQLite VS Code runtime packaging scripts", () => {
  it("stages all sqlite runtime targets and verifies them during vscode:prepublish", () => {
    const scripts = readScripts();
    const prepublish = scripts["vscode:prepublish"];

    expect(prepublish).toContain("npm run native:sqlite:vscode:all");
    expect(prepublish).toContain("npm run native:sqlite:vscode:verify");
    expect(prepublish.indexOf("native:sqlite:vscode:all")).toBeLessThan(
      prepublish.indexOf("native:sqlite:vscode:verify"),
    );
  });

  it("keeps explicit sqlite runtime stage and verify script commands", () => {
    const scripts = readScripts();

    expect(scripts["native:sqlite:vscode"]).toBe(
      "node ./scripts/prepare-vscode-sqlite-runtime.mjs --strict",
    );
    expect(scripts["native:sqlite:vscode:all"]).toBe(
      "node ./scripts/prepare-vscode-sqlite-runtime.mjs --strict --all-targets",
    );
    expect(scripts["native:sqlite:vscode:verify"]).toBe(
      "node ./scripts/verify-vscode-sqlite-runtime.mjs",
    );
  });

  it("fails fast when verify script receives an unsupported --target", () => {
    const scriptPath = fileURLToPath(
      new URL(
        "../../scripts/verify-vscode-sqlite-runtime.mjs",
        import.meta.url,
      ),
    );

    const result = spawnSync(
      process.execPath,
      [scriptPath, "--target", "unsupported-target"],
      {
        encoding: "utf8",
      },
    );

    expect(result.status).not.toBe(0);
    expect(`${result.stdout}\n${result.stderr}`).toContain(
      "Unsupported SQLite runtime target: unsupported-target.",
    );
  });

  it("fails fast when verify script receives unexpected CLI arguments", () => {
    const scriptPath = fileURLToPath(
      new URL(
        "../../scripts/verify-vscode-sqlite-runtime.mjs",
        import.meta.url,
      ),
    );

    const result = spawnSync(process.execPath, [scriptPath, "--targets"], {
      encoding: "utf8",
    });

    expect(result.status).not.toBe(0);
    expect(`${result.stdout}\n${result.stderr}`).toContain(
      "Unexpected argument: --targets.",
    );
  });
});
