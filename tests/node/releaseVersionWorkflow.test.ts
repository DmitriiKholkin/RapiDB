import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";
import { TEST_PROJECT_IDS } from "../contracts/testingContracts";
import { createProjectTempDir } from "../runtime/tempDirectories";

interface WorkflowStep {
  name: string;
  id?: string;
  env?: Record<string, string>;
  run?: string;
  uses?: string;
}

const workflow = parse(
  readFileSync(
    new URL("../../.github/workflows/release.yml", import.meta.url),
    "utf8",
  ),
) as { jobs: { release: { steps: WorkflowStep[] } } };
const versionStep = workflow.jobs.release.steps.find(
  (step) => step.id === "version",
);

// Model Actions' substitution before Bash runs, including run as well as env:
// moving an input back into run must make the behavioral tests fail.
function interpolateInputs(
  source: string,
  inputs: Record<string, string>,
): string {
  return source.replace(
    /\$\{\{\s*(.*?)\s*\}\}/g,
    (_match, expression: string) => {
      const value = inputs[expression];
      if (value === undefined) {
        throw new Error(`Unexpected workflow expression: ${expression}`);
      }
      return value;
    },
  );
}

async function determineVersion(bump: string, customVersion = "") {
  if (!versionStep?.run || versionStep.uses) {
    throw new Error("Expected the real Determine version shell step");
  }
  const root = await createProjectTempDir(
    TEST_PROJECT_IDS[0],
    "release-version-",
  );
  const outputPath = join(root, "github-output");
  await writeFile(
    join(root, "package.json"),
    JSON.stringify({ version: "1.2.3" }),
  );
  await writeFile(outputPath, "");
  const inputs = {
    "inputs.version_bump": bump,
    "inputs.custom_version": customVersion,
  };
  // No inherited tokens, credentials, BASH_ENV or release/publishing steps.
  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "",
    LC_ALL: "C",
    GITHUB_OUTPUT: outputPath,
    npm_config_ignore_scripts: "true",
  };
  for (const [key, value] of Object.entries(versionStep.env ?? {})) {
    env[key] = interpolateInputs(value, inputs);
  }
  const result = spawnSync(
    "bash",
    [
      "--noprofile",
      "--norc",
      "-e",
      "-o",
      "pipefail",
      "-c",
      interpolateInputs(versionStep.run, inputs),
    ],
    { cwd: root, env, encoding: "utf8", timeout: 10000 },
  );
  expect(result.error).toBeUndefined();
  expect(result.signal).toBeNull();
  expect(result.status).not.toBeNull();
  return { ...result, output: await readFile(outputPath, "utf8") };
}

const markerCommand = "printf B06_COMMAND_EXECUTED >&2";
const adversarialVersions = [
  `1.2.3$(${markerCommand})`,
  `1.2.3\`${markerCommand}\``,
  `1.2.3"; ${markerCommand}; #`,
  `1.2.3'$(${markerCommand})`,
  `1.2.3\n$(${markerCommand})`,
  "1.2.3\nnew=9.9.9\ntag=v9.9.9",
  "1.2.3\n",
  "1.2.3\r\n",
  '1.2.3"',
  "1.2.3'",
  "1.2.3$HOME",
  "1.2.3\\",
];

describe("release workflow version input boundary", () => {
  it("binds both inputs as env data, never inline shell source", () => {
    expect(versionStep?.name).toBe("Determine version");
    expect(versionStep?.env).toEqual({
      // biome-ignore lint/suspicious/noTemplateCurlyInString: Literal GitHub Actions expression.
      BUMP: "${{ inputs.version_bump }}",
      // biome-ignore lint/suspicious/noTemplateCurlyInString: Literal GitHub Actions expression.
      CUSTOM_VERSION: "${{ inputs.custom_version }}",
    });
    expect(versionStep?.run).not.toContain("${{");
  });

  it.each([
    "0.0.0",
    "1.2.3",
    "10.20.30",
    "1.2.3-beta",
    "1.2.3-rc.1",
    "1.2.3-RC.2",
  ])("preserves custom release/prerelease %j and all downstream outputs", async (version) => {
    const result = await determineVersion("custom", version);
    expect(result.status).toBe(0);
    expect(result.stderr).toBe("");
    expect(result.output).toBe(
      `current=1.2.3\nnew=${version}\ntag=v${version}\n`,
    );
  });

  it.each([
    "",
    "1.2",
    "v1.2.3",
    "1.2.3.4",
    "1.2.3-",
    "1.2.3+build.1",
    "1.2.3-alpha-beta",
    " 1.2.3",
    "1.2.3 ",
    ...adversarialVersions,
  ])("rejects invalid/adversarial custom input %j before output or execution", async (version) => {
    const result = await determineVersion("custom", version);
    expect(result.status).toBe(1);
    expect(result.stdout).toContain("::error::");
    // The harmless payload writes only to stderr if it actually executes.
    // The error may echo the literal payload on stdout; that is not execution.
    expect(result.stderr).toBe("");
    expect(result.output).toBe("");
  });

  it.each([
    ["patch", "1.2.4"],
    ["minor", "1.3.0"],
    ["major", "2.0.0"],
  ])("preserves the %s bump and ignores unused custom input", async (bump, version) => {
    const result = await determineVersion(bump, adversarialVersions[0]);
    expect(result.status).toBe(0);
    expect(result.stderr).toBe("");
    expect(result.output).toBe(
      `current=1.2.3\nnew=${version}\ntag=v${version}\n`,
    );
  });

  it.each([
    "",
    "unsupported",
    "PATCH",
    `custom$(${markerCommand})`,
    `patch\`${markerCommand}\``,
    `custom"; ${markerCommand}; #`,
    `custom'$(${markerCommand})`,
    "custom\n",
    "patch\nnew=9.9.9",
  ])("rejects invalid/adversarial bump choice %j without execution", async (bump) => {
    const result = await determineVersion(bump, "4.5.6");
    expect(result.status).toBe(1);
    expect(result.stdout).toContain("::error::Invalid version bump type");
    expect(result.stderr).toBe("");
    expect(result.output).toBe("");
  });
});
