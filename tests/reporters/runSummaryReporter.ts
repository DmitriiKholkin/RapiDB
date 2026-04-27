import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import type {
  Reporter,
  TestCase,
  TestModule,
  TestRunEndReason,
} from "vitest/node";
import {
  engineIdForProject,
  isTestProjectId,
  ONE_COMMAND_TEST_MANIFEST,
  TEST_PROJECT_IDS,
  type TestFailureSummary,
  type TestRunSummary,
} from "../contracts/testingContracts";
import {
  relativeToWorkspace,
  resolveSummaryFilePath,
} from "../runtime/testRuntimeConfig";

function readErrorDetails(error: unknown): { message: string; stack?: string } {
  if (error instanceof Error) {
    return {
      message: error.message,
      stack: error.stack,
    };
  }

  if (error && typeof error === "object") {
    const candidate = error as {
      message?: unknown;
      stack?: unknown;
      stacks?: unknown;
    };

    const message =
      typeof candidate.message === "string"
        ? candidate.message
        : JSON.stringify(error);
    const stack =
      typeof candidate.stack === "string"
        ? candidate.stack
        : Array.isArray(candidate.stacks)
          ? candidate.stacks
              .filter((entry): entry is string => typeof entry === "string")
              .join("\n")
          : undefined;

    return { message, stack };
  }

  return { message: String(error) };
}

function initialSummary(): TestRunSummary {
  return {
    command: process.env.npm_lifecycle_event ?? "vitest",
    primaryCommand: ONE_COMMAND_TEST_MANIFEST.primaryCommand,
    selectedProjects: [],
    reason: "running",
    startedAt: new Date().toISOString(),
    passed: 0,
    failed: 0,
    skipped: 0,
    failures: [],
  };
}

export class RunSummaryReporter implements Reporter {
  private summary: TestRunSummary = initialSummary();

  onTestRunStart(): void {
    this.summary = initialSummary();
  }

  onTestCaseResult(testCase: TestCase): void {
    const projectName = String(testCase.project.name);
    if (
      isTestProjectId(projectName) &&
      !this.summary.selectedProjects.includes(projectName)
    ) {
      this.summary.selectedProjects.push(projectName);
    }

    const result = testCase.result();
    if (result.state === "passed") {
      this.summary.passed += 1;
      return;
    }

    if (result.state === "skipped") {
      this.summary.skipped += 1;
      return;
    }

    if (result.state !== "failed") {
      return;
    }

    this.summary.failed += 1;
    const primaryError = result.errors[0];
    const { message, stack } = readErrorDetails(primaryError);
    const failure: TestFailureSummary = {
      projectId: isTestProjectId(projectName)
        ? projectName
        : TEST_PROJECT_IDS[0],
      engineId: isTestProjectId(projectName)
        ? engineIdForProject(projectName)
        : undefined,
      moduleId: relativeToWorkspace(testCase.module.moduleId),
      testName: testCase.fullName,
      message,
      stack,
      durationMs: testCase.diagnostic()?.duration,
    };
    this.summary.failures.push(failure);
  }

  async onTestRunEnd(
    testModules: ReadonlyArray<TestModule>,
    _unhandledErrors: ReadonlyArray<unknown>,
    reason: TestRunEndReason,
  ): Promise<void> {
    for (const testModule of testModules) {
      const projectName = String(testModule.project.name);
      if (
        isTestProjectId(projectName) &&
        !this.summary.selectedProjects.includes(projectName)
      ) {
        this.summary.selectedProjects.push(projectName);
      }
    }

    this.summary.reason = reason;
    this.summary.finishedAt = new Date().toISOString();

    const summaryFilePath = await resolveSummaryFilePath("workspace");
    await mkdir(dirname(summaryFilePath), { recursive: true });
    await writeFile(
      summaryFilePath,
      `${JSON.stringify(this.summary, null, 2)}\n`,
      "utf8",
    );
  }
}
