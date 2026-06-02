# Maintenance Guide

Use this page to decide what documentation must change when the code changes.

## Update Triggers

| If you change... | Update these docs |
|---|---|
| A command, tree view, or panel entry point | [architecture/overview.md](architecture/overview.md), [architecture/file-map.md](architecture/file-map.md), [DEVELOPER_HANDBOOK.md](DEVELOPER_HANDBOOK.md) |
| A shared message or initial state type | [reference/contracts.md](reference/contracts.md), [architecture/runtime-flows.md](architecture/runtime-flows.md) |
| A DB driver or engine capability | [reference/driver-matrix.md](reference/driver-matrix.md), [architecture/file-map.md](architecture/file-map.md) |
| A test command or fixture | [handbook/testing.md](handbook/testing.md) |
| Packaging, sqlite runtime prep, or release behavior | [handbook/packaging.md](handbook/packaging.md) |
| Secret handling or redaction rules | [security/secrets.md](security/secrets.md) |

## Maintenance Principles

| Principle | Practical effect |
|---|---|
| Keep README product-focused | The root README should stay discoverable for users and marketplace visitors. |
| Keep docs source-controlled | Do not rely on generated docs unless the repo adopts a docs build pipeline. |
| Anchor claims to code | Every substantial doc section should point back to a source file or test. |
| Prefer one update per change | When a subsystem changes, update the matching doc in the same pull request. |

## Recommended Review Checklist

| Check | Pass condition |
|---|---|
| Link integrity | Links resolve inside the workspace. |
| Contract fidelity | Message and state shapes match the code. |
| Support accuracy | Secret handling and support boundaries are stated explicitly. |
| Scope discipline | The README still reads like a product README, not a maintainer manual. |
