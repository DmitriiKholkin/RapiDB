# RapiDB Documentation

This folder is the maintainer and support documentation for RapiDB.

## Start Here

- [Developer handbook](DEVELOPER_HANDBOOK.md) - the main entry point for maintainers and bug fixers.
- [Architecture overview](architecture/overview.md) - runtime boundaries and major flows.
- [File map](architecture/file-map.md) - where every major file group lives.
- [Runtime flows](architecture/runtime-flows.md) - connect, query, table, ERD, and browser fallback flows.
- [Contracts reference](reference/contracts.md) - host/webview message and state contracts.
- [Driver matrix](reference/driver-matrix.md) - what each database engine supports.
- [Testing handbook](handbook/testing.md) - how to run the test slices.
- [Packaging handbook](handbook/packaging.md) - build, native sqlite, and release steps.
- [Troubleshooting](handbook/troubleshooting.md) - symptom-to-fix guide.
- [Secrets and security](security/secrets.md) - secret handling and redaction rules.
- [Maintenance guide](maintenance.md) - when to update which docs.

## Audience

This documentation is written for:

- developers who need to understand the extension architecture quickly,
- maintainers fixing bugs in the host, webview, drivers, or tests,
- support engineers diagnosing installation, packaging, or connection issues.

## How To Use It

Start with the handbook, then jump to the architecture or reference section that matches the subsystem you are touching. If you are debugging a specific failure, use the troubleshooting guide first and then verify the relevant contract or driver matrix entry.
