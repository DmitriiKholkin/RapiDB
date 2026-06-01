# Troubleshooting

Use this page when a bug report or support ticket only gives you a symptom.

## Symptom Matrix

| Symptom | Likely cause | First fix to try |
|---|---|---|
| The extension fails to start after install | Native sqlite preparation or another packaged dependency is missing | Re-run the build and the sqlite runtime verification flow. |
| A live DB test service never becomes ready | Docker container is unhealthy or not started | Run `npm run db:check`, then `npm run db:up`, then `npm run db:prepare`. |
| Query results are unexpectedly cut off | The hard cap was hit | Check [security/secrets.md](../security/secrets.md) and [reference/contracts.md](../reference/contracts.md) for the limit and safety rules. |
| A panel opens but shows blank or stale state | The webview initial state or message contract is out of sync | Inspect [reference/contracts.md](../reference/contracts.md), then the matching host panel controller. |
| Table edits fail after preview | Mutation preview or verification rejected the change | Inspect [architecture/runtime-flows.md](../architecture/runtime-flows.md) and the table mutation service. |
| ERD opens but edges look wrong | Metadata loading or scope selection is incomplete | Check the ERD service and the driver metadata implementation. |
| Browser build is missing features | Browser mode is a degraded fallback | Treat the behavior as expected unless the limitation is documented otherwise. |

## Common Support Questions

| Question | Answer |
|---|---|
| Does browser mode behave like desktop mode? | No. Browser mode is intentionally limited. |
| Can I log the full connection URI in a bug report? | No. Strip passwords and SSH secrets before sharing logs. |
| Why do some engines show different filter or DDL behavior? | The drivers are engine-specific and capabilities are not identical. |
| Why does a query run locally but not in the live suite? | The live suite exercises real engine behavior, timeout policy, and packaging assumptions. |

## Debugging Order

1. Identify the runtime surface: host, webview, driver, test harness, or packaging.
2. Check the matching reference page.
3. Reproduce with the smallest command or test slice.
4. Only then widen to the full stack.
