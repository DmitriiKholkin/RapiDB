# Secrets And Redaction

RapiDB handles database credentials, SSH credentials, and derived connection secrets. The support rule is simple: never log or document raw secrets.

## Secret Fields

| Field family | Examples |
|---|---|
| Database secret | `password` |
| SSH secret | `sshPassword`, `sshPrivateKey`, `sshPassphrase` |
| Stored-secret indicators | `hasStoredSecret`, `hasStoredSshPassword`, `hasStoredSshPrivateKey`, `hasStoredSshPassphrase`, `hasStoredApiKey` |

## Sanitized Versus Submission Shapes

| Shape | Meaning |
|---|---|
| `SanitizedConnectionConfig` | Connection config with secret values removed. |
| `ConnectionFormExistingState` | Sanitized config plus flags that indicate existing stored secrets. |
| `ConnectionFormSubmission` | Full payload that may include fresh secret values for save/test operations. |

## Support Rules

| Rule | Why it matters |
|---|---|
| Do not print passwords or SSH private keys in logs | These values can be reused to access databases or bastions. |
| Do not paste raw connection URIs into issues | URIs often contain embedded credentials. |
| Use stored-secret flags to preserve blanks correctly | A blank form field may mean "keep the existing secret", not "clear it". |
| Prefer sanitized snapshots in bug reports | They preserve shape information without exposing credentials. |

## Where To Look In Code

| File | Why it matters |
|---|---|
| [src/extension/connectionSecrets.ts](../../src/extension/connectionSecrets.ts) | Secret persistence and redaction behavior. |
| [src/shared/webviewContracts.ts](../../src/shared/webviewContracts.ts) | Sanitized and submission payload types. |
| [src/shared/safetyContracts.ts](../../src/shared/safetyContracts.ts) | Broader safety rules around operations and updates. |
