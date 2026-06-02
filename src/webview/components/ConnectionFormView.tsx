import React, {
  type CSSProperties,
  type InputHTMLAttributes,
  type ReactElement,
  type ReactNode,
  type SelectHTMLAttributes,
  type TextareaHTMLAttributes,
  useCallback,
  useEffect,
  useId,
  useState,
} from "react";
import type {
  ConnectionSshAuthMethod,
  ConnectionSshHostVerificationMode,
  ConnectionTlsMode,
  SQLiteWalMode,
} from "../../shared/connectionConfig";
import {
  deriveLegacyConnectionTlsFlags,
  getConnectionTlsSupport,
  isConnectionTlsEnabled,
  resolveConnectionTlsMode,
} from "../../shared/connectionConfig";
import {
  type ConnectionType,
  DEFAULT_PORT_BY_CONNECTION_TYPE,
} from "../../shared/connectionTypes";
import type {
  ConnectionFormBrowseTarget,
  ConnectionFormExistingState,
  ConnectionFormSubmission,
} from "../../shared/webviewContracts";
import { buildButtonStyle } from "../utils/buttonStyles";
import {
  buildSelectControlStyle,
  buildTextInputStyle,
} from "../utils/controlStyles";
import { onMessage, postMessage } from "../utils/messaging";
import { Icon } from "./Icon";

interface Props {
  existing?: ConnectionFormExistingState | null;
}

const DB_TYPES: Array<{
  type: ConnectionType;
  label: string;
  short: string;
  color: string;
}> = [
  { type: "pg", label: "PostgreSQL", short: "PG", color: "#336791" },
  {
    type: "mysql",
    label: "MySQL / MariaDB",
    short: "MY",
    color: "#c47900",
  },
  { type: "mssql", label: "SQL Server", short: "MS", color: "#cc2927" },
  { type: "oracle", label: "Oracle", short: "OR", color: "#c74634" },
  { type: "sqlite", label: "SQLite", short: "SQ", color: "#0a7bc4" },
  { type: "mongodb", label: "MongoDB", short: "MO", color: "#00a35c" },
  { type: "redis", label: "Redis", short: "RE", color: "#dc382d" },
  {
    type: "elasticsearch",
    label: "Elasticsearch",
    short: "ES",
    color: "#005571",
  },
  { type: "dynamodb", label: "DynamoDB", short: "DY", color: "#4053d6" },
];

const TLS_MODE_LABELS: Record<ConnectionTlsMode, string> = {
  disabled: "Disabled",
  requireTrustServerCertificate: "Required, trust server certificate",
  requireVerifyCa: "Required, verify CA only",
  requireVerifyFull: "Required, verify full",
  mutualTls: "Mutual TLS",
};

const TLS_MODE_COLORS: Record<ConnectionTlsMode, string> = {
  disabled: "#6b7280",
  requireTrustServerCertificate: "#f59e0b",
  requireVerifyCa: "#3b82f6",
  requireVerifyFull: "#10b981",
  mutualTls: "#8b5cf6",
};

function FocusInput(props: InputHTMLAttributes<HTMLInputElement>) {
  const [focused, setFocused] = useState(false);
  return (
    <input
      {...props}
      style={{
        ...buildTextInputStyle("md", focused),
        ...(props.style ?? {}),
      }}
      onFocus={(e) => {
        setFocused(true);
        props.onFocus?.(e);
      }}
      onBlur={(e) => {
        setFocused(false);
        props.onBlur?.(e);
      }}
    />
  );
}

function FocusSelect(props: SelectHTMLAttributes<HTMLSelectElement>) {
  const [focused, setFocused] = useState(false);

  return (
    <select
      {...props}
      style={{
        ...buildSelectControlStyle("md", focused),
        paddingRight: 28,
        ...(props.style ?? {}),
      }}
      onFocus={(e) => {
        setFocused(true);
        props.onFocus?.(e);
      }}
      onBlur={(e) => {
        setFocused(false);
        props.onBlur?.(e);
      }}
    >
      {props.children}
    </select>
  );
}

function FocusTextArea(props: TextareaHTMLAttributes<HTMLTextAreaElement>) {
  const [focused, setFocused] = useState(false);

  return (
    <textarea
      {...props}
      style={{
        ...buildTextInputStyle("md", focused),
        height: "auto",
        minHeight: 96,
        padding: "6px 8px",
        resize: "vertical",
        lineHeight: 1.45,
        fontFamily: "var(--vscode-editor-font-family, monospace)",
        ...(props.style ?? {}),
      }}
      onFocus={(e) => {
        setFocused(true);
        props.onFocus?.(e);
      }}
      onBlur={(e) => {
        setFocused(false);
        props.onBlur?.(e);
      }}
    />
  );
}

function FieldLabel({ children }: { children: ReactNode }) {
  return (
    <div
      style={{
        fontSize: 11,
        fontWeight: 600,
        marginBottom: 5,
        opacity: 0.65,
        letterSpacing: 0.2,
      }}
    >
      {children}
    </div>
  );
}

function Field({
  label,
  hint,
  error,
  children,
  style,
}: {
  label?: string;
  hint?: string;
  error?: string;
  children: ReactNode;
  style?: CSSProperties;
}) {
  return (
    <div style={{ marginBottom: 14, ...style }}>
      {label && <FieldLabel>{label}</FieldLabel>}
      {children}
      {error && (
        <div
          style={{
            fontSize: 11,
            color: "var(--vscode-errorForeground)",
            marginTop: 4,
          }}
        >
          {error}
        </div>
      )}
      {hint && !error && (
        <div
          style={{ fontSize: 11, opacity: 0.5, marginTop: 4, lineHeight: 1.4 }}
        >
          {hint}
        </div>
      )}
    </div>
  );
}

function Card({
  children,
  style,
}: {
  children: ReactNode;
  style?: CSSProperties;
}) {
  return (
    <div
      style={{
        border: "1px solid var(--vscode-panel-border)",
        borderRadius: 6,
        padding: "14px 16px 4px",
        marginBottom: 10,
        ...style,
      }}
    >
      {children}
    </div>
  );
}

function CardHeader({ icon, label }: { icon: string; label: string }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 6,
        marginBottom: 14,
        paddingBottom: 10,
        borderBottom: "1px solid var(--vscode-panel-border)",
      }}
    >
      <Icon name={icon} size={12} style={{ opacity: 0.5 }} />
      <span
        style={{
          fontSize: 11,
          fontWeight: 700,
          textTransform: "uppercase",
          letterSpacing: 0.7,
          opacity: 0.5,
        }}
      >
        {label}
      </span>
    </div>
  );
}

function ToggleSwitch({
  checked,
  disabled,
  label,
  descriptionId,
  onKeyToggle,
}: {
  checked: boolean;
  disabled?: boolean;
  label: string;
  descriptionId?: string;
  onKeyToggle: () => void;
}) {
  const [focused, setFocused] = useState(false);

  return (
    <div
      role="switch"
      aria-label={label}
      aria-checked={checked}
      aria-disabled={disabled}
      aria-describedby={descriptionId}
      tabIndex={disabled ? -1 : 0}
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
      onKeyDown={(e) => {
        if ((e.key === " " || e.key === "Enter") && !disabled) {
          e.preventDefault();
          onKeyToggle();
        }
      }}
      style={{
        width: 34,
        height: 18,
        borderRadius: 9,
        background: checked
          ? "var(--vscode-button-background)"
          : "var(--vscode-input-border, var(--vscode-widget-border, #555))",
        position: "relative",
        flexShrink: 0,
        opacity: disabled ? 0.45 : 1,
        outline: focused
          ? "2px solid var(--vscode-focusBorder, var(--vscode-button-background))"
          : "none",
        outlineOffset: 2,
        transition: "background 0.15s",
      }}
    >
      <div
        style={{
          position: "absolute",
          top: 2,
          left: checked ? 18 : 2,
          width: 14,
          height: 14,
          borderRadius: "50%",
          background: "var(--vscode-button-foreground, #fff)",
          boxShadow: "0 1px 3px rgba(0,0,0,0.25)",
          transition: "left 0.15s",
        }}
      />
    </div>
  );
}

function Toggle({
  label,
  hint,
  checked,
  onChange,
  disabled,
}: {
  label: string;
  hint?: string;
  checked: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
}) {
  const hintId = useId();

  return (
    <label
      style={{
        display: "flex",
        alignItems: "flex-start",
        gap: 10,
        cursor: disabled ? "default" : "pointer",
        userSelect: "none",
        marginBottom: 12,
      }}
    >
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={(e) => onChange(e.target.checked)}
        style={{ display: "none" }}
      />
      <ToggleSwitch
        checked={checked}
        disabled={disabled}
        label={label}
        descriptionId={hint ? hintId : undefined}
        onKeyToggle={() => onChange(!checked)}
      />
      <div style={{ paddingTop: 1 }}>
        <div style={{ fontSize: 13, lineHeight: 1.3 }}>{label}</div>
        {hint && (
          <div
            id={hintId}
            style={{
              fontSize: 11,
              opacity: 0.55,
              marginTop: 2,
              lineHeight: 1.4,
            }}
          >
            {hint}
          </div>
        )}
      </div>
    </label>
  );
}

function DBTypeSelector({
  value,
  onChange,
}: {
  value: ConnectionType;
  onChange: (t: ConnectionType) => void;
}) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(138px, 1fr))",
        gap: 10,
        marginBottom: 4,
      }}
    >
      {DB_TYPES.map(({ type, label, short, color }) => {
        const selected = value === type;
        return (
          <button
            key={type}
            type="button"
            title={label}
            aria-label={label}
            aria-pressed={selected}
            onClick={() => onChange(type)}
            style={{
              padding: "11px 12px 12px",
              minHeight: 92,
              borderRadius: 12,
              border: selected
                ? `1px solid ${color}`
                : "1px solid var(--vscode-panel-border)",
              background: selected
                ? `linear-gradient(180deg, ${color}2e 0%, ${color}18 100%)`
                : `linear-gradient(180deg, var(--vscode-editorWidget-background, var(--vscode-input-background)) 0%, ${color}10 100%)`,
              boxShadow: selected
                ? `0 0 0 1px ${color}22 inset`
                : "0 1px 0 rgba(255,255,255,0.04) inset",
              cursor: "pointer",
              display: "flex",
              flexDirection: "column",
              alignItems: "flex-start",
              justifyContent: "space-between",
              gap: 10,
              textAlign: "left",
              transition: "background 0.15s ease, border-color 0.15s ease",
            }}
          >
            <div
              style={{
                width: 36,
                height: 36,
                borderRadius: 10,
                background: selected ? color : `${color}24`,
                border: selected ? "none" : `1px solid ${color}55`,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: 11,
                fontWeight: 800,
                letterSpacing: 0.5,
                color: selected ? "#fff" : color,
                flexShrink: 0,
              }}
            >
              {short}
            </div>
            <div style={{ display: "grid", gap: 3, width: "100%" }}>
              <span
                style={{
                  fontSize: 12,
                  fontWeight: selected ? 700 : 600,
                  lineHeight: 1.25,
                  color: "var(--vscode-foreground)",
                }}
              >
                {label}
              </span>
            </div>
          </button>
        );
      })}
    </div>
  );
}

function getDefaultColor(): string {
  if (document.body.classList.contains("vscode-light")) {
    return "#000000";
  }
  return "#ffffff";
}

export function ConnectionFormView({ existing }: Props): ReactElement {
  const isEdit = !!existing;
  const hasStoredSecret = existing?.hasStoredSecret ?? false;
  const hasStoredApiKey = existing?.hasStoredApiKey ?? false;
  const hasStoredSshPassword = existing?.hasStoredSshPassword ?? false;
  const hasStoredSshPrivateKey = existing?.hasStoredSshPrivateKey ?? false;
  const hasStoredSshPassphrase = existing?.hasStoredSshPassphrase ?? false;
  const hasStoredTlsKeyPassphrase =
    existing?.hasStoredTlsKeyPassphrase ?? false;
  const initialTlsMode = resolveConnectionTlsMode(existing ?? {});

  const [name, setName] = useState(existing?.name ?? "");
  const [color, setColor] = useState(existing?.color ?? getDefaultColor());
  const [type, setType] = useState<ConnectionType>(existing?.type ?? "pg");
  const [host, setHost] = useState(existing?.host ?? "localhost");
  const [port, setPort] = useState(
    String(
      existing?.port ?? DEFAULT_PORT_BY_CONNECTION_TYPE[existing?.type ?? "pg"],
    ),
  );
  const [database, setDatabase] = useState(existing?.database ?? "");
  const [username, setUsername] = useState(existing?.username ?? "");
  const [password, setPassword] = useState("");
  const [filePath, setFilePath] = useState(existing?.filePath ?? "");
  const [sqliteWalMode, setSqliteWalMode] = useState<SQLiteWalMode>(
    existing?.sqliteWalMode === "off" ? "off" : "auto",
  );
  const [folder, setFolder] = useState(existing?.folder ?? "");
  const [connectionReadOnly, setConnectionReadOnly] = useState(
    existing?.readOnly ?? false,
  );

  const [oracleServiceName, setOracleServiceName] = useState(
    existing?.serviceName ?? existing?.database ?? "",
  );
  const [mongoConnectionUri, setMongoConnectionUri] = useState(
    existing?.connectionUri ?? existing?.uri ?? "",
  );
  const [elasticsearchEndpoint, setElasticsearchEndpoint] = useState(
    existing?.endpoint ?? existing?.connectionUri ?? "",
  );
  const [elasticsearchApiKey, setElasticsearchApiKey] = useState("");
  const [elasticsearchCloudId, setElasticsearchCloudId] = useState(
    existing?.cloudId ?? "",
  );
  const [awsRegion, setAwsRegion] = useState(existing?.awsRegion ?? "");
  const [awsAccessKeyId, setAwsAccessKeyId] = useState("");
  const [awsSecretAccessKey, setAwsSecretAccessKey] = useState("");
  const [awsSessionToken, setAwsSessionToken] = useState("");
  const [awsProfile, setAwsProfile] = useState(existing?.awsProfile ?? "");
  const [dynamoEndpoint, setDynamoEndpoint] = useState(
    existing?.endpoint ?? existing?.awsEndpoint ?? "",
  );
  const [sshEnabled, setSshEnabled] = useState(existing?.sshEnabled ?? false);
  const [sshHost, setSshHost] = useState(existing?.sshHost ?? "");
  const [sshPort, setSshPort] = useState(String(existing?.sshPort ?? 22));
  const [sshUsername, setSshUsername] = useState(existing?.sshUsername ?? "");
  const [sshAuthMethod, setSshAuthMethod] = useState<ConnectionSshAuthMethod>(
    existing?.sshAuthMethod ?? "privateKey",
  );
  const [sshHostVerificationMode, setSshHostVerificationMode] =
    useState<ConnectionSshHostVerificationMode>(
      existing?.sshHostVerificationMode ?? "manual",
    );
  const [sshPassword, setSshPassword] = useState("");
  const [sshPrivateKey, setSshPrivateKey] = useState("");
  const [sshPassphrase, setSshPassphrase] = useState("");
  const [sshHostFingerprintSha256, setSshHostFingerprintSha256] = useState(
    existing?.sshHostFingerprintSha256 ?? "",
  );

  const [tlsMode, setTlsMode] = useState<ConnectionTlsMode>(initialTlsMode);
  const [tlsCaFilePath, setTlsCaFilePath] = useState(
    existing?.tls?.caFilePath ?? "",
  );
  const [tlsCertFilePath, setTlsCertFilePath] = useState(
    existing?.tls?.certFilePath ?? "",
  );
  const [tlsKeyFilePath, setTlsKeyFilePath] = useState(
    existing?.tls?.keyFilePath ?? "",
  );
  const [tlsKeyPassphrase, setTlsKeyPassphrase] = useState("");
  const [tlsServerNameOverride, setTlsServerNameOverride] = useState(
    existing?.tls?.serverNameOverride ?? "",
  );

  const [useSecretStorage, setUseSecretStorage] = useState(
    isEdit ? (existing?.useSecretStorage ?? false) : true,
  );

  const [testState, setTestState] = useState<
    "idle" | "testing" | "ok" | "fail"
  >("idle");
  const [testError, setTestError] = useState("");
  const [nameError, setNameError] = useState("");
  const [saving, setSaving] = useState(false);

  const isSQLite = type === "sqlite";
  const isOracle = type === "oracle";
  const isMongo = type === "mongodb";
  const isRedis = type === "redis";
  const isElasticsearch = type === "elasticsearch";
  const isDynamo = type === "dynamodb";
  const tlsSupport = getConnectionTlsSupport(type);
  const supportsTls = tlsSupport !== undefined;
  const tlsEnabled = supportsTls && isConnectionTlsEnabled(tlsMode);
  const tlsUsesClientAuth = tlsEnabled && tlsMode === "mutualTls";
  const tlsUsesCaFile =
    tlsEnabled &&
    tlsSupport?.supportsCaFile === true &&
    tlsMode !== "requireTrustServerCertificate";
  const tlsShowsServerNameOverride =
    tlsEnabled &&
    tlsSupport?.supportsServerNameOverride === true &&
    tlsMode !== "requireTrustServerCertificate";
  const tlsShouldShowConfigFields =
    tlsUsesCaFile || tlsUsesClientAuth || tlsShowsServerNameOverride;
  const effectiveSshEnabled = !isSQLite && sshEnabled;
  const tlsPassphraseRequiresSecretStorage =
    tlsUsesClientAuth &&
    (tlsKeyPassphrase.trim().length > 0 || hasStoredTlsKeyPassphrase);
  const secretStorageRequired =
    isElasticsearch ||
    isDynamo ||
    effectiveSshEnabled ||
    tlsPassphraseRequiresSecretStorage;
  const effectiveUseSecretStorage = secretStorageRequired
    ? true
    : useSecretStorage;
  const secretStorageLabel =
    isElasticsearch || effectiveSshEnabled || tlsPassphraseRequiresSecretStorage
      ? "Store secrets in VS Code Secret Storage"
      : "Store password in VS Code Secret Storage";
  const secretStorageHint = isElasticsearch
    ? "Elasticsearch credentials are always saved in your OS keychain and will not appear in settings.json."
    : effectiveSshEnabled
      ? "SSH is enabled, so database and SSH secrets are always saved in your OS keychain and will not appear in settings.json."
      : tlsPassphraseRequiresSecretStorage
        ? "TLS client key passphrases are always saved in your OS keychain and will not appear in settings.json."
        : effectiveUseSecretStorage
          ? "Password saved in your OS keychain — will NOT appear in settings.json."
          : "Password will be saved in plaintext in settings.json. Enable to store securely.";
  const elasticsearchApiKeyHint =
    hasStoredApiKey && elasticsearchApiKey.length === 0
      ? "Leave blank to keep the stored API key unchanged."
      : "Will be stored securely in VS Code Secret Storage (OS keychain)";
  const sshPasswordHint =
    hasStoredSshPassword && sshPassword.length === 0
      ? "Leave blank to keep the stored SSH password unchanged."
      : "Will be stored securely in VS Code Secret Storage (OS keychain).";
  const sshPrivateKeyHint =
    hasStoredSshPrivateKey && sshPrivateKey.length === 0
      ? "Leave blank to keep the stored SSH private key unchanged."
      : "Paste the OpenSSH or PEM private key. Stored securely in VS Code Secret Storage (OS keychain).";
  const sshPassphraseHint =
    hasStoredSshPassphrase && sshPassphrase.length === 0
      ? "Leave blank to keep the stored SSH passphrase unchanged."
      : "Optional. Stored securely in VS Code Secret Storage (OS keychain).";
  const tlsKeyPassphraseHint =
    hasStoredTlsKeyPassphrase && tlsKeyPassphrase.length === 0
      ? "Leave blank to keep the stored TLS client key passphrase unchanged."
      : "Optional. Stored securely in VS Code Secret Storage (OS keychain).";
  const tlsServerNameOverrideHint =
    tlsMode === "requireVerifyFull"
      ? "Optional hostname used for TLS SNI and certificate hostname validation."
      : tlsMode === "requireVerifyCa"
        ? "Optional hostname used for TLS SNI when certificate host matching is relaxed."
        : "Optional hostname used for TLS SNI.";
  const sshFingerprintHint =
    sshHostVerificationMode === "trustOnFirstUse"
      ? sshHostFingerprintSha256.trim().length > 0
        ? `The first accepted SSH fingerprint is pinned automatically. Current pinned fingerprint: ${sshHostFingerprintSha256.trim()}`
        : "The first successful SSH handshake will pin the discovered SHA256 fingerprint automatically and enforce it on future connections."
      : "Required. Use the OpenSSH SHA256 fingerprint format, for example SHA256:AbCdEf...";
  const sqliteWalHint =
    sqliteWalMode === "off"
      ? "Advanced. Automatic WAL handling is disabled for this SQLite connection."
      : connectionReadOnly
        ? "Advanced. WAL is enabled automatically for writable SQLite connections; this read-only session leaves the file untouched."
        : "Advanced. WAL is enabled automatically for writable SQLite connections unless you disable it here.";
  const tlsModeHint =
    tlsMode === "disabled"
      ? "Connection encryption is disabled."
      : tlsMode === "requireVerifyFull"
        ? "Encrypt the connection and verify both the certificate chain and server hostname."
        : tlsMode === "requireVerifyCa"
          ? "Encrypt the connection and verify the certificate chain without strict hostname verification."
          : tlsMode === "requireTrustServerCertificate"
            ? "Encrypt the connection but accept self-signed or otherwise untrusted server certificates."
            : "Encrypt the connection and present a client certificate and private key to the server.";

  useEffect(
    () =>
      onMessage<{
        target: ConnectionFormBrowseTarget;
        filePath: string | null;
      }>("browseFileResult", (p) => {
        if (p.filePath !== null) {
          switch (p.target) {
            case "filePath":
              setFilePath(p.filePath);
              break;
            case "tlsCaFile":
              setTlsCaFilePath(p.filePath);
              break;
            case "tlsCertFile":
              setTlsCertFilePath(p.filePath);
              break;
            case "tlsKeyFile":
              setTlsKeyFilePath(p.filePath);
              break;
          }
        }
      }),
    [],
  );

  useEffect(
    () =>
      onMessage<{ success: boolean; error?: string }>("testResult", (p) => {
        setTestState(p.success ? "ok" : "fail");
        setTestError(p.error ?? "Connection failed");
      }),
    [],
  );

  useEffect(
    () =>
      onMessage<{ success: boolean; error?: string }>("saveResult", (p) => {
        setSaving(false);
        if (p.success) {
          return;
        }
        setTestState("fail");
        setTestError(p.error ?? "Connection failed");
      }),
    [],
  );

  const handleTypeChange = (nextType: ConnectionType) => {
    setType(nextType);
    setPort(String(DEFAULT_PORT_BY_CONNECTION_TYPE[nextType] || ""));
    setTestState("idle");
    setTlsMode("disabled");
    setTlsCaFilePath("");
    setTlsCertFilePath("");
    setTlsKeyFilePath("");
    setTlsKeyPassphrase("");
    setTlsServerNameOverride("");
  };

  const buildPayload = useCallback((): ConnectionFormSubmission => {
    const parsedSshPort = Number.parseInt(sshPort.trim(), 10);
    const tlsConfig = supportsTls
      ? {
          mode: tlsMode,
          caFilePath:
            tlsUsesCaFile && tlsSupport.supportsCaFile
              ? tlsCaFilePath.trim() || undefined
              : undefined,
          certFilePath:
            tlsUsesClientAuth && tlsSupport.supportsClientCertificate
              ? tlsCertFilePath.trim() || undefined
              : undefined,
          keyFilePath:
            tlsUsesClientAuth && tlsSupport.supportsClientKey
              ? tlsKeyFilePath.trim() || undefined
              : undefined,
          keyPassphrase:
            tlsUsesClientAuth && tlsSupport.supportsClientKeyPassphrase
              ? tlsKeyPassphrase
              : undefined,
          serverNameOverride:
            tlsEnabled && tlsSupport.supportsServerNameOverride
              ? tlsServerNameOverride.trim() || undefined
              : undefined,
        }
      : undefined;
    const legacyTlsFlags = deriveLegacyConnectionTlsFlags(tlsConfig);

    return {
      id: existing?.id ?? crypto.randomUUID(),
      name: name.trim(),
      type,
      readOnly: connectionReadOnly,
      folder: folder.trim() || undefined,
      color: color.trim() || undefined,
      useSecretStorage: effectiveUseSecretStorage,
      hasStoredSecret: hasStoredSecret || undefined,
      hasStoredApiKey: hasStoredApiKey || undefined,
      sshEnabled: effectiveSshEnabled,
      sshHost: effectiveSshEnabled ? sshHost.trim() || undefined : undefined,
      sshPort:
        effectiveSshEnabled &&
        Number.isInteger(parsedSshPort) &&
        parsedSshPort > 0
          ? parsedSshPort
          : undefined,
      sshUsername: effectiveSshEnabled
        ? sshUsername.trim() || undefined
        : undefined,
      sshAuthMethod: effectiveSshEnabled ? sshAuthMethod : undefined,
      sshHostVerificationMode: effectiveSshEnabled
        ? sshHostVerificationMode
        : undefined,
      sshPassword:
        effectiveSshEnabled && sshAuthMethod === "password"
          ? sshPassword
          : undefined,
      sshPrivateKey:
        effectiveSshEnabled && sshAuthMethod === "privateKey"
          ? sshPrivateKey
          : undefined,
      sshPassphrase:
        effectiveSshEnabled && sshAuthMethod === "privateKey"
          ? sshPassphrase
          : undefined,
      sshHostFingerprintSha256: effectiveSshEnabled
        ? sshHostFingerprintSha256.trim() || undefined
        : undefined,
      hasStoredSshPassword: hasStoredSshPassword || undefined,
      hasStoredSshPrivateKey: hasStoredSshPrivateKey || undefined,
      hasStoredSshPassphrase: hasStoredSshPassphrase || undefined,
      hasStoredTlsKeyPassphrase: hasStoredTlsKeyPassphrase || undefined,
      ...(isSQLite
        ? {
            filePath: filePath.trim(),
            sqliteWalMode,
          }
        : isDynamo
          ? {
              awsRegion: awsRegion.trim() || undefined,
              awsAccessKeyId: awsAccessKeyId.trim() || undefined,
              awsSecretAccessKey: awsSecretAccessKey.trim() || undefined,
              awsSessionToken: awsSessionToken.trim() || undefined,
              awsProfile: awsProfile.trim() || undefined,
              endpoint: dynamoEndpoint.trim() || undefined,
            }
          : {
              host: host.trim(),
              port: Number(port) || DEFAULT_PORT_BY_CONNECTION_TYPE[type],
              database: database.trim(),
              username: username.trim(),
              password,
              ssl: supportsTls ? legacyTlsFlags.ssl : undefined,
              rejectUnauthorized: supportsTls
                ? legacyTlsFlags.rejectUnauthorized
                : undefined,
              tls: tlsConfig,
              ...(isMongo
                ? {
                    connectionUri: mongoConnectionUri.trim() || undefined,
                  }
                : {}),
              ...(isElasticsearch
                ? {
                    endpoint: elasticsearchEndpoint.trim() || undefined,
                    apiKey: elasticsearchApiKey.trim() || undefined,
                    cloudId: elasticsearchCloudId.trim() || undefined,
                  }
                : {}),
              ...(isOracle
                ? {
                    serviceName: oracleServiceName.trim() || undefined,
                  }
                : {}),
            }),
    };
  }, [
    existing,
    name,
    type,
    connectionReadOnly,
    folder,
    color,
    effectiveUseSecretStorage,
    hasStoredSecret,
    hasStoredApiKey,
    effectiveSshEnabled,
    hasStoredSshPassword,
    hasStoredSshPrivateKey,
    hasStoredSshPassphrase,
    hasStoredTlsKeyPassphrase,
    isSQLite,
    filePath,
    sqliteWalMode,
    host,
    port,
    database,
    awsProfile,
    awsRegion,
    awsAccessKeyId,
    awsSecretAccessKey,
    awsSessionToken,
    dynamoEndpoint,
    sshHost,
    sshPort,
    sshUsername,
    sshAuthMethod,
    sshHostVerificationMode,
    sshPassword,
    sshPrivateKey,
    sshPassphrase,
    sshHostFingerprintSha256,
    username,
    password,
    supportsTls,
    tlsMode,
    tlsEnabled,
    tlsUsesClientAuth,
    tlsUsesCaFile,
    tlsSupport,
    tlsCaFilePath,
    tlsCertFilePath,
    tlsKeyFilePath,
    tlsKeyPassphrase,
    tlsServerNameOverride,
    isOracle,
    oracleServiceName,
    mongoConnectionUri,
    elasticsearchEndpoint,
    elasticsearchApiKey,
    elasticsearchCloudId,
    isMongo,
    isElasticsearch,
    isDynamo,
  ]);

  const validateSubmission = () => {
    if (
      type === "redis" &&
      database.trim().length > 0 &&
      !/^\d+$/.test(database.trim())
    ) {
      setTestState("fail");
      setTestError("Redis database must be a non-negative integer.");
      return false;
    }
    return true;
  };

  const handleTest = () => {
    if (!name.trim()) {
      setNameError("Name is required");
      return;
    }
    if (!validateSubmission()) {
      return;
    }
    setTestState("testing");
    setTestError("");
    postMessage("testConnection", buildPayload());
  };

  const handleSave = () => {
    if (!name.trim()) {
      setNameError("Name is required");
      return;
    }
    if (!validateSubmission()) {
      return;
    }
    setSaving(true);
    postMessage("saveConnection", buildPayload());
  };

  return (
    <div style={{ maxWidth: 520, margin: "0 auto", padding: "20px 20px 40px" }}>
      <Card>
        <CardHeader icon="tag" label="Identity" />
        <Field label="Connection Name" error={nameError}>
          <div
            style={{
              display: "flex",
              gap: 8,
              alignItems: "center",
              width: "100%",
            }}
          >
            <FocusInput
              aria-label="Connection name"
              value={name}
              onChange={(e) => {
                setName(e.target.value);
                setNameError("");
              }}
              placeholder="My Database"
              autoFocus
              style={{ flex: 1 }}
            />
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "flex-end",
                gap: 8,
                flexShrink: 0,
                minWidth: 0,
              }}
            >
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  flexShrink: 0,
                }}
              >
                <label
                  aria-label="Connection icon color"
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    width: 28,
                    height: 28,
                    borderRadius: 8,
                    border:
                      "1px solid var(--vscode-input-border, var(--vscode-widget-border, #555))",
                    background: "var(--vscode-input-background)",
                    cursor: "pointer",
                    position: "relative",
                    flexShrink: 0,
                    overflow: "hidden",
                  }}
                >
                  <span
                    aria-hidden="true"
                    style={{
                      width: 16,
                      height: 16,
                      borderRadius: 4,
                      backgroundColor: color,
                      border: `1px solid ${color}`,
                      flexShrink: 0,
                    }}
                  />
                  <input
                    type="color"
                    aria-label="Connection color"
                    value={color}
                    onChange={(e) => setColor(e.target.value)}
                    style={{
                      opacity: 0,
                      position: "absolute",
                      inset: 0,
                      width: "100%",
                      height: "100%",
                      cursor: "pointer",
                      border: "none",
                      padding: 0,
                    }}
                  />
                </label>
              </div>
            </div>
          </div>
        </Field>
        <Field
          label="Folder"
          hint="Group this connection in the explorer. Leave empty to show at the root."
        >
          <FocusInput
            aria-label="Connection folder"
            value={folder}
            onChange={(e) => setFolder(e.target.value)}
            placeholder="Production"
          />
        </Field>
      </Card>

      <Card>
        <CardHeader icon="database" label="Database Type" />
        <DBTypeSelector value={type} onChange={handleTypeChange} />
      </Card>

      <Card>
        <CardHeader icon="shield" label="Access" />
        <Toggle
          label="Open connection as read-only"
          hint="Blocks data mutations and shows table data in the same read-only mode used for views. Query text stays editable."
          checked={connectionReadOnly}
          onChange={setConnectionReadOnly}
        />
      </Card>

      <Card>
        <CardHeader icon="plug" label="Connection" />
        {isSQLite ? (
          <>
            <Field label="Database File Path">
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <FocusInput
                  aria-label="Database file path"
                  value={filePath}
                  onChange={(e) => setFilePath(e.target.value)}
                  placeholder="/absolute/path/to/database.db"
                  style={{ flex: 1 }}
                />
                <button
                  type="button"
                  style={buildButtonStyle("secondary", { size: "sm" })}
                  onClick={() =>
                    postMessage("browseFile", { target: "filePath" })
                  }
                >
                  Browse…
                </button>
              </div>
            </Field>
            <Field label="WAL Mode" hint={sqliteWalHint}>
              <FocusSelect
                aria-label="SQLite WAL mode"
                value={sqliteWalMode}
                onChange={(e) =>
                  setSqliteWalMode(e.target.value as SQLiteWalMode)
                }
              >
                <option value="auto">Automatic (recommended)</option>
                <option value="off">Disabled</option>
              </FocusSelect>
            </Field>
          </>
        ) : isDynamo ? (
          <>
            <Field label="Region" hint="AWS region, for example us-east-1.">
              <FocusInput
                aria-label="AWS region"
                value={awsRegion}
                onChange={(e) => setAwsRegion(e.target.value)}
                placeholder="us-east-1"
              />
            </Field>
            <Field
              label="AWS Profile"
              hint="Optional shared credentials profile to use instead of explicit keys."
            >
              <FocusInput
                aria-label="AWS profile"
                value={awsProfile}
                onChange={(e) => setAwsProfile(e.target.value)}
                placeholder="default"
              />
            </Field>
            <Field
              label="Endpoint"
              hint="Optional custom endpoint for DynamoDB Local or AWS-compatible services."
            >
              <FocusInput
                aria-label="DynamoDB endpoint"
                value={dynamoEndpoint}
                onChange={(e) => setDynamoEndpoint(e.target.value)}
                placeholder="http://localhost:8000"
              />
            </Field>
            <Field label="Access Key ID">
              <FocusInput
                aria-label="AWS access key id"
                value={awsAccessKeyId}
                onChange={(e) => setAwsAccessKeyId(e.target.value)}
                placeholder="AKIA..."
              />
            </Field>
            <Field label="Secret Access Key">
              <FocusInput
                aria-label="AWS secret access key"
                value={awsSecretAccessKey}
                onChange={(e) => setAwsSecretAccessKey(e.target.value)}
                type="password"
                placeholder="••••••••"
              />
            </Field>
            <Field label="Session Token (optional)">
              <FocusInput
                aria-label="AWS session token"
                value={awsSessionToken}
                onChange={(e) => setAwsSessionToken(e.target.value)}
                placeholder="IQoJ..."
              />
            </Field>
          </>
        ) : (
          <>
            <div style={{ display: "flex", gap: 8, marginBottom: 14 }}>
              <div style={{ flex: 1 }}>
                <FieldLabel>Host</FieldLabel>
                <FocusInput
                  aria-label="Host"
                  value={host}
                  onChange={(e) => setHost(e.target.value)}
                  placeholder="localhost"
                />
              </div>
              <div style={{ width: 90 }}>
                <FieldLabel>Port</FieldLabel>
                <FocusInput
                  aria-label="Port"
                  value={port}
                  onChange={(e) => setPort(e.target.value)}
                  type="text"
                  inputMode="numeric"
                  pattern="[0-9]*"
                  style={
                    {
                      fontFamily: "var(--vscode-editor-font-family, monospace)",
                      MozAppearance: "textfield",
                    } as CSSProperties
                  }
                />
              </div>
            </div>
            {!isElasticsearch && !isOracle && (
              <Field label="Database">
                <FocusInput
                  aria-label="Database"
                  value={database}
                  onChange={(e) => setDatabase(e.target.value)}
                  placeholder={isRedis ? "0" : "mydb"}
                />
              </Field>
            )}
            <Field label="Username">
              <FocusInput
                aria-label="Username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="admin"
              />
            </Field>
            {isMongo && (
              <Field
                label="Connection URI"
                hint="Optional. When set, this URI is used instead of host/port/user fields."
              >
                <FocusInput
                  aria-label="MongoDB connection URI"
                  value={mongoConnectionUri}
                  onChange={(e) => setMongoConnectionUri(e.target.value)}
                  placeholder="mongodb://user:pass@localhost:27017/mydb"
                />
              </Field>
            )}
            {isElasticsearch && (
              <>
                <Field
                  label="Endpoint"
                  hint="HTTP endpoint or connection URI for the cluster. Leave empty when using Cloud ID only."
                >
                  <FocusInput
                    aria-label="Elasticsearch endpoint"
                    value={elasticsearchEndpoint}
                    onChange={(e) => setElasticsearchEndpoint(e.target.value)}
                    placeholder="http://localhost:9200"
                  />
                </Field>
                <Field
                  label="API Key"
                  hint={`Optional API key for Elasticsearch authentication. ${elasticsearchApiKeyHint}`}
                >
                  <FocusInput
                    aria-label="Elasticsearch API key"
                    value={elasticsearchApiKey}
                    onChange={(e) => setElasticsearchApiKey(e.target.value)}
                    placeholder="base64ApiKey"
                  />
                </Field>
                <Field
                  label="Cloud ID"
                  hint="Optional Elastic Cloud deployment identifier."
                >
                  <FocusInput
                    aria-label="Elasticsearch cloud id"
                    value={elasticsearchCloudId}
                    onChange={(e) => setElasticsearchCloudId(e.target.value)}
                    placeholder="deployment-name:base64..."
                  />
                </Field>
              </>
            )}
          </>
        )}
      </Card>

      {!isSQLite && (
        <Card>
          <CardHeader icon="server-process" label="SSH Tunnel" />
          <Toggle
            label="Connect through SSH bastion"
            hint="Tunnel the database connection through a bastion host. SSH credentials are always stored in VS Code Secret Storage."
            checked={effectiveSshEnabled}
            onChange={setSshEnabled}
          />

          {isMongo && (
            <div
              style={{
                fontSize: 11,
                opacity: 0.58,
                marginTop: -2,
                marginBottom: 12,
                lineHeight: 1.4,
              }}
            >
              MongoDB over SSH supports only single-host direct connections in
              v1.
            </div>
          )}

          {effectiveSshEnabled && (
            <>
              <div style={{ display: "flex", gap: 8, marginBottom: 14 }}>
                <div style={{ flex: 1 }}>
                  <FieldLabel>SSH Host</FieldLabel>
                  <FocusInput
                    aria-label="SSH host"
                    value={sshHost}
                    onChange={(e) => setSshHost(e.target.value)}
                    placeholder="bastion.example.com"
                  />
                </div>
                <div style={{ width: 90 }}>
                  <FieldLabel>SSH Port</FieldLabel>
                  <FocusInput
                    aria-label="SSH port"
                    value={sshPort}
                    onChange={(e) => setSshPort(e.target.value)}
                    type="text"
                    inputMode="numeric"
                    pattern="[0-9]*"
                    style={
                      {
                        fontFamily:
                          "var(--vscode-editor-font-family, monospace)",
                        MozAppearance: "textfield",
                      } as CSSProperties
                    }
                  />
                </div>
              </div>

              <Field label="SSH Username">
                <FocusInput
                  aria-label="SSH username"
                  value={sshUsername}
                  onChange={(e) => setSshUsername(e.target.value)}
                  placeholder="tunnel"
                />
              </Field>

              <Field label="SSH Authentication">
                <FocusSelect
                  aria-label="SSH auth method"
                  value={sshAuthMethod}
                  onChange={(e) =>
                    setSshAuthMethod(e.target.value as ConnectionSshAuthMethod)
                  }
                >
                  <option value="privateKey">Private key</option>
                  <option value="password">Password</option>
                </FocusSelect>
              </Field>

              <Field label="SSH Host Verification">
                <FocusSelect
                  aria-label="SSH host verification mode"
                  value={sshHostVerificationMode}
                  onChange={(e) =>
                    setSshHostVerificationMode(
                      e.target.value as ConnectionSshHostVerificationMode,
                    )
                  }
                >
                  <option value="manual">Enter fingerprint manually</option>
                  <option value="trustOnFirstUse">Trust on first use</option>
                </FocusSelect>
              </Field>

              {sshAuthMethod === "password" ? (
                <Field label="SSH Password" hint={sshPasswordHint}>
                  <FocusInput
                    aria-label="SSH password"
                    value={sshPassword}
                    onChange={(e) => setSshPassword(e.target.value)}
                    type="password"
                    placeholder="••••••••"
                    style={{
                      fontFamily: "var(--vscode-editor-font-family, monospace)",
                    }}
                  />
                </Field>
              ) : (
                <>
                  <Field label="SSH Private Key" hint={sshPrivateKeyHint}>
                    <FocusTextArea
                      aria-label="SSH private key"
                      value={sshPrivateKey}
                      onChange={(e) => setSshPrivateKey(e.target.value)}
                      placeholder="-----BEGIN OPENSSH PRIVATE KEY-----\n..."
                      spellCheck={false}
                    />
                  </Field>
                  <Field label="SSH Passphrase" hint={sshPassphraseHint}>
                    <FocusInput
                      aria-label="SSH passphrase"
                      value={sshPassphrase}
                      onChange={(e) => setSshPassphrase(e.target.value)}
                      type="password"
                      placeholder="Optional"
                      style={{
                        fontFamily:
                          "var(--vscode-editor-font-family, monospace)",
                      }}
                    />
                  </Field>
                </>
              )}

              {sshHostVerificationMode === "manual" ? (
                <Field label="SSH Host Fingerprint" hint={sshFingerprintHint}>
                  <FocusInput
                    aria-label="SSH host fingerprint SHA256"
                    value={sshHostFingerprintSha256}
                    onChange={(e) =>
                      setSshHostFingerprintSha256(e.target.value)
                    }
                    placeholder="SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/"
                    style={{
                      fontFamily: "var(--vscode-editor-font-family, monospace)",
                    }}
                  />
                </Field>
              ) : (
                <Field label="SSH Host Fingerprint" hint={sshFingerprintHint}>
                  {null}
                </Field>
              )}
            </>
          )}
        </Card>
      )}

      {!isSQLite && !isDynamo && (
        <Card>
          <CardHeader
            icon="shield"
            label={
              isElasticsearch ? "Credentials & Security" : "Password & Security"
            }
          />

          <Toggle
            label={secretStorageLabel}
            hint={secretStorageHint}
            checked={effectiveUseSecretStorage}
            onChange={setUseSecretStorage}
            disabled={secretStorageRequired}
          />

          <Field
            label="Password"
            hint={
              effectiveUseSecretStorage &&
              hasStoredSecret &&
              password.length === 0
                ? "Leave blank to keep the stored password unchanged."
                : effectiveUseSecretStorage
                  ? "Will be stored securely in VS Code Secret Storage (OS keychain)"
                  : "Will be stored in plaintext in settings.json"
            }
          >
            <div style={{ position: "relative" }}>
              <FocusInput
                aria-label="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                type="password"
                placeholder="••••••••"
                style={{
                  fontFamily: "var(--vscode-editor-font-family, monospace)",
                  paddingRight: 34,
                  borderColor: effectiveUseSecretStorage
                    ? "var(--vscode-testing-iconPassed, #4ec94e)"
                    : "var(--vscode-inputValidation-warningBorder, #b89500)",
                }}
              />
              <span
                title={
                  effectiveUseSecretStorage
                    ? "Stored in OS keychain"
                    : "Stored in plaintext"
                }
                style={{
                  position: "absolute",
                  right: 8,
                  top: "50%",
                  transform: "translateY(-50%)",
                  fontSize: 14,
                  pointerEvents: "none",
                  opacity: 0.75,
                }}
              >
                {effectiveUseSecretStorage ? "🔒" : "⚠️"}
              </span>
            </div>
          </Field>

          <div
            style={{
              borderTop: "1px solid var(--vscode-panel-border)",
              paddingTop: 14,
              marginTop: 4,
            }}
          >
            {supportsTls && tlsSupport ? (
              <>
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 6,
                    marginBottom: 10,
                  }}
                >
                  <Icon name="lock" size={11} style={{ opacity: 0.45 }} />
                  <span
                    style={{
                      fontSize: 11,
                      fontWeight: 700,
                      textTransform: "uppercase",
                      letterSpacing: 0.5,
                      opacity: 0.45,
                    }}
                  >
                    Encryption
                  </span>
                </div>

                <Field
                  hint={tlsModeHint}
                  style={{ marginBottom: tlsEnabled ? 8 : 14 }}
                >
                  <div style={{ position: "relative" }}>
                    <FocusSelect
                      aria-label="TLS mode"
                      value={tlsMode}
                      onChange={(e) =>
                        setTlsMode(e.target.value as ConnectionTlsMode)
                      }
                      style={{ paddingLeft: 28 }}
                    >
                      {tlsSupport.modes.map((mode) => (
                        <option key={mode} value={mode}>
                          {TLS_MODE_LABELS[mode]}
                        </option>
                      ))}
                    </FocusSelect>
                    <span
                      style={{
                        position: "absolute",
                        left: 9,
                        top: "50%",
                        transform: "translateY(-50%)",
                        width: 7,
                        height: 7,
                        borderRadius: "50%",
                        background: TLS_MODE_COLORS[tlsMode],
                        pointerEvents: "none",
                        boxShadow: `0 0 0 2px ${TLS_MODE_COLORS[tlsMode]}44`,
                      }}
                    />
                  </div>
                </Field>

                {tlsShouldShowConfigFields && (
                  <div
                    style={{
                      border: "1px solid var(--vscode-panel-border)",
                      borderRadius: 6,
                      padding: "10px 12px 0",
                      marginBottom: 14,
                      background:
                        "var(--vscode-editorWidget-background, var(--vscode-input-background))",
                    }}
                  >
                    {tlsUsesCaFile && (
                      <Field
                        label="CA Certificate"
                        hint="Optional PEM/CRT bundle used to validate the server certificate chain."
                      >
                        <div
                          style={{
                            display: "flex",
                            gap: 8,
                            alignItems: "center",
                          }}
                        >
                          <FocusInput
                            aria-label="TLS CA certificate file"
                            value={tlsCaFilePath}
                            onChange={(e) => setTlsCaFilePath(e.target.value)}
                            placeholder="/path/to/ca.pem"
                            style={{
                              flex: 1,
                              fontFamily:
                                "var(--vscode-editor-font-family, monospace)",
                              fontSize: 12,
                            }}
                          />
                          <button
                            type="button"
                            style={buildButtonStyle("secondary", {
                              size: "sm",
                            })}
                            onClick={() =>
                              postMessage("browseFile", { target: "tlsCaFile" })
                            }
                          >
                            Browse…
                          </button>
                        </div>
                      </Field>
                    )}

                    {tlsUsesClientAuth &&
                      tlsSupport.supportsClientCertificate && (
                        <Field
                          label="Client Certificate"
                          hint="PEM or CRT file presented to the server for mutual TLS."
                        >
                          <div
                            style={{
                              display: "flex",
                              gap: 8,
                              alignItems: "center",
                            }}
                          >
                            <FocusInput
                              aria-label="TLS client certificate file"
                              value={tlsCertFilePath}
                              onChange={(e) =>
                                setTlsCertFilePath(e.target.value)
                              }
                              placeholder="/path/to/client.crt"
                              style={{
                                flex: 1,
                                fontFamily:
                                  "var(--vscode-editor-font-family, monospace)",
                                fontSize: 12,
                              }}
                            />
                            <button
                              type="button"
                              style={buildButtonStyle("secondary", {
                                size: "sm",
                              })}
                              onClick={() =>
                                postMessage("browseFile", {
                                  target: "tlsCertFile",
                                })
                              }
                            >
                              Browse…
                            </button>
                          </div>
                        </Field>
                      )}

                    {tlsUsesClientAuth && tlsSupport.supportsClientKey && (
                      <Field
                        label="Client Key"
                        hint="Private key file for mutual TLS."
                      >
                        <div
                          style={{
                            display: "flex",
                            gap: 8,
                            alignItems: "center",
                          }}
                        >
                          <FocusInput
                            aria-label="TLS client key file"
                            value={tlsKeyFilePath}
                            onChange={(e) => setTlsKeyFilePath(e.target.value)}
                            placeholder="/path/to/client.key"
                            style={{
                              flex: 1,
                              fontFamily:
                                "var(--vscode-editor-font-family, monospace)",
                              fontSize: 12,
                            }}
                          />
                          <button
                            type="button"
                            style={buildButtonStyle("secondary", {
                              size: "sm",
                            })}
                            onClick={() =>
                              postMessage("browseFile", {
                                target: "tlsKeyFile",
                              })
                            }
                          >
                            Browse…
                          </button>
                        </div>
                      </Field>
                    )}

                    {tlsUsesClientAuth &&
                      tlsSupport.supportsClientKeyPassphrase && (
                        <Field
                          label="Key Passphrase"
                          hint={tlsKeyPassphraseHint}
                        >
                          <FocusInput
                            aria-label="TLS client key passphrase"
                            value={tlsKeyPassphrase}
                            onChange={(e) =>
                              setTlsKeyPassphrase(e.target.value)
                            }
                            type="password"
                            placeholder="Optional"
                            style={{
                              fontFamily:
                                "var(--vscode-editor-font-family, monospace)",
                            }}
                          />
                        </Field>
                      )}

                    {tlsShowsServerNameOverride && (
                      <Field
                        label="Server Name Override"
                        hint={tlsServerNameOverrideHint}
                      >
                        <FocusInput
                          aria-label="TLS server name override"
                          value={tlsServerNameOverride}
                          onChange={(e) =>
                            setTlsServerNameOverride(e.target.value)
                          }
                          placeholder="db.example.com"
                        />
                      </Field>
                    )}
                  </div>
                )}
              </>
            ) : null}
          </div>
        </Card>
      )}

      {/* Oracle advanced options */}
      {isOracle && (
        <Card>
          <CardHeader icon="settings-gear" label="Oracle Options" />
          <Field
            label="Service Name"
            hint="e.g. XEPDB1, ORCL, or your PDB service name."
          >
            <FocusInput
              aria-label="Oracle service name"
              value={oracleServiceName}
              onChange={(e) => setOracleServiceName(e.target.value)}
              placeholder="XEPDB1"
            />
          </Field>
        </Card>
      )}

      {testState !== "idle" && (
        <div
          style={{
            marginBottom: 10,
            padding: "8px 12px",
            borderRadius: 5,
            fontSize: 12,
            display: "flex",
            alignItems: "center",
            gap: 7,
            ...(testState === "ok"
              ? {
                  background: "rgba(50,180,50,0.1)",
                  color: "var(--vscode-testing-iconPassed, #4ec94e)",
                  border: "1px solid rgba(50,180,50,0.25)",
                }
              : testState === "fail"
                ? {
                    background: "var(--vscode-inputValidation-errorBackground)",
                    color: "var(--vscode-errorForeground)",
                    border:
                      "1px solid var(--vscode-inputValidation-errorBorder)",
                  }
                : { opacity: 0.6 }),
          }}
        >
          {testState === "testing" && (
            <>
              <Icon name="sync" size={13} spin /> Testing connection…
            </>
          )}
          {testState === "ok" && (
            <>
              <Icon
                name="check"
                size={13}
                color="var(--vscode-testing-iconPassed)"
              />
              Connection successful
            </>
          )}
          {testState === "fail" && <>✗ {testError}</>}
        </div>
      )}

      <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
        <button
          type="button"
          style={buildButtonStyle("primary", {
            disabled: !name.trim() || saving,
            size: "md",
          })}
          disabled={!name.trim() || saving}
          onClick={handleSave}
        >
          {saving ? "Saving…" : isEdit ? "Save Changes" : "Create Connection"}
        </button>
        <button
          type="button"
          style={buildButtonStyle("secondary", {
            disabled: testState === "testing",
            size: "md",
          })}
          disabled={testState === "testing"}
          onClick={handleTest}
        >
          {testState === "testing" ? "Testing…" : "Test Connection"}
        </button>
        <button
          type="button"
          style={buildButtonStyle("ghost", { size: "md" })}
          onClick={() => postMessage("cancel")}
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
