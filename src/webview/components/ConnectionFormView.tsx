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
} from "../../shared/connectionConfig";
import {
  type ConnectionType,
  DEFAULT_PORT_BY_CONNECTION_TYPE,
} from "../../shared/connectionTypes";
import type {
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

function parseRedisDbInput(value: string): {
  value?: number;
  error?: string;
} {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return {};
  }

  if (!/^\d+$/.test(trimmed)) {
    return { error: "Redis DB must be a non-negative integer." };
  }

  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed)) {
    return { error: "Redis DB must be a non-negative integer." };
  }

  return { value: parsed };
}

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

export function ConnectionFormView({ existing }: Props): ReactElement {
  const isEdit = !!existing;
  const hasStoredSecret = existing?.hasStoredSecret ?? false;
  const hasStoredSshPassword = existing?.hasStoredSshPassword ?? false;
  const hasStoredSshPrivateKey = existing?.hasStoredSshPrivateKey ?? false;
  const hasStoredSshPassphrase = existing?.hasStoredSshPassphrase ?? false;

  const [name, setName] = useState(existing?.name ?? "");
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
  const [folder, setFolder] = useState(existing?.folder ?? "");
  const [connectionReadOnly, setConnectionReadOnly] = useState(
    existing?.readOnly ?? false,
  );

  const [oracleServiceName, setOracleServiceName] = useState(
    existing?.serviceName ?? "",
  );
  const [oracleThickMode, setOracleThickMode] = useState(
    existing?.thickMode ?? false,
  );
  const [oracleClientPath, setOracleClientPath] = useState(
    existing?.clientPath ?? "",
  );
  const [mongoConnectionUri, setMongoConnectionUri] = useState(
    existing?.connectionUri ?? existing?.uri ?? "",
  );
  const [mongoAuthDatabase, setMongoAuthDatabase] = useState(
    existing?.authDatabase ?? existing?.authSource ?? "",
  );
  const [redisUsername, setRedisUsername] = useState(
    existing?.redisUsername ?? existing?.username ?? "",
  );
  const [redisKeyPrefix, setRedisKeyPrefix] = useState(
    existing?.keyPrefix ?? "",
  );
  const [elasticsearchEndpoint, setElasticsearchEndpoint] = useState(
    existing?.endpoint ?? existing?.connectionUri ?? "",
  );
  const [elasticsearchApiKey, setElasticsearchApiKey] = useState("");
  const [elasticsearchCloudId, setElasticsearchCloudId] = useState(
    existing?.cloudId ?? "",
  );
  const [redisDb, setRedisDb] = useState(
    existing?.redisDb === undefined ? "0" : String(existing.redisDb),
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

  const [sslEnabled, setSslEnabled] = useState(existing?.ssl ?? false);
  const [rejectUnauthorized, setRejectUnauthorized] = useState(
    existing?.rejectUnauthorized ?? true,
  );

  const [useSecretStorage, setUseSecretStorage] = useState(
    isEdit ? (existing?.useSecretStorage ?? false) : true,
  );

  const [testState, setTestState] = useState<
    "idle" | "testing" | "ok" | "fail"
  >("idle");
  const [testError, setTestError] = useState("");
  const [nameError, setNameError] = useState("");
  const [redisDbError, setRedisDbError] = useState("");
  const [saving, setSaving] = useState(false);

  const isSQLite = type === "sqlite";
  const isOracle = type === "oracle";
  const isMongo = type === "mongodb";
  const isRedis = type === "redis";
  const isElasticsearch = type === "elasticsearch";
  const isDynamo = type === "dynamodb";
  const supportsSsl = !isSQLite && !isDynamo;
  const effectiveSshEnabled = !isSQLite && sshEnabled;
  const secretStorageRequired =
    isElasticsearch || isDynamo || effectiveSshEnabled;
  const effectiveUseSecretStorage = secretStorageRequired
    ? true
    : useSecretStorage;
  const secretStorageLabel =
    isElasticsearch || effectiveSshEnabled
      ? "Store secrets in VS Code Secret Storage"
      : "Store password in VS Code Secret Storage";
  const secretStorageHint = isElasticsearch
    ? "Elasticsearch credentials are always saved in your OS keychain and will not appear in settings.json."
    : effectiveSshEnabled
      ? "SSH is enabled, so database and SSH secrets are always saved in your OS keychain and will not appear in settings.json."
      : effectiveUseSecretStorage
        ? "Password saved in your OS keychain — will NOT appear in settings.json."
        : "Password will be saved in plaintext in settings.json. Enable to store securely.";
  const elasticsearchApiKeyHint =
    hasStoredSecret && elasticsearchApiKey.length === 0
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
  const sshFingerprintHint =
    sshHostVerificationMode === "trustOnFirstUse"
      ? sshHostFingerprintSha256.trim().length > 0
        ? `The first accepted SSH fingerprint is pinned automatically. Current pinned fingerprint: ${sshHostFingerprintSha256.trim()}`
        : "The first successful SSH handshake will pin the discovered SHA256 fingerprint automatically and enforce it on future connections."
      : "Required. Use the OpenSSH SHA256 fingerprint format, for example SHA256:AbCdEf...";

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
    setSslEnabled(false);
    setRejectUnauthorized(true);
    setRedisDbError("");
  };

  const buildPayload = useCallback((): ConnectionFormSubmission => {
    const parsedRedisDb = parseRedisDbInput(redisDb);
    const parsedSshPort = Number.parseInt(sshPort.trim(), 10);

    return {
      id: existing?.id ?? crypto.randomUUID(),
      name: name.trim(),
      type,
      readOnly: connectionReadOnly,
      folder: folder.trim() || undefined,
      useSecretStorage: effectiveUseSecretStorage,
      hasStoredSecret: hasStoredSecret || undefined,
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
      ...(isSQLite
        ? { filePath: filePath.trim() }
        : isDynamo
          ? {
              database: database.trim() || undefined,
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
              database:
                isRedis || isElasticsearch ? undefined : database.trim(),
              username: isRedis ? undefined : username.trim(),
              password,
              ssl: supportsSsl ? sslEnabled : undefined,
              rejectUnauthorized:
                supportsSsl && sslEnabled ? rejectUnauthorized : undefined,
              ...(isMongo
                ? {
                    connectionUri: mongoConnectionUri.trim() || undefined,
                    authDatabase: mongoAuthDatabase.trim() || undefined,
                  }
                : {}),
              ...(isRedis
                ? {
                    redisUsername: redisUsername.trim() || undefined,
                    redisDb: parsedRedisDb.value,
                    keyPrefix: redisKeyPrefix.trim() || undefined,
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
                    thickMode: oracleThickMode || undefined,
                    clientPath:
                      oracleThickMode && oracleClientPath.trim()
                        ? oracleClientPath.trim()
                        : undefined,
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
    effectiveUseSecretStorage,
    hasStoredSecret,
    effectiveSshEnabled,
    hasStoredSshPassword,
    hasStoredSshPrivateKey,
    hasStoredSshPassphrase,
    isSQLite,
    filePath,
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
    sslEnabled,
    rejectUnauthorized,
    supportsSsl,
    isOracle,
    oracleServiceName,
    oracleThickMode,
    oracleClientPath,
    mongoConnectionUri,
    mongoAuthDatabase,
    redisUsername,
    redisKeyPrefix,
    elasticsearchEndpoint,
    elasticsearchApiKey,
    elasticsearchCloudId,
    redisDb,
    isMongo,
    isRedis,
    isElasticsearch,
    isDynamo,
  ]);

  const validateRedisDb = useCallback((): boolean => {
    if (!isRedis) {
      setRedisDbError("");
      return true;
    }

    const { error } = parseRedisDbInput(redisDb);
    setRedisDbError(error ?? "");
    return !error;
  }, [isRedis, redisDb]);

  const handleTest = () => {
    if (!validateRedisDb()) {
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
    if (!validateRedisDb()) {
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
          <FocusInput
            aria-label="Connection name"
            value={name}
            onChange={(e) => {
              setName(e.target.value);
              setNameError("");
            }}
            placeholder="My Database"
            autoFocus
          />
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
          <Field label="Database File Path">
            <FocusInput
              aria-label="Database file path"
              value={filePath}
              onChange={(e) => setFilePath(e.target.value)}
              placeholder="/absolute/path/to/database.db"
            />
          </Field>
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
            <Field label="Table Namespace" hint="Optional logical namespace.">
              <FocusInput
                aria-label="DynamoDB namespace"
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
                placeholder="default"
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
            {!isRedis && !isElasticsearch && (
              <Field label="Database">
                <FocusInput
                  aria-label="Database"
                  value={database}
                  onChange={(e) => setDatabase(e.target.value)}
                  placeholder="mydb"
                />
              </Field>
            )}
            {isRedis ? (
              <Field label="Redis Username">
                <FocusInput
                  aria-label="Redis username"
                  value={redisUsername}
                  onChange={(e) => setRedisUsername(e.target.value)}
                  placeholder="default"
                />
              </Field>
            ) : (
              <Field label="Username">
                <FocusInput
                  aria-label="Username"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="admin"
                />
              </Field>
            )}
            {isMongo && (
              <>
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
                <Field
                  label="Auth Database"
                  hint="Optional. For root/admin users, set to admin; otherwise leave blank to authenticate against the selected database."
                >
                  <FocusInput
                    aria-label="MongoDB auth database"
                    value={mongoAuthDatabase}
                    onChange={(e) => setMongoAuthDatabase(e.target.value)}
                    placeholder="admin"
                  />
                </Field>
              </>
            )}
            {isRedis && (
              <>
                <Field
                  label="Redis DB"
                  hint="Numeric Redis database index, usually 0."
                  error={redisDbError}
                >
                  <FocusInput
                    aria-label="Redis DB"
                    value={redisDb}
                    onChange={(e) => {
                      setRedisDb(e.target.value);
                      setRedisDbError("");
                    }}
                    placeholder="0"
                  />
                </Field>
                <Field
                  label="Key Prefix"
                  hint="Optional prefix applied when browsing or editing keys."
                >
                  <FocusInput
                    aria-label="Redis key prefix"
                    value={redisKeyPrefix}
                    onChange={(e) => setRedisKeyPrefix(e.target.value)}
                    placeholder="app:"
                  />
                </Field>
              </>
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
              paddingTop: 12,
              marginTop: 2,
            }}
          >
            <Toggle
              label="Enable SSL / TLS"
              hint={
                type === "mssql"
                  ? "Enable connection encryption (recommended for production)"
                  : "Encrypt connection with SSL/TLS"
              }
              checked={sslEnabled}
              onChange={setSslEnabled}
            />
            {sslEnabled && (
              <div
                style={{
                  marginLeft: 44,
                  paddingLeft: 12,
                  borderLeft: "2px solid var(--vscode-panel-border)",
                  marginBottom: 4,
                }}
              >
                <Toggle
                  label="Verify server certificate"
                  hint="Uncheck to accept self-signed certificates"
                  checked={rejectUnauthorized}
                  onChange={setRejectUnauthorized}
                />
              </div>
            )}
          </div>
        </Card>
      )}

      {/* Oracle advanced options */}
      {isOracle && (
        <Card>
          <CardHeader icon="settings-gear" label="Oracle Options" />
          <Field
            label="Service Name"
            hint="e.g. XEPDB1, ORCL, or your PDB service name. Leave empty to use the Database field."
          >
            <FocusInput
              aria-label="Oracle service name"
              value={oracleServiceName}
              onChange={(e) => setOracleServiceName(e.target.value)}
              placeholder="XEPDB1"
            />
          </Field>
          <Toggle
            label="Use thick mode (requires Oracle Instant Client)"
            hint="Enable only if thin mode doesn't work. Requires Oracle Instant Client on this machine."
            checked={oracleThickMode}
            onChange={setOracleThickMode}
          />
          {oracleThickMode && (
            <div
              style={{
                marginLeft: 44,
                paddingLeft: 12,
                borderLeft: "2px solid var(--vscode-panel-border)",
              }}
            >
              <Field
                label="Oracle Instant Client path"
                hint="Directory containing libclntsh.so / oci.dll. Leave empty for system default."
              >
                <FocusInput
                  aria-label="Oracle Instant Client path"
                  value={oracleClientPath}
                  onChange={(e) => setOracleClientPath(e.target.value)}
                  placeholder="/opt/oracle/instantclient_21_9"
                />
              </Field>
            </div>
          )}
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
