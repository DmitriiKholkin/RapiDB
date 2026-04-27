import React, {
  type CSSProperties,
  type InputHTMLAttributes,
  type ReactElement,
  type ReactNode,
  useCallback,
  useEffect,
  useState,
} from "react";
import {
  type ConnectionType,
  DEFAULT_PORT_BY_CONNECTION_TYPE,
} from "../../shared/connectionTypes";
import type {
  ConnectionFormExistingState,
  ConnectionFormSubmission,
} from "../../shared/webviewContracts";
import { buildButtonStyle } from "../utils/buttonStyles";
import { buildTextInputStyle } from "../utils/controlStyles";
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
  { type: "mysql", label: "MySQL", short: "MY", color: "#c47900" },
  { type: "mssql", label: "SQL Server", short: "MS", color: "#cc2927" },
  { type: "oracle", label: "Oracle", short: "OR", color: "#c74634" },
  { type: "sqlite", label: "SQLite", short: "SQ", color: "#0a7bc4" },
];

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
  onKeyToggle,
}: {
  checked: boolean;
  disabled?: boolean;
  label: string;
  onKeyToggle: () => void;
}) {
  return (
    <div
      role="switch"
      aria-label={label}
      aria-checked={checked}
      aria-disabled={disabled}
      tabIndex={disabled ? -1 : 0}
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
        outline: "none",
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
        onKeyToggle={() => onChange(!checked)}
      />
      <div style={{ paddingTop: 1 }}>
        <div style={{ fontSize: 13, lineHeight: 1.3 }}>{label}</div>
        {hint && (
          <div
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
        gridTemplateColumns: "repeat(5, 1fr)",
        gap: 6,
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
              padding: "10px 6px 9px",
              borderRadius: 6,
              border: selected
                ? `2px solid ${color}`
                : "1px solid var(--vscode-panel-border)",
              background: selected ? `${color}1a` : "transparent",
              cursor: "pointer",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              gap: 6,
            }}
          >
            <div
              style={{
                width: 30,
                height: 30,
                borderRadius: "50%",
                background: selected ? color : "var(--vscode-input-background)",
                border: selected
                  ? "none"
                  : "1px solid var(--vscode-panel-border)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: 10,
                fontWeight: 700,
                letterSpacing: 0.3,
                color: selected ? "#fff" : "var(--vscode-foreground)",
                flexShrink: 0,
              }}
            >
              {short}
            </div>
            <span
              style={{
                fontSize: 10,
                fontWeight: selected ? 600 : 400,
                opacity: selected ? 1 : 0.6,
                textAlign: "center",
                lineHeight: 1.2,
                color: selected ? color : "inherit",
              }}
            >
              {label}
            </span>
          </button>
        );
      })}
    </div>
  );
}

export function ConnectionFormView({ existing }: Props): ReactElement {
  const isEdit = !!existing;
  const hasStoredSecret = existing?.hasStoredSecret ?? false;

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

  const [oracleServiceName, setOracleServiceName] = useState(
    existing?.serviceName ?? "",
  );
  const [oracleThickMode, setOracleThickMode] = useState(
    existing?.thickMode ?? false,
  );
  const [oracleClientPath, setOracleClientPath] = useState(
    existing?.clientPath ?? "",
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
  const [saving, setSaving] = useState(false);

  const isSQLite = type === "sqlite";
  const isOracle = type === "oracle";
  const supportsSsl = type !== "sqlite";

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
  };

  const buildPayload = useCallback(
    (): ConnectionFormSubmission => ({
      id: existing?.id ?? crypto.randomUUID(),
      name: name.trim(),
      type,
      folder: folder.trim() || undefined,
      useSecretStorage,
      hasStoredSecret: hasStoredSecret || undefined,
      ...(isSQLite
        ? { filePath: filePath.trim() }
        : {
            host: host.trim(),
            port: Number(port) || DEFAULT_PORT_BY_CONNECTION_TYPE[type],
            database: database.trim(),
            username: username.trim(),
            password,
            ssl: supportsSsl ? sslEnabled : undefined,
            rejectUnauthorized:
              supportsSsl && sslEnabled ? rejectUnauthorized : undefined,
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
    }),
    [
      existing,
      name,
      type,
      folder,
      useSecretStorage,
      hasStoredSecret,
      isSQLite,
      filePath,
      host,
      port,
      database,
      username,
      password,
      sslEnabled,
      rejectUnauthorized,
      supportsSsl,
      isOracle,
      oracleServiceName,
      oracleThickMode,
      oracleClientPath,
    ],
  );

  const handleTest = () => {
    setTestState("testing");
    setTestError("");
    postMessage("testConnection", buildPayload());
  };

  const handleSave = () => {
    if (!name.trim()) {
      setNameError("Name is required");
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
            <Field label="Database">
              <FocusInput
                aria-label="Database"
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
                placeholder="mydb"
              />
            </Field>
            <Field label="Username">
              <FocusInput
                aria-label="Username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="admin"
              />
            </Field>
          </>
        )}
      </Card>

      {!isSQLite && (
        <Card>
          <CardHeader icon="shield" label="Password & Security" />

          <Toggle
            label="Store password in VS Code Secret Storage"
            hint={
              useSecretStorage
                ? "Password saved in your OS keychain — will NOT appear in settings.json."
                : "Password will be saved in plaintext in settings.json. Enable to store securely."
            }
            checked={useSecretStorage}
            onChange={setUseSecretStorage}
          />

          <Field
            label="Password"
            hint={
              useSecretStorage && hasStoredSecret && password.length === 0
                ? "Leave blank to keep the stored password unchanged."
                : useSecretStorage
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
                  borderColor: useSecretStorage
                    ? "var(--vscode-testing-iconPassed, #4ec94e)"
                    : "var(--vscode-inputValidation-warningBorder, #b89500)",
                }}
              />
              <span
                title={
                  useSecretStorage
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
                {useSecretStorage ? "🔒" : "⚠️"}
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
