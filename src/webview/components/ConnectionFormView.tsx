import React, {
  type CSSProperties,
  type InputHTMLAttributes,
  type ReactElement,
  type ReactNode,
  type SelectHTMLAttributes,
  useCallback,
  useEffect,
  useState,
} from "react";
import {
  CONNECTION_TYPE_LABELS,
  CONNECTION_TYPES,
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

const s = {
  input: buildTextInputStyle("md") as CSSProperties,
  label: {
    display: "block",
    fontSize: "11px",
    fontWeight: 500,
    marginBottom: 4,
    opacity: 0.8,
  } as CSSProperties,
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

function Field({
  label,
  hint,
  error,
  children,
}: {
  label: string;
  hint?: string;
  error?: string;
  children: ReactNode;
}) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={s.label}>{label}</div>
      {children}
      {error && (
        <div
          style={{
            fontSize: 11,
            color: "var(--vscode-errorForeground)",
            marginTop: 3,
          }}
        >
          {error}
        </div>
      )}
      {hint && !error && (
        <div style={{ fontSize: 11, opacity: 0.5, marginTop: 3 }}>{hint}</div>
      )}
    </div>
  );
}

function Divider({ label }: { label: string }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        margin: "16px 0 12px",
      }}
    >
      <div
        style={{ flex: 1, height: 1, background: "var(--vscode-panel-border)" }}
      />
      <span style={{ fontSize: 11, opacity: 0.5, whiteSpace: "nowrap" }}>
        {label}
      </span>
      <div
        style={{ flex: 1, height: 1, background: "var(--vscode-panel-border)" }}
      />
    </div>
  );
}

function Toggle({
  label,
  hint,
  checked,
  onChange,
}: {
  label: string;
  hint?: string;
  checked: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <label
      style={{
        display: "flex",
        alignItems: "flex-start",
        gap: 8,
        cursor: "pointer",
        userSelect: "none",
        marginBottom: 10,
      }}
    >
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        style={{
          marginTop: 2,
          accentColor: "var(--vscode-button-background)",
          cursor: "pointer",
        }}
      />
      <span style={{ fontSize: 13 }}>
        {label}
        {hint && (
          <span
            style={{
              display: "block",
              fontSize: 11,
              opacity: 0.55,
              marginTop: 1,
            }}
          >
            {hint}
          </span>
        )}
      </span>
    </label>
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
    existing?.useSecretStorage ?? true,
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
      useSecretStorage: useSecretStorage || undefined,
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
    <div style={{ maxWidth: 520, margin: "0 auto", padding: "24px 24px 40px" }}>
      <h2 style={{ fontSize: 15, fontWeight: 600, marginBottom: 4 }}>
        {isEdit ? `Edit — ${existing?.name ?? ""}` : "New Connection"}
      </h2>
      <div
        style={{
          height: 1,
          background: "var(--vscode-panel-border)",
          marginBottom: 20,
        }}
      />

      <Field label="Name" error={nameError}>
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
        hint="Group this connection under a folder in the explorer. Leave empty to show at the root."
      >
        <FocusInput
          aria-label="Connection folder"
          value={folder}
          onChange={(e) => setFolder(e.target.value)}
          placeholder="Production"
        />
      </Field>

      <Field label="Database Type">
        <FocusSelect
          aria-label="Database type"
          value={type}
          onChange={(e) => handleTypeChange(e.target.value as ConnectionType)}
        >
          {CONNECTION_TYPES.map((connectionType) => (
            <option key={connectionType} value={connectionType}>
              {CONNECTION_TYPE_LABELS[connectionType]}
            </option>
          ))}
        </FocusSelect>
      </Field>

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
          <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
            <div style={{ flex: 1 }}>
              <div style={s.label}>Host</div>
              <FocusInput
                aria-label="Host"
                value={host}
                onChange={(e) => setHost(e.target.value)}
                placeholder="localhost"
              />
            </div>
            <div style={{ width: 90 }}>
              <div style={s.label}>Port</div>
              <FocusInput
                aria-label="Port"
                value={port}
                onChange={(e) => setPort(e.target.value)}
                type="text"
                inputMode="numeric"
                pattern="[0-9]*"
                style={
                  {
                    ...s.input,
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

          <Divider label="Password & Security" />

          <Toggle
            label="Store password in VS Code Secret Storage"
            hint={
              useSecretStorage
                ? "Password will be saved in your OS keychain via VS Code SecretStorage — it will NOT appear in settings.json."
                : "Password will be saved in plaintext in settings.json. Enable this option to store it securely."
            }
            checked={useSecretStorage}
            onChange={setUseSecretStorage}
          />

          <Field
            label="Password"
            hint={
              useSecretStorage && hasStoredSecret && password.length === 0
                ? "Leave blank to keep the stored password in VS Code Secret Storage."
                : useSecretStorage
                  ? "🔒 Will be stored securely in VS Code Secret Storage (OS keychain)"
                  : "⚠ Will be stored in plaintext in settings.json"
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
                  ...s.input,
                  fontFamily: "var(--vscode-editor-font-family, monospace)",
                  paddingRight: 34,
                  borderColor: useSecretStorage
                    ? "var(--vscode-testing-iconPassed, #4ec94e)"
                    : undefined,
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

          {isOracle && (
            <>
              <Divider label="Oracle Connection" />
              <Field
                label="Service Name"
                hint="e.g. XEPDB1, ORCL, or your PDB service name. Recommended over SID. Leave empty to use the Database field above."
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
                hint="Enable only if thin mode doesn't work. Requires Oracle Instant Client installed on this machine."
                checked={oracleThickMode}
                onChange={setOracleThickMode}
              />

              {oracleThickMode && (
                <div
                  style={{
                    paddingLeft: 24,
                    borderLeft: "2px solid var(--vscode-panel-border)",
                    marginBottom: 10,
                  }}
                >
                  <Field
                    label="Oracle Instant Client path"
                    hint="Directory containing libclntsh.so / oci.dll. Leave empty to use system default."
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
            </>
          )}

          <Divider label="SSL / TLS" />

          <Toggle
            label="Enable SSL"
            hint={
              type === "mssql"
                ? "Enables connection encryption (recommended for production)"
                : "Encrypt connection with SSL/TLS"
            }
            checked={sslEnabled}
            onChange={setSslEnabled}
          />

          {sslEnabled && (
            <div
              style={{
                paddingLeft: 24,
                borderLeft: "2px solid var(--vscode-panel-border)",
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
        </>
      )}

      {testState !== "idle" && (
        <div
          style={{
            marginBottom: 12,
            padding: "7px 10px",
            borderRadius: 3,
            fontSize: 12,
            display: "flex",
            alignItems: "center",
            gap: 6,
            ...(testState === "ok"
              ? {
                  background: "rgba(50,180,50,0.12)",
                  color: "var(--vscode-testing-iconPassed, #4ec94e)",
                  border: "1px solid rgba(50,180,50,0.3)",
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
              <SpinIcon /> Testing connection…
            </>
          )}
          {testState === "ok" && (
            <>
              <Icon
                name="check"
                size={13}
                color="var(--vscode-testing-iconPassed)"
                style={{ marginRight: 4 }}
              />
              Connection successful
            </>
          )}
          {testState === "fail" && <>✗ {testError}</>}
        </div>
      )}

      <div
        style={{ display: "flex", gap: 6, alignItems: "center", marginTop: 4 }}
      >
        <button
          type="button"
          style={buildButtonStyle("primary", {
            disabled: !name.trim() || saving,
            size: "md",
          })}
          disabled={!name.trim() || saving}
          onClick={handleSave}
        >
          {saving ? "Saving…" : "Save"}
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

function SpinIcon() {
  return <Icon name="sync" size={14} spin />;
}
