/**
 * Database type selector and TLS mode constants for ConnectionFormView.
 *
 * Extracted from ConnectionFormView.tsx to reduce its size and improve
 * reusability.
 */
import React from "react";
import type { ConnectionType } from "../../../shared/connectionTypes";

// ─── Database Types ─────────────────────────────────────────────────────────

export interface DBTypeEntry {
  type: ConnectionType;
  label: string;
  short: string;
  color: string;
}

export const DB_TYPES: DBTypeEntry[] = [
  { type: "pg", label: "PostgreSQL", short: "PG", color: "#336791" },
  { type: "mysql", label: "MySQL / MariaDB", short: "MY", color: "#c47900" },
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

// ─── DB Type Selector Component ─────────────────────────────────────────────

interface DBTypeSelectorProps {
  value: ConnectionType;
  onChange: (t: ConnectionType) => void;
}

export function DBTypeSelector({
  value,
  onChange,
}: DBTypeSelectorProps): React.ReactElement {
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

// ─── TLS Mode Constants ─────────────────────────────────────────────────────

export const TLS_MODE_LABELS: Record<string, string> = {
  disabled: "Disabled",
  requireTrustServerCertificate: "Required, trust server certificate",
  requireVerifyCa: "Required, verify CA only",
  requireVerifyFull: "Required, verify full",
  mutualTls: "Mutual TLS",
};

export const TLS_MODE_COLORS: Record<string, string> = {
  disabled: "#6b7280",
  requireTrustServerCertificate: "#f59e0b",
  requireVerifyCa: "#3b82f6",
  requireVerifyFull: "#10b981",
  mutualTls: "#8b5cf6",
};
