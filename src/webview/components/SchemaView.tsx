// biome-ignore assist/source/organizeImports: React import order is intentional here.
import React, {
  useEffect,
  useState,
  type CSSProperties,
  type ReactNode,
} from "react";
import type { ColumnMeta, ForeignKeyMeta, IndexMeta } from "../types";
import { getCategoryPresentation, getStructuralBadgePresentation } from "../types";
import { onMessage, postMessage } from "../utils/messaging";
import { Icon } from "./Icon";

interface SchemaData {
  columns: ColumnMeta[];
  indexes: IndexMeta[];
  foreignKeys: ForeignKeyMeta[];
}

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
}

const th: CSSProperties = {
  padding: "5px 12px",
  textAlign: "left",
  background: "var(--vscode-editorGroupHeader-tabsBackground)",
  borderBottom: "2px solid var(--vscode-panel-border)",
  borderRight: "1px solid var(--vscode-panel-border)",
  fontWeight: 600,
  fontSize: 11,
  whiteSpace: "nowrap",
  position: "sticky",
  top: 0,
  zIndex: 1,
  userSelect: "none",
};

const td = (extra?: CSSProperties): CSSProperties => ({
  padding: "4px 12px",
  borderBottom: "1px solid var(--vscode-panel-border)",
  borderRight: "1px solid var(--vscode-panel-border)",
  fontSize: 12,
  fontFamily: "var(--vscode-editor-font-family, monospace)",
  verticalAlign: "middle",
  whiteSpace: "nowrap",
  ...extra,
});

export function SchemaView({
  connectionId: _connectionId,
  database: _database,
  schema,
  table,
}: Props) {
  const [data, setData] = useState<SchemaData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unData = onMessage<SchemaData>("schemaData", (d) => {
      setData(d);
      setLoading(false);
    });
    const unErr = onMessage<{ error: string }>(
      "schemaError",
      ({ error: e }) => {
        setError(e);
        setLoading(false);
      },
    );
    postMessage("ready");
    return () => {
      unData();
      unErr();
    };
  }, []);

  if (loading) {
    return (
      <div style={{ padding: 20, opacity: 0.5, fontSize: 13 }}>
        <Icon name="sync" size={13} spin style={{ marginRight: 6 }} />
        Loading schema…
      </div>
    );
  }

  if (error) {
    return (
      <div
        style={{
          margin: 12,
          padding: "10px 14px",
          borderRadius: 3,
          fontSize: 13,
          background: "var(--vscode-inputValidation-errorBackground)",
          border: "1px solid var(--vscode-inputValidation-errorBorder)",
          color: "var(--vscode-errorForeground)",
        }}
      >
        <strong>Error:</strong> {error}
      </div>
    );
  }

  if (!data) {
    return null;
  }

  const { columns, indexes, foreignKeys } = data;

  return (
    <div style={{ padding: "16px 20px", overflow: "auto", height: "100vh" }}>
      {}
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ fontSize: 15, fontWeight: 600, margin: 0 }}>
          {schema ? `${schema}.` : ""}
          {table}
        </h1>
        <div
          style={{
            marginTop: 6,
            height: 1,
            background: "var(--vscode-panel-border)",
          }}
        />
      </div>

      {}
      <Section title="Columns" count={columns.length}>
        <table
          style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}
        >
          <thead>
            <tr>
              <th style={th}>Name</th>
              <th style={th}>Type</th>
              <th style={th}>Nullable</th>
              <th style={th}>Default</th>
              <th style={th}>Flags</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((col, i) => (
              <ColRow
                key={col.name}
                col={col}
                index={i}
                foreignKeys={foreignKeys}
              />
            ))}
          </tbody>
        </table>
      </Section>

      {}
      {foreignKeys.length > 0 && (
        <Section title="Foreign Keys" count={foreignKeys.length}>
          <table
            style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}
          >
            <thead>
              <tr>
                <th style={th}>Column</th>
                <th style={th}>References</th>
                <th style={th}>Constraint</th>
              </tr>
            </thead>
            <tbody>
              {foreignKeys.map((fk, i) => (
                <FKRow
                  key={`${fk.constraintName}:${fk.column}`}
                  fk={fk}
                  isEven={i % 2 === 0}
                  currentSchema={schema}
                />
              ))}
            </tbody>
          </table>
        </Section>
      )}

      {}
      {indexes.length > 0 && (
        <Section title="Indexes" count={indexes.length}>
          <table
            style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}
          >
            <thead>
              <tr>
                <th style={th}>Name</th>
                <th style={th}>Columns</th>
                <th style={th}>Type</th>
              </tr>
            </thead>
            <tbody>
              {indexes.map((idx, i) => (
                <IdxRow key={idx.name} idx={idx} isEven={i % 2 === 0} />
              ))}
            </tbody>
          </table>
        </Section>
      )}
    </div>
  );
}

function Section({
  title,
  count,
  children,
}: {
  title: string;
  count: number;
  children: ReactNode;
}) {
  return (
    <div style={{ marginBottom: 28 }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          marginBottom: 8,
        }}
      >
        <h2
          style={{
            fontSize: 12,
            fontWeight: 600,
            margin: 0,
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            opacity: 0.7,
          }}
        >
          {title}
        </h2>
        <span
          style={{
            fontSize: 10,
            padding: "1px 6px",
            borderRadius: 8,
            background: "var(--vscode-badge-background)",
            color: "var(--vscode-badge-foreground)",
          }}
        >
          {count}
        </span>
      </div>
      <div
        style={{
          border: "1px solid var(--vscode-panel-border)",
          borderRadius: 3,
          overflow: "hidden",
        }}
      >
        {children}
      </div>
    </div>
  );
}

function ColRow({
  col,
  index,
  foreignKeys,
}: {
  col: ColumnMeta;
  index: number;
  foreignKeys: ForeignKeyMeta[];
}) {
  const [hov, setHov] = useState(false);
  const isEven = index % 2 === 0;
  const bg = hov
    ? "var(--vscode-list-hoverBackground)"
    : isEven
      ? "var(--vscode-editor-background)"
      : "var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04))";

  const badges: React.ReactNode[] = [];
  if (col.isPrimaryKey) {
    const presentation = getStructuralBadgePresentation("pk");
    badges.push(
      <Badge
        key="pk"
        label={presentation.label}
        color={presentation.foreground}
        background={presentation.badgeBackground}
        border={presentation.badgeBorder}
      />,
    );
  }
  if (col.isForeignKey || foreignKeys.some((fk) => fk.column === col.name)) {
    const presentation = getStructuralBadgePresentation("fk");
    badges.push(
      <Badge
        key="fk"
        label={presentation.label}
        color={presentation.foreground}
        background={presentation.badgeBackground}
        border={presentation.badgeBorder}
      />,
    );
  }
  if (col.isAutoIncrement) {
    const presentation = getStructuralBadgePresentation("ai");
    badges.push(
      <Badge
        key="ai"
        label={presentation.label}
        color={presentation.foreground}
        background={presentation.badgeBackground}
        border={presentation.badgeBorder}
      />,
    );
  }
  if (col.category) {
    const categoryPresentation = getCategoryPresentation(col.category);
    badges.push(
      <Badge
        key="cat"
        label={categoryPresentation.label}
        color={categoryPresentation.foreground}
        background={categoryPresentation.badgeBackground}
        border={categoryPresentation.badgeBorder}
      />,
    );
  }

  const rawDefault = col.defaultValue;
  const displayDefault =
    rawDefault != null
      ? rawDefault.replace(/::[A-Za-z_][\w. ]*/g, "").trim()
      : null;

  return (
    <tr
      style={{ background: bg, transition: "background 60ms" }}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
    >
      <td style={{ ...td(), fontWeight: col.isPrimaryKey ? 600 : 400 }}>
        {col.name}
      </td>
      <td
        style={td({
          color: "var(--vscode-charts-green, #4ec94e)",
          opacity: 0.85,
        })}
      >
        {col.type}
      </td>
      <td style={td({ textAlign: "center" })}>
        {col.nullable ? (
          <span style={{ opacity: 0.45, fontSize: 11 }}>NULL</span>
        ) : (
          <span
            style={{
              color: "var(--vscode-errorForeground)",
              fontSize: 11,
              fontWeight: 600,
            }}
          >
            NOT NULL
          </span>
        )}
      </td>
      <td
        style={td({
          opacity: displayDefault ? 0.85 : 0.3,
          fontStyle: displayDefault ? "normal" : "italic",
        })}
      >
        {displayDefault ?? "—"}
      </td>
      <td style={td()}>
        <div style={{ display: "flex", gap: 4 }}>{badges}</div>
      </td>
    </tr>
  );
}

function FKRow({
  fk,
  isEven,
  currentSchema,
}: {
  fk: ForeignKeyMeta;
  isEven: boolean;
  currentSchema: string;
}) {
  const [hov, setHov] = useState(false);
  const bg = hov
    ? "var(--vscode-list-hoverBackground)"
    : isEven
      ? "var(--vscode-editor-background)"
      : "var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04))";

  const refLabel =
    fk.referencedSchema && fk.referencedSchema !== currentSchema
      ? `${fk.referencedSchema}.${fk.referencedTable}.${fk.referencedColumn}`
      : `${fk.referencedTable}.${fk.referencedColumn}`;

  return (
    <tr
      style={{ background: bg, transition: "background 60ms" }}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
    >
      <td style={td()}>{fk.column}</td>
      <td style={td()}>
        <span style={{ color: "inherit" }}>{refLabel}</span>
      </td>
      <td style={td({ opacity: 0.55, fontSize: 11 })}>{fk.constraintName}</td>
    </tr>
  );
}

function IdxRow({ idx, isEven }: { idx: IndexMeta; isEven: boolean }) {
  const [hov, setHov] = useState(false);
  const bg = hov
    ? "var(--vscode-list-hoverBackground)"
    : isEven
      ? "var(--vscode-editor-background)"
      : "var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.04))";

  const typeBadges: React.ReactNode[] = [];
  if (idx.primary) {
    const presentation = getStructuralBadgePresentation("primary");
    typeBadges.push(
      <Badge
        key="pk"
        label={presentation.label}
        color={presentation.foreground}
        background={presentation.badgeBackground}
        border={presentation.badgeBorder}
      />,
    );
  } else if (idx.unique) {
    const presentation = getStructuralBadgePresentation("unique");
    typeBadges.push(
      <Badge
        key="u"
        label={presentation.label}
        color={presentation.foreground}
        background={presentation.badgeBackground}
        border={presentation.badgeBorder}
      />,
    );
  } else {
    const presentation = getStructuralBadgePresentation("index");
    typeBadges.push(
      <Badge
        key="i"
        label={presentation.label}
        color={presentation.foreground}
        background={presentation.badgeBackground}
        border={presentation.badgeBorder}
      />,
    );
  }

  return (
    <tr
      style={{ background: bg, transition: "background 60ms" }}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
    >
      <td style={td({ opacity: 0.85 })}>{idx.name}</td>
      <td style={td()}>{idx.columns.join(", ")}</td>
      <td style={td()}>
        <div style={{ display: "flex", gap: 4 }}>{typeBadges}</div>
      </td>
    </tr>
  );
}

function Badge({
  label,
  color,
  background = "var(--vscode-badge-background, rgba(128,128,128,0.16))",
  border = "none",
}: {
  label: string;
  color: string;
  background?: string;
  border?: CSSProperties["border"];
}) {
  return (
    <span
      style={{
        fontSize: 9,
        padding: "1px 5px",
        borderRadius: 2,
        backgroundColor: background,
        color,
        border,
        fontWeight: 700,
        letterSpacing: "0.05em",
        fontFamily: "var(--vscode-font-family, system-ui)",
      }}
    >
      {label}
    </span>
  );
}
