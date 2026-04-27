import React, {
  type CSSProperties,
  type ReactNode,
  useEffect,
  useState,
} from "react";
import type { ColumnMeta, ForeignKeyMeta, IndexMeta } from "../types";
import { getStructuralBadgePresentation } from "../types";
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
// ─── Design tokens ────────────────────────────────────────────────────────────

const BORDER = "1px solid var(--vscode-panel-border)";
const MONO = "var(--vscode-editor-font-family, 'Menlo', 'Consolas', monospace)";
const UI_FONT = "var(--vscode-font-family, system-ui, sans-serif)";

const th = (extra?: CSSProperties): CSSProperties => ({
  padding: "5px 12px",
  textAlign: "left",
  background: "var(--vscode-editorGroupHeader-tabsBackground)",
  borderBottom: "2px solid var(--vscode-panel-border)",
  borderRight: BORDER,
  fontFamily: UI_FONT,
  fontWeight: 600,
  fontSize: 10,
  letterSpacing: "0.07em",
  textTransform: "uppercase",
  whiteSpace: "nowrap",
  position: "sticky",
  top: 0,
  zIndex: 1,
  userSelect: "none",
  ...extra,
});

const td = (extra?: CSSProperties): CSSProperties => ({
  padding: "7px 12px",
  borderBottom: BORDER,
  borderRight: BORDER,
  verticalAlign: "top",
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
    const unErr = onMessage<{
      error: string;
    }>("schemaError", ({ error: e }) => {
      setError(e);
      setLoading(false);
    });
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

      <Section title="Columns" count={columns.length}>
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            fontSize: 12,
            tableLayout: "fixed",
          }}
        >
          <colgroup>
            <col style={{ width: "26%" }} />
            <col style={{ width: "22%" }} />
            <col style={{ width: 80 }} />
            <col />
          </colgroup>
          <thead>
            <tr>
              <th scope="col" style={th()}>
                Column
              </th>
              <th scope="col" style={th()}>
                Type
              </th>
              <th scope="col" style={th({ textAlign: "center" })}>
                Null
              </th>
              <th scope="col" style={th({ borderRight: "none" })}>
                Default / Generated
              </th>
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

      {foreignKeys.length > 0 && (
        <Section title="Foreign Keys" count={foreignKeys.length}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 12,
              tableLayout: "fixed",
            }}
          >
            <colgroup>
              <col style={{ width: "22%" }} />
              <col />
              <col style={{ width: "36%" }} />
            </colgroup>
            <thead>
              <tr>
                <th scope="col" style={th()}>
                  Column
                </th>
                <th scope="col" style={th()}>
                  References
                </th>
                <th scope="col" style={th({ borderRight: "none" })}>
                  Constraint
                </th>
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

      {indexes.length > 0 && (
        <Section title="Indexes" count={indexes.length}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 12,
              tableLayout: "fixed",
            }}
          >
            <colgroup>
              <col style={{ width: "42%" }} />
              <col />
              <col style={{ width: 100 }} />
            </colgroup>
            <thead>
              <tr>
                <th scope="col" style={th()}>
                  Name
                </th>
                <th scope="col" style={th()}>
                  Columns
                </th>
                <th scope="col" style={th({ borderRight: "none" })}>
                  Type
                </th>
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
            fontFamily: UI_FONT,
            fontSize: 11,
            fontWeight: 600,
            margin: 0,
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            opacity: 0.65,
          }}
        >
          {title}
        </h2>
        <span
          style={{
            fontFamily: UI_FONT,
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
          border: BORDER,
          borderRadius: 4,
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

  const isFk =
    col.isForeignKey || foreignKeys.some((fk) => fk.column === col.name);
  const fragments = columnDefaultFragments(col);

  return (
    <tr
      style={{ background: bg, transition: "background 60ms" }}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
    >
      {/* Name + inline badges */}
      <td style={td()}>
        <div
          style={{
            display: "flex",
            alignItems: "flex-start",
            gap: 6,
            flexWrap: "wrap",
          }}
        >
          <span
            style={{
              fontFamily: MONO,
              fontSize: 12,
              fontWeight: col.isPrimaryKey ? 700 : 400,
              wordBreak: "break-all",
              lineHeight: 1.5,
            }}
          >
            {col.name}
          </span>
          {(col.isPrimaryKey ||
            isFk ||
            col.isAutoIncrement ||
            col.isComputed) && (
            <div
              style={{
                display: "flex",
                gap: 3,
                flexShrink: 0,
                paddingTop: 1,
              }}
            >
              {col.isPrimaryKey && <StructBadge kind="pk" />}
              {isFk && <StructBadge kind="fk" />}
              {col.isAutoIncrement && <StructBadge kind="ai" />}
              {col.isComputed && (
                <Badge
                  label="GEN"
                  color="var(--vscode-terminal-ansiBlue, #356fa8)"
                  background="rgba(53,111,168,0.15)"
                />
              )}
            </div>
          )}
        </div>
      </td>

      {/* Type */}
      <td
        style={td({
          fontFamily: MONO,
          fontSize: 12,
          color: "var(--vscode-charts-green, #4ec94e)",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        })}
        title={col.type}
      >
        {col.type}
      </td>

      {/* Nullable */}
      <td style={td({ textAlign: "center", paddingTop: 9 })}>
        <NullPill nullable={col.nullable} />
      </td>

      {/* Default / Generated */}
      <td style={td({ borderRight: "none" })}>
        {fragments.length > 0 ? (
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            {fragments.map((frag, fi) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: stable fragments
              <SqlChip key={fi} text={frag} />
            ))}
          </div>
        ) : (
          <span
            style={{
              fontFamily: UI_FONT,
              fontSize: 12,
              opacity: 0.28,
              fontStyle: "italic",
            }}
          >
            —
          </span>
        )}
      </td>
    </tr>
  );
}

function columnDefaultFragments(col: ColumnMeta): string[] {
  if (col.isComputed && col.computedExpression) {
    const mode = col.generatedKind ? ` ${col.generatedKind.toUpperCase()}` : "";
    return [`GENERATED ALWAYS AS (${col.computedExpression})${mode}`];
  }
  const lines: string[] = [];
  if (col.defaultValue != null) {
    lines.push(`DEFAULT ${col.defaultValue}`);
  }
  if (col.onUpdateExpression) {
    lines.push(`ON UPDATE ${col.onUpdateExpression}`);
  }
  return lines;
}

function NullPill({ nullable }: { nullable: boolean }) {
  return nullable ? (
    <span
      style={{
        fontFamily: UI_FONT,
        fontSize: 10,
        fontWeight: 500,
        padding: "1px 7px",
        borderRadius: 10,
        background: "rgba(128,128,128,0.12)",
        color: "var(--vscode-descriptionForeground, #6b7280)",
        display: "inline-block",
      }}
    >
      YES
    </span>
  ) : (
    <span
      style={{
        fontFamily: UI_FONT,
        fontSize: 10,
        fontWeight: 600,
        padding: "1px 7px",
        borderRadius: 10,
        background: "rgba(160,61,48,0.12)",
        color: "var(--vscode-errorForeground, #cd3131)",
        display: "inline-block",
      }}
    >
      NO
    </span>
  );
}

function SqlChip({ text }: { text: string }) {
  return (
    <code
      style={{
        fontFamily: MONO,
        fontSize: 11,
        display: "block",
        background:
          "var(--vscode-textCodeBlock-background, rgba(128,128,128,0.1))",
        borderRadius: 3,
        padding: "2px 7px",
        wordBreak: "break-word",
        whiteSpace: "pre-wrap",
        lineHeight: 1.5,
      }}
    >
      {text}
    </code>
  );
}

function StructBadge({ kind }: { kind: "pk" | "fk" | "ai" }) {
  const p = getStructuralBadgePresentation(kind);
  return (
    <Badge
      label={p.label}
      color={p.foreground}
      background={p.badgeBackground}
    />
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
      <td
        style={td({
          fontFamily: MONO,
          fontSize: 12,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        })}
        title={fk.column}
      >
        {fk.column}
      </td>
      <td
        style={td({
          fontFamily: MONO,
          fontSize: 12,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        })}
        title={refLabel}
      >
        {refLabel}
      </td>
      <td
        style={td({
          borderRight: "none",
          fontFamily: MONO,
          fontSize: 11,
          opacity: 0.55,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        })}
        title={fk.constraintName}
      >
        {fk.constraintName}
      </td>
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

  let kind: "primary" | "unique" | "index" = "index";
  if (idx.primary) kind = "primary";
  else if (idx.unique) kind = "unique";
  const p = getStructuralBadgePresentation(kind);

  return (
    <tr
      style={{ background: bg, transition: "background 60ms" }}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
    >
      <td
        style={td({
          fontFamily: MONO,
          fontSize: 12,
          opacity: 0.85,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        })}
        title={idx.name}
      >
        {idx.name}
      </td>
      <td
        style={td({
          fontFamily: MONO,
          fontSize: 12,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        })}
        title={idx.columns.join(", ")}
      >
        {idx.columns.join(", ")}
      </td>
      <td style={td({ borderRight: "none", verticalAlign: "middle" })}>
        <Badge
          label={p.label}
          color={p.foreground}
          background={p.badgeBackground}
        />
      </td>
    </tr>
  );
}
function Badge({
  label,
  color,
  background = "rgba(128,128,128,0.15)",
}: {
  label: string;
  color: string;
  background?: string;
}) {
  return (
    <span
      style={{
        fontFamily: UI_FONT,
        fontSize: 9,
        fontWeight: 700,
        padding: "1px 5px",
        borderRadius: 3,
        backgroundColor: background,
        color,
        letterSpacing: "0.05em",
        whiteSpace: "nowrap",
        display: "inline-block",
      }}
    >
      {label}
    </span>
  );
}
