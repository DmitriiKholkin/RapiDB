/**
 * Reusable form primitives for the connection form.
 *
 * These are controlled input components that integrate with VS Code
 * theming and provide consistent focus styling.
 */
import React, {
  type CSSProperties,
  type InputHTMLAttributes,
  type ReactNode,
  type SelectHTMLAttributes,
  type TextareaHTMLAttributes,
  useId,
  useState,
} from "react";
import {
  buildSelectControlStyle,
  buildTextInputStyle,
} from "../../utils/controlStyles";
import { Icon } from "../Icon";

// ─── Focus-Aware Inputs ─────────────────────────────────────────────────────

export function FocusInput(props: InputHTMLAttributes<HTMLInputElement>) {
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

export function FocusSelect(props: SelectHTMLAttributes<HTMLSelectElement>) {
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

export function FocusTextArea(
  props: TextareaHTMLAttributes<HTMLTextAreaElement>,
) {
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

// ─── Layout Primitives ──────────────────────────────────────────────────────

export function FieldLabel({ children }: { children: ReactNode }) {
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

export function Field({
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

export function Card({
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

export function CardHeader({ icon, label }: { icon: string; label: string }) {
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

// ─── Toggle ─────────────────────────────────────────────────────────────────

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

export function Toggle({
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
