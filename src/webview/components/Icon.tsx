// biome-ignore lint/style/useImportType: <explanation>
import React from "react";

interface IconProps {
  name: string;
  size?: number;
  color?: string;
  spin?: boolean;
  className?: string;
  title?: string;
  style?: React.CSSProperties;
}

export function Icon({
  name,
  size = 14,
  color,
  spin,
  className,
  title,
  style,
}: IconProps) {
  return (
    <i
      className={`codicon codicon-${name}${className ? ` ${className}` : ""}`}
      title={title}
      aria-hidden={!title}
      aria-label={title}
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        verticalAlign: "bottom",
        fontSize: size,
        width: size,
        height: size,
        lineHeight: 1,
        color,
        animation: spin ? "rapidb-spin 1s linear infinite" : undefined,
        flexShrink: 0,
        ...style,
      }}
    />
  );
}

const SPIN_STYLE_ID = "rapidb-spin-keyframe";
if (
  typeof document !== "undefined" &&
  !document.getElementById(SPIN_STYLE_ID)
) {
  const s = document.createElement("style");
  s.id = SPIN_STYLE_ID;
  s.textContent =
    "@keyframes rapidb-spin { from { transform: rotate(0deg) } to { transform: rotate(360deg) } }";
  document.head.appendChild(s);
}
