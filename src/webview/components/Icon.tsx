import React, { CSSProperties, ReactElement } from "react";

const visuallyHiddenStyle: CSSProperties = {
  position: "absolute",
  width: 1,
  height: 1,
  padding: 0,
  margin: -1,
  overflow: "hidden",
  clip: "rect(0, 0, 0, 0)",
  whiteSpace: "nowrap",
  border: 0,
};

interface IconProps {
  name: string;
  size?: number;
  color?: string;
  spin?: boolean;
  className?: string;
  title?: string;
  style?: CSSProperties;
}

export function Icon({
  name,
  size = 14,
  color,
  spin,
  className,
  title,
  style,
}: IconProps): ReactElement {
  return (
    <span
      className={`codicon codicon-${name}${className ? ` ${className}` : ""}`}
      title={title}
      role={title ? "img" : undefined}
      aria-hidden={title ? undefined : true}
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
    >
      {title ? <span style={visuallyHiddenStyle}>{title}</span> : null}
    </span>
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
