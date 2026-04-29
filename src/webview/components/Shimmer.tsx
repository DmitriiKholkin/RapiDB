import React, { type CSSProperties } from "react";

const SHIMMER_STYLE_ID = "rapidb-shimmer-keyframe";
if (typeof document !== "undefined" && !document.getElementById(SHIMMER_STYLE_ID)) {
  const s = document.createElement("style");
  s.id = SHIMMER_STYLE_ID;
  s.textContent =
    "@keyframes rapidb-shimmer { 0% { background-position: -600px 0 } 100% { background-position: 600px 0 } }";
  document.head.appendChild(s);
}

export const SHIMMER_ANIMATION = "rapidb-shimmer 1.6s linear infinite";

interface ShimmerBarProps {
  width?: number | string;
  height?: number;
  borderRadius?: number;
  style?: CSSProperties;
}

export function ShimmerBar({
  width = "100%",
  height = 10,
  borderRadius = 3,
  style,
}: ShimmerBarProps): React.ReactElement {
  return (
    <div
      aria-hidden
      style={{
        width,
        height,
        borderRadius,
        background:
          "linear-gradient(90deg, var(--vscode-editor-background) 25%, var(--vscode-list-hoverBackground, rgba(128,128,128,0.07)) 50%, var(--vscode-editor-background) 75%)",
        backgroundSize: "1200px 100%",
        animation: SHIMMER_ANIMATION,
        willChange: "background-position",
        flexShrink: 0,
        ...style,
      }}
    />
  );
}
