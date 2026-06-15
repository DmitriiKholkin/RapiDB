/**
 * ERD cardinality rendering — SVG glyphs for relationship endpoints.
 */

import { Position } from "@xyflow/react";
import React from "react";
import type { ErdGraph } from "../../../shared/webviewContracts";

// ─── Types ──────────────────────────────────────────────────────────────────

export type CardinalityEnd =
  | "one"
  | "many"
  | "oneOnly"
  | "zeroOrOne"
  | "oneOrMany"
  | "zeroOrMany";

// ─── Cardinality Mapping ────────────────────────────────────────────────────

export function mapCardinalityEnds(
  cardinality: ErdGraph["edges"][number]["cardinality"] | undefined,
  sourceNullable: boolean | undefined,
): { start: CardinalityEnd; end: CardinalityEnd } {
  if (cardinality === "one-to-one") {
    return {
      start: sourceNullable ? "zeroOrOne" : "oneOnly",
      end: "zeroOrOne",
    };
  }
  if (cardinality === "many-to-one") {
    return {
      start: sourceNullable ? "zeroOrMany" : "oneOrMany",
      end: sourceNullable ? "zeroOrOne" : "oneOnly",
    };
  }
  return { start: "many", end: "one" };
}

// ─── Vector Helpers ─────────────────────────────────────────────────────────

export function outwardVector(position: Position): {
  x: number;
  y: number;
  angle: number;
} {
  if (position === Position.Right) return { x: 0, y: 0, angle: 180 };
  if (position === Position.Top) return { x: 0, y: 0, angle: 90 };
  if (position === Position.Bottom) return { x: 0, y: 0, angle: -90 };
  return { x: 0, y: 0, angle: 0 };
}

// ─── Cardinality Glyph Component ────────────────────────────────────────────

interface CardinalityGlyphProps {
  x: number;
  y: number;
  angle: number;
  kind: CardinalityEnd;
  color: string;
  strokeWidth: number;
}

/**
 * Renders the cardinality symbol (one, many, optional, etc.) at an edge endpoint.
 * Each kind maps to a specific SVG path pattern.
 */
export function CardinalityGlyph({
  x,
  y,
  angle,
  kind,
  color,
  strokeWidth,
}: CardinalityGlyphProps): React.JSX.Element {
  return (
    <g
      transform={`translate(${x} ${y}) rotate(${angle})`}
      style={{ pointerEvents: "none" }}
    >
      {renderGlyph(kind, color, strokeWidth)}
    </g>
  );
}

// ─── Glyph Renderers ────────────────────────────────────────────────────────

function renderGlyph(
  kind: CardinalityEnd,
  color: string,
  strokeWidth: number,
): React.JSX.Element | null {
  switch (kind) {
    case "one":
      return <OneLine color={color} strokeWidth={strokeWidth} />;

    case "many":
      return <CrowsFoot color={color} strokeWidth={strokeWidth} />;

    case "oneOnly":
      return (
        <>
          <MandatoryBar color={color} strokeWidth={strokeWidth} />
          <OneLine color={color} strokeWidth={strokeWidth} />
        </>
      );

    case "zeroOrOne":
      return (
        <>
          <OptionalCircle color={color} strokeWidth={strokeWidth} />
          <OneLine color={color} strokeWidth={strokeWidth} />
        </>
      );

    case "oneOrMany":
      return (
        <>
          <MandatoryBar color={color} strokeWidth={strokeWidth} />
          <CrowsFoot color={color} strokeWidth={strokeWidth} />
        </>
      );

    case "zeroOrMany":
      return (
        <>
          <OptionalCircle color={color} strokeWidth={strokeWidth} />
          <CrowsFoot color={color} strokeWidth={strokeWidth} />
        </>
      );

    default:
      return null;
  }
}

// ─── Primitive Glyph Shapes ─────────────────────────────────────────────────

function OneLine({
  color,
  strokeWidth,
}: {
  color: string;
  strokeWidth: number;
}) {
  return (
    <path
      d="M -6 -7 L -6 7"
      stroke={color}
      strokeWidth={strokeWidth}
      fill="none"
    />
  );
}

function CrowsFoot({
  color,
  strokeWidth,
}: {
  color: string;
  strokeWidth: number;
}) {
  return (
    <>
      <path
        d="M -12 0 L 0 -5"
        stroke={color}
        strokeWidth={strokeWidth}
        fill="none"
      />
      <path
        d="M -12 0 L 0 0"
        stroke={color}
        strokeWidth={strokeWidth}
        fill="none"
      />
      <path
        d="M -12 0 L 0 5"
        stroke={color}
        strokeWidth={strokeWidth}
        fill="none"
      />
    </>
  );
}

function MandatoryBar({
  color,
  strokeWidth,
}: {
  color: string;
  strokeWidth: number;
}) {
  return (
    <path
      d="M -14 -7 L -14 7"
      stroke={color}
      strokeWidth={strokeWidth}
      fill="none"
    />
  );
}

function OptionalCircle({
  color,
  strokeWidth,
}: {
  color: string;
  strokeWidth: number;
}) {
  return (
    <circle
      cx={-16}
      cy={0}
      r={5}
      stroke={color}
      strokeWidth={strokeWidth}
      fill="var(--vscode-editor-background)"
    />
  );
}
