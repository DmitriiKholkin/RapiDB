import type { ColumnTypeMeta as ColumnMeta } from "../../../shared/tableTypes";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";

export type StructuredCellLanguage = "json" | "xml" | "plaintext";

export interface StructuredCellDialogValue {
  kind: "json" | "array" | "xml";
  language: StructuredCellLanguage;
  formattedText: string;
}

export interface StructuredCellDialogState {
  rowKind: "persisted" | "draft";
  rowIdx: number | null;
  column: ColumnMeta;
  title: string;
  description: string;
  language: StructuredCellLanguage;
  initialText: string;
  draftText: string;
  originalValue: unknown;
  nullable: boolean;
  readOnly: boolean;
  initialIsNull: boolean;
  isNull: boolean;
}

function resolveStructuredCellKind(
  column: Pick<ColumnMeta, "category" | "nativeType">,
): StructuredCellDialogValue["kind"] | null {
  const normalizedNativeType = column.nativeType.trim().toLowerCase();

  if (column.category === "array" || normalizedNativeType.endsWith("[]")) {
    return "array";
  }

  if (column.category === "json" || normalizedNativeType.includes("json")) {
    return "json";
  }

  if (normalizedNativeType.includes("xml")) {
    return "xml";
  }

  return null;
}

function formatJsonText(text: string): string | null {
  return prettyPrintJsonPreservingRawTokens(text);
}

function formatStructuredJsonValue(value: unknown): string | null {
  if (Array.isArray(value) || (value !== null && typeof value === "object")) {
    return JSON.stringify(value, null, 2);
  }

  return null;
}

const JSON_STRING_RE = /"(?:\\.|[^"\\])*"/y;
const JSON_NUMBER_RE = /-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?/y;
const JSON_KEYWORD_RE = /\b(?:true|false|null)\b/y;

function prettyPrintJsonPreservingRawTokens(input: string): string | null {
  const tokens = tokenizeJsonPreservingRawTokens(input);
  if (tokens === null) {
    return null;
  }
  if (!isValidJsonStructure(tokens)) {
    return null;
  }
  let out = "";
  let indent = 0;
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token === "[" || token === "{") {
      const next = tokens[i + 1];
      if (next === "]" || next === "}") {
        out += token + next;
        i++;
        continue;
      }
      out += `${token}\n${"  ".repeat(indent + 1)}`;
      indent++;
      continue;
    }
    if (token === "]" || token === "}") {
      indent = Math.max(0, indent - 1);
      out += `\n${"  ".repeat(indent)}${token}`;
      continue;
    }
    if (token === ",") {
      out += `,\n${"  ".repeat(indent)}`;
      continue;
    }
    if (token === ":") {
      out += ": ";
      continue;
    }
    out += token;
  }
  return out;
}

function tokenizeJsonPreservingRawTokens(input: string): string[] | null {
  const tokens: string[] = [];
  let pos = 0;
  while (pos < input.length) {
    const ch = input[pos];
    if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      pos++;
      continue;
    }
    if (ch === '"') {
      JSON_STRING_RE.lastIndex = pos;
      const match = JSON_STRING_RE.exec(input);
      if (!match || match.index !== pos) {
        return null;
      }
      tokens.push(match[0]);
      pos += match[0].length;
      continue;
    }
    if (ch === "-" || (ch >= "0" && ch <= "9")) {
      JSON_NUMBER_RE.lastIndex = pos;
      const match = JSON_NUMBER_RE.exec(input);
      if (!match || match.index !== pos) {
        return null;
      }
      tokens.push(match[0]);
      pos += match[0].length;
      continue;
    }
    if (ch === "t" || ch === "f" || ch === "n") {
      JSON_KEYWORD_RE.lastIndex = pos;
      const match = JSON_KEYWORD_RE.exec(input);
      if (!match || match.index !== pos) {
        return null;
      }
      tokens.push(match[0]);
      pos += match[0].length;
      continue;
    }
    if (
      ch === "[" ||
      ch === "]" ||
      ch === "{" ||
      ch === "}" ||
      ch === "," ||
      ch === ":"
    ) {
      tokens.push(ch);
      pos++;
      continue;
    }
    return null;
  }
  return tokens;
}

function isValidJsonStructure(tokens: string[]): boolean {
  let pos = 0;
  const parseValue = (): boolean => {
    if (pos >= tokens.length) {
      return false;
    }
    const token = tokens[pos];
    if (token === "{") {
      pos++;
      if (tokens[pos] === "}") {
        pos++;
        return true;
      }
      while (pos < tokens.length) {
        const key = tokens[pos];
        if (!key?.startsWith('"')) {
          return false;
        }
        pos++;
        if (tokens[pos] !== ":") {
          return false;
        }
        pos++;
        if (!parseValue()) {
          return false;
        }
        if (tokens[pos] === ",") {
          pos++;
          continue;
        }
        if (tokens[pos] === "}") {
          pos++;
          return true;
        }
        return false;
      }
      return false;
    }
    if (token === "[") {
      pos++;
      if (tokens[pos] === "]") {
        pos++;
        return true;
      }
      while (pos < tokens.length) {
        if (!parseValue()) {
          return false;
        }
        if (tokens[pos] === ",") {
          pos++;
          continue;
        }
        if (tokens[pos] === "]") {
          pos++;
          return true;
        }
        return false;
      }
      return false;
    }
    if (
      token === "true" ||
      token === "false" ||
      token === "null" ||
      /^-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(token) ||
      token.startsWith('"')
    ) {
      pos++;
      return true;
    }
    return false;
  };
  if (!parseValue()) {
    return false;
  }
  return pos === tokens.length;
}

function isXmlLikeText(text: string, nativeType: string): boolean {
  const trimmedNativeType = nativeType.trim().toLowerCase();
  const trimmed = text.trim();

  if (!trimmed.startsWith("<") || !trimmed.endsWith(">")) {
    return false;
  }

  if (trimmedNativeType.includes("xml") || trimmed.startsWith("<?xml")) {
    return true;
  }

  return (
    /^<([A-Za-z_][\w:.-]*)(?:\s[^>]*)?>[\s\S]*<\/\1>$/.test(trimmed) ||
    /^<([A-Za-z_][\w:.-]*)(?:\s[^>]*)?\/>$/.test(trimmed)
  );
}

function shouldIncreaseXmlIndent(token: string): boolean {
  if (
    token.startsWith("</") ||
    token.startsWith("<?") ||
    token.startsWith("<!") ||
    token.endsWith("/>") ||
    token.includes("</")
  ) {
    return false;
  }

  return /^<[^/][^>]*>$/.test(token);
}

function formatXmlText(text: string): string {
  const normalized = text.trim().replace(/>\s*</g, ">\n<");
  const tokens = normalized
    .split("\n")
    .map((token) => token.trim())
    .filter((token) => token.length > 0);

  let indentLevel = 0;
  const lines: string[] = [];

  for (const token of tokens) {
    if (token.startsWith("</")) {
      indentLevel = Math.max(0, indentLevel - 1);
    }

    lines.push(`${"  ".repeat(indentLevel)}${token}`);

    if (shouldIncreaseXmlIndent(token)) {
      indentLevel += 1;
    }
  }

  return lines.join("\n");
}

export function serializeStructuredCellDialogDraft(
  draftText: string,
  column: Pick<ColumnMeta, "category" | "nativeType">,
): string {
  const structuredKind = resolveStructuredCellKind(column);
  const trimmed = draftText.trim();

  if (!structuredKind) {
    return trimmed;
  }

  if (structuredKind === "json" || structuredKind === "array") {
    return trimmed;
  }

  if (!trimmed) {
    return "";
  }

  return trimmed.replace(/>\s+</g, "><");
}

export function getStructuredCellDialogValue(
  value: unknown,
  column: Pick<ColumnMeta, "category" | "nativeType">,
): StructuredCellDialogValue | null {
  const structuredKind = resolveStructuredCellKind(column);
  if (!structuredKind) {
    return null;
  }

  if (value === null || value === undefined) {
    return {
      kind: structuredKind,
      language: structuredKind === "xml" ? "xml" : "json",
      formattedText: "",
    };
  }

  if (structuredKind !== "xml") {
    const formattedStructuredValue = formatStructuredJsonValue(value);
    if (formattedStructuredValue !== null) {
      return {
        kind: structuredKind,
        language: "json",
        formattedText: formattedStructuredValue,
      };
    }
  }

  const rawText = formatScalarValueForDisplay(value);
  const trimmed = rawText.trim();

  if (!trimmed) {
    return null;
  }

  if (structuredKind === "json") {
    const formattedJson = formatJsonText(trimmed);
    return {
      kind: "json",
      language: "json",
      formattedText: formattedJson ?? rawText,
    };
  }

  if (structuredKind === "array") {
    const formattedJson = formatJsonText(trimmed);
    return {
      kind: "array",
      language: formattedJson ? "json" : "plaintext",
      formattedText: formattedJson ?? rawText,
    };
  }

  return {
    kind: "xml",
    language: "xml",
    formattedText: isXmlLikeText(trimmed, column.nativeType)
      ? formatXmlText(trimmed)
      : rawText,
  };
}
