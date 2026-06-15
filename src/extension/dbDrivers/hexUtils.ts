/**
 * Hex/binary utility functions.
 * Extracted from BaseDBDriver for single-responsibility adherence.
 */

/** Convert a Buffer to a 0x-prefixed hex string. Empty buffers produce "". */
export function hexFromBuffer(val: Buffer): string {
  return val.length === 0 ? "" : `0x${val.toString("hex")}`;
}

/**
 * Returns true when `value` looks like a hex-encoded binary string.
 * Accepts 0x, \\x prefixes or a bare even-length hex string.
 */
export function isHexLike(value: string): boolean {
  if (
    value.startsWith("\\x") ||
    value.startsWith("\\X") ||
    value.startsWith("0x") ||
    value.startsWith("0X")
  ) {
    return /^[0-9a-fA-F]*$/.test(value.slice(2));
  }
  return /^[0-9a-fA-F]+$/.test(value) && value.length % 2 === 0;
}

/**
 * Parse a hex-encoded string into a Buffer.
 * Supports 0x and \\x prefixed forms as well as bare hex.
 * Throws on odd digit count or non-hex characters.
 */
export function parseHexToBuffer(value: string): Buffer {
  const stripped =
    value.startsWith("\\x") ||
    value.startsWith("\\X") ||
    value.startsWith("0x") ||
    value.startsWith("0X")
      ? value.slice(2)
      : value;
  if (/^[0-9a-fA-F]*$/.test(stripped)) {
    if (stripped.length % 2 !== 0) {
      throw new Error(
        `Invalid hex value: odd number of hex digits in "${value}". ` +
          "Each byte requires exactly 2 hex digits.",
      );
    }
    return Buffer.from(stripped, "hex");
  }
  throw new Error(`Invalid hex string: "${value}"`);
}
