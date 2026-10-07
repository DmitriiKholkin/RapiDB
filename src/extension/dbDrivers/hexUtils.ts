/**
 * Hex/binary utility functions.
 * Extracted from BaseDBDriver for single-responsibility adherence.
 */

/** Convert a Buffer to a 0x-prefixed hex string, including empty buffers. */
export function hexFromBuffer(val: Buffer): string {
  return `0x${val.toString("hex")}`;
}

/**
 * Returns true when `value` looks like a hex-encoded binary string.
 * Requires complete byte pairs, with optional 0x or \\x prefixes.
 * A prefix alone is accepted as an empty buffer.
 */
export function isHexLike(value: string): boolean {
  if (
    value.startsWith("\\x") ||
    value.startsWith("\\X") ||
    value.startsWith("0x") ||
    value.startsWith("0X")
  ) {
    const digits = value.slice(2);
    return digits.length % 2 === 0 && /^[0-9a-fA-F]*$/.test(digits);
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
