export function hexFromBuffer(val: Buffer): string {
  return `0x${val.toString("hex")}`;
}

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
