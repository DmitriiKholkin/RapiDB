/** Expand validated numeric filter text within the backend's DECIMAL limits.
 * Only digit counts/exponents use Number; the significand is never rounded.
 */
export function exactNumericFilterLiteral(
  raw: string,
  columnName: string,
  maxPrecision: number,
  maxScale: number,
): { value: string; scale: number } {
  const match = /^([+-]?)(\d*)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(raw);
  const invalid = () =>
    new Error(
      `[RapiDB Filter] Column ${columnName} expects a number representable as DECIMAL(${maxPrecision}, scale <= ${maxScale}).`,
    );
  if (!match || (!match[2] && !match[3])) throw invalid();
  const significand = `${match[2]}${match[3] ?? ""}`;
  const leading = /^0*/.exec(significand)?.[0].length ?? 0;
  if (leading === significand.length) return { value: "0", scale: 0 };
  let end = significand.length;
  while (significand[end - 1] === "0") end -= 1;
  const digits = significand.slice(leading, end);
  const exponent = Number(match[4] ?? "0");
  const decimalIndex = match[2].length + exponent - leading;
  const scale = Math.max(0, digits.length - decimalIndex);
  // Check before expanding: even underflowing exponents can be arbitrarily big.
  if (
    !Number.isSafeInteger(decimalIndex) ||
    scale > maxScale ||
    Math.max(0, decimalIndex) + scale > maxPrecision
  )
    throw invalid();
  const unsigned =
    decimalIndex <= 0
      ? `0.${"0".repeat(-decimalIndex)}${digits}`
      : decimalIndex >= digits.length
        ? digits + "0".repeat(decimalIndex - digits.length)
        : `${digits.slice(0, decimalIndex)}.${digits.slice(decimalIndex)}`;
  return { value: `${match[1] === "-" ? "-" : ""}${unsigned}`, scale };
}
