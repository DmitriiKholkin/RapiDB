/** PostgreSQL array text grammar. Numeric elements deliberately remain strings. */
export function parsePostgresArrayLiteral(
  input: string,
  parseElement: (token: string) => unknown = (token) => token,
): unknown[] {
  let pos = 0;
  const fail = (): never => {
    throw new Error(`Invalid PostgreSQL array literal at position ${pos}`);
  };
  const isWhitespace = (ch: string): boolean => /[ \t\n\r\f\v]/.test(ch);
  const skipWhitespace = (): void => {
    while (pos < input.length && isWhitespace(input[pos])) pos++;
  };
  const expect = (ch: string): void => {
    if (input[pos] !== ch) fail();
    pos++;
  };
  if (input.includes("\0")) fail();

  const bounds: number[] = [];
  const readBound = (): number => {
    const match = /^[+-]?\d+/.exec(input.slice(pos));
    if (!match) return fail();
    pos += match[0].length;
    const value = Number(match[0]);
    if (!Number.isInteger(value) || value < -2147483648 || value > 2147483647)
      fail();
    return value;
  };
  skipWhitespace();
  while (input[pos] === "[") {
    pos++;
    const lower = readBound();
    expect(":");
    const upper = readBound();
    expect("]");
    const dimension = upper - lower + 1;
    // PostgreSQL ArrayCheckBounds checks lower + dimension, i.e. the exclusive
    // upper bound. Even a single element at INT32_MAX would overflow int32.
    if (
      dimension <= 0 ||
      dimension > 2147483647 ||
      lower + dimension > 2147483647 ||
      bounds.length === 6
    )
      fail();
    bounds.push(dimension);
    skipWhitespace();
  }
  if (bounds.length > 0) expect("=");

  const readElement = (): unknown => {
    let token = "";
    let escaped = false;
    if (input[pos] === '"') {
      pos++;
      while (pos < input.length) {
        const ch = input[pos++];
        if (ch === '"') return parseElement(token);
        if (ch === "\\") {
          if (pos === input.length) fail();
          token += input[pos++];
        } else {
          token += ch;
        }
      }
      return fail();
    }
    // Unescaped trailing whitespace is insignificant; escaped whitespace is data.
    let whitespace = "";
    while (pos < input.length && input[pos] !== "," && input[pos] !== "}") {
      const ch = input[pos++];
      if (ch === "{" || ch === '"') fail();
      if (ch === "\\") {
        if (pos === input.length) fail();
        token += whitespace + input[pos++];
        whitespace = "";
        escaped = true;
      } else if (isWhitespace(ch)) {
        whitespace += ch;
      } else {
        token += whitespace + ch;
        whitespace = "";
      }
    }
    if (token === "") fail();
    return !escaped && token.toUpperCase() === "NULL"
      ? null
      : parseElement(token);
  };

  const readArray = (depth: number): { values: unknown[]; shape: number[] } => {
    if (depth > 6) fail();
    skipWhitespace();
    expect("{");
    const values: unknown[] = [];
    let childShape: number[] | undefined;
    let nested: boolean | undefined;
    skipWhitespace();
    if (input[pos] === "}") {
      pos++;
      if (depth > 1) fail();
      return { values, shape: [0] };
    }
    while (pos < input.length) {
      skipWhitespace();
      const isNested = input[pos] === "{";
      if (nested !== undefined && nested !== isNested) fail();
      nested = isNested;
      if (isNested) {
        const child = readArray(depth + 1);
        if (childShape && childShape.join(",") !== child.shape.join(","))
          fail();
        childShape = child.shape;
        values.push(child.values);
      } else {
        values.push(readElement());
      }
      skipWhitespace();
      if (input[pos] === "}") {
        pos++;
        return { values, shape: [values.length, ...(childShape ?? [])] };
      }
      if (input[pos] !== ",") fail();
      pos++;
    }
    return fail();
  };

  const result = readArray(1);
  skipWhitespace();
  if (pos !== input.length) fail();
  if (bounds.length > 0 && bounds.join(",") !== result.shape.join(",")) fail();
  return result.values;
}

export function parsePostgresBooleanArrayElement(token: string): boolean {
  if (token === "t" || token === "true") return true;
  if (token === "f" || token === "false") return false;
  throw new Error(`Invalid PostgreSQL boolean array element: ${token}`);
}

export function hasPostgresArrayBounds(input: string): boolean {
  return /^\[\s*[+-]?\d+\s*:/.test(input);
}
