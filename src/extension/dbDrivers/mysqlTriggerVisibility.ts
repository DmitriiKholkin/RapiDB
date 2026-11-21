import { MutationNotExecutedError } from "./mutationExecutionState";

/**
 * Thrown before any DML when RapiDB cannot prove from the grants that
 * information_schema.TRIGGERS shows every trigger of the target tables.
 * Nothing was written. The user can explicitly accept the missing proof
 * for these tables and retry; visible triggers still block the write.
 */
export class UnverifiedTriggerVisibilityError extends MutationNotExecutedError {
  readonly code = "RAPIDB_UNVERIFIED_TRIGGER_VISIBILITY";

  constructor(readonly tables: readonly string[]) {
    super(
      `Cannot verify rollback safety for ${tables.join(", ")}: full trigger visibility could not be confirmed from this account's grants (TRIGGER privilege, including active roles). Ask your database administrator to verify TRIGGER privileges for ${tables.length === 1 ? "this table" : "these tables"}, or confirm the write without that check.`,
    );
    this.name = "UnverifiedTriggerVisibilityError";
  }
}

// Drivers wrap it as a "not executed" error; look through the cause chain.
export function findUnverifiedTriggerVisibilityError(
  error: unknown,
): UnverifiedTriggerVisibilityError | undefined {
  const seen = new Set<unknown>();
  let current = error;
  while (current && typeof current === "object" && !seen.has(current)) {
    if (current instanceof UnverifiedTriggerVisibilityError) return current;
    seen.add(current);
    current = (current as { cause?: unknown }).cause;
  }
  return undefined;
}

export function isUnverifiedTriggerVisibilityError(error: unknown): boolean {
  return findUnverifiedTriggerVisibilityError(error) !== undefined;
}

interface ParsedIdentifier {
  text: string;
  quoted: boolean;
}

// Parses one identifier as printed by SHOW GRANTS: `name`, "name" (ANSI
// quotes), a bare name, or *. Returns the rest of the input.
function readIdentifier(
  input: string,
): { identifier: ParsedIdentifier | "*"; rest: string } | undefined {
  if (input.startsWith("*")) return { identifier: "*", rest: input.slice(1) };
  const delimiter = input[0];
  if (delimiter === "`" || delimiter === '"') {
    let text = "";
    for (let index = 1; index < input.length; index += 1) {
      if (input[index] !== delimiter) {
        text += input[index];
        continue;
      }
      if (input[index + 1] === delimiter) {
        text += delimiter;
        index += 1;
        continue;
      }
      return {
        identifier: { text, quoted: true },
        rest: input.slice(index + 1),
      };
    }
    return undefined;
  }
  const bare = /^[A-Za-z0-9_$]+/.exec(input)?.[0];
  return bare
    ? {
        identifier: { text: bare, quoted: false },
        rest: input.slice(bare.length),
      }
    : undefined;
}

// LIKE-style matching used by MySQL/MariaDB database-level grants: unescaped
// '_' and '%' are wildcards, '\' escapes the next character.
function grantPatternMatches(
  pattern: string,
  name: string,
  caseInsensitive: boolean,
): boolean {
  const fold = (value: string) =>
    caseInsensitive ? value.toLowerCase() : value;
  let source = "";
  for (let index = 0; index < pattern.length; index += 1) {
    const char = pattern[index];
    if (char === "\\" && index + 1 < pattern.length) {
      index += 1;
      source += fold(pattern[index]).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    } else if (char === "_") {
      source += "[\\s\\S]";
    } else if (char === "%") {
      source += "[\\s\\S]*";
    } else {
      source += fold(char).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    }
  }
  return new RegExp(`^${source}$`, "u").test(fold(name));
}

// With MySQL 8 partial_revokes enabled, wildcards in schema grants are
// literal characters (escapes are still removed).
function grantPatternLiteral(pattern: string): string {
  return pattern.replace(/\\([\s\S])/g, "$1");
}

/**
 * Recognize positive TRIGGER grants (global, table, or database level).
 * The rows can include the expansion of the active roles (SHOW GRANTS ...
 * USING, or the grants of a MariaDB role); role membership lines themselves
 * carry no privileges and are ignored. An unrecognized grant is not evidence
 * of visibility. SHOW GRANTS can include partial revokes in MySQL; don't
 * infer effective grants from a listing with restrictions.
 *
 * Database-level grants: the first matching row of an account applies, so
 * every database-level grant that may match (as a LIKE pattern or, with
 * partial_revokes, as a literal; case-insensitively, to cover
 * lower_case_table_names) must include TRIGGER, and at least one must name
 * the database under both interpretations (e.g. `sales_eu` or `my\_app`).
 */
export function mysqlGrantsShowTriggerAccess(
  grants: unknown,
  database: string,
  table: string,
): boolean {
  if (!Array.isArray(grants) || grants.length === 0) return false;
  const statements: string[] = [];
  for (const row of grants) {
    if (!Array.isArray(row) || row.length !== 1 || typeof row[0] !== "string") {
      return false;
    }
    statements.push(row[0]);
  }
  if (statements.some((statement) => statement.startsWith("REVOKE "))) {
    return false;
  }
  // Both servers list the static global grant (including USAGE) before any
  // dynamic global grants. Only the static TRIGGER privilege is relevant.
  let sawGlobalGrant = false;
  let tableGrant = false;
  let globalGrant = false;
  const databaseGrants: Array<{ pattern: string; trigger: boolean }> = [];
  for (const statement of statements) {
    const match =
      /^GRANT ([A-Z_]+(?: [A-Z_]+)*(?:, [A-Z_]+(?: [A-Z_]+)*)*) ON /.exec(
        statement,
      );
    if (!match) continue;
    const first = readIdentifier(statement.slice(match[0].length));
    if (!first?.rest.startsWith(".")) continue;
    const second = readIdentifier(first.rest.slice(1));
    if (!second?.rest.startsWith(" TO ")) continue;
    const privileges = match[1].split(", ");
    const trigger =
      privileges.includes("TRIGGER") || match[1] === "ALL PRIVILEGES";
    if (first.identifier === "*") {
      if (second.identifier !== "*") continue;
      if (sawGlobalGrant) continue;
      sawGlobalGrant = true;
      globalGrant ||= trigger;
      continue;
    }
    if (second.identifier === "*") {
      databaseGrants.push({ pattern: first.identifier.text, trigger });
      continue;
    }
    // Table-level grants name the database literally.
    if (
      trigger &&
      first.identifier.text === database &&
      second.identifier.text === table
    ) {
      tableGrant = true;
    }
  }
  if (globalGrant || tableGrant) return true;
  const mayApply = databaseGrants.filter(
    ({ pattern }) =>
      grantPatternMatches(pattern, database, true) ||
      grantPatternLiteral(pattern).toLowerCase() === database.toLowerCase(),
  );
  return (
    mayApply.length > 0 &&
    mayApply.every(({ trigger }) => trigger) &&
    mayApply.some(
      ({ pattern }) =>
        grantPatternLiteral(pattern) === database &&
        grantPatternMatches(pattern, database, false),
    )
  );
}

/**
 * Parses CURRENT_ROLE() output: MySQL returns NONE or a comma-separated list
 * of `name`@`host`; MariaDB returns a bare role name or NULL. Returns quoted
 * role specifications safe to splice into SHOW GRANTS, or undefined if the
 * value cannot be parsed (fail closed: roles are then not expanded).
 */
export function parseMysqlCurrentRoles(
  value: unknown,
  mariaDb: boolean,
): string[] | undefined {
  if (value === null || value === undefined) return [];
  const text = Buffer.isBuffer(value) ? value.toString("utf8") : value;
  if (typeof text !== "string") return undefined;
  const quote = (name: string) => `\`${name.replaceAll("`", "``")}\``;
  if (mariaDb) return text === "" ? [] : [quote(text)];
  if (text === "NONE" || text === "") return [];
  const roles: string[] = [];
  let rest = text;
  while (rest.length > 0) {
    const name = readIdentifier(rest);
    if (!name || name.identifier === "*" || !name.rest.startsWith("@")) {
      return undefined;
    }
    const host = readIdentifier(name.rest.slice(1));
    if (!host || host.identifier === "*") return undefined;
    roles.push(`${quote(name.identifier.text)}@${quote(host.identifier.text)}`);
    rest = host.rest;
    if (rest.startsWith(",")) rest = rest.slice(1);
    else if (rest.length > 0) return undefined;
  }
  return roles;
}

/** Roles granted to a role (MariaDB lists them as GRANT `role` TO `grantee`). */
export function mariaDbGrantedRoles(grants: unknown): string[] {
  if (!Array.isArray(grants)) return [];
  const roles: string[] = [];
  for (const row of grants) {
    const statement = Array.isArray(row) ? row[0] : undefined;
    if (typeof statement !== "string" || !statement.startsWith("GRANT ")) {
      continue;
    }
    const role = readIdentifier(statement.slice("GRANT ".length));
    if (
      role &&
      role.identifier !== "*" &&
      role.identifier.quoted &&
      role.rest.startsWith(" TO ")
    ) {
      roles.push(`\`${role.identifier.text.replaceAll("`", "``")}\``);
    }
  }
  return roles;
}
