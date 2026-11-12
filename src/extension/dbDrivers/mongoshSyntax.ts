import ts from "typescript";

const DB_METHODS = new Set([
  "getSiblingDB",
  "getCollection",
  "runCommand",
  "adminCommand",
  "getCollectionNames",
  "createCollection",
  "createView",
]);
const BSON_CONSTRUCTORS = new Set([
  "Date",
  "ISODate",
  "RegExp",
  "BSONRegExp",
  "ObjectId",
  "BinData",
  "DBRef",
  "BSONSymbol",
  "NumberLong",
  "NumberInt",
  "NumberDecimal",
  "Timestamp",
  "Code",
  "MinKey",
  "MaxKey",
  "Double",
]);
const ARITHMETIC_OPERATORS = new Set([
  ts.SyntaxKind.PlusToken,
  ts.SyntaxKind.MinusToken,
  ts.SyntaxKind.AsteriskToken,
  ts.SyntaxKind.AsteriskAsteriskToken,
  ts.SyntaxKind.SlashToken,
  ts.SyntaxKind.PercentToken,
  ts.SyntaxKind.LessThanLessThanToken,
  ts.SyntaxKind.GreaterThanGreaterThanToken,
  ts.SyntaxKind.GreaterThanGreaterThanGreaterThanToken,
  ts.SyntaxKind.AmpersandToken,
  ts.SyntaxKind.BarToken,
  ts.SyntaxKind.CaretToken,
]);

type HandleKind = "database" | "collection" | "operation";
type Handle = HandleKind | { owner: HandleKind; name: string };

function unsupported(reason: string): never {
  throw new Error(
    `[RapiDB] Unsupported mongosh JavaScript: ${reason}. Only standalone db operations/cursor chains with literal data and BSON constructors are supported. MongoDB operation results and db/collection handles cannot be used as values; variables, conditions, spreads, functions and reflection are not supported. No operations were submitted.`,
  );
}

/**
 * This is a syntax boundary, not a mongosh interpreter or a dataflow heuristic.
 * No operation/namespace expression is admitted in the data grammar, and no
 * binding, arbitrary call, mutation or control flow is admitted in the program.
 * Validate the ENTIRE source before the VM sees any of it, using the same text.
 * TypeScript is deliberately bundled by esbuild, not required from the VSIX's
 * production node_modules (it is a build-time devDependency).
 */
export function assertMongoshSupportedSyntax(text: string): void {
  if (text.length > 1_000_000)
    unsupported("query exceeds 1,000,000 characters");
  const filename = "/rapidb-mongosh.js";
  const source = ts.createSourceFile(
    filename,
    text,
    ts.ScriptTarget.ESNext,
    false,
    ts.ScriptKind.JS,
  );
  // Use public syntactic diagnostics, with an entirely in-memory compiler host.
  // No library, package resolution, filesystem reads, type checking or emit.
  const program = ts.createProgram(
    [filename],
    {
      allowJs: true,
      noLib: true,
      noResolve: true,
      target: ts.ScriptTarget.ESNext,
    },
    {
      getSourceFile: (name) => (name === filename ? source : undefined),
      getDefaultLibFileName: () => "",
      writeFile: () => {},
      getCurrentDirectory: () => "/",
      getCanonicalFileName: (name) => name,
      useCaseSensitiveFileNames: () => true,
      getNewLine: () => "\n",
      fileExists: (name) => name === filename,
      readFile: (name) => (name === filename ? text : undefined),
    },
  );
  if (program.getSyntacticDiagnostics(source).length) {
    unsupported("invalid JavaScript syntax");
  }
  const checkDepth = (depth: number) => {
    if (depth > 128) unsupported("expression nesting exceeds 128 levels");
  };
  const data = (node: ts.Expression, depth = 0): void => {
    checkDepth(depth);
    if (
      ts.isStringLiteral(node) ||
      ts.isNumericLiteral(node) ||
      ts.isBigIntLiteral(node) ||
      ts.isRegularExpressionLiteral(node) ||
      ts.isNoSubstitutionTemplateLiteral(node) ||
      node.kind === ts.SyntaxKind.TrueKeyword ||
      node.kind === ts.SyntaxKind.FalseKeyword ||
      node.kind === ts.SyntaxKind.NullKeyword
    ) {
      return;
    }
    if (ts.isIdentifier(node)) {
      // Non-finite values still receive the existing precise BSON validation.
      if (["undefined", "NaN", "Infinity"].includes(node.text)) return;
      unsupported(`identifier "${node.text}" is not literal data`);
    }
    if (ts.isParenthesizedExpression(node)) {
      data(node.expression, depth + 1);
      return;
    }
    if (ts.isArrayLiteralExpression(node)) {
      for (const element of node.elements) {
        if (!ts.isOmittedExpression(element)) data(element, depth + 1);
      }
      return;
    }
    if (ts.isObjectLiteralExpression(node)) {
      for (const property of node.properties) {
        if (!ts.isPropertyAssignment(property)) {
          unsupported(
            "object properties must be literal key/value assignments",
          );
        }
        const key = property.name;
        if (
          !ts.isIdentifier(key) &&
          !ts.isStringLiteral(key) &&
          !ts.isNumericLiteral(key) &&
          !(
            ts.isComputedPropertyName(key) &&
            (ts.isStringLiteral(key.expression) ||
              ts.isNumericLiteral(key.expression))
          )
        ) {
          unsupported("object keys must be static names");
        }
        data(property.initializer, depth + 1);
      }
      return;
    }
    if (ts.isPrefixUnaryExpression(node)) {
      if (
        [
          ts.SyntaxKind.PlusToken,
          ts.SyntaxKind.MinusToken,
          ts.SyntaxKind.ExclamationToken,
          ts.SyntaxKind.TildeToken,
        ].includes(node.operator)
      ) {
        data(node.operand, depth + 1);
        return;
      }
    }
    if (
      ts.isBinaryExpression(node) &&
      ARITHMETIC_OPERATORS.has(node.operatorToken.kind)
    ) {
      data(node.left, depth + 1);
      data(node.right, depth + 1);
      return;
    }
    if (ts.isTemplateExpression(node)) {
      for (const span of node.templateSpans) data(span.expression, depth + 1);
      return;
    }
    if (ts.isCallExpression(node) || ts.isNewExpression(node)) {
      if (
        !ts.isIdentifier(node.expression) ||
        !BSON_CONSTRUCTORS.has(node.expression.text) ||
        node.typeArguments?.length ||
        (ts.isCallExpression(node) && node.questionDotToken)
      ) {
        unsupported(
          `only BSON constructor calls are data (${ts.isIdentifier(node.expression) ? node.expression.text : ts.SyntaxKind[node.expression.kind]})`,
        );
      }
      for (const argument of node.arguments ?? []) data(argument, depth + 1);
      return;
    }
    unsupported(`${ts.SyntaxKind[node.kind]} is not literal data`);
  };
  const handle = (node: ts.Expression, depth = 0): Handle => {
    checkDepth(depth);
    if (ts.isParenthesizedExpression(node))
      return handle(node.expression, depth + 1);
    if (ts.isIdentifier(node) && node.text === "db") return "database";
    if (
      ts.isPropertyAccessExpression(node) ||
      ts.isElementAccessExpression(node)
    ) {
      if (node.questionDotToken)
        unsupported("optional namespace/method access");
      const owner = handle(node.expression, depth + 1);
      if (typeof owner !== "string")
        unsupported("method references are not values");
      let name: string;
      if (ts.isPropertyAccessExpression(node) && ts.isIdentifier(node.name)) {
        name = node.name.text;
      } else if (
        ts.isElementAccessExpression(node) &&
        ts.isStringLiteral(node.argumentExpression)
      ) {
        name = node.argumentExpression.text;
      } else {
        unsupported("namespace and method names must be static strings");
      }
      if (owner === "database" && !DB_METHODS.has(name)) return "collection";
      return { owner, name };
    }
    if (ts.isCallExpression(node)) {
      if (node.questionDotToken || node.typeArguments?.length) {
        unsupported("optional or typed operation calls");
      }
      const method = handle(node.expression, depth + 1);
      if (typeof method === "string")
        unsupported("expected an operation method");
      for (const argument of node.arguments) data(argument, depth + 1);
      if (method.owner === "database") {
        if (method.name === "getSiblingDB") return "database";
        if (method.name === "getCollection") return "collection";
      }
      return "operation";
    }
    unsupported(`${ts.SyntaxKind[node.kind]} is not a standalone db operation`);
  };
  for (const statement of source.statements) {
    if (ts.isEmptyStatement(statement)) continue;
    if (!ts.isExpressionStatement(statement)) {
      unsupported(
        `${ts.SyntaxKind[statement.kind]} statements are not supported`,
      );
    }
    if (handle(statement.expression) !== "operation") {
      unsupported("each statement must call a db/collection operation");
    }
  }
}
