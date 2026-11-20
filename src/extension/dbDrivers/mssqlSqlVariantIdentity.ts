const byteTypes = [
  "binary",
  "varbinary",
  "char",
  "varchar",
  "nchar",
  "nvarchar",
];
const scalarTypes = [
  "bigint",
  "bit",
  "date",
  "datetime",
  "datetime2",
  "datetimeoffset",
  "decimal",
  "float",
  "int",
  "money",
  "numeric",
  "real",
  "smalldatetime",
  "smallint",
  "smallmoney",
  "time",
  "tinyint",
  "uniqueidentifier",
];

export function mssqlSqlVariantDisplayExpression(
  name: string,
  datetimeOffsetStyle: 126 | 127 = 127,
): string {
  return `CASE CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(${name}, 'BaseType'))
    WHEN 'binary' THEN CONVERT(nvarchar(max), CONVERT(varbinary(8000), ${name}), 1)
    WHEN 'varbinary' THEN CONVERT(nvarchar(max), CONVERT(varbinary(8000), ${name}), 1)
    WHEN 'float' THEN CONVERT(nvarchar(max), CONVERT(float, ${name}), 3)
    WHEN 'real' THEN CONVERT(nvarchar(max), CONVERT(real, ${name}), 3)
    WHEN 'money' THEN CONVERT(nvarchar(max), CONVERT(decimal(19,4), ${name}))
    WHEN 'smallmoney' THEN CONVERT(nvarchar(max), CONVERT(decimal(10,4), ${name}))
    WHEN 'datetimeoffset' THEN CONVERT(nvarchar(max), CONVERT(datetimeoffset(7), ${name}), ${datetimeOffsetStyle})
    WHEN 'datetime2' THEN CONVERT(nvarchar(max), CONVERT(datetime2(7), ${name}), 126)
    WHEN 'datetime' THEN CONVERT(nvarchar(max), CONVERT(datetime, ${name}), 121)
    WHEN 'smalldatetime' THEN CONVERT(nvarchar(max), CONVERT(smalldatetime, ${name}), 120)
    WHEN 'date' THEN CONVERT(nvarchar(max), CONVERT(date, ${name}), 23)
    WHEN 'time' THEN CONVERT(nvarchar(max), CONVERT(time(7), ${name}))
    ELSE CONVERT(nvarchar(max), ${name}) END`;
}

/** The same server-produced token is used by reads, mutations and INSERT verification. */
export function mssqlSqlVariantPrimaryKeyExpression(name: string): string {
  const fields = [
    "BaseType",
    "Precision",
    "Scale",
    "MaxLength",
    "Collation",
  ].map(
    (property) =>
      `CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(${name}, '${property}'))`,
  );
  const [baseType] = fields;
  const quoted = (types: string[]) =>
    types.map((type) => `'${type}'`).join(", ");
  // Keep the raw text/binary bytes (including padding). Use stable, lossless
  // conversions for scalars rather than depending on their internal binary
  // layouts. Style 126 keeps datetimeoffset's original offset; 127 does not.
  const bytes = `CASE WHEN ${baseType} IN (${quoted(byteTypes)}) THEN CONVERT(varbinary(8000), ${name}) ELSE CONVERT(varbinary(8000), ${mssqlSqlVariantDisplayExpression(name, 126)}) END`;
  // Hex makes length significant even for binary values ending in zero. BIN2
  // compares the token, not the values under a column/database collation.
  return `(CASE WHEN ${baseType} IN (${quoted([...byteTypes, ...scalarTypes])}) THEN CONCAT(N'v1:', ${fields.join(", N':', ")}, N':', CONVERT(varchar(max), ${bytes}, 2)) END COLLATE Latin1_General_100_BIN2)`;
}
