export function sqlStringLiteral(value: string): string {
  // Escape single quotes for SQL string literals.
  return `'${value.replace(/'/g, "''")}'`;
}

export function sqlIdentifier(value: string): string {
  // Escape double quotes for SQL identifiers.
  return `"${value.replace(/"/g, '""')}"`;
}

/**
 * Convert values returned by DuckDB into JSON-safe values.
 *
 * DuckDB may return `bigint` for integer aggregates. We preserve precision by
 * encoding values outside JS safe-integer range as strings.
 */
export function toJsonValue(value: unknown): unknown {
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    return Number.isSafeInteger(asNumber) ? asNumber : value.toString();
  }
  return value;
}

/**
 * Basic safety validation for types used when creating tables for Parquet
 * export. This intentionally rejects anything containing quotes/semicolons.
 *
 * Allowed examples:
 * - VARCHAR
 * - BIGINT
 * - DOUBLE
 * - BOOLEAN
 * - DECIMAL(18,2)
 */
export function normalizeDuckDbType(raw: string): string {
  const value = raw.trim();
  if (!value) throw new Error("columns[].type must be non-empty");

  // Allow `NAME` or `NAME(p)` or `NAME(p,s)` where p/s are ints.
  const ok =
    /^[A-Za-z][A-Za-z0-9_]*(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?$/.test(value);
  if (!ok) {
    throw new Error(
      `Unsupported/unsafe DuckDB type: "${raw}" (allowed: VARCHAR, BIGINT, DOUBLE, BOOLEAN, DECIMAL(p,s), etc.)`,
    );
  }
  return value;
}

