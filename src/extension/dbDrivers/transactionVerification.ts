import { throwIfTransactionCancelled } from "./timeout";
import type {
  IDBDriver,
  TransactionContext,
  TransactionOperation,
  TransactionVerification,
} from "./types";

export interface TransactionVerificationFailure {
  rowIndex: number;
  columns: string[];
  message: string;
  mutation?: "insert" | "update";
}

export class TransactionVerificationError extends Error {
  readonly name = "TransactionVerificationError";
  constructor(readonly verificationFailure: TransactionVerificationFailure) {
    super(
      `${verificationFailure.mutation === "insert" ? "INSERT" : "UPDATE"} verification failed; the transaction was not committed. ${verificationFailure.message}`,
    );
  }
}

/** Capture per-operation identities for verification on the active transaction. */
export class TransactionIdentityStore {
  private readonly rows = new Map<number, Record<string, unknown>[]>();

  async capture(
    index: number,
    operation: TransactionOperation,
    returnedRows: Record<string, unknown>[],
    read: (
      sql: string,
      params: unknown[],
    ) => Promise<Record<string, unknown>[]>,
  ): Promise<void> {
    const capture = operation.captureIdentity;
    if (!capture) return;
    this.rows.set(
      index,
      capture.sql
        ? await read(capture.sql, capture.params ?? [])
        : returnedRows,
    );
  }

  resolve(
    verifications: TransactionVerification[] | undefined,
  ): TransactionVerification[] | undefined {
    return verifications?.map((verification) => {
      if (!verification.identity) return verification;
      const { operationIndex, parameterIndexes, allowNull } =
        verification.identity;
      const rows = this.rows.get(operationIndex);
      const params = [...verification.params];
      parameterIndexes.forEach((parameterIndex, index) => {
        const value = rows?.[0]?.[`__col_${index}`];
        if (
          rows?.length !== 1 ||
          (value === null && !allowNull) ||
          value === undefined ||
          (typeof value === "number" &&
            (!Number.isFinite(value) ||
              (Number.isInteger(value) && !Number.isSafeInteger(value))))
        ) {
          throw new TransactionVerificationError({
            rowIndex: verification.rowIndex,
            mutation: verification.mutation,
            columns: verification.values.map(({ column }) => column.name),
            message:
              "The inserted row identity could not be captured reliably.",
          });
        }
        params[parameterIndex] = value;
      });
      return { ...verification, params };
    });
  }
}

/** Verify on the existing transaction connection; rows use positional __col_N keys. */
export async function verifyTransaction(
  driver: Pick<IDBDriver, "checkPersistedEdit">,
  verifications: readonly TransactionVerification[] | undefined,
  read: (
    verification: TransactionVerification,
  ) => Promise<Record<string, unknown>[]>,
  context?: TransactionContext,
): Promise<void> {
  for (const verification of verifications ?? []) {
    throwIfTransactionCancelled(context);
    let rows: Record<string, unknown>[];
    try {
      rows = await read(verification);
    } catch (error) {
      throwIfTransactionCancelled(context);
      throw new TransactionVerificationError({
        rowIndex: verification.rowIndex,
        mutation: verification.mutation,
        columns: verification.values.map(({ column }) => column.name),
        message: `Verification query failed: ${error instanceof Error ? error.message : String(error)}`,
      });
    }
    throwIfTransactionCancelled(context);
    if (rows.length !== 1) {
      throw new TransactionVerificationError({
        rowIndex: verification.rowIndex,
        mutation: verification.mutation,
        columns: verification.values.map(({ column }) => column.name),
        message:
          "The persisted row could not be read back uniquely for verification.",
      });
    }
    const columns: string[] = [];
    const messages: string[] = [];
    verification.values.forEach(({ column, expectedValue }, index) => {
      const check = driver.checkPersistedEdit(column, expectedValue, {
        persistedValue: rows[0][`__col_${index}`],
      });
      if (!check?.ok) {
        columns.push(column.name);
        messages.push(
          check?.message ??
            `${column.name} could not be confirmed against the persisted value.`,
        );
      }
    });
    if (columns.length) {
      throw new TransactionVerificationError({
        rowIndex: verification.rowIndex,
        mutation: verification.mutation,
        columns,
        message: messages.join("; "),
      });
    }
    throwIfTransactionCancelled(context);
  }
}
