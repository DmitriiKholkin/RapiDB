export type MssqlCompletionEvent = "done" | "doneInProc" | "doneProc";

/** Raw TDS metadata, available only while the matching native event is emitted.
 * Missing metadata means unknown command, never proof of DML. */
export interface MssqlStatementCompletionMetadata {
  curCmd: number;
  more: boolean;
  rowCount?: number;
}

export function isMssqlDmlCompletion(
  metadata: MssqlStatementCompletionMetadata | undefined,
): boolean {
  // Microsoft SqlClient Parser/TdsEnums.cs: INSERT, DELETE, UPDATE, BULKINSERT,
  // MERGE. https://github.com/dotnet/SqlClient/blob/main/src/Microsoft.Data.SqlClient/src/Microsoft/Data/SqlClient/Parser/TdsEnums.cs
  // CurCmd is server-specific; do not guess unknown values or parse SQL text.
  return [0xc3, 0xc4, 0xc5, 0xf0, 0x117].includes(metadata?.curCmd ?? -1);
}

export function isMssqlNonDmlCompletion(
  metadata: MssqlStatementCompletionMetadata | undefined,
): boolean {
  // SELECT, ABORT, BEGINXACT, ENDXACT, OPENCURSOR (same SqlClient constants).
  return [0xc1, 0xd2, 0xd4, 0xd5, 0x20].includes(metadata?.curCmd ?? -1);
}

interface TokenHandler {
  request?: object;
  onDone?: (token: unknown) => unknown;
  onDoneInProc?: (token: unknown) => unknown;
  onDoneProc?: (token: unknown) => unknown;
}
interface ParserConnection {
  createTokenStreamParser?: (
    message: unknown,
    handler: TokenHandler,
  ) => unknown;
}

/** Per-connection/per-request bridge for tedious 20: its parser retains CurCmd
 * but RequestTokenHandler drops it before request.emit('done*'). No prototypes
 * or dependency files are patched. An incompatible SDK yields no metadata. */
export function bridgeMssqlCompletionMetadata(connection: object | undefined) {
  const natives = new WeakSet<object>();
  const active = new WeakMap<
    object,
    { event: MssqlCompletionEvent; metadata: MssqlStatementCompletionMetadata }
  >();
  const restorers: Array<() => void> = [];
  const seen = new WeakSet<object>();
  const protocol = connection as ParserConnection | undefined;
  const original = protocol?.createTokenStreamParser;
  if (protocol && typeof original === "function") {
    const hadOwn = Object.hasOwn(protocol, "createTokenStreamParser");
    const wrapped = function (
      this: ParserConnection,
      message: unknown,
      handler: TokenHandler,
    ) {
      if (
        handler?.request &&
        natives.has(handler.request) &&
        !seen.has(handler)
      ) {
        seen.add(handler);
        const native = handler.request;
        for (const [method, event] of [
          ["onDone", "done"],
          ["onDoneInProc", "doneInProc"],
          ["onDoneProc", "doneProc"],
        ] as const) {
          const originalHandler = handler[method];
          if (typeof originalHandler !== "function") continue;
          const hadOwnHandler = Object.hasOwn(handler, method);
          const wrappedHandler = function (this: TokenHandler, token: unknown) {
            const data =
              token as Partial<MssqlStatementCompletionMetadata> | null;
            if (
              data &&
              Number.isInteger(data.curCmd) &&
              (data.curCmd as number) >= 0 &&
              (data.curCmd as number) <= 0xffff &&
              typeof data.more === "boolean" &&
              (data.rowCount === undefined ||
                (Number.isSafeInteger(data.rowCount) && data.rowCount >= 0))
            ) {
              active.set(native, {
                event,
                metadata: {
                  curCmd: data.curCmd as number,
                  more: data.more,
                  rowCount: data.rowCount,
                },
              });
            }
            try {
              return originalHandler.call(this, token);
            } finally {
              active.delete(native);
            }
          };
          handler[method] = wrappedHandler;
          restorers.push(() => {
            if (handler[method] !== wrappedHandler) return;
            if (hadOwnHandler) handler[method] = originalHandler;
            else Reflect.deleteProperty(handler, method);
          });
        }
      }
      return original.call(this, message, handler);
    };
    protocol.createTokenStreamParser = wrapped;
    restorers.push(() => {
      if (protocol.createTokenStreamParser !== wrapped) return;
      if (hadOwn) protocol.createTokenStreamParser = original;
      else Reflect.deleteProperty(protocol, "createTokenStreamParser");
    });
  }
  return {
    register: (native: object) => natives.add(native),
    read: (native: object, event: MssqlCompletionEvent, args: unknown[]) => {
      const current = active.get(native);
      return current?.event === event &&
        current.metadata.rowCount === args[0] &&
        current.metadata.more === args[1]
        ? current.metadata
        : undefined;
    },
    dispose: () => {
      for (const restore of restorers.splice(0)) restore();
    },
  };
}
