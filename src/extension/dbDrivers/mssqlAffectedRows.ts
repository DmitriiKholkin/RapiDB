/** Count and rowset classification supplied by the SDK bridge/caller. */
export interface MssqlCompletionCount {
  count: number;
  closesRecordset: boolean;
  command: "dml" | "nonDml" | "unknown";
}

export class MssqlAffectedRows {
  private total = 0;
  private ambiguous = false;
  private unresolvedInProcCount = false;

  complete(
    event: "done" | "doneInProc" | "doneProc",
    pending?: MssqlCompletionCount,
  ): void {
    if (pending && pending.command === "unknown" && pending.count > 0) {
      this.ambiguous = true;
    }
    if (event === "doneProc") {
      // DONEPROC closes *a* procedure, not necessarily the top-level statement.
      // TDS supplies no parent ID or nesting depth. In particular these events
      // cannot distinguish EXEC p; UPDATE from UPDATE with a trigger calling p.
      // Neither committing nor discarding its body count gives a reliable sum.
      this.ambiguous = true;
      this.unresolvedInProcCount = false;
    } else if (event === "doneInProc") {
      if (pending && pending.command === "dml")
        this.unresolvedInProcCount = true;
    } else {
      // CurCmd distinguishes OUTPUT DML from SELECT even when both return a
      // rowset. Count each DML completion once, independently of rowset sizes.
      if (pending && pending.command === "dml") this.total += pending.count;
      // A counted parent supersedes trigger-body counts. With NOCOUNT there
      // is no parent count to substitute; do not promote a child count instead.
      if (pending) this.unresolvedInProcCount = false;
      else if (this.unresolvedInProcCount) this.ambiguous = true;
    }
  }

  get value(): number | undefined {
    return this.ambiguous || this.unresolvedInProcCount
      ? undefined
      : this.total;
  }
}
