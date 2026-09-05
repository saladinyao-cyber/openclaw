import { DatabaseSync } from "node:sqlite";
import { runSqliteImmediateTransactionSync } from "openclaw/plugin-sdk/sqlite-runtime";
import {
  assertMemoryIndexIncrementalCommitCurrent,
  readMemoryIndexGenerationSnapshot,
} from "./manager-db.js";

const [dbPath, nextHash] = process.argv.slice(2);
if (!dbPath || !nextHash || !process.send) {
  throw new Error("manager incremental CAS child requires a database path, hash, and IPC");
}

const db = new DatabaseSync(dbPath);
db.exec("PRAGMA busy_timeout = 5000");
const generationAtPrepare = readMemoryIndexGenerationSnapshot(db);
const row = db
  .prepare("SELECT hash FROM memory_index_sources WHERE path = 'MEMORY.md' AND source = 'memory'")
  // SAFETY: the parent fixture inserts this exact required row before spawning both children.
  .get() as { hash: string };
process.send({ type: "ready" });

process.once("message", () => {
  try {
    runSqliteImmediateTransactionSync(db, () => {
      assertMemoryIndexIncrementalCommitCurrent({
        db,
        path: "MEMORY.md",
        source: "memory",
        revisionAtPrepare: generationAtPrepare.revision,
        identityAtPrepare: generationAtPrepare.identity,
        sourceHashAtPrepare: row.hash,
      });
      db.prepare(
        "UPDATE memory_index_sources SET hash = ? WHERE path = 'MEMORY.md' AND source = 'memory'",
      ).run(nextHash);
    });
    process.send?.({ type: "done", outcome: "committed" });
  } catch (error) {
    process.send?.({
      type: "done",
      outcome: "conflict",
      code: error && typeof error === "object" && "code" in error ? error.code : undefined,
    });
  } finally {
    db.close();
  }
});
