// Memory Core plugin module owns memory and session source indexing.
import { createSubsystemLogger } from "openclaw/plugin-sdk/memory-core-host-engine-foundation";
import {
  buildSessionEntry,
  sessionPathForSessionIdentity,
  type SessionTranscriptCorpusEntry,
} from "openclaw/plugin-sdk/memory-core-host-engine-sessions";
import {
  MEMORY_INDEX_FTS_TABLE,
  runWithConcurrency,
} from "openclaw/plugin-sdk/memory-core-host-engine-storage";
import { runSqliteImmediateTransactionSync } from "openclaw/plugin-sdk/sqlite-runtime";
import { withMemoryWorkspaceLock } from "../memory-workspace-lock.js";
import { MemoryIndexIncrementalConflictError } from "./manager-db.js";
import { MemoryManagerSessionSyncOps } from "./manager-session-sync-ops.js";
import {
  isMemorySessionIndexable,
  resolveMemorySessionSyncPlan,
} from "./manager-session-sync-state.js";
import {
  loadMemorySourceFileState,
  resolveMemorySourceFileEntries,
  resolveMemorySourceExistingHash,
  type MemorySourceFileStateRow,
} from "./manager-source-state.js";
import type {
  MemoryIndexEntry,
  MemoryIndexWorkItem,
  MemorySourceSyncPlan,
  MemorySyncProgressState,
} from "./manager-sync-base.js";

const FTS_TABLE = MEMORY_INDEX_FTS_TABLE;
const SESSION_SYNC_YIELD_EVERY = 10;
const SOURCE_WIDE_SESSION_INDEX_FLUSH_FILES = 128;
const log = createSubsystemLogger("memory");

function assertSourceRowStillCurrent(
  live: MemorySourceFileStateRow | undefined,
  expected: MemorySourceFileStateRow,
): void {
  if (!live) {
    return;
  }
  if (live.hash !== expected.hash || live.mtime !== expected.mtime || live.size !== expected.size) {
    throw new MemoryIndexIncrementalConflictError(
      `Memory index source ${expected.path} changed before stale cleanup; retry the incremental sync.`,
    );
  }
}

function createSessionSyncYield(total: number): () => Promise<void> {
  let completed = 0;
  return async () => {
    completed += 1;
    if (completed < total && completed % SESSION_SYNC_YIELD_EVERY === 0) {
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
    }
  };
}

export abstract class MemoryManagerSourceSyncOps extends MemoryManagerSessionSyncOps {
  protected override async syncMemoryFiles(params: {
    needsFullReindex: boolean;
    progress?: MemorySyncProgressState;
    deferIndex?: boolean;
  }): Promise<MemorySourceSyncPlan> {
    const deleteFileByPathAndSource = this.db.prepare(
      `DELETE FROM memory_index_sources WHERE path = ? AND source = ?`,
    );
    const deleteChunksByPathAndSource = this.db.prepare(
      `DELETE FROM memory_index_chunks WHERE path = ? AND source = ?`,
    );
    const deleteFtsRowsByPathAndSource =
      this.fts.enabled && this.fts.available
        ? this.db.prepare(`DELETE FROM ${FTS_TABLE} WHERE path = ? AND source = ?`)
        : null;

    const fileEntries = await resolveMemorySourceFileEntries({
      workspaceDir: this.workspaceDir,
      settings: this.settings,
      concurrency: this.getIndexConcurrency(),
    });
    log.debug("memory sync: indexing memory files", {
      files: fileEntries.length,
      needsFullReindex: params.needsFullReindex,
      batch: this.batch.enabled,
      concurrency: this.getIndexConcurrency(),
    });
    const existingState = loadMemorySourceFileState({
      db: this.db,
      source: "memory",
    });
    const existingRows = existingState.rows;
    const existingHashes = existingState.hashes;
    const activePaths = new Set(fileEntries.map((entry) => entry.path));
    if (params.progress) {
      params.progress.total += fileEntries.length;
      params.progress.report({
        completed: params.progress.completed,
        total: params.progress.total,
        label: this.batch.enabled ? "Indexing memory files (batch)..." : "Indexing memory files…",
      });
    }

    const deleteStaleRows = async () => {
      await withMemoryWorkspaceLock(this.workspaceDir, async () => {
        // Re-resolve the source while excluding other workspace writers. A path
        // recreated after the original scan must never be deleted as stale.
        const latestActivePaths = new Set(
          (
            await resolveMemorySourceFileEntries({
              workspaceDir: this.workspaceDir,
              settings: this.settings,
              concurrency: this.getIndexConcurrency(),
            })
          ).map((entry) => entry.path),
        );
        runSqliteImmediateTransactionSync(this.db, () => {
          const liveState = new Map(
            loadMemorySourceFileState({ db: this.db, source: "memory" }).rows.map((row) => [
              row.path,
              row,
            ]),
          );
          for (const stale of existingRows) {
            if (activePaths.has(stale.path) || latestActivePaths.has(stale.path)) {
              continue;
            }
            const live = liveState.get(stale.path);
            assertSourceRowStillCurrent(live, stale);
            if (!live) {
              continue;
            }
            deleteFileByPathAndSource.run(stale.path, "memory");
            this.deleteVectorRowsForSource(stale.path, "memory");
            deleteChunksByPathAndSource.run(stale.path, "memory");
            deleteFtsRowsByPathAndSource?.run(stale.path, "memory");
          }
        });
      });
    };

    if (this.batch.enabled) {
      const dirtyEntries: MemoryIndexEntry[] = [];
      for (const entry of fileEntries) {
        if (!params.needsFullReindex && existingHashes.get(entry.path) === entry.hash) {
          this.advanceSyncProgress(params.progress);
          continue;
        }
        dirtyEntries.push(entry);
      }
      const indexItems = dirtyEntries.map(
        (entry): MemoryIndexWorkItem => ({ entry, source: "memory" }),
      );
      if (params.deferIndex) {
        return { indexItems, finalize: deleteStaleRows };
      }
      await this.indexQueuedFiles(indexItems, params.progress);
    } else {
      const tasks = fileEntries.map((entry) => async () => {
        if (!params.needsFullReindex && existingHashes.get(entry.path) === entry.hash) {
          this.advanceSyncProgress(params.progress);
          return;
        }
        await this.indexFile(entry, { source: "memory" });
        this.advanceSyncProgress(params.progress);
      });
      await runWithConcurrency(tasks, this.getIndexConcurrency());
    }

    await deleteStaleRows();
    return this.emptySourceSyncPlan();
  }

  protected override async syncArchiveFiles(params: {
    needsFullReindex: boolean;
    targetArchiveFiles?: string[];
    corpusEntries?: readonly SessionTranscriptCorpusEntry[];
    progress?: MemorySyncProgressState;
    deferIndex?: boolean;
    prefixIndexItems?: MemoryIndexWorkItem[];
  }): Promise<MemorySourceSyncPlan> {
    const deleteFileByPathAndSource = this.db.prepare(
      `DELETE FROM memory_index_sources WHERE path = ? AND source = ?`,
    );
    const deleteChunksByPathAndSource = this.db.prepare(
      `DELETE FROM memory_index_chunks WHERE path = ? AND source = ?`,
    );
    const updateUnchangedSessionSourceMetadata = this.db.prepare(
      `UPDATE memory_index_sources
       SET mtime = ?, size = ?
       WHERE path = ? AND source = 'sessions' AND hash = ?`,
    );
    const refreshUnchangedSessionSourceMetadata = (entry: MemoryIndexEntry): boolean => {
      // Hash equality preserves chunks and embeddings; only converge the source
      // fingerprint so restored sessions do not repeat catch-up on every startup.
      return (
        updateUnchangedSessionSourceMetadata.run(entry.mtimeMs, entry.size, entry.path, entry.hash)
          .changes === 1
      );
    };
    const canSkipUnchangedSessionEntry = (
      entry: MemoryIndexEntry,
      absPath: string,
      existingHash: string | undefined,
    ): boolean => {
      if (params.needsFullReindex || existingHash !== entry.hash) {
        return false;
      }
      return !this.sessionsDirtyFiles.has(absPath) || refreshUnchangedSessionSourceMetadata(entry);
    };
    const deleteFtsRowsByPathAndSource =
      this.fts.enabled && this.fts.available
        ? this.db.prepare(`DELETE FROM ${FTS_TABLE} WHERE path = ? AND source = ?`)
        : null;

    const corpusEntries = params.corpusEntries ?? (await this.listSessionCorpusEntries());
    const targetArchiveFiles = params.needsFullReindex
      ? null
      : this.normalizeTargetArchiveFiles(params.targetArchiveFiles, corpusEntries, true);
    const corpusEntryByPath = new Map<string, SessionTranscriptCorpusEntry>(
      corpusEntries.map((entry) => [entry.sessionFile, entry]),
    );
    const corpusEntryForPath = (file: string): SessionTranscriptCorpusEntry => {
      const entry = corpusEntryByPath.get(file);
      if (!entry) {
        throw new Error(`Missing session corpus entry for ${file}`);
      }
      return entry;
    };
    const files = targetArchiveFiles
      ? Array.from(targetArchiveFiles)
      : corpusEntries.map((entry) => entry.sessionFile);
    const sessionPlan = resolveMemorySessionSyncPlan({
      needsFullReindex: params.needsFullReindex,
      files,
      targetSessionFiles: targetArchiveFiles,
      existingRows: targetArchiveFiles
        ? null
        : loadMemorySourceFileState({
            db: this.db,
            source: "sessions",
          }).rows,
      sessionPathForFile: (file) => this.sessionPathForCorpusEntry(corpusEntryForPath(file)),
    });
    const { activePaths, existingRows, existingHashes, indexAll } = sessionPlan;
    log.debug("memory sync: indexing session files", {
      files: files.length,
      indexAll,
      dirtyFiles: this.sessionsDirtyFiles.size,
      targetedFiles: targetArchiveFiles?.size ?? 0,
      batch: this.batch.enabled,
      concurrency: this.getIndexConcurrency(),
    });
    if (params.progress) {
      params.progress.total += files.length;
      params.progress.report({
        completed: params.progress.completed,
        total: params.progress.total,
        label: this.batch.enabled ? "Indexing session files (batch)..." : "Indexing session files…",
      });
    }

    const yieldAfterSessionFile = createSessionSyncYield(files.length);
    const deleteIndexedSessionPath = (memoryPath: string) => {
      deleteFileByPathAndSource.run(memoryPath, "sessions");
      this.deleteVectorRowsForSource(memoryPath, "sessions");
      deleteChunksByPathAndSource.run(memoryPath, "sessions");
      deleteFtsRowsByPathAndSource?.run(memoryPath, "sessions");
    };
    const deleteStaleRows = async () => {
      if (activePaths === null) {
        return;
      }

      await withMemoryWorkspaceLock(this.workspaceDir, async () => {
        const latestActivePaths = new Set(
          (await this.listSessionCorpusEntries()).map((entry) =>
            this.sessionPathForCorpusEntry(entry),
          ),
        );
        const staleRows = existingRows ?? [];
        runSqliteImmediateTransactionSync(this.db, () => {
          const liveState = new Map(
            loadMemorySourceFileState({ db: this.db, source: "sessions" }).rows.map((row) => [
              row.path,
              row,
            ]),
          );
          for (const stale of staleRows) {
            if (activePaths.has(stale.path) || latestActivePaths.has(stale.path)) {
              continue;
            }
            const live = liveState.get(stale.path);
            assertSourceRowStillCurrent(live, stale);
            if (live) {
              deleteIndexedSessionPath(stale.path);
            }
          }
        });
      });
    };
    const deleteTargetArchiveStaleLiveRows = async () => {
      if (!targetArchiveFiles) {
        return;
      }
      await withMemoryWorkspaceLock(this.workspaceDir, async () => {
        // Targeted sync owns one resolved corpus snapshot; do not enumerate a
        // different session store while finalizing that same target.
        const activeCorpusPaths = new Set(
          corpusEntries
            .filter((entry) => entry.artifactKind === "active-session")
            .map((entry) => this.sessionPathForCorpusEntry(entry)),
        );
        runSqliteImmediateTransactionSync(this.db, () => {
          const existingSessionPaths = new Set(
            loadMemorySourceFileState({
              db: this.db,
              source: "sessions",
            }).rows.map((row) => row.path),
          );
          for (const file of targetArchiveFiles) {
            const corpusEntry = corpusEntryForPath(file);
            const staleAgentId = corpusEntry.agentId;
            const staleLivePaths = [
              sessionPathForSessionIdentity(staleAgentId, corpusEntry.sessionId),
              this.legacyExtensionlessSessionPathForIdentity(staleAgentId, corpusEntry.sessionId),
            ];
            for (const staleLivePath of staleLivePaths) {
              if (
                activeCorpusPaths.has(staleLivePath) ||
                !existingSessionPaths.has(staleLivePath)
              ) {
                continue;
              }
              deleteIndexedSessionPath(staleLivePath);
            }
          }
        });
      });
    };
    const resolveSessionIndexEntry = async (absPath: string): Promise<MemoryIndexEntry | null> => {
      if (!indexAll && !this.sessionsDirtyFiles.has(absPath)) {
        this.advanceSyncProgress(params.progress);
        return null;
      }
      const entry = await buildSessionEntry(
        absPath,
        this.buildSessionEntryOptions(corpusEntryForPath(absPath)),
      );
      if (!entry) {
        this.advanceSyncProgress(params.progress);
        return null;
      }
      if (!isMemorySessionIndexable(entry)) {
        // Archived runs may reveal their internal origin only while parsing.
        // Remove earlier index artifacts before excluding that transcript.
        deleteIndexedSessionPath(entry.path);
        this.advanceSyncProgress(params.progress);
        return null;
      }
      const existingHash = resolveMemorySourceExistingHash({
        db: this.db,
        source: "sessions",
        path: entry.path,
        existingHashes,
      });
      if (canSkipUnchangedSessionEntry(entry, absPath, existingHash)) {
        this.advanceSyncProgress(params.progress);
        return null;
      }
      return { ...entry, sessionId: corpusEntryForPath(absPath).sessionId };
    };

    if (params.deferIndex) {
      const pendingIndexItems = [...(params.prefixIndexItems ?? [])];
      const flushPendingIndexItems = async () => {
        if (pendingIndexItems.length === 0) {
          return;
        }
        const current = pendingIndexItems.splice(0);
        const sources = new Set(current.map((item) => item.source));
        await this.indexQueuedFiles(
          current,
          params.progress,
          sources.size > 1 ? "Indexing memory sources (batch)..." : undefined,
        );
      };

      // Session entries carry flattened transcript content; flush bounded groups
      // so source-wide batching cannot retain the whole dirty transcript corpus.
      for (let start = 0; start < files.length; start += SOURCE_WIDE_SESSION_INDEX_FLUSH_FILES) {
        const fileBatch = files.slice(start, start + SOURCE_WIDE_SESSION_INDEX_FLUSH_FILES);
        const dirtyEntries = (
          await runWithConcurrency(
            fileBatch.map((absPath) => async (): Promise<MemoryIndexEntry | null> => {
              try {
                return await resolveSessionIndexEntry(absPath);
              } finally {
                await yieldAfterSessionFile();
              }
            }),
            this.getIndexConcurrency(),
          )
        ).filter((entry): entry is MemoryIndexEntry => entry !== null);
        pendingIndexItems.push(
          ...dirtyEntries.map(
            (entry): MemoryIndexWorkItem => ({
              entry,
              source: "sessions",
            }),
          ),
        );
        if (pendingIndexItems.length >= SOURCE_WIDE_SESSION_INDEX_FLUSH_FILES) {
          await flushPendingIndexItems();
        }
      }

      await flushPendingIndexItems();
      await deleteTargetArchiveStaleLiveRows();
      await deleteStaleRows();
      return this.emptySourceSyncPlan();
    }
    if ((params.prefixIndexItems?.length ?? 0) > 0) {
      throw new Error("Memory session sync prefix requires deferred source-wide indexing.");
    }

    const tasks = files.map((absPath) => async () => {
      try {
        const entry = await resolveSessionIndexEntry(absPath);
        if (!entry) {
          return;
        }
        await this.indexFile(entry, { source: "sessions", content: entry.content });
        this.advanceSyncProgress(params.progress);
      } finally {
        await yieldAfterSessionFile();
      }
    });
    await runWithConcurrency(tasks, this.getIndexConcurrency());

    await deleteTargetArchiveStaleLiveRows();
    await deleteStaleRows();
    return this.emptySourceSyncPlan();
  }
}
