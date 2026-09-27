import {
  CommandId,
  isImportedAgentSessionMessageId,
  MessageId,
  type AgentSessionImportSource,
  type OrchestrationThread,
  type ProjectId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import {
  createAgentSessionTranscriptRecordReader,
  decodeAgentSessionTranscriptRecord,
  parseAgentSessionRecords,
  type AgentSessionTranscriptRecord,
} from "./AgentSessionScanner.ts";

const FOLLOW_INTERVAL = Duration.seconds(10);
const READ_CHUNK_BYTES = 64 * 1024;
const MAX_FOLLOWER_HISTORY_BYTES = 32 * 1024 * 1024;
const MAX_FOLLOWER_RECORDS = 100_000;
const MAX_IMPORTED_MESSAGES = 200;
const TRANSCRIPT_MESSAGE_ID_WIDTH = 16;
const warnedUnprovenReplacementThreads = new Set<string>();

type ImportedTranscript = {
  readonly threadId: OrchestrationThread["id"];
  readonly source: AgentSessionImportSource;
};

function transcriptIdentity(filePath: string, stats: FileSystem.File.Info) {
  return {
    filePath,
    size: Number(stats.size),
    mtimeMs: Option.match(stats.mtime, {
      onNone: () => null,
      onSome: (date) => date.getTime(),
    }),
    device: stats.dev,
    inode: Option.getOrNull(stats.ino),
    birthtimeMs: Option.match(stats.birthtime, {
      onNone: () => null,
      onSome: (date) => date.getTime(),
    }),
  };
}

function sameTranscriptIdentity(
  left: ReturnType<typeof transcriptIdentity>,
  right: ReturnType<typeof transcriptIdentity>,
): boolean {
  return (
    left.filePath === right.filePath &&
    left.size === right.size &&
    left.mtimeMs === right.mtimeMs &&
    left.device === right.device &&
    left.inode === right.inode &&
    left.birthtimeMs === right.birthtimeMs
  );
}

function canReadAfterCursor(
  previous: AgentSessionImportSource,
  current: ReturnType<typeof transcriptIdentity>,
): boolean {
  return (
    previous.filePath === current.filePath &&
    current.size >= previous.size &&
    (current.size > previous.size || current.mtimeMs === previous.mtimeMs) &&
    previous.device === current.device &&
    (previous.inode === null || current.inode === null || previous.inode === current.inode) &&
    (previous.birthtimeMs === null ||
      current.birthtimeMs === null ||
      previous.birthtimeMs === current.birthtimeMs)
  );
}

function hasOnlyImportedMessages(thread: OrchestrationThread): boolean {
  return thread.messages.every((message) => isImportedAgentSessionMessageId(message.id));
}

function isDifferentFile(
  previous: AgentSessionImportSource,
  current: ReturnType<typeof transcriptIdentity>,
): boolean {
  return (
    previous.device !== current.device ||
    (previous.inode !== null && current.inode !== null && previous.inode !== current.inode) ||
    (previous.birthtimeMs !== null &&
      current.birthtimeMs !== null &&
      previous.birthtimeMs !== current.birthtimeMs)
  );
}

function replacementHistoryMatches(
  thread: OrchestrationThread,
  parsed: NonNullable<ReturnType<typeof parseAgentSessionRecords>>,
  boundary: number,
  completeRecordEndOffsets: ReadonlyArray<number>,
): boolean {
  if (!completeRecordEndOffsets.includes(boundary)) return false;
  const prefix = parsed.thread.messages.filter((_, index) => {
    const offset = parsed.messageOffsets[index];
    return offset !== undefined && offset < boundary;
  });
  const retainedPrefix =
    prefix.length > MAX_IMPORTED_MESSAGES
      ? [prefix[0]!, ...prefix.slice(-(MAX_IMPORTED_MESSAGES - 1))]
      : prefix;
  return (
    retainedPrefix.length === thread.messages.length &&
    retainedPrefix.every(
      (message, index) =>
        message.role === thread.messages[index]?.role &&
        message.text === thread.messages[index]?.text,
    )
  );
}

function warnUnprovenReplacementOnce(threadId: string, filePath: string) {
  if (warnedUnprovenReplacementThreads.has(threadId)) return Effect.void;
  warnedUnprovenReplacementThreads.add(threadId);
  return Effect.logWarning(
    "Skipping a changed imported transcript because its saved history boundary could not be verified",
    { threadId, filePath },
  );
}

const readTranscriptAfter = Effect.fn("AgentSessionTranscriptFollower.readAfterCursor")(function* (
  filePath: string,
  expected: ReturnType<typeof transcriptIdentity>,
  startByteOffset: number,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* Effect.scoped(
    fileSystem.open(filePath, { flag: "r" }).pipe(
      Effect.flatMap((file) =>
        Effect.gen(function* () {
          if (!sameTranscriptIdentity(expected, transcriptIdentity(filePath, yield* file.stat))) {
            return null;
          }
          yield* file.seek(BigInt(startByteOffset), "start");

          const records: Array<AgentSessionTranscriptRecord> = [];
          const recordOffsets: Array<number> = [];
          const completeRecordEndOffsets: Array<number> = [];
          let historyBytes = 0;
          let bytesRead = startByteOffset;
          let recordStartOffset = startByteOffset;
          let recordBytes = 0;
          let recordCount = 0;
          let recordStarted = false;
          let discardRecord = false;
          let historyLimitReached = false;
          let recordLimitReached = false;
          const newReader = () =>
            createAgentSessionTranscriptRecordReader((bytes) => {
              recordBytes += bytes;
              if (historyBytes + recordBytes > MAX_FOLLOWER_HISTORY_BYTES) {
                historyLimitReached = true;
                throw new Error("Transcript history exceeds the follower memory limit");
              }
            });
          let reader = newReader();
          let decoder = new TextDecoder();
          let lastCompleteByteOffset = startByteOffset;

          const resetRecord = () => {
            recordBytes = 0;
            recordStarted = false;
            discardRecord = false;
            reader = newReader();
            decoder = new TextDecoder();
          };

          const finishRecord = (recordEndOffset: number) => {
            recordCount += 1;
            if (!discardRecord) {
              try {
                reader.write(decoder.decode());
                const decoded = decodeAgentSessionTranscriptRecord(reader.finish());
                if (Option.isSome(decoded)) {
                  records.push(decoded.value);
                  recordOffsets.push(recordStartOffset);
                  historyBytes += recordBytes;
                }
              } catch {
                discardRecord = true;
              }
            }
            if (historyLimitReached) return true;
            lastCompleteByteOffset = recordEndOffset;
            completeRecordEndOffsets.push(recordEndOffset);
            resetRecord();
            recordLimitReached = recordCount >= MAX_FOLLOWER_RECORDS;
            return true;
          };

          while (bytesRead < expected.size && !recordLimitReached) {
            const next = yield* file.readAlloc(
              Math.min(READ_CHUNK_BYTES, expected.size - bytesRead),
            );
            if (Option.isNone(next)) {
              return null;
            }
            const chunkOffset = bytesRead;
            bytesRead += next.value.byteLength;
            let start = 0;
            while (start < next.value.byteLength && !historyLimitReached && !recordLimitReached) {
              const newline = next.value.indexOf(10, start);
              const end = newline === -1 ? next.value.byteLength : newline;
              recordStarted = true;
              if (!discardRecord) {
                const accepted = yield* Effect.try({
                  try: () => {
                    reader.write(decoder.decode(next.value.subarray(start, end), { stream: true }));
                    return true;
                  },
                  catch: () => false,
                });
                if (!accepted) discardRecord = true;
              }
              if (newline === -1) break;
              const recordEndOffset = chunkOffset + newline + 1;
              if (!finishRecord(recordEndOffset)) return null;
              if (historyLimitReached || recordLimitReached) break;
              start = newline + 1;
              recordStartOffset = recordEndOffset;
            }
          }

          if (!sameTranscriptIdentity(expected, transcriptIdentity(filePath, yield* file.stat))) {
            return null;
          }

          if (recordStarted && !historyLimitReached && !recordLimitReached) {
            if (!discardRecord) {
              const decoded = yield* Effect.try({
                try: () => {
                  reader.write(decoder.decode());
                  const value = reader.finish();
                  return value === undefined
                    ? undefined
                    : decodeAgentSessionTranscriptRecord(value);
                },
                catch: () => {
                  discardRecord = true;
                  return undefined;
                },
              });
              if (!discardRecord && decoded !== undefined) {
                if (Option.isSome(decoded)) {
                  records.push(decoded.value);
                  recordOffsets.push(recordStartOffset);
                }
                historyBytes += recordBytes;
                lastCompleteByteOffset = expected.size;
                completeRecordEndOffsets.push(expected.size);
              }
            }
          }
          return { records, recordOffsets, completeRecordEndOffsets, lastCompleteByteOffset };
        }),
      ),
    ),
  ).pipe(Effect.orElseSucceed(() => null));
});

const followRecordedTranscript = Effect.fn(
  "AgentSessionTranscriptFollower.followRecordedTranscript",
)(function* (projectId: ProjectId, imported: ImportedTranscript) {
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const source = imported.source;
  const statExit = yield* fileSystem.stat(source.filePath).pipe(Effect.exit);
  if (Exit.isFailure(statExit) || statExit.value.type !== "File") return;
  const currentIdentity = transcriptIdentity(source.filePath, statExit.value);
  const savedCursor = source.lastCompleteByteOffset ?? source.size;
  if (
    !Number.isSafeInteger(currentIdentity.size) ||
    currentIdentity.size < 0 ||
    (sameTranscriptIdentity(source, currentIdentity) && savedCursor >= currentIdentity.size)
  ) {
    return;
  }

  const threadOption = yield* snapshots.getThreadDetailById(imported.threadId);
  if (Option.isNone(threadOption)) return;
  const thread = threadOption.value;
  if (
    thread.projectId !== projectId ||
    thread.latestTurn !== null ||
    thread.session !== null ||
    !hasOnlyImportedMessages(thread)
  ) {
    return;
  }

  const readingAppend = canReadAfterCursor(source, currentIdentity);
  const cursor = readingAppend
    ? Math.min(source.lastCompleteByteOffset ?? source.size, currentIdentity.size)
    : 0;
  const snapshot = yield* readTranscriptAfter(source.filePath, currentIdentity, cursor);
  if (snapshot === null) return;

  const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
  const parsed = parseAgentSessionRecords(
    {
      source: source.provider,
      providerInstanceId: source.providerInstanceId,
      fallbackSessionId: source.providerSessionId,
      previousProviderSessionId: source.providerSessionId,
      lastActiveAtMs: currentIdentity.mtimeMs ?? nowMs,
    },
    snapshot.records,
    snapshot.recordOffsets,
    true,
  );

  let lastCompleteByteOffset =
    parsed?.lastAmbiguousCodexUserOffset === undefined
      ? snapshot.lastCompleteByteOffset
      : Math.min(snapshot.lastCompleteByteOffset, parsed.lastAmbiguousCodexUserOffset);

  if (parsed === null || parsed.thread.providerSessionId !== source.providerSessionId) {
    if (readingAppend) {
      yield* Effect.logWarning("Could not parse a changed imported transcript", {
        threadId: imported.threadId,
        filePath: source.filePath,
      });
    } else {
      lastCompleteByteOffset = currentIdentity.size;
      yield* warnUnprovenReplacementOnce(imported.threadId, source.filePath);
    }
  } else {
    const differentFile = isDifferentFile(source, currentIdentity);
    const replacementBoundary = source.lastCompleteByteOffset ?? source.size;
    const replacementBoundaryProven =
      readingAppend ||
      replacementHistoryMatches(
        thread,
        parsed,
        replacementBoundary,
        snapshot.completeRecordEndOffsets,
      );
    if (!readingAppend && !replacementBoundaryProven) {
      lastCompleteByteOffset = currentIdentity.size;
      yield* warnUnprovenReplacementOnce(imported.threadId, source.filePath);
    }
    const appendBoundary = readingAppend ? cursor : replacementBoundary;
    const existingMessageIds = new Set(thread.messages.map((message) => message.id));
    const codexEventOffsets = new Set(parsed.codexEventMessageOffsets);
    const lastExistingUserMessage = thread.messages
      .toReversed()
      .find((message) => message.role === "user");
    const lastAssistantIndex = thread.messages.findLastIndex(
      (message) => message.role === "assistant",
    );
    const userIsThreadTail =
      lastExistingUserMessage !== undefined &&
      thread.messages.findLastIndex((message) => message.role === "user") > lastAssistantIndex;
    let latestCreatedAt = thread.messages.reduce(
      (latest, message) => (message.createdAt > latest ? message.createdAt : latest),
      thread.messages[0]?.createdAt ?? "",
    );

    const appendMessages = replacementBoundaryProven
      ? parsed.thread.messages.flatMap((message, index) => {
          const offset = parsed.messageOffsets[index];
          if (offset === undefined || offset >= lastCompleteByteOffset) return [];
          if (offset < appendBoundary) return [];
          if (
            source.provider === "codex" &&
            message.role === "user" &&
            codexEventOffsets.has(offset) &&
            userIsThreadTail &&
            lastExistingUserMessage?.text.trim() === message.text.trim()
          ) {
            return [];
          }

          let messageId = MessageId.make(
            `${imported.threadId}:transcript:${String(offset).padStart(TRANSCRIPT_MESSAGE_ID_WIDTH, "0")}`,
          );
          if (existingMessageIds.has(messageId)) {
            if (!differentFile) return [];
            const identitySuffix = `${currentIdentity.device}-${currentIdentity.inode ?? currentIdentity.birthtimeMs ?? "replacement"}`;
            messageId = MessageId.make(
              `${imported.threadId}:transcript:${String(offset).padStart(TRANSCRIPT_MESSAGE_ID_WIDTH, "0")}:${identitySuffix}`,
            );
            if (existingMessageIds.has(messageId)) return [];
          }
          existingMessageIds.add(messageId);
          const createdAt =
            message.createdAt > latestCreatedAt ? message.createdAt : latestCreatedAt;
          latestCreatedAt = createdAt;
          return [
            {
              messageId,
              role: message.role,
              text: message.text,
              createdAt,
            },
          ];
        })
      : [];

    if (appendMessages.length > 0) {
      yield* engine.dispatch({
        type: "thread.history.import",
        commandId: CommandId.make(yield* crypto.randomUUIDv4),
        threadId: imported.threadId,
        messages: appendMessages,
      });
    }
  }

  yield* directory.recordImportedTranscript({
    threadId: imported.threadId,
    source: {
      ...currentIdentity,
      lastCompleteByteOffset,
      provider: source.provider,
      providerInstanceId: source.providerInstanceId,
      providerSessionId: source.providerSessionId,
    },
  });
});

export const pollOnce = Effect.fn("AgentSessionTranscriptFollower.pollOnce")(function* () {
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const projectsResult = yield* snapshots.getProjectShells().pipe(Effect.exit);
  if (Exit.isFailure(projectsResult)) {
    yield* Effect.logWarning("Could not list projects while following imported transcripts", {
      cause: projectsResult.cause,
    });
    return;
  }

  for (const project of projectsResult.value) {
    const sourcesResult = yield* snapshots
      .getImportedAgentSessionSources(project.id)
      .pipe(Effect.exit);
    if (Exit.isFailure(sourcesResult)) {
      yield* Effect.logWarning("Could not list imported transcripts for a project", {
        projectId: project.id,
        cause: sourcesResult.cause,
      });
      continue;
    }

    for (const imported of sourcesResult.value) {
      yield* followRecordedTranscript(project.id, imported).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Could not follow an imported transcript", {
            threadId: imported.threadId,
            filePath: imported.source.filePath,
            cause,
          }),
        ),
      );
    }
  }
});

export const run = Effect.gen(function* () {
  const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;
  yield* startup.awaitCommandReady;
  yield* pollOnce().pipe(Effect.repeat(Schedule.spaced(FOLLOW_INTERVAL)), Effect.asVoid);
});
