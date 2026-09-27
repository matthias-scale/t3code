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
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import { parseAgentSessionTranscriptRecords } from "./AgentSessionScanner.ts";

const FOLLOW_INTERVAL = Duration.seconds(10);
const READ_CHUNK_BYTES = 64 * 1024;
const TRANSCRIPT_MESSAGE_ID_WIDTH = 16;
const decodeJsonRecord = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

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

          const chunks: Array<Uint8Array> = [];
          let bytesRead = startByteOffset;
          while (bytesRead < expected.size) {
            const next = yield* file.readAlloc(
              Math.min(READ_CHUNK_BYTES, expected.size - bytesRead),
            );
            if (Option.isNone(next)) return null;
            chunks.push(next.value);
            bytesRead += next.value.byteLength;
          }

          if (!sameTranscriptIdentity(expected, transcriptIdentity(filePath, yield* file.stat))) {
            return null;
          }

          const bytes = new Uint8Array(bytesRead - startByteOffset);
          let chunkOffset = 0;
          for (const chunk of chunks) {
            bytes.set(chunk, chunkOffset);
            chunkOffset += chunk.byteLength;
          }

          const finalNewline = bytes.lastIndexOf(10);
          let completeByteLength = finalNewline + 1;
          if (completeByteLength < bytes.byteLength) {
            const trailingRecord = new TextDecoder().decode(bytes.subarray(completeByteLength));
            if (Option.isSome(decodeJsonRecord(trailingRecord))) {
              completeByteLength = bytes.byteLength;
            }
            // Invalid or partial JSON stays at the cursor for the next pass.
          }

          return {
            contents: new TextDecoder().decode(bytes.subarray(0, completeByteLength)),
            lastCompleteByteOffset: startByteOffset + completeByteLength,
          };
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
  if (
    !Number.isSafeInteger(currentIdentity.size) ||
    currentIdentity.size < 0 ||
    sameTranscriptIdentity(source, currentIdentity)
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
  const parsed = parseAgentSessionTranscriptRecords(
    {
      source: source.provider,
      providerInstanceId: source.providerInstanceId,
      fallbackSessionId: source.providerSessionId,
      previousProviderSessionId: source.providerSessionId,
      lastActiveAtMs: currentIdentity.mtimeMs ?? nowMs,
      contents: snapshot.contents,
      startByteOffset: cursor,
    },
    undefined,
    true,
  );

  const lastCompleteByteOffset =
    parsed?.lastAmbiguousCodexUserOffset === undefined
      ? snapshot.lastCompleteByteOffset
      : Math.min(snapshot.lastCompleteByteOffset, parsed.lastAmbiguousCodexUserOffset);

  if (parsed === null || parsed.thread.providerSessionId !== source.providerSessionId) {
    yield* Effect.logWarning("Could not parse a changed imported transcript", {
      threadId: imported.threadId,
      filePath: source.filePath,
    });
  } else {
    const existingMessageIds = new Set(thread.messages.map((message) => message.id));
    const existingMessageContentCounts = new Map<string, number>();
    for (const message of thread.messages) {
      const contentKey = `${message.role}\0${message.text}`;
      existingMessageContentCounts.set(
        contentKey,
        (existingMessageContentCounts.get(contentKey) ?? 0) + 1,
      );
    }
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

    const appendMessages = parsed.thread.messages.flatMap((message, index) => {
      const offset = parsed.messageOffsets[index];
      if (offset === undefined || offset >= lastCompleteByteOffset) return [];
      if (readingAppend && offset < cursor) return [];
      if (!readingAppend) {
        const contentKey = `${message.role}\0${message.text}`;
        const existingCount = existingMessageContentCounts.get(contentKey) ?? 0;
        if (existingCount > 0) {
          if (existingCount === 1) existingMessageContentCounts.delete(contentKey);
          else existingMessageContentCounts.set(contentKey, existingCount - 1);
          return [];
        }
      }
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
        const identitySuffix = `${currentIdentity.device}-${currentIdentity.inode ?? currentIdentity.birthtimeMs ?? "replacement"}`;
        messageId = MessageId.make(
          `${imported.threadId}:transcript:${String(offset).padStart(TRANSCRIPT_MESSAGE_ID_WIDTH, "0")}:${identitySuffix}`,
        );
      }
      existingMessageIds.add(messageId);
      return [
        {
          messageId,
          role: message.role,
          text: message.text,
          createdAt: message.createdAt,
        },
      ];
    });

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
