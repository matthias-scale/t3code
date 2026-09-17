import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionSource,
  AgentSessionScanError,
  type AgentSessionImportSource,
  isImportedAgentSessionMessageId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  type AgentSessionImportInput,
  type AgentSessionImportResult,
  type OrchestrationThread,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  hasLegacyCodexContextTitle,
  manualImportedTitleSyncPolicy,
} from "../orchestration/ImportedTitleSyncPolicy.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";

const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' changed before its history import completed.`;
  }
}

function hasImportedHistory(thread: OrchestrationThread): boolean {
  return thread.messages.some((message) => isImportedAgentSessionMessageId(message.id));
}

function hasNativeActivity(thread: OrchestrationThread): boolean {
  return (
    thread.latestTurn !== null ||
    thread.session !== null ||
    thread.messages.some((message) => !isImportedAgentSessionMessageId(message.id)) ||
    thread.proposedPlans.length > 0
  );
}

/** Recover a fallback title from the imported user messages already in the projection. */
function deriveImportedCodexTitle(thread: OrchestrationThread): string | null {
  for (const message of thread.messages) {
    if (message.role !== "user" || !isImportedAgentSessionMessageId(message.id)) continue;
    const title = AgentSessionScanner.deriveImportedThreadTitle(message.text, "codex");
    if (title !== null) return title;
  }
  return null;
}

function nextImportedMessages(
  threadId: ThreadId,
  existingThread: OrchestrationThread,
  providerThread: AgentSessionScanner.AgentSessionThread,
) {
  const importedMessages = existingThread.messages.filter((message) =>
    isImportedAgentSessionMessageId(message.id),
  );
  const latestImported = importedMessages.at(-1);
  if (latestImported === undefined) return [];

  const messageIdPrefix = `${threadId}:`;
  const importedIndex = (messageId: MessageId) => {
    if (!messageId.startsWith(messageIdPrefix)) return null;
    const suffix = messageId.slice(messageIdPrefix.length);
    return /^\d+$/.test(suffix) ? Number(suffix) : null;
  };
  const matchesLatestImported = (index: number) => {
    const message = providerThread.messages[index];
    return (
      message !== undefined &&
      message.role === latestImported.role &&
      message.text === latestImported.text &&
      message.createdAt === latestImported.createdAt
    );
  };

  const latestImportedIndex = importedIndex(latestImported.id);
  let latestProviderIndex =
    latestImportedIndex === null
      ? -1
      : providerThread.messages.findIndex(
          (message, index) =>
            message.importIndex === latestImportedIndex && matchesLatestImported(index),
        );
  const providerIndicesMatch = latestProviderIndex !== -1;
  if (!providerIndicesMatch) {
    for (let index = providerThread.messages.length - 1; index >= 0; index -= 1) {
      if (matchesLatestImported(index)) {
        latestProviderIndex = index;
        break;
      }
    }
  }
  if (latestProviderIndex === -1) return [];

  const nextMessageIndex =
    importedMessages.reduce((maximum, message) => {
      const index = importedIndex(message.id);
      return index === null ? maximum : Math.max(maximum, index);
    }, -1) + 1;

  return providerThread.messages.slice(latestProviderIndex + 1).map((message, offset) => ({
    messageId: MessageId.make(
      `${threadId}:${String(providerIndicesMatch ? message.importIndex : nextMessageIndex + offset).padStart(6, "0")}`,
    ),
    role: message.role,
    text: message.text,
    createdAt: message.createdAt,
  }));
}

function hasImportBlockingActivity(
  thread: OrchestrationThread,
  importedHistoryPresent: boolean,
): boolean {
  return (
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    thread.latestTurn !== null ||
    thread.session !== null ||
    thread.messages.some((message) => !isImportedAgentSessionMessageId(message.id)) ||
    thread.proposedPlans.length > 0 ||
    thread.activities.length > 0 ||
    thread.checkpoints.length > 0 ||
    thread.snoozedUntil != null ||
    thread.snoozedAt != null ||
    thread.pinnedAt != null ||
    thread.pinOrderKey != null ||
    thread.titleRegeneration != null ||
    thread.linkedPullRequest != null ||
    thread.unsettledAt != null ||
    (importedHistoryPresent
      ? thread.settledOverride !== "settled"
      : thread.settledOverride !== null || thread.settledAt !== null)
  );
}

/** Import recent transcript text and persist the cursor needed to resume its provider session. */
export const importRecentAgentThreads = Effect.fn("importRecentAgentThreads")(function* (
  input: AgentSessionImportInput,
) {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const crypto = yield* Crypto.Crypto;
  const project = yield* snapshots.getProjectShellById(input.projectId).pipe(
    Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
    Effect.flatMap(
      Option.match({
        onNone: () =>
          Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId: input.projectId })),
        onSome: Effect.succeed,
      }),
    ),
  );
  const workspaceRoot = project.workspaceRoot;
  if (
    input.expectedWorkspaceRoot !== undefined &&
    normalizeProjectPathForComparison(workspaceRoot) !==
      normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
  ) {
    return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
  }
  const completedSources = yield* snapshots
    .getImportedAgentSessionSources(input.projectId)
    .pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
    );
  const threads = scanner.recentThreads(
    workspaceRoot,
    completedSources.map((entry) => entry.source),
  );
  const importedThreadIds = new Set<ThreadId>();
  let importedCount = 0;
  let skippedCount = 0;

  const syncImportedTitle = Effect.fn("syncImportedTitle")(function* (
    threadId: ThreadId,
    providerTitle: string | null,
    existingThread?: OrchestrationThread,
  ) {
    const resolvedThread =
      existingThread === undefined
        ? yield* snapshots.getThreadDetailById(threadId)
        : Option.some(existingThread);
    if (Option.isNone(resolvedThread) || hasNativeActivity(resolvedThread.value)) return;
    const existingTitle = resolvedThread.value.title;
    const manualPolicy =
      resolvedThread.value.titleState?.source === "manual"
        ? manualImportedTitleSyncPolicy(existingTitle)
        : undefined;
    if (manualPolicy === null) return;
    const replacementTitleBody =
      providerTitle ??
      (hasLegacyCodexContextTitle(manualPolicy?.titleForFallback ?? existingTitle)
        ? deriveImportedCodexTitle(resolvedThread.value)
        : null);
    const replacementTitle =
      replacementTitleBody === null ? null : `${manualPolicy?.prefix ?? ""}${replacementTitleBody}`;
    if (replacementTitle === null || existingTitle === replacementTitle) {
      return;
    }
    yield* engine.dispatch({
      type: "thread.title.import.sync",
      commandId: CommandId.make(yield* crypto.randomUUIDv4),
      threadId,
      title: replacementTitle,
      expectedTitle: existingTitle,
      expectedVersion: resolvedThread.value.titleState?.version ?? null,
    });
  });

  const repinImportedCodexHome = Effect.fn("repinImportedCodexHome")(function* (
    threadId: ThreadId,
    source: AgentSessionImportSource,
    sessionHomePath: string | undefined,
    existingThread?: OrchestrationThread,
  ) {
    if (source.provider !== "codex" || sessionHomePath === undefined) return;
    const resolvedThread =
      existingThread === undefined
        ? yield* snapshots.getThreadDetailById(threadId)
        : Option.some(existingThread);
    if (Option.isNone(resolvedThread) || hasNativeActivity(resolvedThread.value)) return;

    const binding = yield* directory.getBinding(threadId);
    if (Option.isNone(binding)) return;
    const current = binding.value;
    const cursor = current.resumeCursor;
    if (
      current.status !== "stopped" ||
      current.provider !== "codex" ||
      current.providerInstanceId !== source.providerInstanceId ||
      cursor === null ||
      typeof cursor !== "object" ||
      Array.isArray(cursor) ||
      !("threadId" in cursor) ||
      cursor.threadId !== source.providerSessionId ||
      ("homePath" in cursor && cursor.homePath === sessionHomePath)
    ) {
      return;
    }

    yield* directory.upsert(
      {
        threadId,
        provider: current.provider,
        providerInstanceId: current.providerInstanceId,
        resumeCursor: { threadId: source.providerSessionId, homePath: sessionHomePath },
      },
      { onConflict: "updateStoppedMatchingSession" },
    );
  });

  yield* Stream.runForEach(threads, (outcome) =>
    Effect.gen(function* () {
      if (outcome._tag === "Skipped") {
        skippedCount += 1;
        return;
      }
      if (outcome._tag === "AlreadyImported" || outcome._tag === "Duplicate") {
        const threadId = ThreadId.make(
          `import:${outcome.source.providerInstanceId}:${outcome.source.providerSessionId}`,
        );
        if (outcome._tag === "AlreadyImported") {
          if (outcome.source.provider === "codex") {
            yield* syncImportedTitle(threadId, outcome.canonicalTitle).pipe(
              Effect.catch((cause) =>
                Effect.logWarning("Could not sync an imported Codex thread title", {
                  threadId,
                  cause,
                }),
              ),
            );
            yield* repinImportedCodexHome(threadId, outcome.source, outcome.sessionHomePath).pipe(
              Effect.catch((cause) =>
                Effect.logWarning("Could not repin an imported Codex thread home", {
                  threadId,
                  cause,
                }),
              ),
            );
          }
          importedThreadIds.add(threadId);
          importedCount += 1;
        } else if (importedThreadIds.has(threadId)) {
          const recorded = yield* directory
            .recordImportedTranscript({ threadId, source: outcome.source })
            .pipe(Effect.result);
          if (recorded._tag === "Failure") {
            skippedCount += 1;
            yield* Effect.logWarning("Could not record an imported transcript copy", {
              threadId,
              cause: recorded.failure,
            });
          }
        }
        return;
      }
      const thread = outcome.thread;
      const threadId = ThreadId.make(
        `import:${thread.providerInstanceId}:${thread.providerSessionId}`,
      );
      const imported = yield* Effect.gen(function* () {
        const provider = ProviderDriverKind.make(thread.source);
        const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[provider] ?? DEFAULT_MODEL;
        const existingThread = yield* snapshots.getThreadDetailById(threadId);
        const existingBinding = yield* directory.getBinding(threadId);

        if (
          thread.source === "claudeAgent" &&
          !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
        ) {
          return yield* new AgentSessionUnresumableSessionError({
            source: thread.source,
            providerSessionId: thread.providerSessionId,
          });
        }

        if (Option.isSome(existingThread) && existingThread.value.projectId !== input.projectId) {
          return yield* new AgentSessionThreadProjectConflictError({
            threadId,
            expectedProjectId: input.projectId,
            actualProjectId: existingThread.value.projectId,
          });
        }

        const importedHistoryPresent = Option.isSome(existingThread)
          ? hasImportedHistory(existingThread.value)
          : false;
        if (
          Option.isSome(existingThread) &&
          importedHistoryPresent &&
          Option.isSome(existingBinding)
        ) {
          if (!hasNativeActivity(existingThread.value)) {
            const appendedMessages = nextImportedMessages(threadId, existingThread.value, thread);
            if (appendedMessages.length > 0) {
              yield* engine.dispatch({
                type: "thread.history.append",
                commandId: CommandId.make(yield* crypto.randomUUIDv4),
                threadId,
                messages: appendedMessages,
              });
            }
            yield* syncImportedTitle(threadId, thread.title, existingThread.value);
            yield* repinImportedCodexHome(
              threadId,
              outcome.source,
              thread.sessionHomePath,
              existingThread.value,
            );
          }
          yield* directory.recordImportedTranscript({ threadId, source: outcome.source });
          return true;
        }

        if (
          Option.isSome(existingThread) &&
          hasImportBlockingActivity(existingThread.value, importedHistoryPresent)
        ) {
          return yield* new AgentSessionThreadModifiedError({ threadId });
        }

        if (
          Option.isSome(existingBinding) &&
          (existingBinding.value.provider !== provider ||
            existingBinding.value.providerInstanceId !== thread.providerInstanceId ||
            existingBinding.value.status !== "stopped")
        ) {
          return yield* new AgentSessionThreadModifiedError({ threadId });
        }

        // Install the cursor before the thread becomes visible. A concurrent
        // real session can replace it, while insert-ignore keeps this import
        // from replacing that newer binding.
        if (Option.isNone(existingBinding)) {
          yield* directory.upsert(
            {
              threadId,
              provider,
              providerInstanceId: thread.providerInstanceId,
              status: "stopped",
              runtimeMode: DEFAULT_RUNTIME_MODE,
              resumeCursor:
                thread.source === "codex"
                  ? {
                      threadId: thread.providerSessionId,
                      ...(thread.sessionHomePath === undefined
                        ? {}
                        : { homePath: thread.sessionHomePath }),
                    }
                  : { threadId, resume: thread.providerSessionId },
              runtimePayload: { cwd: workspaceRoot },
            },
            { onConflict: "ignore" },
          );
        }

        if (Option.isNone(existingThread)) {
          yield* engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make(yield* crypto.randomUUIDv4),
            threadId,
            projectId: input.projectId,
            title: thread.title,
            modelSelection: { instanceId: thread.providerInstanceId, model },
            runtimeMode: DEFAULT_RUNTIME_MODE,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: null,
            worktreePath: null,
            createdAt: thread.createdAt,
            historyImport: true,
          });
        }

        if (!importedHistoryPresent) {
          yield* engine.dispatch({
            type: "thread.history.import",
            commandId: CommandId.make(yield* crypto.randomUUIDv4),
            threadId,
            messages: thread.messages.map((message) => ({
              messageId: MessageId.make(
                `${threadId}:${String(message.importIndex).padStart(6, "0")}`,
              ),
              role: message.role,
              text: message.text,
              createdAt: message.createdAt,
            })),
          });
        }

        yield* directory.recordImportedTranscript({ threadId, source: outcome.source });

        return true;
      }).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Could not import an agent session", {
            provider: thread.source,
            sessionId: thread.providerSessionId,
            cause,
          }).pipe(Effect.as(false)),
        ),
      );

      if (imported) {
        importedThreadIds.add(threadId);
        importedCount += 1;
      } else {
        skippedCount += 1;
      }
    }),
  );

  return { importedCount, skippedCount } satisfies AgentSessionImportResult;
});
