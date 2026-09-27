import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type AgentSessionImportSource as AgentSessionImportSourceType,
  type OrchestrationCommand,
  type OrchestrationProjectShell,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderSessionDirectoryPersistenceError } from "../provider/Errors.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import { pollOnce } from "./AgentSessionTranscriptFollower.ts";

const CREATED_AT = "2026-09-27T10:00:00.000Z";
const PROJECT_A = ProjectId.make("follower-project-a");
const PROJECT_B = ProjectId.make("follower-project-b");
const CLAUDE_INSTANCE = ProviderInstanceId.make("claudeAgent");
const CODEX_INSTANCE = ProviderInstanceId.make("codex");
const encodeRecord = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

type SourceEntry = {
  projectId: ProjectId;
  threadId: ThreadId;
  source: AgentSessionImportSourceType;
};

const makeProject = (id: ProjectId): OrchestrationProjectShell => ({
  id,
  title: "Imported sessions",
  workspaceRoot: "/tmp/follower-project",
  defaultModelSelection: null,
  scripts: [],
  createdAt: CREATED_AT,
  updatedAt: CREATED_AT,
});

const makeThread = (
  threadId: ThreadId,
  projectId: ProjectId,
  input: {
    readonly extraMessages?: OrchestrationThread["messages"];
    readonly activity?: "turn" | "session";
    readonly lifecycle?: Partial<OrchestrationThread>;
  } = {},
): OrchestrationThread => ({
  id: threadId,
  projectId,
  title: "Imported thread",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "default" },
  runtimeMode: "full-access",
  interactionMode: "default",
  pullRequests: [],
  branch: null,
  worktreePath: null,
  latestTurn:
    input.activity === "turn"
      ? {
          turnId: TurnId.make("turn-running"),
          state: "running",
          requestedAt: CREATED_AT,
          startedAt: CREATED_AT,
          completedAt: null,
          assistantMessageId: null,
        }
      : null,
  createdAt: CREATED_AT,
  updatedAt: CREATED_AT,
  archivedAt: null,
  settledOverride: "settled",
  settledAt: CREATED_AT,
  unsettledAt: null,
  snoozedUntil: null,
  snoozedAt: null,
  pinnedAt: null,
  pinOrderKey: null,
  activeOrderKey: null,
  autoSettleDisabledAt: null,
  titleRegeneration: null,
  titleState: null,
  deletedAt: null,
  messages: [
    {
      id: MessageId.make(`${threadId}:000000`),
      role: "user",
      text: "Original prompt",
      turnId: null,
      streaming: false,
      createdAt: CREATED_AT,
      updatedAt: CREATED_AT,
    },
    ...(input.extraMessages ?? []),
  ],
  proposedPlans: [],
  activities: [],
  checkpoints: [],
  session:
    input.activity === "session"
      ? {
          threadId,
          status: "running",
          providerName: "codex",
          providerInstanceId: CODEX_INSTANCE,
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: CREATED_AT,
        }
      : null,
  ...input.lifecycle,
});

const claudeRecord = (role: "user" | "assistant", text: string, timestamp = CREATED_AT) =>
  encodeRecord({ type: role, timestamp, message: { content: text } });

const sourceFromFile = Effect.fn("AgentSessionTranscriptFollowerTest.sourceFromFile")(function* (
  filePath: string,
  sessionId: string,
  lastCompleteByteOffset: number,
  provider: "claudeAgent" | "codex" = "claudeAgent",
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const stats = yield* fileSystem.stat(filePath);
  return {
    provider,
    providerInstanceId: provider === "codex" ? CODEX_INSTANCE : CLAUDE_INSTANCE,
    providerSessionId: sessionId,
    filePath,
    size: Number(stats.size),
    lastCompleteByteOffset,
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
  } satisfies AgentSessionImportSourceType;
});

const makeHarness = (input: {
  readonly projects: ReadonlyArray<OrchestrationProjectShell>;
  readonly entries: Array<SourceEntry>;
  readonly threads: Map<string, OrchestrationThread>;
  readonly sourceWriteFailures?: { remaining: number };
}) => {
  const commands: Array<OrchestrationCommand> = [];
  const recordedSources: Array<AgentSessionImportSourceType> = [];
  const threadDetailsRead: Array<ThreadId> = [];
  const layer = Layer.mergeAll(
    Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
      getProjectShells: () => Effect.succeed(input.projects),
      getImportedAgentSessionSources: (projectId) =>
        Effect.succeed(
          input.entries
            .filter((entry) => entry.projectId === projectId)
            .map(({ threadId, source }) => ({ threadId, source })),
        ),
      getThreadDetailById: (threadId) =>
        Effect.sync(() => {
          threadDetailsRead.push(threadId);
          return input.threads.has(threadId)
            ? Option.some(input.threads.get(threadId)!)
            : Option.none();
        }),
    }),
    Layer.mock(OrchestrationEngine.OrchestrationEngineService)({
      dispatch: (command) =>
        Effect.sync(() => {
          commands.push(command);
          if (command.type === "thread.history.import") {
            const thread = input.threads.get(command.threadId);
            if (thread !== undefined) {
              input.threads.set(command.threadId, {
                ...thread,
                messages: [
                  ...thread.messages,
                  ...command.messages.map((message) => ({
                    id: message.messageId,
                    role: message.role,
                    text: message.text,
                    turnId: null,
                    streaming: false,
                    createdAt: message.createdAt,
                    updatedAt: message.createdAt,
                  })),
                ],
              });
            }
          }
          return { sequence: commands.length };
        }),
      readEvents: () => Stream.empty,
      readThreadEvents: () => Stream.empty,
      getThreadReplayStats: () => Effect.die("unused"),
      streamDomainEvents: Stream.empty,
      subscribeDomainEvents: Effect.succeed(Stream.empty),
      latestSequence: Effect.succeed(0),
    }),
    Layer.mock(ProviderSessionDirectory.ProviderSessionDirectory)({
      recordImportedTranscript: ({ threadId, source }) => {
        if (input.sourceWriteFailures?.remaining) {
          input.sourceWriteFailures.remaining -= 1;
          return Effect.fail(
            new ProviderSessionDirectoryPersistenceError({
              operation: "recordImportedTranscript",
              detail: "source write failed",
            }),
          );
        }
        return Effect.sync(() => {
          recordedSources.push(source);
          const index = input.entries.findIndex(
            (entry) => entry.threadId === threadId && entry.source.filePath === source.filePath,
          );
          if (index >= 0) input.entries[index] = { ...input.entries[index]!, source };
        });
      },
    }),
  );
  const poll = () => pollOnce().pipe(Effect.provide(layer));
  return { commands, entries: input.entries, layer, poll, recordedSources, threadDetailsRead };
};

const observeReads = (
  fileSystem: FileSystem.FileSystem,
  onOpen: (filePath: string) => void,
  onSeek: (filePath: string, offset: number) => void,
  onRead: (filePath: string, size: number) => void,
  reusableBuffer?: Uint8Array,
) =>
  FileSystem.FileSystem.of({
    ...fileSystem,
    open: (filePath, options) => {
      onOpen(filePath);
      return fileSystem.open(filePath, options).pipe(
        Effect.map((file) => ({
          ...file,
          stat: file.stat,
          seek: (offset, from) => {
            onSeek(filePath, Number(offset));
            return file.seek(offset, from);
          },
          readAlloc: (size) => {
            onRead(filePath, size);
            return file.readAlloc(size).pipe(
              Effect.map(
                reusableBuffer === undefined
                  ? (chunk) => chunk
                  : Option.map((chunk) => {
                      reusableBuffer.set(chunk);
                      return reusableBuffer.subarray(0, chunk.byteLength);
                    }),
              ),
            );
          },
        })),
      );
    },
  });

it.layer(NodeServices.layer)("AgentSessionTranscriptFollower", (it) => {
  describe("pollOnce", () => {
    it.effect("appends visible records once and reads only bytes after the saved cursor", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const filePath = `${yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-follower-" })}/session.jsonl`;
        const initial = `${claudeRecord("user", "Original prompt")}\n`;
        const appended = [
          claudeRecord("user", "Next prompt", "2026-09-27T10:01:00.000Z"),
          claudeRecord("assistant", "Next answer", "2026-09-27T10:01:00.000Z"),
        ].join("\n");
        yield* fileSystem.writeFileString(filePath, initial);
        const initialBytes = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:session-1");
        const source = yield* sourceFromFile(filePath, "session-1", initialBytes);
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A)]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        yield* fileSystem.writeFileString(filePath, `${appended}\n`, { flag: "a" });
        const opened: Array<string> = [];
        const seeks: Array<number> = [];
        let readBytes = 0;
        const observedFileSystem = observeReads(
          fileSystem,
          (openedPath) => opened.push(openedPath),
          (_openedPath, offset) => seeks.push(offset),
          (_openedPath, size) => void (readBytes += size),
        );

        yield* harness
          .poll()
          .pipe(Effect.provideService(FileSystem.FileSystem, observedFileSystem));

        const command = harness.commands[0];
        expect(command?.type).toBe("thread.history.import");
        if (command?.type !== "thread.history.import") return;
        expect(command.messages.map((message) => message.text)).toEqual([
          "Next prompt",
          "Next answer",
        ]);
        expect(command.messages.map((message) => message.messageId)).toEqual([
          `${threadId}:transcript:${String(initialBytes).padStart(16, "0")}`,
          `${threadId}:transcript:${String(
            initialBytes +
              new TextEncoder().encode(
                `${claudeRecord("user", "Next prompt", "2026-09-27T10:01:00.000Z")}\n`,
              ).byteLength,
          ).padStart(16, "0")}`,
        ]);
        expect(opened).toEqual([filePath]);
        expect(seeks).toEqual([initialBytes]);
        expect(readBytes).toBe(new TextEncoder().encode(`${appended}\n`).byteLength);

        const savedThread = threads.get(threadId)!;
        const originalMessage = savedThread.messages[0];
        expect(originalMessage?.text).toBe("Original prompt");
        expect(savedThread.messages.slice(1).map((message) => message.text)).toEqual([
          "Next prompt",
          "Next answer",
        ]);

        yield* harness
          .poll()
          .pipe(Effect.provideService(FileSystem.FileSystem, observedFileSystem));
        expect(harness.commands).toHaveLength(1);
        expect(opened).toEqual([filePath]);
      }),
    );

    it.effect("does not append again when saving the cursor fails after dispatch", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-cursor-retry-",
        });
        const filePath = `${directory}/session.jsonl`;
        const initial = `${claudeRecord("user", "Original prompt")}\n`;
        yield* fileSystem.writeFileString(filePath, initial);
        const cursor = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:cursor-retry");
        const source = yield* sourceFromFile(filePath, "cursor-retry", cursor);
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A)]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
          sourceWriteFailures: { remaining: 1 },
        });
        yield* fileSystem.writeFileString(
          filePath,
          `${claudeRecord("assistant", "Dispatched once", "2026-09-27T10:01:00.000Z")}\n`,
          { flag: "a" },
        );

        yield* harness.poll();
        yield* harness.poll();

        expect(harness.commands).toHaveLength(1);
        expect(harness.commands[0]).toMatchObject({
          type: "thread.history.import",
          messages: [{ text: "Dispatched once" }],
        });
        expect(
          threads.get(threadId)?.messages.filter((message) => message.text === "Dispatched once"),
        ).toHaveLength(1);
        expect(harness.entries[0]?.source.lastCompleteByteOffset).toBe(
          new TextEncoder().encode(
            `${initial}${claudeRecord("assistant", "Dispatched once", "2026-09-27T10:01:00.000Z")}\n`,
          ).byteLength,
        );
      }),
    );

    it.effect("preserves repeated appended messages with equal timestamps", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-repeat-",
        });
        const filePath = `${directory}/session.jsonl`;
        const initial = `${claudeRecord("user", "Original prompt")}\n`;
        yield* fileSystem.writeFileString(filePath, initial);
        const cursor = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:repeated-messages");
        const source = yield* sourceFromFile(filePath, "repeated-messages", cursor);
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A)]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        const repeatedMessage = claudeRecord(
          "assistant",
          "Repeated answer",
          "2026-09-27T10:01:00.000Z",
        );
        yield* fileSystem.writeFileString(filePath, `${repeatedMessage}\n${repeatedMessage}\n`, {
          flag: "a",
        });

        yield* harness.poll();

        expect(harness.commands).toHaveLength(1);
        expect(harness.commands[0]).toMatchObject({
          type: "thread.history.import",
          messages: [
            { role: "assistant", text: "Repeated answer" },
            { role: "assistant", text: "Repeated answer" },
          ],
        });
      }),
    );

    it.effect("holds a partial record at its start until the record completes", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-partial-",
        });
        const filePath = `${directory}/session.jsonl`;
        const initial = `${claudeRecord("user", "Original prompt")}\n`;
        yield* fileSystem.writeFileString(filePath, initial);
        const initialBytes = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:partial-session");
        const source = yield* sourceFromFile(filePath, "partial-session", initialBytes);
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A)]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        const record = claudeRecord("assistant", "Finished answer", "2026-09-27T10:02:00.000Z");
        const splitAt = Math.floor(record.length / 2);
        yield* fileSystem.writeFileString(filePath, record.slice(0, splitAt), { flag: "a" });
        yield* harness.poll();
        expect(harness.commands).toHaveLength(0);
        expect(harness.entries[0]?.source.lastCompleteByteOffset).toBe(initialBytes);

        yield* fileSystem.writeFileString(filePath, `${record.slice(splitAt)}\n`, { flag: "a" });
        yield* harness.poll();
        yield* harness.poll();
        expect(harness.commands).toHaveLength(1);
        expect(harness.commands[0]).toMatchObject({
          type: "thread.history.import",
          messages: [{ role: "assistant", text: "Finished answer" }],
        });
        expect(threads.get(threadId)?.messages.map((message) => message.text)).toEqual([
          "Original prompt",
          "Finished answer",
        ]);
      }),
    );

    it.effect("sees sources imported after the follower starts its first poll", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-follower-new-" });
        const filePath = `${directory}/session.jsonl`;
        const initial = `${claudeRecord("user", "Original prompt")}\n`;
        yield* fileSystem.writeFileString(filePath, initial);
        const cursor = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:late-session");
        const source = yield* sourceFromFile(filePath, "late-session", cursor);
        const threads = new Map<string, OrchestrationThread>();
        const entries: Array<SourceEntry> = [];
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries,
          threads,
        });

        yield* harness.poll();
        threads.set(threadId, makeThread(threadId, PROJECT_A));
        entries.push({ projectId: PROJECT_A, threadId, source });
        yield* fileSystem.writeFileString(
          filePath,
          `${claudeRecord("assistant", "Arrived after startup", "2026-09-27T10:03:00.000Z")}\n`,
          { flag: "a" },
        );
        yield* harness.poll();

        expect(harness.commands).toHaveLength(1);
        expect(threads.get(threadId)?.messages.at(-1)?.text).toBe("Arrived after startup");
      }),
    );

    it.effect.each([
      { pinnedAt: CREATED_AT, pinOrderKey: "a" },
      { snoozedAt: CREATED_AT, snoozedUntil: "2026-09-28T10:00:00.000Z" },
      { unsettledAt: CREATED_AT, settledOverride: "active" as const },
    ])("follows imported threads with lifecycle state %s", (lifecycle) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-state-",
        });
        const filePath = `${directory}/session.jsonl`;
        const initial = `${claudeRecord("user", "Original prompt")}\n`;
        yield* fileSystem.writeFileString(filePath, initial);
        const cursor = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make(`import:claudeAgent:state-${Object.keys(lifecycle)[0]}`);
        const source = yield* sourceFromFile(
          filePath,
          `state-${Object.keys(lifecycle)[0]}`,
          cursor,
        );
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A, { lifecycle })]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        yield* fileSystem.writeFileString(
          filePath,
          `${claudeRecord("assistant", "Lifecycle does not block", "2026-09-27T10:04:00.000Z")}\n`,
          { flag: "a" },
        );

        yield* harness.poll();
        expect(harness.commands).toHaveLength(1);
        expect(threads.get(threadId)?.messages.at(-1)?.text).toBe("Lifecycle does not block");
      }),
    );

    it.effect.each(["turn", "session"] as const)(
      "does not append while an imported thread has a T3 %s",
      (activity) =>
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const directory = yield* fileSystem.makeTempDirectoryScoped({
            prefix: "t3-follower-active-",
          });
          const filePath = `${directory}/session.jsonl`;
          const initial = `${claudeRecord("user", "Original prompt")}\n`;
          yield* fileSystem.writeFileString(filePath, initial);
          const cursor = new TextEncoder().encode(initial).byteLength;
          const threadId = ThreadId.make(`import:claudeAgent:active-${activity}`);
          const source = yield* sourceFromFile(filePath, `active-${activity}`, cursor);
          const threads = new Map([[threadId, makeThread(threadId, PROJECT_A, { activity })]]);
          const harness = makeHarness({
            projects: [makeProject(PROJECT_A)],
            entries: [{ projectId: PROJECT_A, threadId, source }],
            threads,
          });
          yield* fileSystem.writeFileString(
            filePath,
            `${claudeRecord("assistant", "Must not append", "2026-09-27T10:04:00.000Z")}\n`,
            { flag: "a" },
          );

          yield* harness.poll();

          expect(harness.commands).toHaveLength(0);
          expect(harness.recordedSources).toHaveLength(0);
          expect(threads.get(threadId)?.messages.map((message) => message.text)).toEqual([
            "Original prompt",
          ]);
        }),
    );

    it.effect("holds a Codex response prompt until its event copy arrives", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-codex-",
        });
        const filePath = `${directory}/rollout.jsonl`;
        const initial = `${encodeRecord({ type: "session_meta", payload: { id: "codex-split" } })}\n`;
        yield* fileSystem.writeFileString(filePath, initial);
        const cursor = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make("import:codex:codex-split");
        const source = yield* sourceFromFile(filePath, "codex-split", cursor, "codex");
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A)]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        const responseUser = encodeRecord({
          type: "response_item",
          timestamp: "2026-09-27T10:05:00.000Z",
          payload: {
            type: "message",
            role: "user",
            content: [{ type: "input_text", text: "Split prompt" }],
            internal_chat_message_metadata_passthrough: { turn_id: "turn-split" },
          },
        });
        const eventUser = encodeRecord({
          type: "event_msg",
          timestamp: "2026-09-27T10:05:00.000Z",
          payload: { type: "user_message", message: "Split prompt" },
        });
        yield* fileSystem.writeFileString(filePath, `${responseUser}\n`, { flag: "a" });
        yield* harness.poll();
        expect(harness.commands).toHaveLength(0);
        expect(harness.entries[0]?.source.lastCompleteByteOffset).toBe(cursor);

        yield* fileSystem.writeFileString(filePath, `${eventUser}\n`, { flag: "a" });
        yield* harness.poll();
        expect(harness.commands).toHaveLength(1);
        expect(harness.commands[0]).toMatchObject({
          type: "thread.history.import",
          messages: [{ role: "user", text: "Split prompt" }],
        });
        expect(
          threads.get(threadId)?.messages.filter((message) => message.text === "Split prompt"),
        ).toHaveLength(1);
      }),
    );

    it.effect("ignores malformed, tool, and reasoning records while keeping visible text", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-filter-",
        });
        const filePath = `${directory}/session.jsonl`;
        const initial = `${claudeRecord("user", "Original prompt")}\n`;
        yield* fileSystem.writeFileString(filePath, initial);
        const cursor = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:filter-session");
        const source = yield* sourceFromFile(filePath, "filter-session", cursor);
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A)]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        const appended = [
          "{broken json",
          encodeRecord({ type: "tool_use", timestamp: CREATED_AT, message: { content: "secret" } }),
          encodeRecord({
            type: "assistant",
            timestamp: CREATED_AT,
            message: { content: [{ type: "thinking", text: "private reasoning" }] },
          }),
          claudeRecord("assistant", "Visible answer", "2026-09-27T10:06:00.000Z"),
        ].join("\n");
        yield* fileSystem.writeFileString(filePath, `${appended}\n`, { flag: "a" });

        yield* harness.poll();
        expect(harness.commands).toHaveLength(1);
        expect(harness.commands[0]).toMatchObject({
          type: "thread.history.import",
          messages: [{ role: "assistant", text: "Visible answer" }],
        });
      }),
    );

    it.effect("skips a multi-megabyte tool record with bounded reads", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-large-tool-",
        });
        const filePath = `${directory}/session.jsonl`;
        const initial = `${claudeRecord("user", "Original prompt")}\n`;
        yield* fileSystem.writeFileString(filePath, initial);
        const cursor = new TextEncoder().encode(initial).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:large-tool");
        const source = yield* sourceFromFile(filePath, "large-tool", cursor);
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A)]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        const largeToolRecord = encodeRecord({
          type: "event_msg",
          payload: {
            type: "tool_call_output",
            output: "x".repeat(5 * 1024 * 1024),
          },
        });
        const assistantRecord = claudeRecord(
          "assistant",
          "After large tool output",
          "2026-09-27T10:07:00.000Z",
        );
        yield* fileSystem.writeFileString(filePath, `${largeToolRecord}\n${assistantRecord}\n`, {
          flag: "a",
        });
        let maxReadSize = 0;
        const openedPaths: Array<string> = [];
        const seekOffsets: Array<number> = [];
        let readCount = 0;
        const observedFileSystem = observeReads(
          fileSystem,
          (path) => openedPaths.push(path),
          (_path, offset) => seekOffsets.push(offset),
          (_path, size) => {
            readCount += 1;
            maxReadSize = Math.max(maxReadSize, size);
          },
          new Uint8Array(64 * 1024),
        );

        yield* harness
          .poll()
          .pipe(Effect.provideService(FileSystem.FileSystem, observedFileSystem));

        const finalByteOffset = new TextEncoder().encode(
          `${initial}${largeToolRecord}\n${assistantRecord}\n`,
        ).byteLength;
        expect(openedPaths).toEqual([filePath]);
        expect(seekOffsets).toEqual([cursor]);
        expect(readCount).toBeGreaterThan(0);
        expect(harness.entries[0]?.source.lastCompleteByteOffset).toBe(finalByteOffset);
        expect(harness.commands).toHaveLength(1);
        expect(harness.commands[0]).toMatchObject({
          type: "thread.history.import",
          messages: [{ text: "After large tool output" }],
        });
        expect(maxReadSize).toBeLessThanOrEqual(64 * 1024);
      }),
    );

    it.effect("checks 500 recorded files and reads only the five changed files", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-follower-500-" });
        const projectDirs = [`${directory}/a`, `${directory}/b`];
        yield* fileSystem.makeDirectory(projectDirs[0]!, { recursive: true });
        yield* fileSystem.makeDirectory(projectDirs[1]!, { recursive: true });
        const entries: Array<SourceEntry> = [];
        const threads = new Map<string, OrchestrationThread>();
        const changedPaths = new Set<string>();
        for (let index = 0; index < 500; index += 1) {
          const projectId = index < 250 ? PROJECT_A : PROJECT_B;
          const sessionId = `bulk-${index}`;
          const threadId = ThreadId.make(`import:claudeAgent:${sessionId}`);
          const filePath = `${projectDirs[index < 250 ? 0 : 1]}/${sessionId}.jsonl`;
          const initial = `${claudeRecord("user", `Prompt ${index}`)}\n`;
          yield* fileSystem.writeFileString(filePath, initial);
          const cursor = new TextEncoder().encode(initial).byteLength;
          const source = yield* sourceFromFile(filePath, sessionId, cursor);
          entries.push({ projectId, threadId, source });
          threads.set(threadId, makeThread(threadId, projectId));
          if (index === 0 || index === 31 || index === 249 || index === 250 || index === 499) {
            const appended = `${claudeRecord("assistant", `Answer ${index}`, "2026-09-27T10:07:00.000Z")}\n`;
            yield* fileSystem.writeFileString(filePath, appended, { flag: "a" });
            changedPaths.add(filePath);
          }
        }
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A), makeProject(PROJECT_B)],
          entries,
          threads,
        });
        const opened: Array<string> = [];
        const observedFileSystem = observeReads(
          fileSystem,
          (filePath) => opened.push(filePath),
          () => {},
          () => {},
        );

        yield* harness
          .poll()
          .pipe(Effect.provideService(FileSystem.FileSystem, observedFileSystem));

        expect(opened).toHaveLength(5);
        expect(new Set(opened)).toEqual(changedPaths);
        expect(harness.commands).toHaveLength(5);
        expect(harness.threadDetailsRead).toHaveLength(5);
      }),
    );

    it.effect("reparses a replacement once, skips existing content, and stores its identity", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-replaced-",
        });
        const filePath = `${directory}/session.jsonl`;
        const original = `${encodeRecord({ type: "user", message: { content: "Original prompt" } })}\n`;
        yield* fileSystem.writeFileString(filePath, original);
        const oldCursor = new TextEncoder().encode(original).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:replaced-session");
        const source = yield* sourceFromFile(filePath, "replaced-session", oldCursor);
        const threads = new Map([[threadId, makeThread(threadId, PROJECT_A)]]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        const replacementPath = `${directory}/replacement.jsonl`;
        const replacement = `${original}${claudeRecord(
          "assistant",
          "New replacement answer",
          "2026-09-27T10:08:00.000Z",
        )}\n`;
        yield* fileSystem.writeFileString(replacementPath, replacement);
        yield* fileSystem.rename(replacementPath, filePath);
        const opened: Array<string> = [];
        const observedFileSystem = observeReads(
          fileSystem,
          (openedPath) => opened.push(openedPath),
          () => {},
          () => {},
        );

        yield* harness
          .poll()
          .pipe(Effect.provideService(FileSystem.FileSystem, observedFileSystem));
        const savedIdentity = harness.entries[0]?.source;
        yield* harness
          .poll()
          .pipe(Effect.provideService(FileSystem.FileSystem, observedFileSystem));

        expect(harness.commands).toHaveLength(1);
        expect(harness.commands[0]).toMatchObject({
          type: "thread.history.import",
          messages: [{ role: "assistant", text: "New replacement answer" }],
        });
        expect(opened).toEqual([filePath]);
        expect(savedIdentity?.size).toBe(new TextEncoder().encode(replacement).byteLength);
        expect(threads.get(threadId)?.messages.map((message) => message.text)).toEqual([
          "Original prompt",
          "New replacement answer",
        ]);
      }),
    );

    it.effect("does not revive history omitted from a 201-message replacement", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-follower-replaced-cap-",
        });
        const filePath = `${directory}/session.jsonl`;
        const transcriptMessages = Array.from({ length: 201 }, (_, index) => ({
          role: index === 0 ? ("user" as const) : ("assistant" as const),
          text: `Imported message ${index}`,
        }));
        const original = `${transcriptMessages
          .map((message) => claudeRecord(message.role, message.text))
          .join("\n")}\n`;
        yield* fileSystem.writeFileString(filePath, original);
        const oldCursor = new TextEncoder().encode(original).byteLength;
        const threadId = ThreadId.make("import:claudeAgent:replaced-cap");
        const source = yield* sourceFromFile(filePath, "replaced-cap", oldCursor);
        const importedMessages = [transcriptMessages[0]!, ...transcriptMessages.slice(-199)];
        const projectedMessages: OrchestrationThread["messages"] = importedMessages.map(
          (message, index) => ({
            id: MessageId.make(`${threadId}:${String(index).padStart(6, "0")}`),
            role: message.role,
            text: message.text,
            turnId: null,
            streaming: false,
            createdAt: CREATED_AT,
            updatedAt: CREATED_AT,
          }),
        );
        const threads = new Map([
          [threadId, { ...makeThread(threadId, PROJECT_A), messages: projectedMessages }],
        ]);
        const harness = makeHarness({
          projects: [makeProject(PROJECT_A)],
          entries: [{ projectId: PROJECT_A, threadId, source }],
          threads,
        });
        const replacement = `${original}${claudeRecord(
          "assistant",
          "New replacement message",
          "2026-09-27T10:08:00.000Z",
        )}\n`;
        const replacementPath = `${directory}/replacement.jsonl`;
        yield* fileSystem.writeFileString(replacementPath, replacement);
        yield* fileSystem.rename(replacementPath, filePath);

        yield* harness.poll();

        expect(harness.commands).toHaveLength(1);
        expect(harness.commands[0]).toMatchObject({
          type: "thread.history.import",
          messages: [{ role: "assistant", text: "New replacement message" }],
        });
        expect(threads.get(threadId)?.messages.map((message) => message.text)).toEqual([
          ...importedMessages.map((message) => message.text),
          "New replacement message",
        ]);
      }),
    );
  });
});
