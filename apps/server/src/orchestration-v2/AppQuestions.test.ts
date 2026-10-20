import * as Schema from "effect/Schema";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  OrchestrationV2Command,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import * as EventStore from "./EventStore.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as Option from "effect/Option";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for metadata controls"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerCore = Layer.mergeAll(
  layerDatabase,
  ProjectionMaintenance.layer.pipe(
    Layer.provide(EventStore.layer.pipe(Layer.provide(layerDatabase))),
    Layer.provide(ProjectionStore.layer.pipe(Layer.provide(layerDatabase))),
    Layer.provide(layerDatabase),
  ),
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "control-reads" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

const layerTest = ThreadManagement.layer.pipe(
  Layer.provideMerge(layerCore),
  Layer.provide(
    Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
      ensureTranscript: () => Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
    }),
  ),
  Layer.provide(
    Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
      get: () => Effect.succeed(Option.none()),
    }),
  ),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
);

const questions = [
  { id: "color", header: "Color", question: "Which color?", options: [], allowCustomAnswer: true },
] as const;
const setupQuestionThread = Effect.fn("test.setupQuestionThread")(function* (suffix: string) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const threadId = ThreadId.make(`thread:app-question:${suffix}`);
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${suffix}`),
    threadId,
    projectId: ProjectId.make(`project:${suffix}`),
    title: "App questions",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(`start:${suffix}`),
    threadId,
    messageId: MessageId.make(`message:${suffix}`),
    text: "Work independently",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  const projection = yield* orchestrator.getThreadProjection(threadId);
  return { orchestrator, threadId, runId: projection.runs[0]!.id };
});

it.effect("creates a nonblocking durable question, pins it, and deduplicates retries", () =>
  Effect.gen(function* () {
    const { orchestrator, threadId, runId } = yield* setupQuestionThread("create");
    const command = {
      type: "user_input_request.record" as const,
      commandId: CommandId.make("question:create"),
      threadId,
      runId,
      providerInstanceId: instanceId,
      requestId: RuntimeRequestId.make("app-question:create"),
      questions,
    };
    yield* orchestrator.dispatch(command);
    yield* orchestrator.dispatch({ ...command, commandId: CommandId.make("question:retry") });
    const after = yield* orchestrator.getThreadProjection(threadId);
    assert.lengthOf(after.runtimeRequests, 1);
    assert.equal(after.runtimeRequests[0]?.origin, "app_question");
    assert.deepEqual(after.runtimeRequests[0]?.responseCapability, { type: "message" });
    assert.equal(after.runs[0]?.status, "starting");
    const node = after.nodes.find((node) => node.runtimeRequestId === command.requestId)!;
    assert.isFalse(node.countsForRun);
    assert.equal(node.kind, "user_input_request");
    assert.isNotNull(after.thread.pinnedAt);
    assert.isTrue(after.thread.questionPinOwned);
    assert.equal(
      (yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadShell(threadId))
        ?.pendingRuntimeRequest?.blocking,
      false,
    );
    assert.lengthOf(
      after.turnItems.filter((item) => item.type === "user_input_request"),
      1,
    );
  }).pipe(Effect.provide(layerTest)),
);

const createQuestion = Effect.fn("test.createQuestion")(function* (
  context: Effect.Success<ReturnType<typeof setupQuestionThread>>,
  key: string,
) {
  const threads = yield* ThreadManagement.ThreadManagementService;
  return yield* threads.createUserInputRequest({
    commandId: CommandId.make(`create-question:${key}`),
    threadId: context.threadId,
    runId: context.runId,
    providerInstanceId: instanceId,
    clientRequestId: key,
    questions,
  });
});

it.effect(
  "deduplicates across service calls, rejects changed payloads and stale or foreign runs",
  () =>
    Effect.gen(function* () {
      const context = yield* setupQuestionThread("adversarial");
      const threads = yield* ThreadManagement.ThreadManagementService;
      const first = yield* createQuestion(context, "stable");
      const retry = {
        commandId: CommandId.make("retry:new-session"),
        threadId: context.threadId,
        runId: context.runId,
        providerInstanceId: instanceId,
        clientRequestId: "stable",
        questions,
      };
      assert.equal((yield* threads.createUserInputRequest(retry)).requestId, first.requestId);
      const mismatch = yield* threads
        .createUserInputRequest({
          ...retry,
          commandId: CommandId.make("retry:changed"),
          questions: [{ ...questions[0], question: "Different question?" }],
        })
        .pipe(Effect.result);
      assert.isTrue(mismatch._tag === "Failure");
      const foreign = yield* setupQuestionThread("foreign");
      const wrongRun = yield* threads
        .createUserInputRequest({
          ...retry,
          commandId: CommandId.make("wrong-run"),
          runId: foreign.runId,
        })
        .pipe(Effect.result);
      assert.isTrue(wrongRun._tag === "Failure");
      const wrongProvider = yield* threads
        .createUserInputRequest({
          ...retry,
          commandId: CommandId.make("wrong-provider"),
          providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        })
        .pipe(Effect.result);
      assert.isTrue(wrongProvider._tag === "Failure");
      const foreignWithdrawal = yield* threads
        .withdrawUserInputRequest({
          commandId: CommandId.make("foreign-withdraw"),
          threadId: foreign.threadId,
          requestId: first.requestId,
        })
        .pipe(Effect.result);
      assert.isTrue(foreignWithdrawal._tag === "Failure");
      yield* context.orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("end:adversarial"),
        threadId: context.threadId,
        runId: context.runId,
      });
      const stale = yield* threads
        .createUserInputRequest({ ...retry, commandId: CommandId.make("stale-run") })
        .pipe(Effect.result);
      assert.isTrue(stale._tag === "Failure");
      const after = yield* context.orchestrator.getThreadProjection(context.threadId);
      assert.lengthOf(after.runtimeRequests, 1);
      assert.equal(after.runtimeRequests[0]?.kind, "user_input");
      assert.isUndefined(after.runtimeRequests[0]?.decision);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "keeps multiple questions pinned through replay and releases the last automatic pin once",
  () =>
    Effect.gen(function* () {
      const context = yield* setupQuestionThread("multiple");
      const threads = yield* ThreadManagement.ThreadManagementService;
      const first = yield* createQuestion(context, "first");
      const second = yield* createQuestion(context, "second");
      yield* (yield* ProjectionMaintenance.ProjectionMaintenanceV2).rebuild;
      const pending = yield* context.orchestrator.getThreadProjection(context.threadId);
      assert.lengthOf(
        pending.runtimeRequests.filter((request) => request.status === "pending"),
        2,
      );
      assert.isTrue(pending.thread.questionPinOwned);
      yield* threads.withdrawUserInputRequest({
        commandId: CommandId.make("withdraw:first"),
        threadId: context.threadId,
        requestId: first.requestId,
      });
      assert.isNotNull(
        (yield* context.orchestrator.getThreadProjection(context.threadId)).thread.pinnedAt,
      );
      yield* threads.withdrawUserInputRequest({
        commandId: CommandId.make("withdraw:second"),
        threadId: context.threadId,
        requestId: second.requestId,
      });
      yield* threads.withdrawUserInputRequest({
        commandId: CommandId.make("withdraw:second:retry"),
        threadId: context.threadId,
        requestId: second.requestId,
      });
      const after = yield* context.orchestrator.getThreadProjection(context.threadId);
      assert.isNull(after.thread.pinnedAt);
      assert.isFalse(after.thread.questionPinOwned);
      assert.isTrue(after.runtimeRequests.every((request) => request.status === "resolved"));
      assert.lengthOf(after.messages, 1);
      assert.isNull(
        (yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadShell(context.threadId))
          ?.pendingRuntimeRequest,
      );
      const retry = yield* threads.createUserInputRequest({
        commandId: CommandId.make("create:terminal:retry"),
        threadId: context.threadId,
        runId: context.runId,
        providerInstanceId: instanceId,
        clientRequestId: "second",
        questions,
      });
      assert.equal(retry.status, "resolved");
      assert.isNull(
        (yield* context.orchestrator.getThreadProjection(context.threadId)).thread.pinnedAt,
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["before", "during", "unpin"] as const)(
  "preserves manual pin ownership: %s",
  (manual) =>
    Effect.gen(function* () {
      const context = yield* setupQuestionThread(`manual:${manual}`);
      const threads = yield* ThreadManagement.ThreadManagementService;
      const pin = (action: "thread.pin" | "thread.unpin", key: string) =>
        context.orchestrator.dispatch({
          type: action,
          commandId: CommandId.make(`manual:${manual}:${key}`),
          threadId: context.threadId,
        });
      if (manual === "before") yield* pin("thread.pin", "before");
      const first = yield* createQuestion(context, `manual:${manual}`);
      if (manual === "during") yield* pin("thread.pin", "during");
      if (manual === "unpin") yield* pin("thread.unpin", "unpin");
      yield* threads.withdrawUserInputRequest({
        commandId: CommandId.make(`manual:${manual}:withdraw`),
        threadId: context.threadId,
        requestId: first.requestId,
      });
      const after = yield* context.orchestrator.getThreadProjection(context.threadId);
      if (manual === "unpin") assert.isNull(after.thread.pinnedAt);
      else assert.isNotNull(after.thread.pinnedAt);
      assert.notEqual(after.thread.questionPinOwned, true);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("answers after the turn ends exactly once and clears its automatic pin", () =>
  Effect.gen(function* () {
    const context = yield* setupQuestionThread("answer");
    const first = yield* createQuestion(context, "answer");
    yield* context.orchestrator.dispatch({
      type: "run.interrupt",
      commandId: CommandId.make("end:answer"),
      threadId: context.threadId,
      runId: context.runId,
    });
    const command = {
      type: "runtime-request.respond" as const,
      commandId: CommandId.make("answer:once"),
      threadId: context.threadId,
      requestId: first.requestId,
      answers: { color: "Blue" },
    };
    yield* context.orchestrator.dispatch(command);
    yield* context.orchestrator.dispatch(command);
    const duplicate = yield* context.orchestrator
      .dispatch({ ...command, commandId: CommandId.make("answer:duplicate") })
      .pipe(Effect.result);
    assert.isTrue(duplicate._tag === "Failure");
    const after = yield* context.orchestrator.getThreadProjection(context.threadId);
    assert.lengthOf(
      after.messages.filter((message) => message.text === "Which color?\nBlue"),
      1,
    );
    assert.equal(after.runtimeRequests[0]?.status, "resolved");
    assert.isNull(after.thread.pinnedAt);
    assert.isUndefined(after.runtimeRequests[0]?.decision);
  }).pipe(Effect.provide(layerTest)),
);

const decodeClientCommand = Schema.decodeUnknownResult(OrchestrationV2Command);

it("keeps app question creation and withdrawal outside generic client command authority", () => {
  for (const type of ["user_input_request.record", "user_input_request.withdraw"]) {
    assert.equal(decodeClientCommand({ type })._tag, "Failure");
  }
});

it.effect("keeps a stable request across later runs and rejects duplicate question IDs", () =>
  Effect.gen(function* () {
    const context = yield* setupQuestionThread("later-run");
    const first = yield* createQuestion(context, "later");
    const threads = yield* ThreadManagement.ThreadManagementService;
    const invalid = yield* threads
      .createUserInputRequest({
        commandId: CommandId.make("question:duplicate-ids"),
        threadId: context.threadId,
        runId: context.runId,
        providerInstanceId: instanceId,
        clientRequestId: "duplicate-ids",
        questions: [questions[0], questions[0]],
      })
      .pipe(Effect.result);
    assert.equal(invalid._tag, "Failure");
    yield* context.orchestrator.dispatch({
      type: "run.interrupt",
      commandId: CommandId.make("later:interrupt"),
      threadId: context.threadId,
      runId: context.runId,
    });
    yield* context.orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("later:start"),
      threadId: context.threadId,
      messageId: MessageId.make("later:message"),
      text: "Continue independent work",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    const current = yield* context.orchestrator.getThreadProjection(context.threadId);
    const retry = yield* threads.createUserInputRequest({
      commandId: CommandId.make("later:retry"),
      threadId: context.threadId,
      runId: current.runs.find((run) => run.status === "starting")!.id,
      providerInstanceId: instanceId,
      clientRequestId: "later",
      questions,
    });
    assert.equal(retry.requestId, first.requestId);
    assert.equal(retry.status, "pending");
    assert.lengthOf(
      (yield* context.orchestrator.getThreadProjection(context.threadId)).runtimeRequests,
      1,
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect.each([false, true])(
  "normalizes multiselect answers and rejects empty arrays (wrapped=%s)",
  (wrapped) =>
    Effect.gen(function* () {
      const context = yield* setupQuestionThread(`multiselect:${wrapped}`);
      const threads = yield* ThreadManagement.ThreadManagementService;
      const request = yield* threads.createUserInputRequest({
        commandId: CommandId.make(`multi:create:${wrapped}`),
        threadId: context.threadId,
        runId: context.runId,
        providerInstanceId: instanceId,
        clientRequestId: "colors",
        questions: [{ ...questions[0]!, multiSelect: true }],
      });
      const respond = (values: string[], key: string) =>
        context.orchestrator.dispatch({
          type: "runtime-request.respond",
          commandId: CommandId.make(`multi:${wrapped}:${key}`),
          threadId: context.threadId,
          requestId: request.requestId,
          answers: { color: wrapped ? { answers: values } : values },
        });
      const empty = yield* respond([], "empty").pipe(Effect.result);
      assert.equal(empty._tag, "Failure");
      const pending = yield* context.orchestrator.getThreadProjection(context.threadId);
      assert.equal(pending.runtimeRequests[0]?.status, "pending");
      assert.isNotNull(pending.thread.pinnedAt);
      yield* respond(["Blue", "Green"], "valid");
      const after = yield* context.orchestrator.getThreadProjection(context.threadId);
      assert.lengthOf(
        after.messages.filter((message) => message.text === "Which color?\nBlue\nGreen"),
        1,
      );
      assert.deepEqual(after.runtimeRequests[0]?.answers, { color: ["Blue", "Green"] });
      assert.equal(after.runtimeRequests[0]?.status, "resolved");
      assert.isNull(after.thread.pinnedAt);
    }).pipe(Effect.provide(layerTest)),
);
