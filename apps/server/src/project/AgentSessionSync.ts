import { type ProjectId } from "@t3tools/contracts";
import { makeDrainableWorker, type DrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import { forkParked } from "../serverActivation.ts";
import { importRecentAgentThreads } from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";

const DEFAULT_INITIAL_DELAY = Duration.seconds(5);
const DEFAULT_INTERVAL = Duration.minutes(5);

export class AgentSessionSync extends Context.Service<
  AgentSessionSync,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly request: Effect.Effect<void>;
    readonly drain: Effect.Effect<void>;
  }
>()("t3/project/AgentSessionSync") {}

export const makeSyncController = Effect.fn("AgentSessionSync.makeSyncController")(function* (
  sync: Effect.Effect<void>,
  options?: {
    readonly initialDelay?: Duration.Input;
    readonly interval?: Duration.Input;
  },
) {
  const state = yield* Ref.make({ running: false, queued: false, rerun: false });
  const worker: DrainableWorker<void> = yield* makeDrainableWorker(() =>
    Effect.gen(function* () {
      yield* Ref.update(state, (current) => ({ ...current, queued: false, running: true }));
      yield* Effect.gen(function* () {
        let followUp = true;
        while (followUp) {
          yield* Ref.update(state, (current) => ({ ...current, rerun: false }));
          yield* sync;
          followUp = yield* Ref.modify(state, (current) => [
            current.rerun,
            { ...current, rerun: false },
          ]);
        }
      }).pipe(
        Effect.ensuring(
          Ref.modify(state, (current) =>
            current.rerun
              ? ([true, { running: false, queued: true, rerun: false }] as const)
              : ([false, { ...current, running: false }] as const),
          ).pipe(Effect.flatMap((enqueue) => (enqueue ? worker.enqueue(undefined) : Effect.void))),
        ),
      );
    }),
  );

  const request = Ref.modify(state, (current) => {
    if (current.running) return [false, { ...current, rerun: true }] as const;
    if (current.queued) return [false, current] as const;
    return [true, { ...current, queued: true }] as const;
  }).pipe(Effect.flatMap((enqueue) => (enqueue ? worker.enqueue(undefined) : Effect.void)));

  const initialDelay = options?.initialDelay ?? DEFAULT_INITIAL_DELAY;
  const interval = options?.interval ?? DEFAULT_INTERVAL;
  const start: AgentSessionSync["Service"]["start"] = Effect.fn("AgentSessionSync.start")(
    function* () {
      yield* forkParked(
        Effect.sleep(initialDelay).pipe(
          Effect.andThen(request),
          Effect.andThen(Effect.forever(Effect.sleep(interval).pipe(Effect.andThen(request)))),
        ),
      );
    },
  );

  return AgentSessionSync.of({ start, request, drain: worker.drain });
});

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const crypto = yield* Crypto.Crypto;

  const runImport = (projectId: ProjectId, expectedWorkspaceRoot: string) =>
    importRecentAgentThreads({ projectId, expectedWorkspaceRoot }).pipe(
      Effect.provideService(AgentSessionScanner.AgentSessionScanner, scanner),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, engine),
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, snapshots),
      Effect.provideService(ProviderSessionDirectory.ProviderSessionDirectory, directory),
      Effect.provideService(Crypto.Crypto, crypto),
    );

  const sweep = Effect.gen(function* () {
    const scan = yield* scanner.scan;
    const projects = new Map<ProjectId, string>();
    for (const candidate of scan.candidates) {
      if (candidate.projectId !== undefined && candidate.alreadyImported) {
        projects.set(candidate.projectId, candidate.path);
      }
    }

    yield* Effect.forEach(
      projects,
      ([projectId, workspaceRoot]) =>
        Effect.gen(function* () {
          const importedSources = yield* snapshots.getImportedAgentSessionSources(projectId);
          if (importedSources.length === 0) return;
          yield* runImport(projectId, workspaceRoot);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Imported agent session sync skipped a project", {
              projectId,
              cause: Cause.pretty(cause),
            }),
          ),
        ),
      { discard: true },
    );
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Imported agent session sync failed", { cause: Cause.pretty(cause) }),
    ),
  );

  return yield* makeSyncController(sweep);
});

export const layer = Layer.effect(AgentSessionSync, make);
