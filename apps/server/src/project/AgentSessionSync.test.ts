import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationProjectShell,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import { make, makeSyncController } from "./AgentSessionSync.ts";

it.effect("runs shortly after startup and then every five minutes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let runs = 0;
      const sync = yield* makeSyncController(
        Effect.sync(() => void (runs += 1)),
        { initialDelay: "1 second", interval: "5 minutes" },
      );

      yield* sync.start();
      yield* TestClock.adjust("999 millis");
      expect(runs).toBe(0);
      yield* TestClock.adjust("1 millis");
      yield* sync.drain;
      expect(runs).toBe(1);

      yield* TestClock.adjust("5 minutes");
      yield* sync.drain;
      expect(runs).toBe(2);
    }),
  ),
);

it.effect("serializes runs and coalesces concurrent requests into one follow-up", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      let runs = 0;
      let active = 0;
      let maximumActive = 0;
      const sync = yield* makeSyncController(
        Effect.gen(function* () {
          runs += 1;
          active += 1;
          maximumActive = Math.max(maximumActive, active);
          if (runs === 1) {
            yield* Deferred.succeed(firstStarted, undefined).pipe(Effect.orDie);
            yield* Deferred.await(releaseFirst);
          }
          active -= 1;
        }),
      );

      yield* sync.request;
      yield* Deferred.await(firstStarted);
      yield* Effect.all([sync.request, sync.request, sync.request]);
      yield* Deferred.succeed(releaseFirst, undefined);
      yield* sync.drain;

      expect(runs).toBe(2);
      expect(maximumActive).toBe(1);
    }),
  ),
);

it.effect("imports new sessions only for projects that already imported provider history", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const projectId = ProjectId.make("project-with-imports");
      const emptyProjectId = ProjectId.make("project-without-imports");
      const workspaceRoot = "/tmp/project-with-imports";
      const project: OrchestrationProjectShell = {
        id: projectId,
        title: "Imported project",
        workspaceRoot,
        defaultModelSelection: null,
        scripts: [],
        createdAt: "2026-09-17T10:00:00.000Z",
        updatedAt: "2026-09-17T10:00:00.000Z",
      };
      const source = {
        provider: "codex" as const,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerSessionId: "existing-session",
        filePath: "/tmp/existing.jsonl",
        size: 10,
        mtimeMs: 1,
        device: 1,
        inode: 1,
        birthtimeMs: 1,
      };
      const commands: Array<OrchestrationCommand> = [];
      const scannedRoots: string[] = [];
      const scanner = AgentSessionScanner.AgentSessionScanner.of({
        scan: Effect.succeed({
          candidates: [
            {
              path: workspaceRoot,
              title: "Imported project",
              projectId,
              sources: ["codex"],
              threadCount: 2,
              lastActiveAt: "2026-09-17T10:00:00.000Z",
              alreadyImported: true,
              git: null,
            },
            {
              path: "/tmp/project-without-imports",
              title: "Empty project",
              projectId: emptyProjectId,
              sources: ["codex"],
              threadCount: 1,
              lastActiveAt: "2026-09-17T10:00:00.000Z",
              alreadyImported: true,
              git: null,
            },
          ],
          scannedAt: "2026-09-17T10:00:00.000Z",
        }),
        recentThreads: (root) => {
          scannedRoots.push(root);
          return Stream.succeed({
            _tag: "Importable" as const,
            source: { ...source, providerSessionId: "new-session", filePath: "/tmp/new.jsonl" },
            thread: {
              source: "codex" as const,
              providerInstanceId: ProviderInstanceId.make("codex"),
              providerSessionId: "new-session",
              title: "New provider session",
              model: null,
              createdAt: "2026-09-17T10:00:00.000Z",
              updatedAt: "2026-09-17T10:01:00.000Z",
              messages: [
                {
                  importIndex: 0,
                  role: "user" as const,
                  text: "New work",
                  createdAt: "2026-09-17T10:00:00.000Z",
                },
              ],
            },
          });
        },
      });
      const engine = OrchestrationEngine.OrchestrationEngineService.of({
        dispatch: (command) => Effect.sync(() => ({ sequence: commands.push(command) })),
        readEvents: () => Stream.empty,
        readThreadEvents: () => Stream.empty,
        getThreadReplayStats: () => Effect.die("unused"),
        streamDomainEvents: Stream.empty,
        subscribeDomainEvents: Effect.succeed(Stream.empty),
        latestSequence: Effect.succeed(0),
      });
      const snapshots = Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
        getImportedAgentSessionSources: (id) =>
          Effect.succeed(
            id === projectId
              ? [{ threadId: ThreadId.make("import:codex:existing-session"), source }]
              : [],
          ),
        getProjectShellById: (id) =>
          Effect.succeed(id === projectId ? Option.some(project) : Option.none()),
        getThreadDetailById: () => Effect.succeed(Option.none()),
      });
      const directory = ProviderSessionDirectory.ProviderSessionDirectory.of({
        upsert: () => Effect.void,
        recordImportedTranscript: () => Effect.void,
        getProvider: () => Effect.die("unused"),
        getBinding: () => Effect.succeed(Option.none()),
        listThreadIds: () => Effect.die("unused"),
        listBindings: () => Effect.die("unused"),
      });
      const sync = yield* make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(AgentSessionScanner.AgentSessionScanner, scanner),
            Layer.succeed(OrchestrationEngine.OrchestrationEngineService, engine),
            Layer.succeed(ProviderSessionDirectory.ProviderSessionDirectory, directory),
            snapshots,
            NodeServices.layer,
          ),
        ),
      );

      yield* sync.request;
      yield* sync.drain;

      expect(scannedRoots).toEqual([workspaceRoot]);
      expect(commands.map((command) => command.type)).toEqual([
        "thread.create",
        "thread.history.import",
      ]);
    }),
  ),
);
