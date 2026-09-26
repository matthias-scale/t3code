import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schedule from "effect/Schedule";

import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import { followImportedAgentThreads } from "./AgentSessionImporter.ts";

const FOLLOW_INTERVAL = Duration.seconds(10);

const pollImportedTranscripts = Effect.fn("AgentSessionTranscriptFollower.poll")(function* () {
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
    if (sourcesResult.value.length === 0) continue;

    const followResult = yield* followImportedAgentThreads({ projectId: project.id }).pipe(
      Effect.exit,
    );
    if (Exit.isFailure(followResult)) {
      yield* Effect.logWarning("Could not follow imported transcripts for a project", {
        projectId: project.id,
        cause: followResult.cause,
      });
    }
  }
});

export const run = Effect.gen(function* () {
  const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;
  yield* startup.awaitCommandReady;
  yield* pollImportedTranscripts().pipe(
    Effect.repeat(Schedule.spaced(FOLLOW_INTERVAL)),
    Effect.asVoid,
  );
});
