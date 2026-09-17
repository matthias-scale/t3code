import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../Migrations.ts";

it.layer(NodeSqliteClient.layerMemory())("053_ProjectionThreadsLatestImportedMessage", (it) => {
  it.effect("backfills the latest imported message by absolute timestamp", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 52 });
      yield* sql`
          INSERT INTO projection_threads (
            thread_id, project_id, title, model_selection_json, runtime_mode,
            created_at, updated_at
          ) VALUES (
            'import:codex:session', 'project-1', 'Imported thread',
            '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access',
            '2026-01-01T00:00:00.000Z', '2026-02-01T00:00:00.000Z'
          )
        `;
      yield* sql`
          INSERT INTO projection_thread_messages (
            message_id, thread_id, turn_id, role, text, is_streaming, created_at, updated_at
          ) VALUES
            ('import:codex:session:0', 'import:codex:session', NULL, 'user', 'first', 0,
              '2026-01-03T10:00:00.000+02:00', '2026-01-03T10:00:00.000+02:00'),
            ('import:codex:session:1', 'import:codex:session', NULL, 'assistant', 'latest', 0,
              '2026-01-03T09:00:00.000Z', '2026-01-03T09:00:00.000Z'),
            ('native-message', 'import:codex:session', NULL, 'user', 'native', 0,
              '2026-01-04T00:00:00.000Z', '2026-01-04T00:00:00.000Z')
        `;

      yield* runMigrations({ toMigrationInclusive: 53 });

      const rows = yield* sql<{ readonly latestImportedMessageAt: string | null }>`
          SELECT latest_imported_message_at AS "latestImportedMessageAt"
          FROM projection_threads
          WHERE thread_id = 'import:codex:session'
        `;
      assert.deepEqual(rows, [{ latestImportedMessageAt: "2026-01-03T09:00:00.000Z" }]);
    }),
  );
});
