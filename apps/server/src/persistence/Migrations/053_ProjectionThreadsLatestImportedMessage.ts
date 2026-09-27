import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    ALTER TABLE projection_threads
    ADD COLUMN latest_imported_message_at TEXT
  `;
  yield* sql`
    UPDATE projection_threads AS thread
    SET latest_imported_message_at = (
      SELECT message.created_at
      FROM projection_thread_messages AS message
      WHERE message.thread_id = thread.thread_id
        AND message.message_id GLOB 'import:*'
        AND julianday(message.created_at) IS NOT NULL
      ORDER BY julianday(message.created_at) DESC
      LIMIT 1
    )
  `;
});
