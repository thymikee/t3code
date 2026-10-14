import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Existing tasks keep skipping a slot that comes due while their previous
  // run is busy; new tasks opt in to catching up when they are saved.
  yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN catch_up INTEGER NOT NULL DEFAULT 0`;
  // The run the task last started, so the poller can tell when the agent is
  // still working on it. Kept out of the read model like the webhook columns.
  yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN last_dispatched_run_id TEXT`;
});
