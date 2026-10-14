import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // The thread the task's last run launched, so the poller can tell when the
  // agent is still working there. Kept out of the read model like the webhook
  // columns.
  yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN last_thread_id TEXT`;
});
