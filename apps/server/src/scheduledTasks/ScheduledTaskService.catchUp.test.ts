import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import { type ScheduledTask, ScheduledTaskUpsertInput } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ScheduledTaskService from "./ScheduledTaskService.ts";

const decodeUpsertInput = Schema.decodeUnknownEffect(ScheduledTaskUpsertInput);

/** 1 July 2026 at the given local wall-clock time; schedules run in local time. */
const localTime = (hour: number, minute: number, second = 0) =>
  DateTime.makeZonedUnsafe(
    { year: 2026, month: 7, day: 1, hour, minute, second, millisecond: 0 },
    { timeZone: DateTime.zoneMakeLocal(), adjustForTimeZone: true },
  );
const isoOf = (value: DateTime.DateTime) => DateTime.formatIso(DateTime.toUtc(value));

const taskInput = (overrides: Record<string, unknown> = {}) =>
  decodeUpsertInput({
    id: "scheduled-task:half-hourly",
    title: "Triage issues",
    prompt: "Label the issues opened since the last run.",
    enabled: true,
    schedule: { type: "cron", expression: "0,30 * * * *" },
    projectId: "project-catch-up",
    workspaceStrategy: { type: "root" },
    modelSelection: { instanceId: "codex", model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    ...overrides,
  });

interface Launch {
  readonly runId: string;
  /** Agent runs still working when this one was dispatched. */
  readonly busyRunsAtLaunch: number;
}

/**
 * Runs `body` against a service whose launches start an agent run that keeps
 * working, as a real launch does, until the test settles it with `finishRun`.
 */
const withService = <A, E>(
  body: (input: {
    readonly service: ScheduledTaskService.ScheduledTaskService["Service"];
    readonly launches: Queue.Queue<Launch>;
    /** Settles an agent run, `completed` unless the test says otherwise. */
    readonly finishRun: (
      runId: string,
      at: DateTime.DateTime,
      status?: string,
    ) => Effect.Effect<void>;
    /** The first task list emission that satisfies `predicate`. */
    readonly awaitTask: (
      predicate: (task: ScheduledTask) => boolean,
    ) => Effect.Effect<ScheduledTask>;
  }) => Effect.Effect<A, E, SqlClient.SqlClient>,
  describeCron: ScheduledTaskService.DescribeCron = () => Effect.succeed(null),
) =>
  Effect.gen(function* () {
    // Set before the scheduler starts: moving a running clock this far would
    // replay every five-second poll since the epoch.
    yield* TestClock.setTime(DateTime.toEpochMillis(localTime(9, 59)));
    const sql = yield* SqlClient.SqlClient;
    const launches = yield* Queue.unbounded<Launch>();
    const launched = yield* Ref.make(0);
    const layerDependencies = Layer.mergeAll(
      NodeCrypto.layer,
      Scheduler.layer,
      Layer.mock(ThreadLaunchService.ThreadLaunchService)({
        launch: () =>
          Effect.gen(function* () {
            const n = yield* Ref.updateAndGet(launched, (count) => count + 1);
            const runId = `run-${n}`;
            const busy = yield* sql<{ readonly count: number }>`
              SELECT COUNT(*) AS count FROM orchestration_v2_projection_runs
              WHERE status = 'running'
            `;
            yield* sql`
              INSERT INTO orchestration_v2_projection_threads
                (thread_id, project_id, title, default_provider, runtime_mode,
                 interaction_mode, created_at, updated_at, payload_json)
              VALUES (${`thread-${n}`}, 'project-catch-up', 'Triage issues', 'codex',
                'full-access', 'default', '2026-07-01', '2026-07-01', '{}')
            `;
            yield* sql`
              INSERT INTO orchestration_v2_projection_runs
                (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
              VALUES (${runId}, ${`thread-${n}`}, 1, 'codex', 'running', '2026-07-01', '{}')
            `;
            yield* Queue.offer(launches, { runId, busyRunsAtLaunch: busy[0]?.count ?? -1 });
            return { threadId: `thread-${n}`, resumed: false } as never;
          }).pipe(Effect.orDie),
      }),
      Layer.mock(ThreadManagementService.ThreadManagementService)({}),
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.succeed(ScheduledTaskService.ScheduledTaskCronDescriber, describeCron),
    );
    const finishRun = (runId: string, at: DateTime.DateTime, status = "completed") =>
      sql`
        UPDATE orchestration_v2_projection_runs
        SET status = ${status}, completed_at = ${isoOf(at)}
        WHERE run_id = ${runId}
      `.pipe(Effect.asVoid, Effect.orDie);
    return yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      const awaitTask = (predicate: (task: ScheduledTask) => boolean) =>
        service.subscribeList().pipe(
          Stream.map((list) => list.tasks[0]),
          Stream.filter((task): task is ScheduledTask => task !== undefined && predicate(task)),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
          Effect.orDie,
        );
      return yield* body({ service, launches, finishRun, awaitTask });
    }).pipe(Effect.provide(ScheduledTaskService.layer.pipe(Layer.provide(layerDependencies))));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory));

/** Advances the clock to `at`, polling every five seconds on the way, then polls once more. */
const pollAt = (at: DateTime.DateTime) =>
  Clock.currentTimeMillis.pipe(
    Effect.flatMap((now) => TestClock.adjust(Duration.millis(DateTime.toEpochMillis(at) - now))),
    Effect.andThen(TestClock.adjust("6 seconds")),
  );

const lastThreadId = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly last_thread_id: string | null }>`
    SELECT last_thread_id FROM scheduled_tasks
  `;
  return rows[0]?.last_thread_id;
});

it.effect("runs a slot that came due during a long run as soon as that run finishes", () =>
  withService(({ service, launches, finishRun, awaitTask }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* taskInput());
      assert.isTrue(task.catchUp);
      assert.equal(task.nextRunAt, isoOf(localTime(10, 0)));

      yield* pollAt(localTime(10, 0));
      assert.deepEqual(yield* Queue.take(launches), { runId: "run-1", busyRunsAtLaunch: 0 });
      yield* awaitTask((current) => current.runCount === 1);
      assert.equal(yield* lastThreadId, "thread-1");

      // The agent is still on run-1 when the 10:30 slot comes, and the
      // scheduler keeps polling while it works.
      yield* pollAt(localTime(10, 46));
      // It finishes at 10:46, long past the slot's grace window; the slot
      // still runs, once, instead of being dropped as missed.
      yield* finishRun("run-1", localTime(10, 46));
      yield* TestClock.adjust("6 seconds");
      assert.deepEqual(yield* Queue.take(launches), { runId: "run-2", busyRunsAtLaunch: 0 });
      const caughtUp = yield* awaitTask((current) => current.runCount === 2);
      assert.equal(caughtUp.nextRunAt, isoOf(localTime(11, 0)));
      assert.equal(yield* lastThreadId, "thread-2");
      assert.equal(yield* Queue.size(launches), 0);
    }),
  ),
);

it.effect("skips a slot that comes due during a long run when catch-up is off", () =>
  withService(({ service, launches, finishRun, awaitTask }) =>
    Effect.gen(function* () {
      yield* service.upsert(yield* taskInput({ catchUp: false }));

      yield* pollAt(localTime(10, 0));
      assert.equal((yield* Queue.take(launches)).runId, "run-1");
      yield* awaitTask((current) => current.runCount === 1);

      yield* pollAt(localTime(10, 30));
      const skipped = yield* awaitTask((current) => current.nextRunAt === isoOf(localTime(11, 0)));
      assert.equal(skipped.runCount, 1);
      assert.equal(yield* Queue.size(launches), 0);

      yield* finishRun("run-1", localTime(10, 46));
      yield* pollAt(localTime(11, 0));
      assert.deepEqual(yield* Queue.take(launches), { runId: "run-2", busyRunsAtLaunch: 0 });
    }),
  ),
);

it.effect("keeps waiting while a restart continuation carries on the previous run", () =>
  withService(({ service, launches, finishRun, awaitTask }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* service.upsert(yield* taskInput());
      yield* pollAt(localTime(10, 0));
      assert.equal((yield* Queue.take(launches)).runId, "run-1");
      yield* awaitTask((current) => current.runCount === 1);

      // A restart interrupts run-1 and the agent continues in a new run of
      // the same thread; the task's work is not done yet.
      yield* finishRun("run-1", localTime(10, 20), "interrupted");
      yield* sql`
        INSERT INTO orchestration_v2_projection_runs
          (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
        VALUES ('run-1-continued', 'thread-1', 2, 'codex', 'running', '2026-07-01', '{}')
      `;
      yield* pollAt(localTime(10, 40));
      assert.equal(yield* Queue.size(launches), 0);

      yield* finishRun("run-1-continued", localTime(10, 41));
      yield* TestClock.adjust("6 seconds");
      assert.equal((yield* Queue.take(launches)).runId, "run-2");
    }),
  ),
);

it.effect("catches up by default and keeps a stored choice when an edit omits it", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const created = yield* service.upsert(yield* taskInput());
      assert.isTrue(created.task.catchUp);
      const optedOut = yield* service.upsert(yield* taskInput({ catchUp: false }));
      assert.isFalse(optedOut.task.catchUp);
      const renamed = yield* service.upsert(yield* taskInput({ title: "Renamed" }));
      assert.isFalse(renamed.task.catchUp);
    }),
  ),
);

it.effect("keeps tasks saved before catch-up existed on their old skipping behavior", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
        task_id: "scheduled-task:legacy",
        title: "Legacy",
        prompt: "Run",
        enabled: 1,
        schedule_json: '{"type":"interval","everyMs":60000}',
        project_id: "project-catch-up",
        thread_id: null,
        workspace_strategy_json: '{"type":"root"}',
        model_selection_json: '{"instanceId":"codex","model":"gpt-5"}',
        runtime_mode: "full-access",
        interaction_mode: "default",
        created_by: "user",
        creation_source: "web",
        created_at: "2026-07-01T00:00:00.000Z",
        updated_at: "2026-07-01T00:00:00.000Z",
        next_run_at: null,
        last_run_at: null,
        last_run_status: "never",
        last_run_error: null,
        run_count: 0,
      })}`;
      const { tasks } = yield* service.list();
      assert.isFalse(tasks[0]?.catchUp);
    }),
  ),
);

it.effect("describes a cron schedule after saving and only for the expression it describes", () =>
  Effect.gen(function* () {
    const releaseFirst = yield* Deferred.make<void>();
    const requested = yield* Ref.make<ReadonlyArray<string>>([]);
    yield* withService(
      ({ service, awaitTask }) =>
        Effect.gen(function* () {
          const created = yield* service.upsert(
            yield* taskInput({ schedule: { type: "cron", expression: "17  0,4,8-20 * * *" } }),
          );
          // The save does not wait for the model.
          assert.deepEqual(created.task.schedule, {
            type: "cron",
            expression: "17 0,4,8-20 * * *",
            description: null,
          });

          // Changing the expression before the first description lands
          // drops that description rather than mislabelling the new one.
          yield* service.upsert(
            yield* taskInput({ schedule: { type: "cron", expression: "0 9 * * 1-5" } }),
          );
          const described = yield* awaitTask(
            (task) => task.schedule.type === "cron" && task.schedule.description != null,
          );
          assert.deepEqual(described.schedule, {
            type: "cron",
            expression: "0 9 * * 1-5",
            description: "Weekdays at 09:00",
          });
          yield* Deferred.succeed(releaseFirst, undefined);

          // An edit that keeps the expression keeps its description without
          // asking the model again.
          const renamed = yield* service.upsert(
            yield* taskInput({
              title: "Renamed",
              schedule: { type: "cron", expression: "0 9 * * 1-5" },
            }),
          );
          assert.equal(
            renamed.task.schedule.type === "cron" ? renamed.task.schedule.description : null,
            "Weekdays at 09:00",
          );
          assert.deepEqual(yield* Ref.get(requested), ["17 0,4,8-20 * * *", "0 9 * * 1-5"]);
        }),
      ({ expression }) =>
        Ref.update(requested, (all) => [...all, expression]).pipe(
          Effect.andThen(
            expression === "0 9 * * 1-5"
              ? Effect.succeed("Weekdays at 09:00")
              : Deferred.await(releaseFirst).pipe(Effect.as("Hourly at :17")),
          ),
        ),
    );
  }),
);
