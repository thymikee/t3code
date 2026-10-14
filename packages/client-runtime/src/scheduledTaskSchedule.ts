import {
  type ScheduledTask,
  ScheduledTaskCronExpression,
  type ScheduledTaskSchedule,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

/** Whether a typed expression is one the server accepts. */
export const isCronExpression = Schema.is(ScheduledTaskCronExpression);

export const CRON_EXPRESSION_HINT =
  "Use five fields: minute, hour, day of month, month, weekday. For example, 17 0,4,8-20 * * *.";

/**
 * The server polls every five seconds, so a due run starts within seconds.
 * One still pending after a minute is waiting for the agent to finish the
 * task's previous run.
 */
const WAITING_AFTER_MS = 60_000;

/** "Next run" text for a task held behind its previous run, or null when it is not waiting. */
export function formatWaitingForPreviousRun(
  task: Pick<ScheduledTask, "enabled" | "nextRunAt">,
  now: number,
): string | null {
  if (!task.enabled || task.nextRunAt === null) return null;
  const overdueMs = now - Date.parse(task.nextRunAt);
  if (!(overdueMs >= WAITING_AFTER_MS)) return null;
  const minutes = Math.floor(overdueMs / 60_000);
  const due = minutes < 60 ? `${minutes} min ago` : `${Math.floor(minutes / 60)} h ago`;
  return `Next run when the current one finishes · due ${due}`;
}

/** A cron task's label: the model's description once it arrives, the expression until then. */
export function cronScheduleLabel(
  schedule: Extract<ScheduledTaskSchedule, { readonly type: "cron" }>,
): string {
  return schedule.description ?? `Cron ${schedule.expression}`;
}
