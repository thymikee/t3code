import { MIN_SCHEDULED_TASK_INTERVAL_MS, type ScheduledTaskSchedule } from "@t3tools/contracts";
import * as Cron from "effect/Cron";
import * as DateTime from "effect/DateTime";
import * as Result from "effect/Result";

const MINUTE_MS = 60_000;

export function parseTimeOfDay(value: string): { hour: number; minute: number } | null {
  const match = /^([01]?\d|2[0-3]):([0-5]\d)$/.exec(value.trim());
  if (!match) return null;
  return { hour: Number(match[1]), minute: Number(match[2]) };
}

export function nextScheduledRunAt(
  schedule: ScheduledTaskSchedule,
  from: DateTime.DateTime,
): DateTime.DateTime | null {
  if (schedule.type === "webhook") return null;
  if (schedule.type === "interval") {
    // Persisted rows created before the one-minute floor remain readable, but
    // they must not retain their old high-frequency execution rate.
    return DateTime.add(from, {
      milliseconds: Math.max(schedule.everyMs, MIN_SCHEDULED_TASK_INTERVAL_MS),
    });
  }
  if (schedule.type === "cron") {
    // Evaluated in `from`'s zone, the environment's local time like fixed_time.
    const zone = DateTime.isZoned(from) ? from.zone : undefined;
    const cron = Cron.parse(schedule.expression, zone);
    if (Result.isFailure(cron)) return null;
    const next = DateTime.fromDateUnsafe(Cron.next(cron.success, from));
    return zone === undefined ? next : DateTime.setZone(next, zone);
  }

  const time = parseTimeOfDay(schedule.timeOfDay);
  if (time === null) return null;
  const weekdays =
    schedule.weekdays && schedule.weekdays.length > 0 ? new Set(schedule.weekdays) : null;
  for (let offset = 0; offset <= 7; offset += 1) {
    const candidate = DateTime.setParts(DateTime.add(from, { days: offset }), {
      hour: time.hour,
      minute: time.minute,
      second: 0,
      millisecond: 0,
    });
    if (DateTime.toEpochMillis(candidate) <= DateTime.toEpochMillis(from)) continue;
    if (weekdays !== null && !weekdays.has(DateTime.toParts(candidate).weekDay)) continue;
    return candidate;
  }
  return null;
}

/**
 * Canonical form of a weekday mask, mirroring how `nextScheduledRunAt` reads
 * it: order and duplicates are irrelevant, and an empty/omitted mask means the
 * same as explicitly listing all seven days — daily.
 */
function weekdayKey(weekdays: ReadonlyArray<number> | undefined): string {
  const unique = [...new Set(weekdays ?? [])].toSorted((x, y) => x - y);
  if (unique.length === 0 || unique.length === 7) return "daily";
  return unique.join(",");
}

export function normalizeCronExpression(expression: string): string {
  return expression.trim().split(/\s+/).join(" ");
}

/** Semantic equality for schedules: true iff both fire at the same times. */
export function isSameSchedule(a: ScheduledTaskSchedule, b: ScheduledTaskSchedule): boolean {
  if (a.type === "interval") {
    return b.type === "interval" && a.everyMs === b.everyMs;
  }
  // Webhook tasks never have a next run, whatever their signature settings.
  if (a.type === "webhook") return b.type === "webhook";
  // The description is derived from the expression, so it never makes two
  // schedules differ; spacing between fields does not change the meaning.
  if (a.type === "cron") {
    return (
      b.type === "cron" &&
      normalizeCronExpression(a.expression) === normalizeCronExpression(b.expression)
    );
  }
  if (b.type !== "fixed_time") return false;
  // The contract accepts padded and unpadded hours ("9:00" and "09:00"), so
  // compare the parsed time — string equality would treat a format-only edit
  // as a schedule change and recompute the pending run.
  const aTime = parseTimeOfDay(a.timeOfDay);
  const bTime = parseTimeOfDay(b.timeOfDay);
  return (
    aTime !== null &&
    bTime !== null &&
    aTime.hour === bTime.hour &&
    aTime.minute === bTime.minute &&
    weekdayKey(a.weekdays) === weekdayKey(b.weekdays)
  );
}

/**
 * How late a wall-clock run (fixed time or cron) may fire before it counts as
 * missed. Covers poll jitter and short sleeps, while a server booted hours
 * after the slot skips to the next occurrence instead of firing stale work at
 * a random time.
 */
const MISSED_WALL_CLOCK_GRACE_MS = 10 * MINUTE_MS;

/** True when a wall-clock run that became runnable at `dueAt` is too stale to fire. */
function isMissedWallClockRun(
  schedule: ScheduledTaskSchedule,
  dueAt: DateTime.DateTime,
  now: DateTime.DateTime,
): boolean {
  if (schedule.type !== "fixed_time" && schedule.type !== "cron") return false;
  return DateTime.toEpochMillis(now) - DateTime.toEpochMillis(dueAt) > MISSED_WALL_CLOCK_GRACE_MS;
}

/** The agent's work from a task's previous scheduled run, as the poller sees it. */
export interface PreviousRun {
  /** The agent is still working, or has work queued, in the task's thread. */
  readonly busy: boolean;
  readonly finishedAt: DateTime.DateTime | null;
}

/**
 * What the poller does with a run whose slot has come:
 * - `wait`: the previous run is busy and the task catches up, so the run
 *   stays due and starts once that work finishes.
 * - `skip`: move to the next occurrence. Either the previous run is busy and
 *   the task does not catch up, or a wall-clock run is too stale to fire (the
 *   server was off or asleep). A run that waited for its predecessor counts
 *   lateness from when that work finished, not from its slot. Interval
 *   schedules are never stale; one late run is how they catch up.
 * - `run`: start it now.
 */
export function dueRunAction(input: {
  readonly schedule: ScheduledTaskSchedule;
  readonly catchUp: boolean;
  readonly slotAt: DateTime.DateTime;
  readonly now: DateTime.DateTime;
  readonly previousRun: PreviousRun;
}): "run" | "wait" | "skip" {
  const { finishedAt, busy } = input.previousRun;
  if (busy) return input.catchUp ? "wait" : "skip";
  const dueAt =
    finishedAt !== null && DateTime.toEpochMillis(finishedAt) > DateTime.toEpochMillis(input.slotAt)
      ? finishedAt
      : input.slotAt;
  return isMissedWallClockRun(input.schedule, dueAt, input.now) ? "skip" : "run";
}
