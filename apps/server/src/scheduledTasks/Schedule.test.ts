import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";

import { dueRunAction, isSameSchedule, nextScheduledRunAt, parseTimeOfDay } from "./Schedule.ts";

describe("scheduled task schedule calculation", () => {
  it("parses 24-hour times", () => {
    expect(parseTimeOfDay("09:30")).toEqual({ hour: 9, minute: 30 });
    expect(parseTimeOfDay("23:59")).toEqual({ hour: 23, minute: 59 });
    expect(parseTimeOfDay("25:00")).toBeNull();
  });

  it("calculates interval schedules from the supplied instant", () => {
    const next = nextScheduledRunAt(
      { type: "interval", everyMs: 5 * 60_000 },
      DateTime.makeUnsafe("2026-07-01T16:00:00.000Z"),
    );
    expect(next ? DateTime.formatIso(DateTime.toUtc(next)) : null).toBe("2026-07-01T16:05:00.000Z");
  });

  it("clamps legacy sub-minute intervals to the one-minute execution floor", () => {
    const next = nextScheduledRunAt(
      { type: "interval", everyMs: 1_000 },
      DateTime.makeUnsafe("2026-07-01T16:00:00.000Z"),
    );
    expect(next ? DateTime.formatIso(DateTime.toUtc(next)) : null).toBe("2026-07-01T16:01:00.000Z");
  });

  it("skips to the next matching fixed-time weekday", () => {
    const next = nextScheduledRunAt(
      { type: "fixed_time", timeOfDay: "09:00", weekdays: [1, 2, 3, 4, 5] },
      DateTime.makeZonedUnsafe(
        {
          year: 2026,
          month: 7,
          day: 3,
          hour: 10,
          minute: 0,
          second: 0,
          millisecond: 0,
        },
        { timeZone: "America/Los_Angeles", adjustForTimeZone: true },
      ),
    );
    const parts = next ? DateTime.toParts(next) : null;
    expect(parts?.weekDay).toBe(1);
    expect(parts?.hour).toBe(9);
    expect(parts?.minute).toBe(0);
  });

  it("finds the next cron match in the environment's time zone", () => {
    const schedule = { type: "cron", expression: "17 0,4,8-20 * * *" } as const;
    const losAngeles = (day: number, hour: number, minute: number) =>
      DateTime.makeZonedUnsafe(
        { year: 2026, month: 10, day, hour, minute, second: 0, millisecond: 0 },
        { timeZone: "America/Los_Angeles", adjustForTimeZone: true },
      );
    const localParts = (from: DateTime.DateTime) => {
      const next = nextScheduledRunAt(schedule, from);
      if (next === null) return null;
      const { day, hour, minute } = DateTime.toParts(next);
      return { day, hour, minute };
    };
    // Hourly through the day; a run exactly on a match moves to the next one.
    expect(localParts(losAngeles(14, 10, 17))).toEqual({ day: 14, hour: 11, minute: 17 });
    // After 20:17 the overnight hours take over, crossing midnight.
    expect(localParts(losAngeles(14, 20, 17))).toEqual({ day: 15, hour: 0, minute: 17 });
    expect(localParts(losAngeles(15, 4, 17))).toEqual({ day: 15, hour: 8, minute: 17 });
  });

  describe("deciding a due run", () => {
    const cron = { type: "cron", expression: "0,30 * * * *" } as const;
    const at = (time: string) => DateTime.makeUnsafe(`2026-07-01T${time}:00.000Z`);
    const idle = { busy: false, finishedAt: null };
    const busy = { busy: true, finishedAt: null };

    it("fires a run that is on time or only slightly late", () => {
      expect(
        dueRunAction({
          schedule: cron,
          slotAt: at("09:00"),
          now: at("09:05"),
          previousRun: idle,
        }),
      ).toBe("run");
    });

    it("skips a wall-clock run missed while the server was away", () => {
      for (const schedule of [cron, { type: "fixed_time", timeOfDay: "09:00" } as const]) {
        expect(
          dueRunAction({
            schedule,
            slotAt: at("09:00"),
            now: at("15:00"),
            previousRun: { busy: false, finishedAt: at("08:40") },
          }),
        ).toBe("skip");
      }
      // An interval catches up with a single late run instead.
      expect(
        dueRunAction({
          schedule: { type: "interval", everyMs: 60_000 },
          slotAt: at("09:00"),
          now: at("15:00"),
          previousRun: idle,
        }),
      ).toBe("run");
    });

    it("never starts a run while the previous one is busy", () => {
      expect(
        dueRunAction({ schedule: cron, slotAt: at("10:00"), now: at("10:05"), previousRun: busy }),
      ).toBe("wait");
    });

    it("catches up as soon as a long previous run finishes, however late its slot", () => {
      // The 10:00 slot waited for a run that finished at 10:16.
      expect(
        dueRunAction({
          schedule: cron,
          slotAt: at("10:00"),
          now: at("10:16"),
          previousRun: { busy: false, finishedAt: at("10:16") },
        }),
      ).toBe("run");
      // But not when the server was away for hours after it finished.
      expect(
        dueRunAction({
          schedule: cron,
          slotAt: at("10:00"),
          now: at("14:00"),
          previousRun: { busy: false, finishedAt: at("10:16") },
        }),
      ).toBe("skip");
    });
  });

  it("compares schedules structurally", () => {
    expect(
      isSameSchedule({ type: "interval", everyMs: 60_000 }, { type: "interval", everyMs: 60_000 }),
    ).toBe(true);
    expect(
      isSameSchedule({ type: "interval", everyMs: 60_000 }, { type: "interval", everyMs: 30_000 }),
    ).toBe(false);
    expect(
      isSameSchedule(
        { type: "fixed_time", timeOfDay: "09:00", weekdays: [1, 2] },
        { type: "fixed_time", timeOfDay: "09:00", weekdays: [1, 2] },
      ),
    ).toBe(true);
    // Weekday masks are sets: order and duplicates do not change firing.
    expect(
      isSameSchedule(
        { type: "fixed_time", timeOfDay: "09:00", weekdays: [5, 1] },
        { type: "fixed_time", timeOfDay: "09:00", weekdays: [1, 5, 5] },
      ),
    ).toBe(true);
    // Omitted, empty, and all-seven masks all mean daily.
    expect(
      isSameSchedule(
        { type: "fixed_time", timeOfDay: "09:00" },
        { type: "fixed_time", timeOfDay: "09:00", weekdays: [0, 1, 2, 3, 4, 5, 6] },
      ),
    ).toBe(true);
    expect(
      isSameSchedule(
        { type: "fixed_time", timeOfDay: "09:00", weekdays: [] },
        { type: "fixed_time", timeOfDay: "09:00" },
      ),
    ).toBe(true);
    expect(
      isSameSchedule(
        { type: "fixed_time", timeOfDay: "09:00", weekdays: [1, 2] },
        { type: "fixed_time", timeOfDay: "09:00", weekdays: [1, 3] },
      ),
    ).toBe(false);
    expect(
      isSameSchedule(
        { type: "fixed_time", timeOfDay: "09:00" },
        { type: "fixed_time", timeOfDay: "09:30" },
      ),
    ).toBe(false);
    expect(
      isSameSchedule(
        { type: "interval", everyMs: 60_000 },
        { type: "fixed_time", timeOfDay: "09:00" },
      ),
    ).toBe(false);
    // A cron schedule is its expression; spacing and the generated
    // description do not change when it fires.
    expect(
      isSameSchedule(
        { type: "cron", expression: "17  0,4,8-20 * * *", description: "Hourly by day" },
        { type: "cron", expression: "17 0,4,8-20 * * *" },
      ),
    ).toBe(true);
    expect(
      isSameSchedule(
        { type: "cron", expression: "17 * * * *" },
        { type: "cron", expression: "18 * * * *" },
      ),
    ).toBe(false);
  });

  it.each([
    ["9:00", "09:00"],
    ["09:00", "9:00"],
    ["0:30", "00:30"],
  ])("treats %s and %s as the same fixed-time schedule", (before, after) => {
    expect(
      isSameSchedule(
        { type: "fixed_time", timeOfDay: before },
        { type: "fixed_time", timeOfDay: after },
      ),
    ).toBe(true);
  });
});
