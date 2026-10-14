import { describe, expect, it } from "vite-plus/test";

import {
  cronScheduleLabel,
  formatWaitingForPreviousRun,
  isCronExpression,
} from "./scheduledTaskSchedule.ts";

describe("formatWaitingForPreviousRun", () => {
  const dueAt = "2026-07-01T10:00:00.000Z";
  const at = (time: string) => Date.parse(`2026-07-01T${time}.000Z`);

  it("leaves a run that is about to start alone", () => {
    // Due runs start within a poll, so a few seconds late is not waiting.
    expect(formatWaitingForPreviousRun({ enabled: true, nextRunAt: dueAt }, at("10:00:30"))).toBe(
      null,
    );
  });

  it("says a run held behind the previous one is waiting, and for how long", () => {
    expect(formatWaitingForPreviousRun({ enabled: true, nextRunAt: dueAt }, at("10:17:20"))).toBe(
      "Next run when the current one finishes · due 17 min ago",
    );
    expect(formatWaitingForPreviousRun({ enabled: true, nextRunAt: dueAt }, at("12:05:00"))).toBe(
      "Next run when the current one finishes · due 2 h ago",
    );
  });

  it("does not report paused or unscheduled tasks as waiting", () => {
    expect(formatWaitingForPreviousRun({ enabled: false, nextRunAt: dueAt }, at("11:00:00"))).toBe(
      null,
    );
    expect(formatWaitingForPreviousRun({ enabled: true, nextRunAt: null }, at("11:00:00"))).toBe(
      null,
    );
  });
});

describe("cronScheduleLabel", () => {
  it("shows the expression until the description arrives", () => {
    expect(cronScheduleLabel({ type: "cron", expression: "17 0,4,8-20 * * *" })).toBe(
      "Cron 17 0,4,8-20 * * *",
    );
    expect(
      cronScheduleLabel({
        type: "cron",
        expression: "0 9 * * 1-5",
        description: "Weekdays at 09:00",
      }),
    ).toBe("Weekdays at 09:00");
  });
});

describe("isCronExpression", () => {
  it("accepts the five-field expressions the server runs", () => {
    for (const expression of ["17 0,4,8-20 * * *", "*/15  * * * *", "0 9 * * 1-5"]) {
      expect(isCronExpression(expression)).toBe(true);
    }
  });

  it("rejects seconds, missing fields, and out-of-range values", () => {
    for (const expression of ["", "0 0 9 * * *", "0 9 * *", "61 * * * *", "every hour"]) {
      expect(isCronExpression(expression)).toBe(false);
    }
  });
});
