import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import { ScheduledTaskCronDescriber } from "./ScheduledTaskService.ts";

/** A label is not worth holding a model run open for longer than this. */
const DESCRIBE_TIMEOUT = "2 minutes";

/** Describes cron schedules with the project's thread title model. */
export const layer = Layer.effect(
  ScheduledTaskCronDescriber,
  Effect.gen(function* () {
    const projects = yield* ProjectStore.ProjectStoreV2;
    const serverSettings = yield* ServerSettings.ServerSettingsService;
    const textGeneration = yield* TextGeneration.TextGeneration;
    return ({ expression, projectId }) =>
      Effect.gen(function* () {
        const project = yield* projects.get(projectId);
        if (Option.isNone(project)) return null;
        const settings = resolveProjectSettings(
          yield* serverSettings.getSettings,
          projectId,
        ).settings;
        const result = yield* textGeneration.generateScheduleDescription({
          cwd: project.value.workspaceRoot,
          expression,
          modelSelection: settings.textGenerationModelSelection,
        });
        // An empty label leaves the task labelled by its expression.
        return result.description || null;
      }).pipe(
        Effect.timeout(DESCRIBE_TIMEOUT),
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.interrupt
            : Effect.logWarning("Cron schedule description failed", { projectId, cause }).pipe(
                Effect.as(null),
              ),
        ),
      );
  }),
);
