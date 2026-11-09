import { ComponentType } from "react";
import { featureFlags, FEATURES } from "utils/flags";
import { SchedulerInProgress } from "./SchedulerInProgress";

/** Wrap scheduler routes so we skip API calls when the server has scheduler disabled. */
export function schedulerRouteElement<T extends object = object>(
  Page: ComponentType<T>,
): ComponentType<T> {
  return function SchedulerGatedPage(props: T) {
    if (!featureFlags.isEnabled(FEATURES.SCHEDULER_BACKEND_ENABLED, true)) {
      return <SchedulerInProgress />;
    }
    return <Page {...props} />;
  };
}
