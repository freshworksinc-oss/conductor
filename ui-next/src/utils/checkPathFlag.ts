import { FEATURES, featureFlags } from "utils/flags";

type PathFlagMap = {
  [key: string]: any;
};

const aiIntegrationsEnabled = featureFlags.isEnabled(
  FEATURES.CONDUCTOR_INTEGRATIONS_AI_ENABLED,
);

const pathFlagMap: PathFlagMap = {
  "/workflowDef": true,
  "/taskDef": true,
  "/get-started": featureFlags.isEnabled(FEATURES.SHOW_GET_STARTED_PAGE),
  "/api-reference": featureFlags.isEnabled(FEATURES.SHOW_DOCUMENTATION, true),
  "/scheduleDef": featureFlags.isEnabled(FEATURES.SCHEDULER),
  "/schedulerExecs": featureFlags.isEnabled(FEATURES.SCHEDULER),
  "/newScheduleDef": featureFlags.isEnabled(FEATURES.SCHEDULER),
  "/agents": aiIntegrationsEnabled,
  "/agentExecutions": aiIntegrationsEnabled,
  "/agentSecrets": aiIntegrationsEnabled,
  "/skills": aiIntegrationsEnabled,
  "/runAgent": aiIntegrationsEnabled,
  "/secrets": featureFlags.isEnabled(FEATURES.SECRETS),
  "/human": featureFlags.isEnabled(FEATURES.HUMAN_TASK),
  "/configure-webhook": featureFlags.isEnabled(FEATURES.WEBHOOKS),
  "/newWebhook": featureFlags.isEnabled(FEATURES.WEBHOOKS),
  "/userManagement": featureFlags.isEnabled(FEATURES.RBAC),
  "/groupManagement": featureFlags.isEnabled(FEATURES.RBAC),
  "/ai_prompts": featureFlags.isEnabled(FEATURES.INTEGRATIONS),
  "/integrations": featureFlags.isEnabled(FEATURES.INTEGRATIONS),
  "/": true,
};

const pathFlagMapWithoutSKU: PathFlagMap = {
  "/api-reference": featureFlags.isEnabled(FEATURES.SHOW_DOCUMENTATION, true),
  "/scheduleDef": featureFlags.isEnabled(FEATURES.SCHEDULER),
  "/schedulerExecs": featureFlags.isEnabled(FEATURES.SCHEDULER),
  "/newScheduleDef": featureFlags.isEnabled(FEATURES.SCHEDULER),
  "/agents": aiIntegrationsEnabled,
  "/agentExecutions": aiIntegrationsEnabled,
  "/agentSecrets": aiIntegrationsEnabled,
  "/skills": aiIntegrationsEnabled,
  "/runAgent": aiIntegrationsEnabled,
  "/human": featureFlags.isEnabled(FEATURES.HUMAN_TASK),
  "/ai_prompts": featureFlags.isEnabled(FEATURES.INTEGRATIONS),
  "/integrations": featureFlags.isEnabled(FEATURES.INTEGRATIONS),
  "/remote-services": featureFlags.isEnabled(FEATURES.REMOTE_SERVICES),
  "/newRemoteServiceDef": featureFlags.isEnabled(FEATURES.REMOTE_SERVICES),
  "/": true,
};

export const checkPathFlag = (path: string) => {
  if (!featureFlags.isEnabled(FEATURES.SKU_ENABLED)) {
    for (const key in pathFlagMapWithoutSKU) {
      if (path.startsWith(key)) {
        return pathFlagMapWithoutSKU[key];
      }
    }
  }
  for (const key in pathFlagMap) {
    if (path.startsWith(key)) {
      return pathFlagMap[key];
    }
  }
  return false;
};
