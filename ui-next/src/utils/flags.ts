declare global {
  interface Window {
    conductor: any;
    heap?: any;
    authConfig?: {
      domain?: string;
      clientId?: string;
      useIdToken?: boolean;
      audience?: string;
      type?: string;
      issuer?: string;
      idps?: any[]; //Typing as any. else would need to import okta types. and this is Oidc
      useInteractionCodeFlow?: boolean;
      authorizationEndpoint?: string;
      tokenEndpoint?: string;
      endSessionEndpoint?: string;
      isTestEnvironment?: boolean;
    };
    auth0Identifiers?: {
      domain?: string;
      clientId?: string;
    };
  }
}

export const FEATURES = Object.freeze({
  ACCESS_MANAGEMENT: "ACCESS_MANAGEMENT",
  COPY_TOKEN: "COPY_TOKEN",
  TASK_VISIBILITY: "TASK_VISIBILITY",
  PLAYGROUND: "PLAYGROUND",
  SCHEDULER: "SCHEDULER",
  CREATOR_ENABLE_CREATOR: "CREATOR_ENABLE_CREATOR",
  CREATOR_ENABLE_REAFLOW_DIAGRAM: "CREATOR_ENABLE_REAFLOW_DIAGRAM",
  ENABLE_DARK_MODE_TOGGLE: "ENABLE_DARK_MODE_TOGGLE",
  NAVBAR_ELEMENTS_VARIANT: "NAVBAR_ELEMENTS_VARIANT",
  SHOW_START_TITLE: "SHOW_START_TITLE",
  SHOW_CLOUD_LINK: "SHOW_CLOUD_LINK",
  SHOW_FEEDBACK_FORM: "SHOW_FEEDBACK_FORM",
  SHOW_SUPPORT_FORM: "SHOW_SUPPORT_FORM",
  SHOW_DOCUMENTATION: "SHOW_DOCUMENTATION",
  /** Left-nav Help submenu (Docs / Requests / Support). */
  SHOW_HELP_MENU: "SHOW_HELP_MENU",
  /** Help → Docs link (defaults to Orkes docs when unset). */
  HELP_DOCS_URL: "HELP_DOCS_URL",
  HELP_DOCS_LABEL: "HELP_DOCS_LABEL",
  /** Help → Support link (Slack, Zendesk, etc.). Shown when set even if SHOW_SUPPORT_FORM is false. */
  HELP_SUPPORT_URL: "HELP_SUPPORT_URL",
  SHOW_JOIN_SLACK_COMMUNITY: "SHOW_JOIN_SLACK_COMMUNITY",
  BETA_KEYBOARD_FLOW: "BETA_KEYBOARD_FLOW",
  ENABLE_METRICS_DASHBOARD: "ENABLE_METRICS_DASHBOARD",
  METRICS_ORIGIN_URL: "METRICS_ORIGIN_URL",
  HIDE_JAVASCRIPT_OPTION: "HIDE_JAVASCRIPT_OPTION",
  HUMAN_TASK: "HUMAN_TASK",
  LOGIN_REDIRECT_TYPE: "LOGIN_REDIRECT_TYPE",
  DRAG_DROP_TASK_INCREMENT_THRESHOLD: "DRAG_DROP_TASK_INCREMENT_THRESHOLD",
  INTEGRATIONS: "INTEGRATIONS",
  INTEGRATIONS_FLAT_LAYOUT: "INTEGRATIONS_FLAT_LAYOUT",
  ANNOUNCEMENT_EXPIRY_DATE: "ANNOUNCEMENT_EXPIRY_DATE",
  SHOW_NEWS_ICON: "SHOW_NEWS_ICON",
  HEAP_APP_ID: "HEAP_APP_ID",
  DISABLE_EXPAND_WORKFLOW: "ENABLE_EXPAND_WORKFLOW",
  DISABLE_TASK_STATS: "ENABLE_TASK_STATS",
  ENABLE_TASK_DEFINITION_FORM: "ENABLE_TASK_DEFINITION_FORM",
  SECRETS: "SECRETS",
  WEBHOOKS: "WEBHOOKS",
  RBAC: "RBAC",
  SHOW_ONBOARDING_QUIZ: "SHOW_ONBOARDING_QUIZ",
  SKU_ENABLED: "SKU_ENABLED",
  TASK_INDEXING: "TASK_INDEXING",
  // Task documents in OSS store the workflow's name as workflowType. Enterprise
  // search accepts workflowName. Unset keeps workflowName, so conductor-ui is
  // unchanged; OSS context.js sets this to workflowType.
  TASK_SEARCH_WORKFLOW_FIELD: "TASK_SEARCH_WORKFLOW_FIELD",
  // OSS does not index a task's reference name. Hidden unless a deployment
  // sets this true (conductor-ui does).
  SHOW_TASK_REFERENCE_NAME: "SHOW_TASK_REFERENCE_NAME",
  TRIGGER_WORKFLOW: "TRIGGER_WORKFLOW",
  ENV_IS_PRODUCTION: "ENV_IS_PRODUCTION",
  ENABLE_WHITE_BACKGROUND_FORM: "ENABLE_WHITE_BACKGROUND_FORM",
  ADVANCED_ERROR_INSPECTOR_VALIDATIONS: "ADVANCED_ERROR_INSPECTOR_VALIDATIONS",
  CUSTOM_LOGO_URL: "CUSTOM_LOGO_URL",
  SHOW_END_TIME_IN_DATEPICKER: "SHOW_END_TIME_IN_DATEPICKER",
  SHOW_EVENT_MONITOR: "SHOW_EVENT_MONITOR",
  SENDGRID_TASK: "SENDGRID_TASK",
  LOG_ROCKET_KEY: "LOG_ROCKET_KEY",
  SHOW_AI_STUDIO_BANNER_FLAG: "SHOW_AI_STUDIO_BANNER_FLAG",
  SHOW_GET_STARTED_PAGE: "SHOW_GET_STARTED_PAGE",
  GET_STARTED_VIDEO_URL: "GET_STARTED_VIDEO_URL",
  REMOTE_SERVICES: "REMOTE_SERVICES",
  MULTITENANCY_TYPE: "MULTITENANCY_TYPE",
  DEFAULT_ROLES: "DEFAULT_ROLES",
  HIDE_IMPORT_BPMN: "HIDE_IMPORT_BPMN",
  GATEWAY_ENABLED: "GATEWAY_ENABLED",
  CLOUD_TEMPLATES_SOURCE: "CLOUD_TEMPLATES_SOURCE",
  AI_PROMPTS_VERSIONING: "AI_PROMPTS_VERSIONING",
  GROWTHBOOK_CLIENT_KEY: "GROWTHBOOK_CLIENT_KEY",
  ENABLE_RERUN_FROM_FORK_AND_DOWHILE_TASKS:
    "ENABLE_RERUN_FROM_FORK_AND_DOWHILE_TASKS",
  GOOGLE_CLIENT_ID: "GOOGLE_CLIENT_ID",
  NOTIFY_HUMAN_TASK: "NOTIFY_HUMAN_TASK",
  WORKFLOW_INTROSPECTION: "WORKFLOW_INTROSPECTION",
  WORKFLOW_SUMMARIZE: "WORKFLOW_SUMMARIZE",
  ENABLE_CONFETTI: "ENABLE_CONFETTI",
  OIDC_REMOVE_OFFLINE_ACCESS: "OIDC_REMOVE_OFFLINE_ACCESS",
  SHOW_ROLES_MENU_ITEM: "SHOW_ROLES_MENU_ITEM",
  SHOW_AGENT: "SHOW_AGENT",
  ENABLE_AGENT_AUDIO_INPUT: "ENABLE_AGENT_AUDIO_INPUT",
  // Driven by the server's conductor.integrations.ai.enabled (injected via /context.js).
  // Gates the embedded Conductor-Agents pages (Agents, Executions, Skills, Secrets).
  CONDUCTOR_INTEGRATIONS_AI_ENABLED: "CONDUCTOR_INTEGRATIONS_AI_ENABLED",
  AI_CODER_WORKER: "AI_CODER_WORKER",
  AI_CODER_CLOUD_WORKER: "AI_CODER_CLOUD_WORKER",
  TAG_VISIBILITY: "TAG_VISIBILITY",
  CONNECTED_APPS_ENABLED: "CONNECTED_APPS_ENABLED",
  // The next two gate task definition controls whose behavior lives in the
  // execution core. Both default on; a distribution that swaps in its own core
  // turns off the ones it does not honor, so the form never offers a setting
  // the server will silently ignore.
  TASK_STATUS_LISTENER: "TASK_STATUS_LISTENER",
  TASK_RETRY_TIMEOUT_LIMITS: "TASK_RETRY_TIMEOUT_LIMITS",
  // Freshworks auth-proxy: tenant dropdown + role gates (userinfo + x-tenant-id cookie).
  TENANT_SELECTION: "TENANT_SELECTION",
  /** Sidebar footer: Orkes logo, platform version, copyright. */
  SHOW_SIDEBAR_VERSION_BLOCK: "SHOW_SIDEBAR_VERSION_BLOCK",
  /** Browser tab icon; path under public/ (see applyBrandingFromContext). */
  FAVICON_URL: "FAVICON_URL",
  /**
   * When true and Vite dev server (import.meta.env.DEV), use mock tenants if
   * /api/userinfo fails — local UI without auth-proxy. Ignored in production builds.
   */
  LOCAL_DEV_MOCK_USERINFO: "LOCAL_DEV_MOCK_USERINFO",
  /** Full origin for legacy UI (separate DNS), e.g. https://sandbox-caas-ui.freshworkscorp.com */
  LEGACY_UI_BASE_URL: "LEGACY_UI_BASE_URL",
  /**
   * Same hostname: set conductor-ui-variant cookie; sandbox auth-proxy Lua routes
   * to legacy UI vs ui-next. Requires UI_VARIANT_COOKIE_NAME in envoy (no local Vite effect).
   */
  UI_VARIANT_COOKIE_ENABLED: "UI_VARIANT_COOKIE_ENABLED",
});

const mapOfLocalStorageValues = Object.fromEntries(
  Object.entries(FEATURES).map(([k, v]) => [
    k,
    localStorage.getItem(v) === null ? undefined : localStorage.getItem(v),
  ]),
);
const mapOfEnvValues = Object.fromEntries(
  Object.entries(FEATURES).map(([k, v]) => [
    k,
    process.env[`REACT_APP_FEATURE_${v}`],
  ]),
);

// window.conductor is read lazily (at call time, not module load) so that
// bootstrap code (e.g. enterprise main.tsx) can set defaults on window.conductor
// before any component renders, without requiring per-customer backend config.
// Priority: localStorage > env vars > window.conductor
const getResolvedValue = (feature: string) => {
  if (mapOfLocalStorageValues[feature] != null)
    return mapOfLocalStorageValues[feature];
  if (mapOfEnvValues[feature] != null) return mapOfEnvValues[feature];
  return window.conductor?.[feature];
};

export const featureFlags = {
  isEnabled: (feature: string, defaultValue = false) => {
    const val = getResolvedValue(feature);
    if (val === undefined || val === null) return defaultValue;
    return val === "true" || val === true;
  },
  getValue: (feature: string, defaultValue?: string) => {
    return getResolvedValue(feature) || defaultValue;
  },
  getContextValue: (feature: string) => {
    return window.conductor && window.conductor[feature];
  },
};
