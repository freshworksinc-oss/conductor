// OSS Conductor UI Runtime Configuration
// This file configures feature flags at runtime for the OSS UI

window.conductor = {
  // Authentication - DISABLED for OSS
  ACCESS_MANAGEMENT: false,
  RBAC: false,
  COPY_TOKEN: false,

  // OSS Core Features
  SCHEDULER: true,
  // Scylla: keep conductor.scheduler.enabled=false on server; UI still shows Scheduler nav.
  SCHEDULER_BACKEND_ENABLED: false,
  TASK_VISIBILITY: "READ",
  CREATOR_ENABLE_CREATOR: true,
  CREATOR_ENABLE_REAFLOW_DIAGRAM: true,
  TASK_INDEXING: false,
  // OSS task search stores the workflow name as workflowType. conductor-ui
  // leaves this unset and queries workflowName.
  TASK_SEARCH_WORKFLOW_FIELD: "workflowType",
  // The task reference name is not on the OSS task index. conductor-ui leaves
  // this unset and keeps the filter.
  SHOW_TASK_REFERENCE_NAME: false,
  SHOW_EVENT_MONITOR: true,
  ENABLE_DARK_MODE_TOGGLE: true,

  // Enterprise Features - DISABLED for OSS
  TAG_VISIBILITY: false,
  WORKFLOW_INTROSPECTION: false,
  WORKFLOW_SUMMARIZE: false,
  HUMAN_TASK: false,
  INTEGRATIONS: false,
  SECRETS: false,
  WEBHOOKS: false,
  SERVICE_REGISTRY: false,
  GATEWAY_ENABLED: false,
  REMOTE_SERVICES: false,
  SENDGRID_TASK_ENABLED: false,
  SKU_ENABLED: false,

  // Conductor-Agents: nav, routes, workflow editor agent tasks, execution tabs.
  // Server may override via AgentSpanUiContextController when bundled with server.
  CONDUCTOR_INTEGRATIONS_AI_ENABLED: false,
  SHOW_AGENT: false,

  // External docs / help (Orkes links, API reference sidebar, header doc buttons)
  SHOW_DOCUMENTATION: false,
  SHOW_HELP_MENU: true,
  HELP_DOCS_URL:
    "https://confluence.freshworks.com/pages/viewpage.action?pageId=549619447&spaceKey=SHIEL&title=Onboarding%2Bto%2BCAAS",
  HELP_DOCS_LABEL: "Onboarding to CAAS",
  HELP_SUPPORT_URL:
    "https://freshworks.enterprise.slack.com/archives/C08SQ76HBQ9",
  SHOW_JOIN_SLACK_COMMUNITY: false,
  SHOW_FEEDBACK_FORM: false,
  SHOW_SUPPORT_FORM: false,

  // UI Configuration
  PLAYGROUND: false,
  ENABLE_METRICS_DASHBOARD: false,
  METRICS_ORIGIN_URL: "",
  CUSTOM_LOGO_URL: "",
  MULTITENANCY_TYPE: "user_based",
  DEFAULT_ROLES: "ADMIN",

  // Freshworks sandbox / auth-proxy: tenant dropdown + role gates (/api/userinfo, x-tenant-id).
  TENANT_SELECTION: true,
  // pnpm dev only: mock tenants when server/userinfo is down (ignored in production build).
  LOCAL_DEV_MOCK_USERINFO: true,

  // Hide Orkes footer (logo + platform version + copyright).
  SHOW_SIDEBAR_VERSION_BLOCK: false,

  // Browser tab icon — replace files under public/icons/freshworks/ if brand team supplies assets.
  FAVICON_URL: "/icons/freshworks/favicon.ico",
  DOCUMENT_TITLE: "Conductor",

  // Classic CRA UI: same host, cookie read by sandbox conductor-auth-proxy Lua.
  UI_VARIANT_COOKIE_ENABLED: true,
};

// No authentication configuration for OSS
window.authConfig = undefined;
window.auth0Identifiers = undefined;
