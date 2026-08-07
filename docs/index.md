# Conductor

Freshworks' fork of [Netflix Conductor OSS](https://github.com/conductor-oss/conductor), a microservices
orchestration engine for distributed and asynchronous workflows. It exposes REST/gRPC APIs for defining
and executing workflows, with pluggable persistence, messaging, and task integrations.

- **Owner:** cloud-engineering/workflow-engine
- **System:** cloud-engineering/conductor-as-a-service
- **Source:** [freshworksinc-oss/conductor](https://github.com/freshworksinc-oss/conductor)
- **Upstream:** [conductor-oss/conductor](https://github.com/conductor-oss/conductor)

## What this fork adds over upstream

- Multi-tenancy support across metadata, workflow/task search, and Redis/S3 storage
- Tenant-scoped metadata APIs (`/workflow/names`, `/workflow/{name}/versions`) and tenant context propagation
- Central event/webhook publishing for task status changes, including composite workflow status listeners
- Role-based access control for workflow and task definition APIs
- Pyroscope continuous profiling integration
- Operational fixes and performance work (paginated workflow definition listing, Lucene-syntax search, etc.)

See [Freshworks Fork: Customizations Over Upstream](freshworks-customizations.md) for a detailed
breakdown of each of these, including source files and configuration properties.

## Documentation

The [Developer's Guide](devguide/concepts/why.md) and [Documentation](documentation/api/index.md) sections
cover general Conductor concepts, architecture, and API/configuration reference inherited from upstream.
They are not yet updated with Freshworks-specific behavior described above.
