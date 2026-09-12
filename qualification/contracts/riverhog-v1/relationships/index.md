# Riverhog-centered authority relationships

[Atlas](../index.md)

This generated view explains how the frozen authorities fit together. It does not transfer ownership: every edge comes from release roles, dependency metadata, extension ownership, protocol ownership, image composition, or installation roots already present in executable authorities.

## Riverhog service boundary

| Layer | Authority | Purpose | Contract elements |
|---|---|---|---:|
| Public service | [riverhog](../authorities/riverhog/index.md) | Riverhog archive service. | 471 |
| Packaged implementation | [riverhog-server](components/riverhog-server.md) | Encrypted archive management, catalog, and retrieval. | 1 |

## Relationship shape

| Component role | Count |
|---|---:|
| `deployed_implementation` | 1 |
| `internal_build_unit` | 3 |
| `reference_application` | 6 |
| `reference_component` | 37 |
| `reusable_library` | 24 |

| Node kind | Count |
|---|---:|
| `component` | 71 |
| `extension-point` | 5 |
| `installation` | 1 |
| `process-protocol` | 4 |
| `runtime-image` | 13 |

| Relationship | Count |
|---|---:|
| `binds-protocol` | 4 |
| `depends-on` | 202 |
| `implements-extension-point` | 14 |
| `implements-protocol` | 11 |
| `installed-as` | 4 |
| `owns-extension-point` | 5 |
| `owns-protocol` | 4 |
| `packaged-in` | 16 |

## Drill down

- [Riverhog-owned reusable contracts and libraries](riverhog-libraries/index.md)
- [Independently implementable extension boundaries](extensions/index.md)
- [Runtime-image composition](runtime-images/index.md)
- [Installed end-user and recovery surfaces](installation/index.md)
- [Nonnormative reference ecosystem](references/index.md)
- [Complete component relationship inventory](components/index.md)

## Direct Riverhog product relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](components/http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [lifecycle-events](components/lifecycle-events.md) | `required` |
| outgoing | `depends-on` | [riverhog-age](components/riverhog-age.md) | `required` |
| outgoing | `depends-on` | [riverhog-application-access](components/riverhog-application-access.md) | `required` |
| outgoing | `depends-on` | [riverhog-archive-contracts](components/riverhog-archive-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](components/riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance](components/riverhog-provenance.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance-contracts](components/riverhog-provenance-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-protocol](components/riverhog-storage-adapter-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-support](components/riverhog-storage-adapter-support.md) | `required` |
| outgoing | `depends-on` | [state-schema](components/state-schema.md) | `required` |
| outgoing | `depends-on` | [time-formats](components/time-formats.md) | `required` |
| outgoing | `packaged-in` | `riverhog` | `` |
