# Riverhog repository v1 contract audit

> **Audit question:** Is this exactly the external contract the Riverhog repository should support for v1 — no more, no less?

This generated snapshot accounts for the externally exposed surfaces discovered from this repository revision. Discovery means inclusion; no separate acceptance decision is required.

Included contract elements: **4209** · Extent decisions: **2003**. Complete accounting does not establish desirable contracts, behavioral proof, freeze approval, or release readiness.

## Audit references

- [Governing policies](policies/index.md) — compatibility, publication, and extent rules; applicable contract elements link exact definitions.
- [Accounting checks](evidence/index.md) — closure results and reconciliation inventories.
- [Sources and qualifications](evidence/sources.md) — source bindings, candidate tests, and unestablished obligations; no executed attestation.
- [Configuration comparison](evidence/configuration.md) — owners, consumers, and default expressions across settings.
- [Declared relationships](evidence/relationships.md) — exact dependency, packaging, and extension joins.
- [Snapshot identities](evidence/identities.md) and [machine artifact (raw JSON)](../riverhog-v1.json?raw=1) — match semantic, accounting, trace, and presentation records.

**📦** Some guarantees for work spanning pages or chunks still lack supporting evidence. Follow the package marker for the affected guarantees and contracts. This records an evidence gap, not an observed bug; unmarked entries imply no approval.

## Authorities and interfaces

- [a-gogurt-linux-listener](authorities/a-gogurt-linux-listener/index.md) — Linux systemd user listener for Gogurt. Provider for extension: [gogurt.listener-host-providers](extensions/extension-point-gogurt-listener-host-providers.md).
  - [Configuration Environment](authorities/a-gogurt-linux-listener/configuration-environment/index.md) (2)
  - [Python](authorities/a-gogurt-linux-listener/python/index.md) (12)

- [a-gogurt-linux-volume](authorities/a-gogurt-linux-volume/index.md) — Linux mounted-volume provider for Gogurt. Provider for extension: [gogurt.mounted-volume-providers](extensions/extension-point-gogurt-mounted-volume-providers.md).
  - [Python](authorities/a-gogurt-linux-volume/python/index.md) (3)

- [a-gogurt-macos-listener](authorities/a-gogurt-macos-listener/index.md) — macOS launchd listener for Gogurt. Provider for extension: [gogurt.listener-host-providers](extensions/extension-point-gogurt-listener-host-providers.md).
  - [Python](authorities/a-gogurt-macos-listener/python/index.md) (12)

- [a-gogurt-macos-volume](authorities/a-gogurt-macos-volume/index.md) — macOS mounted-volume provider for Gogurt. Provider for extension: [gogurt.mounted-volume-providers](extensions/extension-point-gogurt-mounted-volume-providers.md).
  - [Python](authorities/a-gogurt-macos-volume/python/index.md) (3)

- [a-gogurt-windows-listener](authorities/a-gogurt-windows-listener/index.md) — Windows Task Scheduler listener for Gogurt. Provider for extension: [gogurt.listener-host-providers](extensions/extension-point-gogurt-listener-host-providers.md).
  - [Configuration Environment](authorities/a-gogurt-windows-listener/configuration-environment/index.md) (3)
  - [Python](authorities/a-gogurt-windows-listener/python/index.md) (16)

- [a-gogurt-windows-volume](authorities/a-gogurt-windows-volume/index.md) — Windows mounted-volume provider for Gogurt. Provider for extension: [gogurt.mounted-volume-providers](extensions/extension-point-gogurt-mounted-volume-providers.md).
  - [Python](authorities/a-gogurt-windows-volume/python/index.md) (3)

- [a-review0-materializer](authorities/a-review0-materializer/index.md) — Review0 materialization target for Stove0. Implements protocol: [stove0-target](extensions/process-protocol-stove0-target.md).
  - [CLI](authorities/a-review0-materializer/cli/index.md) (1)
  - [Configuration Environment](authorities/a-review0-materializer/configuration-environment/index.md) (10)
  - [Python](authorities/a-review0-materializer/python/index.md) (9)

- [a-review0-nvenc-av1-opus-sampler](authorities/a-review0-nvenc-av1-opus-sampler/index.md) — NVENC AV1 and Opus sampler for Review0. Implements protocol: [review0-sampler](extensions/process-protocol-review0-sampler.md).
  - [CLI](authorities/a-review0-nvenc-av1-opus-sampler/cli/index.md) (1)
  - [Configuration Environment](authorities/a-review0-nvenc-av1-opus-sampler/configuration-environment/index.md) (8)
  - [Python](authorities/a-review0-nvenc-av1-opus-sampler/python/index.md) (3)

- [a-review0-opus-sampler](authorities/a-review0-opus-sampler/index.md) — Opus sampler for Review0. Implements protocol: [review0-sampler](extensions/process-protocol-review0-sampler.md).
  - [CLI](authorities/a-review0-opus-sampler/cli/index.md) (1)
  - [Configuration Environment](authorities/a-review0-opus-sampler/configuration-environment/index.md) (8)
  - [Python](authorities/a-review0-opus-sampler/python/index.md) (3)

- [a-review0-rclone-target](authorities/a-review0-rclone-target/index.md) — Review0 rclone delivery target for Stove0. Implements protocol: [stove0-target](extensions/process-protocol-stove0-target.md).
  - [CLI](authorities/a-review0-rclone-target/cli/index.md) (1)
  - [Configuration Environment](authorities/a-review0-rclone-target/configuration-environment/index.md) (15)
  - [Python](authorities/a-review0-rclone-target/python/index.md) (11)

- [a-riverhog-aws-store](authorities/a-riverhog-aws-store/index.md) — AWS-backed Riverhog archive and retrieval store. Implements protocol: [riverhog-storage-adapter](extensions/process-protocol-riverhog-storage-adapter.md).
  - [CLI](authorities/a-riverhog-aws-store/cli/index.md) (1)
  - [Configuration Environment](authorities/a-riverhog-aws-store/configuration-environment/index.md) (30)
  - [Python](authorities/a-riverhog-aws-store/python/index.md) (7)

- [a-riverhog-b2-store](authorities/a-riverhog-b2-store/index.md) — Backblaze B2-backed Riverhog archive and retrieval store. Implements protocol: [riverhog-storage-adapter](extensions/process-protocol-riverhog-storage-adapter.md).
  - [CLI](authorities/a-riverhog-b2-store/cli/index.md) (1)
  - [Configuration Environment](authorities/a-riverhog-b2-store/configuration-environment/index.md) (20)

- [a-riverhog-cli](authorities/a-riverhog-cli/index.md) — Command-line client for Riverhog.
  - [CLI](authorities/a-riverhog-cli/cli/index.md) (86)
  - [Configuration Environment](authorities/a-riverhog-cli/configuration-environment/index.md) (7)

- [a-riverhog-cli-local](authorities/a-riverhog-cli-local/index.md) — Command-line client for Riverhog.
  - [Durable State](authorities/a-riverhog-cli-local/durable-state/index.md) (7)

- [a-riverhog-event-relay](authorities/a-riverhog-event-relay/index.md) — CloudEvents event relay for Riverhog.
  - [CLI](authorities/a-riverhog-event-relay/cli/index.md) (5)
  - [Configuration Documents](authorities/a-riverhog-event-relay/configuration/index.md) (1)

- [a-riverhog-event-relay-cursor](authorities/a-riverhog-event-relay-cursor/index.md) — CloudEvents event relay for Riverhog.
  - [Durable State](authorities/a-riverhog-event-relay-cursor/durable-state/index.md) (2)

- [a-riverhog-filesystem-store](authorities/a-riverhog-filesystem-store/index.md) — Filesystem-backed Riverhog archive and retrieval store. Implements protocol: [riverhog-storage-adapter](extensions/process-protocol-riverhog-storage-adapter.md).
  - [CLI](authorities/a-riverhog-filesystem-store/cli/index.md) (2)
  - [Configuration Environment](authorities/a-riverhog-filesystem-store/configuration-environment/index.md) (8)
  - [Python](authorities/a-riverhog-filesystem-store/python/index.md) (21)

- **[a-riverhog-ftp-spool](authorities/a-riverhog-ftp-spool/index.md)** [📦](authorities/a-riverhog-ftp-spool/evidence-gaps.md) — FTP upload spool and ingestion adapter for Riverhog.
  - **[HTTP Operations](authorities/a-riverhog-ftp-spool/http-operations/index.md)** [📦](authorities/a-riverhog-ftp-spool/http-operations/evidence-gaps.md) (5)
  - [HTTP Schemas](authorities/a-riverhog-ftp-spool/http-schemas/index.md) (5)
  - [HTTP Service Declaration](authorities/a-riverhog-ftp-spool/http-service-declaration/index.md) (1)
  - [HTTP Security Schemes](authorities/a-riverhog-ftp-spool/http-security-schemes/index.md) (1)
  - [CLI](authorities/a-riverhog-ftp-spool/cli/index.md) (7)
  - [Configuration Documents](authorities/a-riverhog-ftp-spool/configuration/index.md) (2)
  - [Configuration Environment](authorities/a-riverhog-ftp-spool/configuration-environment/index.md) (3)
  - [Python](authorities/a-riverhog-ftp-spool/python/index.md) (13)

- [a-riverhog-ftp-spool-client](authorities/a-riverhog-ftp-spool-client/index.md) — Client for the Riverhog FTP upload spool.
  - [Configuration Environment](authorities/a-riverhog-ftp-spool-client/configuration-environment/index.md) (5)
  - [Python](authorities/a-riverhog-ftp-spool-client/python/index.md) (11)

- [a-riverhog-ftp-spool-custody](authorities/a-riverhog-ftp-spool-custody/index.md) — FTP upload spool and ingestion adapter for Riverhog.
  - [Durable State](authorities/a-riverhog-ftp-spool-custody/durable-state/index.md) (6)

- [a-riverhog-linux-provenance-contract-lib](authorities/a-riverhog-linux-provenance-contract-lib/index.md) — Linux filesystem observation contracts for Riverhog provenance. Provider for extension: [riverhog.provenance-contracts](extensions/extension-point-riverhog-provenance-contracts.md).
  - [Schemas](authorities/a-riverhog-linux-provenance-contract-lib/schema/index.md) (5)
  - [Python](authorities/a-riverhog-linux-provenance-contract-lib/python/index.md) (4)

- [a-riverhog-macos-provenance-contract-lib](authorities/a-riverhog-macos-provenance-contract-lib/index.md) — macOS filesystem observation contracts for Riverhog provenance. Provider for extension: [riverhog.provenance-contracts](extensions/extension-point-riverhog-provenance-contracts.md).
  - [Schemas](authorities/a-riverhog-macos-provenance-contract-lib/schema/index.md) (4)
  - [Python](authorities/a-riverhog-macos-provenance-contract-lib/python/index.md) (4)

- [a-riverhog-recovery-tool](authorities/a-riverhog-recovery-tool/index.md) — Independent recovery tool for Riverhog archives.
  - [CLI](authorities/a-riverhog-recovery-tool/cli/index.md) (1)
  - [Python](authorities/a-riverhog-recovery-tool/python/index.md) (7)

- [a-riverhog-windows-provenance-contract-lib](authorities/a-riverhog-windows-provenance-contract-lib/index.md) — Windows filesystem observation contracts for Riverhog provenance. Provider for extension: [riverhog.provenance-contracts](extensions/extension-point-riverhog-provenance-contracts.md).
  - [Schemas](authorities/a-riverhog-windows-provenance-contract-lib/schema/index.md) (10)
  - [Python](authorities/a-riverhog-windows-provenance-contract-lib/python/index.md) (4)

- [a-stove0-cli](authorities/a-stove0-cli/index.md) — Command-line client for Stove0.
  - [CLI](authorities/a-stove0-cli/cli/index.md) (37)

- [a-stove0-exiftool-observer](authorities/a-stove0-exiftool-observer/index.md) — ExifTool media metadata observer for Stove0. Implements protocol: [stove0-observer](extensions/process-protocol-stove0-observer.md).
  - [CLI](authorities/a-stove0-exiftool-observer/cli/index.md) (1)
  - [Configuration Environment](authorities/a-stove0-exiftool-observer/configuration-environment/index.md) (8)
  - [Python](authorities/a-stove0-exiftool-observer/python/index.md) (4)

- [a-stove0-ffprobe-sampling-observer](authorities/a-stove0-ffprobe-sampling-observer/index.md) — FFprobe media sampling observer for Stove0. Implements protocol: [stove0-observer](extensions/process-protocol-stove0-observer.md).
  - [CLI](authorities/a-stove0-ffprobe-sampling-observer/cli/index.md) (1)
  - [Configuration Environment](authorities/a-stove0-ffprobe-sampling-observer/configuration-environment/index.md) (8)
  - [Python](authorities/a-stove0-ffprobe-sampling-observer/python/index.md) (4)

- [a-stove0-nvenc-av1-opus-target](authorities/a-stove0-nvenc-av1-opus-target/index.md) — NVENC AV1 and Opus transformation target for Stove0. Implements protocol: [stove0-target](extensions/process-protocol-stove0-target.md).
  - [CLI](authorities/a-stove0-nvenc-av1-opus-target/cli/index.md) (1)
  - [Configuration Environment](authorities/a-stove0-nvenc-av1-opus-target/configuration-environment/index.md) (10)
  - [Python](authorities/a-stove0-nvenc-av1-opus-target/python/index.md) (8)

- [a-stove0-opus-target](authorities/a-stove0-opus-target/index.md) — Opus transformation target for Stove0. Implements protocol: [stove0-target](extensions/process-protocol-stove0-target.md).
  - [CLI](authorities/a-stove0-opus-target/cli/index.md) (1)
  - [Configuration Environment](authorities/a-stove0-opus-target/configuration-environment/index.md) (9)
  - [Python](authorities/a-stove0-opus-target/python/index.md) (8)

- [extent-contract](authorities/extent-contract/index.md) — Repository-wide v1 external extent principles and rules.
  - [Extent Contract](authorities/extent-contract/extent/index.md) (12)

- [gogurt](authorities/gogurt/index.md) — Mounted-volume ingestion application for Riverhog.
  - [CLI](authorities/gogurt/cli/index.md) (21)

- [gogurt-core](authorities/gogurt-core/index.md) — Portable Gogurt marker, routing, action, and watch semantics.
  - [Configuration Documents](authorities/gogurt-core/configuration/index.md) (1)
  - [Python](authorities/gogurt-core/python/index.md) (43) — Defines extension: [gogurt.mounted-volume-providers](extensions/extension-point-gogurt-mounted-volume-providers.md).

- [gogurt-listener](authorities/gogurt-listener/index.md) — Portable durable listener runtime and native-platform port for Gogurt.
  - [Durable State](authorities/gogurt-listener/durable-state/index.md) (4)

- [gogurt-listener-runtime](authorities/gogurt-listener-runtime/index.md) — Portable durable listener runtime and native-platform port for Gogurt.
  - [Python](authorities/gogurt-listener-runtime/python/index.md) (55) — Defines extension: [gogurt.listener-host-providers](extensions/extension-point-gogurt-listener-host-providers.md).

- [gogurt-path-volume-support](authorities/gogurt-path-volume-support/index.md) — Path-mounted volume support shared by Gogurt providers.
  - [Python](authorities/gogurt-path-volume-support/python/index.md) (7)

- [http-api-contracts](authorities/http-api-contracts/index.md) — Public typed HTTP error, health, client, and operation contracts.
  - [Python](authorities/http-api-contracts/python/index.md) (63)

- [lifecycle-events](authorities/lifecycle-events/index.md) — Durable CloudEvents lifecycle log and client primitives.
  - [Python](authorities/lifecycle-events/python/index.md) (26)

- [release](authorities/release/index.md) — Coordinated v1 compatibility and publication promises.
  - [Runtime Images](authorities/release/runtime-images/index.md) (13)
  - [Python Distributions](authorities/release/python-distributions/index.md) (72)
  - [Installation Roots](authorities/release/installation-roots/index.md) (4)
  - [Release Artifacts](authorities/release/release-artifacts/index.md) (12)
  - [Publication Locations](authorities/release/publication-locations/index.md) (2)
  - [Artifact Verification](authorities/release/artifact-verification/index.md) (3)
  - [Versioning and Tags](authorities/release/versioning-tags/index.md) (5)
  - [Compatibility Guarantees](authorities/release/compatibility-guarantees/index.md) (9)
  - [Publication policies](policies/publication/index.md) — 3 policy-owned promises

- [review0-planner](authorities/review0-planner/index.md) — Review0 plan construction for Stove0 workflows.
  - [CLI](authorities/review0-planner/cli/index.md) (1)
  - [Python](authorities/review0-planner/python/index.md) (4)

- [review0-sampler-client](authorities/review0-sampler-client/index.md) — HTTP client for Review0 samplers.
  - [Python](authorities/review0-sampler-client/python/index.md) (7)

- [review0-sampler-lib](authorities/review0-sampler-lib/index.md) — Runtime and conformance support for Review0 samplers.
  - [Process Protocol](authorities/review0-sampler-lib/process-protocol/index.md) (1)
  - [Process Protocol Operations](authorities/review0-sampler-lib/process-protocol-operations/index.md) (2)
  - [Process Protocol Schemas](authorities/review0-sampler-lib/process-protocol-schemas/index.md) (5)
  - [CLI](authorities/review0-sampler-lib/cli/index.md) (2)
  - [Python](authorities/review0-sampler-lib/python/index.md) (21)

- [review0-sampler-protocol](authorities/review0-sampler-protocol/index.md) — Protocol contracts for Review0 samplers.
  - [Python](authorities/review0-sampler-protocol/python/index.md) (36) — Defines protocol: [review0-sampler](extensions/process-protocol-review0-sampler.md).

- [review0-target-contracts](authorities/review0-target-contracts/index.md) — Review0 target execution and artifact contracts.
  - [Python](authorities/review0-target-contracts/python/index.md) (26)

- [review0-target-lib](authorities/review0-target-lib/index.md) — Shared runtime support for Review0 targets.
  - [Configuration Documents](authorities/review0-target-lib/configuration/index.md) (1)
  - [Python](authorities/review0-target-lib/python/index.md) (20)

- **[riverhog](authorities/riverhog/index.md)** [📦](authorities/riverhog/evidence-gaps.md) — The Riverhog service API and its maintained cross-interface operation parity.
  - **[HTTP Operations](authorities/riverhog/http-operations/index.md)** [📦](authorities/riverhog/http-operations/evidence-gaps.md) (109)
  - **[HTTP Schemas](authorities/riverhog/http-schemas/index.md)** [📦](authorities/riverhog/http-schemas/evidence-gaps.md) (257)
  - [HTTP Service Declaration](authorities/riverhog/http-service-declaration/index.md) (1)
  - [HTTP Security Schemes](authorities/riverhog/http-security-schemes/index.md) (1)

- [riverhog-age](authorities/riverhog-age/index.md) — Resumable age encryption used by the Riverhog protocol.
  - [Python](authorities/riverhog-age/python/index.md) (34)

- [riverhog-application-access](authorities/riverhog-application-access/index.md) — Public Riverhog application-access contracts and canonical grant grammar.
  - [Python](authorities/riverhog-application-access/python/index.md) (47)

- **[riverhog-archive-contracts](authorities/riverhog-archive-contracts/index.md)** [📦](authorities/riverhog-archive-contracts/evidence-gaps.md) — Dependency-light immutable Riverhog archive recovery contracts.
  - **[Schemas](authorities/riverhog-archive-contracts/schema/index.md)** [📦](authorities/riverhog-archive-contracts/schema/evidence-gaps.md) (4)
  - [Python](authorities/riverhog-archive-contracts/python/index.md) (72)

- [riverhog-canonical-json](authorities/riverhog-canonical-json/index.md) — Shared RFC 8785 JSON value and exact-scalar contracts.
  - [Python](authorities/riverhog-canonical-json/python/index.md) (13)

- [riverhog-catalog](authorities/riverhog-catalog/index.md) — Encrypted archive management, catalog, and retrieval.
  - [Durable State](authorities/riverhog-catalog/durable-state/index.md) (91)

- [riverhog-client](authorities/riverhog-client/index.md) — Typed generic Riverhog client and capability-scoped collection-processing runtime.
  - [Configuration Environment](authorities/riverhog-client/configuration-environment/index.md) (12)
  - [Python](authorities/riverhog-client/python/index.md) (268)

- [riverhog-protocol](authorities/riverhog-protocol/index.md) — Canonical Riverhog wire and identity contracts.
  - [Schemas](authorities/riverhog-protocol/schema/index.md) (1)
  - [Python](authorities/riverhog-protocol/python/index.md) (324)

- **[riverhog-provenance](authorities/riverhog-provenance/index.md)** [📦](authorities/riverhog-provenance/evidence-gaps.md) — Portable Riverhog v1 per-file provenance journals and validation.
  - **[Schemas](authorities/riverhog-provenance/schema/index.md)** [📦](authorities/riverhog-provenance/schema/evidence-gaps.md) (7)
  - [Configuration Environment](authorities/riverhog-provenance/configuration-environment/index.md) (3)
  - [Python](authorities/riverhog-provenance/python/index.md) (105) — Defines extension: [riverhog.provenance-observers](extensions/extension-point-riverhog-provenance-observers.md).

- [riverhog-provenance-contracts](authorities/riverhog-provenance-contracts/index.md) — Canonical Riverhog provenance identity and reference contracts.
  - [Python](authorities/riverhog-provenance-contracts/python/index.md) (16) — Defines extension: [riverhog.provenance-contracts](extensions/extension-point-riverhog-provenance-contracts.md).

- [riverhog-provenance-installation](authorities/riverhog-provenance-installation/index.md) — Portable Riverhog v1 per-file provenance journals and validation.
  - [Durable State](authorities/riverhog-provenance-installation/durable-state/index.md) (1)

- [riverhog-server](authorities/riverhog-server/index.md) — Encrypted archive management, catalog, and retrieval.
  - [CLI](authorities/riverhog-server/cli/index.md) (5)
  - [Configuration Environment](authorities/riverhog-server/configuration-environment/index.md) (52)

- [riverhog-storage-adapter-asgi-support](authorities/riverhog-storage-adapter-asgi-support/index.md) — Authenticated ASGI shell for independently scoped Riverhog storage adapters.
  - [Python](authorities/riverhog-storage-adapter-asgi-support/python/index.md) (1)

- [riverhog-storage-adapter-protocol](authorities/riverhog-storage-adapter-protocol/index.md) — Provider-neutral opaque-object capability contracts for Riverhog storage adapters.
  - [Python](authorities/riverhog-storage-adapter-protocol/python/index.md) (121) — Defines protocol: [riverhog-storage-adapter](extensions/process-protocol-riverhog-storage-adapter.md).

- [riverhog-storage-adapter-s3-support](authorities/riverhog-storage-adapter-s3-support/index.md) — S3 store support shared by Riverhog storage adapters.
  - [Python](authorities/riverhog-storage-adapter-s3-support/python/index.md) (24)

- **[riverhog-storage-adapter-support](authorities/riverhog-storage-adapter-support/index.md)** [📦](authorities/riverhog-storage-adapter-support/evidence-gaps.md) — HTTP binding and conformance support for Riverhog storage adapters.
  - [Process Protocol](authorities/riverhog-storage-adapter-support/process-protocol/index.md) (1)
  - [Process Protocol Operations](authorities/riverhog-storage-adapter-support/process-protocol-operations/index.md) (15)
  - **[Process Protocol Schemas](authorities/riverhog-storage-adapter-support/process-protocol-schemas/index.md)** [📦](authorities/riverhog-storage-adapter-support/process-protocol-schemas/evidence-gaps.md) (23)
  - [CLI](authorities/riverhog-storage-adapter-support/cli/index.md) (2)
  - [Python](authorities/riverhog-storage-adapter-support/python/index.md) (45)

- [state-schema](authorities/state-schema/index.md) — Forward-only relational state schema and migration contracts.
  - [Python](authorities/state-schema/python/index.md) (58)

- **[stove0](authorities/stove0/index.md)** [📦](authorities/stove0/evidence-gaps.md) — The Stove0 application API and its maintained cross-interface operation parity.
  - **[HTTP Operations](authorities/stove0/http-operations/index.md)** [📦](authorities/stove0/http-operations/evidence-gaps.md) (33)
  - **[HTTP Schemas](authorities/stove0/http-schemas/index.md)** [📦](authorities/stove0/http-schemas/evidence-gaps.md) (137)
  - [HTTP Service Declaration](authorities/stove0/http-service-declaration/index.md) (1)

- [stove0-api-client](authorities/stove0-api-client/index.md) — Official Python client for the Stove0 v1 workflow API.
  - [Configuration Environment](authorities/stove0-api-client/configuration-environment/index.md) (5)
  - [Python](authorities/stove0-api-client/python/index.md) (34)

- [stove0-control](authorities/stove0-control/index.md) — Content-opaque transformation application for Riverhog.
  - [Durable State](authorities/stove0-control/durable-state/index.md) (23)

- [stove0-media-archive-target-contracts](authorities/stove0-media-archive-target-contracts/index.md) — Shared media archive target contracts for Stove0.
  - [Python](authorities/stove0-media-archive-target-contracts/python/index.md) (30)

- [stove0-media-archive-target-support](authorities/stove0-media-archive-target-support/index.md) — Shared media archive projection support for Stove0.
  - [Python](authorities/stove0-media-archive-target-support/python/index.md) (24)

- [stove0-media-metadata-observer-contracts](authorities/stove0-media-metadata-observer-contracts/index.md) — Shared media metadata observation contracts for Stove0. Provider for extension: [stove0.observer-semantic-validators](extensions/extension-point-stove0-observer-semantic-validators.md).
  - [Python](authorities/stove0-media-metadata-observer-contracts/python/index.md) (19)

- [stove0-media-sampling-observer-contracts](authorities/stove0-media-sampling-observer-contracts/index.md) — Shared media sampling contracts for Stove0. Provider for extension: [stove0.observer-semantic-validators](extensions/extension-point-stove0-observer-semantic-validators.md).
  - [Python](authorities/stove0-media-sampling-observer-contracts/python/index.md) (16)

- [stove0-observer-client](authorities/stove0-observer-client/index.md) — Narrow HTTP client for Stove0 content observers.
  - [Python](authorities/stove0-observer-client/python/index.md) (6) — Defines extension: [stove0.observer-semantic-validators](extensions/extension-point-stove0-observer-semantic-validators.md).

- [stove0-observer-protocol](authorities/stove0-observer-protocol/index.md) — Dependency-light public contracts for external Stove0 content observers.
  - [Python](authorities/stove0-observer-protocol/python/index.md) (81) — Defines protocol: [stove0-observer](extensions/process-protocol-stove0-observer.md).

- [stove0-observer-support](authorities/stove0-observer-support/index.md) — External-author protocol, runtime, and conformance support for Stove0 content observers.
  - [Process Protocol](authorities/stove0-observer-support/process-protocol/index.md) (1)
  - [Process Protocol Operations](authorities/stove0-observer-support/process-protocol-operations/index.md) (2)
  - [Process Protocol Schemas](authorities/stove0-observer-support/process-protocol-schemas/index.md) (7)
  - [CLI](authorities/stove0-observer-support/cli/index.md) (2)
  - [Python](authorities/stove0-observer-support/python/index.md) (37)

- [stove0-operator-contracts](authorities/stove0-operator-contracts/index.md) — Canonical public state contracts for the Stove0 v1 operator surface.
  - [Configuration Documents](authorities/stove0-operator-contracts/configuration/index.md) (1)
  - [Python](authorities/stove0-operator-contracts/python/index.md) (124)

- [stove0-protocol](authorities/stove0-protocol/index.md) — Canonical content-opaque collection orchestration contracts for Stove0.
  - [Python](authorities/stove0-protocol/python/index.md) (205)

- [stove0-recipe-config](authorities/stove0-recipe-config/index.md) — Portable deployment-owned Stove0 recipe catalog contracts and validation.
  - [Configuration Documents](authorities/stove0-recipe-config/configuration/index.md) (1)
  - [Python](authorities/stove0-recipe-config/python/index.md) (30)

- [stove0-server](authorities/stove0-server/index.md) — Content-opaque transformation application for Riverhog.
  - [CLI](authorities/stove0-server/cli/index.md) (7)
  - [Configuration Environment](authorities/stove0-server/configuration-environment/index.md) (25)
  - [Python](authorities/stove0-server/python/index.md) (324)

- [stove0-target-client](authorities/stove0-target-client/index.md) — Narrow HTTP client for Stove0 transform targets.
  - [Python](authorities/stove0-target-client/python/index.md) (15)

- [stove0-target-jobs](authorities/stove0-target-jobs/index.md) — Hardware-neutral target protocol, runtime, and conformance support for Stove0.
  - [Durable State](authorities/stove0-target-jobs/durable-state/index.md) (3)

- [stove0-target-protocol](authorities/stove0-target-protocol/index.md) — Dependency-light public contracts for external Stove0 targets.
  - [Python](authorities/stove0-target-protocol/python/index.md) (136) — Defines protocol: [stove0-target](extensions/process-protocol-stove0-target.md).

- [stove0-target-support](authorities/stove0-target-support/index.md) — Hardware-neutral target protocol, runtime, and conformance support for Stove0.
  - [Process Protocol](authorities/stove0-target-support/process-protocol/index.md) (1)
  - [Process Protocol Operations](authorities/stove0-target-support/process-protocol-operations/index.md) (5)
  - [Process Protocol Schemas](authorities/stove0-target-support/process-protocol-schemas/index.md) (8)
  - [CLI](authorities/stove0-target-support/cli/index.md) (2)
  - [Configuration Environment](authorities/stove0-target-support/configuration-environment/index.md) (1)
  - [Python](authorities/stove0-target-support/python/index.md) (151)
