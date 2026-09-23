# Riverhog repository v1 contract audit

> **Audit question:** Is this exactly the external contract the Riverhog repository should support for v1 — no more, no less?

This generated snapshot accounts for the externally exposed surfaces discovered from this repository revision. Discovery means inclusion; no separate acceptance decision is required.

Included contract elements: **4192** · Extent decisions: **2057**. Complete accounting does not establish desirable contracts, behavioral proof, freeze approval, or release readiness.

Checked-in references form a closed, tightly scoped, maintainer-selected, nonnormative conformance set.

## Audit references

- [Governing policies](policies/index.md) — compatibility, publication, and extent rules; applicable contract elements link exact definitions.
- [Accounting checks](evidence/index.md) — closure results and reconciliation inventories.
- [Sources and qualifications](evidence/sources.md) — source bindings, candidate tests, and unestablished obligations; no executed attestation.
- [Configuration comparison](evidence/configuration.md) — owners, consumers, and default expressions across settings.
- [Declared relationships](evidence/relationships.md) — exact dependency, packaging, and extension joins.
- [Snapshot identities](evidence/identities.md) and [machine artifact (raw JSON)](../riverhog-v1.json?raw=1) — match semantic, accounting, trace, and presentation records.

**📦** Some guarantees for work spanning pages or chunks still lack supporting evidence. Follow the package marker for the affected guarantees and contracts. This records an evidence gap, not an observed bug; unmarked entries imply no approval.

## Authorities and interfaces

- [extent-contract](authorities/extent-contract/index.md) — Repository-wide v1 external extent principles and rules.
  - [Extent Contract](authorities/extent-contract/extent/index.md) (12)

- [gogurt](authorities/gogurt/index.md) — Optional nonnormative mounted-volume ingestion reference application for Riverhog.
  - [CLI](authorities/gogurt/cli/index.md) (21)

- [gogurt-core](authorities/gogurt-core/index.md) — Portable Gogurt marker, routing, action, and watch semantics.
  - [Configuration Documents](authorities/gogurt-core/configuration/index.md) (1)
  - [Python](authorities/gogurt-core/python/index.md) (43) — Defines extension: [gogurt.mounted-volume-providers](extensions/extension-point-gogurt-mounted-volume-providers.md).

- [gogurt-linux-listener-host](authorities/gogurt-linux-listener-host/index.md) — Optional nonnormative Linux systemd-user listener-host reference for Gogurt. Provider for extension: [gogurt.listener-host-providers](extensions/extension-point-gogurt-listener-host-providers.md).
  - [Configuration Environment](authorities/gogurt-linux-listener-host/configuration-environment/index.md) (2)
  - [Python](authorities/gogurt-linux-listener-host/python/index.md) (12)

- [gogurt-linux-mounted-volume](authorities/gogurt-linux-mounted-volume/index.md) — Optional nonnormative Linux mounted-volume reference for Gogurt. Provider for extension: [gogurt.mounted-volume-providers](extensions/extension-point-gogurt-mounted-volume-providers.md).
  - [Python](authorities/gogurt-linux-mounted-volume/python/index.md) (3)

- [gogurt-listener](authorities/gogurt-listener/index.md) — Portable durable listener runtime and native-platform port for Gogurt.
  - [Durable State](authorities/gogurt-listener/durable-state/index.md) (4)

- [gogurt-listener-runtime](authorities/gogurt-listener-runtime/index.md) — Portable durable listener runtime and native-platform port for Gogurt.
  - [Python](authorities/gogurt-listener-runtime/python/index.md) (55) — Defines extension: [gogurt.listener-host-providers](extensions/extension-point-gogurt-listener-host-providers.md).

- [gogurt-macos-listener-host](authorities/gogurt-macos-listener-host/index.md) — Optional nonnormative macOS launchd listener-host reference for Gogurt. Provider for extension: [gogurt.listener-host-providers](extensions/extension-point-gogurt-listener-host-providers.md).
  - [Python](authorities/gogurt-macos-listener-host/python/index.md) (12)

- [gogurt-macos-mounted-volume](authorities/gogurt-macos-mounted-volume/index.md) — Optional nonnormative macOS mounted-volume reference for Gogurt. Provider for extension: [gogurt.mounted-volume-providers](extensions/extension-point-gogurt-mounted-volume-providers.md).
  - [Python](authorities/gogurt-macos-mounted-volume/python/index.md) (3)

- [gogurt-path-volume-support](authorities/gogurt-path-volume-support/index.md) — Optional nonnormative path-mounted-volume support for Gogurt reference providers.
  - [Python](authorities/gogurt-path-volume-support/python/index.md) (7)

- [gogurt-windows-listener-host](authorities/gogurt-windows-listener-host/index.md) — Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt. Provider for extension: [gogurt.listener-host-providers](extensions/extension-point-gogurt-listener-host-providers.md).
  - [Configuration Environment](authorities/gogurt-windows-listener-host/configuration-environment/index.md) (3)
  - [Python](authorities/gogurt-windows-listener-host/python/index.md) (16)

- [gogurt-windows-mounted-volume](authorities/gogurt-windows-mounted-volume/index.md) — Optional nonnormative Windows mounted-volume reference for Gogurt. Provider for extension: [gogurt.mounted-volume-providers](extensions/extension-point-gogurt-mounted-volume-providers.md).
  - [Python](authorities/gogurt-windows-mounted-volume/python/index.md) (3)

- [http-api-contracts](authorities/http-api-contracts/index.md) — Public typed HTTP error, health, client, and operation contracts.
  - [Python](authorities/http-api-contracts/python/index.md) (63)

- [lifecycle-events](authorities/lifecycle-events/index.md) — Durable CloudEvents lifecycle log and client primitives.
  - [Python](authorities/lifecycle-events/python/index.md) (26)

- [mango-fish](authorities/mango-fish/index.md) — Optional nonnormative CloudEvents reference application for Riverhog.
  - [CLI](authorities/mango-fish/cli/index.md) (5)
  - [Configuration Documents](authorities/mango-fish/configuration/index.md) (1)

- [mango-fish-cursor](authorities/mango-fish-cursor/index.md) — Optional nonnormative CloudEvents reference application for Riverhog.
  - [Durable State](authorities/mango-fish-cursor/durable-state/index.md) (2)

- [piggity](authorities/piggity/index.md) — Optional nonnormative Piggity reference client for Riverhog.
  - [CLI](authorities/piggity/cli/index.md) (86)
  - [Configuration Environment](authorities/piggity/configuration-environment/index.md) (7)

- [piggity-local](authorities/piggity-local/index.md) — Optional nonnormative Piggity reference client for Riverhog.
  - [Durable State](authorities/piggity-local/durable-state/index.md) (7)

- [release](authorities/release/index.md) — Coordinated v1 compatibility and publication promises.
  - [Runtime Images](authorities/release/runtime-images/index.md) (13)
  - [Python Distributions](authorities/release/python-distributions/index.md) (71)
  - [Installation Roots](authorities/release/installation-roots/index.md) (4)
  - [Release Artifacts](authorities/release/release-artifacts/index.md) (12)
  - [Publication Locations](authorities/release/publication-locations/index.md) (2)
  - [Artifact Verification](authorities/release/artifact-verification/index.md) (3)
  - [Versioning and Tags](authorities/release/versioning-tags/index.md) (5)
  - [Compatibility Guarantees](authorities/release/compatibility-guarantees/index.md) (9)
  - [Publication policies](policies/publication/index.md) — 3 policy-owned promises

- **[riverhog](authorities/riverhog/index.md)** [📦](authorities/riverhog/evidence-gaps.md) — The Riverhog service API and its maintained cross-interface operation parity.
  - **[HTTP Operations](authorities/riverhog/http-operations/index.md)** [📦](authorities/riverhog/http-operations/evidence-gaps.md) (109)
  - **[HTTP Schemas](authorities/riverhog/http-schemas/index.md)** [📦](authorities/riverhog/http-schemas/evidence-gaps.md) (253)
  - [HTTP Service Declaration](authorities/riverhog/http-service-declaration/index.md) (1)
  - [HTTP Security Schemes](authorities/riverhog/http-security-schemes/index.md) (1)

- [riverhog-age](authorities/riverhog-age/index.md) — Resumable age encryption used by the Riverhog protocol.
  - [Python](authorities/riverhog-age/python/index.md) (34)

- [riverhog-application-access](authorities/riverhog-application-access/index.md) — Public Riverhog application-access contracts and canonical grant grammar.
  - [Python](authorities/riverhog-application-access/python/index.md) (47)

- **[riverhog-archive-contracts](authorities/riverhog-archive-contracts/index.md)** [📦](authorities/riverhog-archive-contracts/evidence-gaps.md) — Dependency-light immutable Riverhog archive recovery contracts.
  - **[Schemas](authorities/riverhog-archive-contracts/schema/index.md)** [📦](authorities/riverhog-archive-contracts/schema/evidence-gaps.md) (4)
  - [Python](authorities/riverhog-archive-contracts/python/index.md) (72)

- [riverhog-catalog](authorities/riverhog-catalog/index.md) — Encrypted archive management, catalog, and retrieval.
  - [Durable State](authorities/riverhog-catalog/durable-state/index.md) (91)

- [riverhog-client](authorities/riverhog-client/index.md) — Typed generic Riverhog client and capability-scoped collection-processing runtime.
  - [Configuration Environment](authorities/riverhog-client/configuration-environment/index.md) (12)
  - [Python](authorities/riverhog-client/python/index.md) (269)

- **[riverhog-ftp-adapter](authorities/riverhog-ftp-adapter/index.md)** [📦](authorities/riverhog-ftp-adapter/evidence-gaps.md) — Optional nonnormative FTP ingress reference for Riverhog.
  - **[HTTP Operations](authorities/riverhog-ftp-adapter/http-operations/index.md)** [📦](authorities/riverhog-ftp-adapter/http-operations/evidence-gaps.md) (5)
  - [HTTP Schemas](authorities/riverhog-ftp-adapter/http-schemas/index.md) (5)
  - [HTTP Service Declaration](authorities/riverhog-ftp-adapter/http-service-declaration/index.md) (1)
  - [HTTP Security Schemes](authorities/riverhog-ftp-adapter/http-security-schemes/index.md) (1)
  - [CLI](authorities/riverhog-ftp-adapter/cli/index.md) (7)
  - [Configuration Documents](authorities/riverhog-ftp-adapter/configuration/index.md) (2)
  - [Configuration Environment](authorities/riverhog-ftp-adapter/configuration-environment/index.md) (3)
  - [Python](authorities/riverhog-ftp-adapter/python/index.md) (13)

- [riverhog-ftp-adapter-api-client](authorities/riverhog-ftp-adapter-api-client/index.md) — Optional nonnormative client for the Riverhog FTP ingress reference.
  - [Configuration Environment](authorities/riverhog-ftp-adapter-api-client/configuration-environment/index.md) (5)
  - [Python](authorities/riverhog-ftp-adapter-api-client/python/index.md) (11)

- [riverhog-ftp-custody](authorities/riverhog-ftp-custody/index.md) — Optional nonnormative FTP ingress reference for Riverhog.
  - [Durable State](authorities/riverhog-ftp-custody/durable-state/index.md) (6)

- [riverhog-protocol](authorities/riverhog-protocol/index.md) — Canonical Riverhog wire and identity contracts.
  - [Schemas](authorities/riverhog-protocol/schema/index.md) (1)
  - [Python](authorities/riverhog-protocol/python/index.md) (323)

- **[riverhog-provenance](authorities/riverhog-provenance/index.md)** [📦](authorities/riverhog-provenance/evidence-gaps.md) — Portable Riverhog v1 per-file provenance journals and validation.
  - **[Schemas](authorities/riverhog-provenance/schema/index.md)** [📦](authorities/riverhog-provenance/schema/evidence-gaps.md) (7)
  - [Configuration Environment](authorities/riverhog-provenance/configuration-environment/index.md) (3)
  - [Python](authorities/riverhog-provenance/python/index.md) (105) — Defines extension: [riverhog.provenance-observers](extensions/extension-point-riverhog-provenance-observers.md).

- [riverhog-provenance-contracts](authorities/riverhog-provenance-contracts/index.md) — Canonical Riverhog provenance identity and reference contracts.
  - [Python](authorities/riverhog-provenance-contracts/python/index.md) (16) — Defines extension: [riverhog.provenance-contracts](extensions/extension-point-riverhog-provenance-contracts.md).

- [riverhog-provenance-installation](authorities/riverhog-provenance-installation/index.md) — Portable Riverhog v1 per-file provenance journals and validation.
  - [Durable State](authorities/riverhog-provenance-installation/durable-state/index.md) (1)

- [riverhog-provenance-linux-contracts](authorities/riverhog-provenance-linux-contracts/index.md) — Optional nonnormative Linux observation-contract reference for Riverhog provenance. Provider for extension: [riverhog.provenance-contracts](extensions/extension-point-riverhog-provenance-contracts.md).
  - [Schemas](authorities/riverhog-provenance-linux-contracts/schema/index.md) (5)
  - [Python](authorities/riverhog-provenance-linux-contracts/python/index.md) (4)

- [riverhog-provenance-macos-contracts](authorities/riverhog-provenance-macos-contracts/index.md) — Optional nonnormative macOS observation-contract reference for Riverhog provenance. Provider for extension: [riverhog.provenance-contracts](extensions/extension-point-riverhog-provenance-contracts.md).
  - [Schemas](authorities/riverhog-provenance-macos-contracts/schema/index.md) (4)
  - [Python](authorities/riverhog-provenance-macos-contracts/python/index.md) (4)

- [riverhog-provenance-windows-contracts](authorities/riverhog-provenance-windows-contracts/index.md) — Optional nonnormative Windows observation-contract reference for Riverhog provenance. Provider for extension: [riverhog.provenance-contracts](extensions/extension-point-riverhog-provenance-contracts.md).
  - [Schemas](authorities/riverhog-provenance-windows-contracts/schema/index.md) (10)
  - [Python](authorities/riverhog-provenance-windows-contracts/python/index.md) (4)

- [riverhog-recover](authorities/riverhog-recover/index.md) — Optional nonnormative independent recovery reference application for Riverhog archives.
  - [CLI](authorities/riverhog-recover/cli/index.md) (1)
  - [Python](authorities/riverhog-recover/python/index.md) (7)

- [riverhog-server](authorities/riverhog-server/index.md) — Encrypted archive management, catalog, and retrieval.
  - [CLI](authorities/riverhog-server/cli/index.md) (5)
  - [Configuration Environment](authorities/riverhog-server/configuration-environment/index.md) (52)

- [riverhog-storage-adapter-asgi-support](authorities/riverhog-storage-adapter-asgi-support/index.md) — Authenticated ASGI shell for independently scoped Riverhog storage adapters.
  - [Python](authorities/riverhog-storage-adapter-asgi-support/python/index.md) (1)

- [riverhog-storage-adapter-aws](authorities/riverhog-storage-adapter-aws/index.md) — Optional nonnormative AWS storage reference for Riverhog. Implements protocol: [riverhog-storage-adapter](extensions/process-protocol-riverhog-storage-adapter.md).
  - [CLI](authorities/riverhog-storage-adapter-aws/cli/index.md) (1)
  - [Configuration Environment](authorities/riverhog-storage-adapter-aws/configuration-environment/index.md) (30)
  - [Python](authorities/riverhog-storage-adapter-aws/python/index.md) (7)

- [riverhog-storage-adapter-backblaze](authorities/riverhog-storage-adapter-backblaze/index.md) — Optional nonnormative Backblaze B2 storage reference for Riverhog. Implements protocol: [riverhog-storage-adapter](extensions/process-protocol-riverhog-storage-adapter.md).
  - [CLI](authorities/riverhog-storage-adapter-backblaze/cli/index.md) (1)
  - [Configuration Environment](authorities/riverhog-storage-adapter-backblaze/configuration-environment/index.md) (20)

- [riverhog-storage-adapter-filesystem](authorities/riverhog-storage-adapter-filesystem/index.md) — Optional nonnormative Linux filesystem storage reference for Riverhog. Implements protocol: [riverhog-storage-adapter](extensions/process-protocol-riverhog-storage-adapter.md).
  - [CLI](authorities/riverhog-storage-adapter-filesystem/cli/index.md) (2)
  - [Configuration Environment](authorities/riverhog-storage-adapter-filesystem/configuration-environment/index.md) (8)
  - [Python](authorities/riverhog-storage-adapter-filesystem/python/index.md) (21)

- [riverhog-storage-adapter-protocol](authorities/riverhog-storage-adapter-protocol/index.md) — Provider-neutral opaque-object capability contracts for Riverhog storage adapters.
  - [Python](authorities/riverhog-storage-adapter-protocol/python/index.md) (121) — Defines protocol: [riverhog-storage-adapter](extensions/process-protocol-riverhog-storage-adapter.md).

- [riverhog-storage-adapter-s3-support](authorities/riverhog-storage-adapter-s3-support/index.md) — Optional nonnormative S3 support for Riverhog storage references.
  - [Python](authorities/riverhog-storage-adapter-s3-support/python/index.md) (24)

- **[riverhog-storage-adapter-support](authorities/riverhog-storage-adapter-support/index.md)** [📦](authorities/riverhog-storage-adapter-support/evidence-gaps.md) — HTTP binding and conformance support for Riverhog storage adapters.
  - [Process Protocol](authorities/riverhog-storage-adapter-support/process-protocol/index.md) (1)
  - [Process Protocol Operations](authorities/riverhog-storage-adapter-support/process-protocol-operations/index.md) (15)
  - **[Process Protocol Schemas](authorities/riverhog-storage-adapter-support/process-protocol-schemas/index.md)** [📦](authorities/riverhog-storage-adapter-support/process-protocol-schemas/evidence-gaps.md) (23)
  - [CLI](authorities/riverhog-storage-adapter-support/cli/index.md) (2)
  - [Python](authorities/riverhog-storage-adapter-support/python/index.md) (45)

- [state-schema](authorities/state-schema/index.md) — Forward-only relational state schema and migration contracts.
  - [Python](authorities/state-schema/python/index.md) (58)

- **[stove0](authorities/stove0/index.md)** [📦](authorities/stove0/evidence-gaps.md) — The Stove0 reference application API and its maintained cross-interface operation parity.
  - **[HTTP Operations](authorities/stove0/http-operations/index.md)** [📦](authorities/stove0/http-operations/evidence-gaps.md) (33)
  - **[HTTP Schemas](authorities/stove0/http-schemas/index.md)** [📦](authorities/stove0/http-schemas/evidence-gaps.md) (136)
  - [HTTP Service Declaration](authorities/stove0/http-service-declaration/index.md) (1)

- [stove0-api-client](authorities/stove0-api-client/index.md) — Official Python client for the stove0 v1 workflow API.
  - [Configuration Environment](authorities/stove0-api-client/configuration-environment/index.md) (5)
  - [Python](authorities/stove0-api-client/python/index.md) (34)

- [stove0-client](authorities/stove0-client/index.md) — Optional nonnormative command-line client for the Stove0 reference application.
  - [CLI](authorities/stove0-client/cli/index.md) (37)

- [stove0-control](authorities/stove0-control/index.md) — Optional nonnormative content-opaque transformation reference application for Riverhog.
  - [Durable State](authorities/stove0-control/durable-state/index.md) (23)

- [stove0-exiftool-observer](authorities/stove0-exiftool-observer/index.md) — Optional nonnormative ExifTool observer reference for Stove0. Implements protocol: [stove0-observer](extensions/process-protocol-stove0-observer.md).
  - [CLI](authorities/stove0-exiftool-observer/cli/index.md) (1)
  - [Configuration Environment](authorities/stove0-exiftool-observer/configuration-environment/index.md) (8)
  - [Python](authorities/stove0-exiftool-observer/python/index.md) (4)

- [stove0-ffprobe-sampling-observer](authorities/stove0-ffprobe-sampling-observer/index.md) — Optional nonnormative FFprobe sampling-observer reference for Stove0. Implements protocol: [stove0-observer](extensions/process-protocol-stove0-observer.md).
  - [CLI](authorities/stove0-ffprobe-sampling-observer/cli/index.md) (1)
  - [Configuration Environment](authorities/stove0-ffprobe-sampling-observer/configuration-environment/index.md) (8)
  - [Python](authorities/stove0-ffprobe-sampling-observer/python/index.md) (4)

- [stove0-media-archive-target-contracts](authorities/stove0-media-archive-target-contracts/index.md) — Optional nonnormative media-archive contract reference for Stove0 targets.
  - [Python](authorities/stove0-media-archive-target-contracts/python/index.md) (30)

- [stove0-media-archive-target-support](authorities/stove0-media-archive-target-support/index.md) — Optional nonnormative projection support for Stove0 media-archive references.
  - [Python](authorities/stove0-media-archive-target-support/python/index.md) (24)

- [stove0-media-metadata-observer-contracts](authorities/stove0-media-metadata-observer-contracts/index.md) — Optional nonnormative media-metadata contract reference for Stove0 observers. Provider for extension: [stove0.observer-semantic-validators](extensions/extension-point-stove0-observer-semantic-validators.md).
  - [Python](authorities/stove0-media-metadata-observer-contracts/python/index.md) (19)

- [stove0-media-sampling-observer-contracts](authorities/stove0-media-sampling-observer-contracts/index.md) — Optional nonnormative media-sampling contract reference for Stove0 observers. Provider for extension: [stove0.observer-semantic-validators](extensions/extension-point-stove0-observer-semantic-validators.md).
  - [Python](authorities/stove0-media-sampling-observer-contracts/python/index.md) (16)

- [stove0-nvenc-av1-opus-review-sampler](authorities/stove0-nvenc-av1-opus-review-sampler/index.md) — Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0. Implements protocol: [stove0-review-sampler](extensions/process-protocol-stove0-review-sampler.md).
  - [CLI](authorities/stove0-nvenc-av1-opus-review-sampler/cli/index.md) (1)
  - [Configuration Environment](authorities/stove0-nvenc-av1-opus-review-sampler/configuration-environment/index.md) (8)
  - [Python](authorities/stove0-nvenc-av1-opus-review-sampler/python/index.md) (3)

- [stove0-nvenc-av1-opus-target](authorities/stove0-nvenc-av1-opus-target/index.md) — Optional nonnormative NVENC AV1 and Opus target reference for Stove0. Implements protocol: [stove0-target](extensions/process-protocol-stove0-target.md).
  - [CLI](authorities/stove0-nvenc-av1-opus-target/cli/index.md) (1)
  - [Configuration Environment](authorities/stove0-nvenc-av1-opus-target/configuration-environment/index.md) (10)
  - [Python](authorities/stove0-nvenc-av1-opus-target/python/index.md) (8)

- [stove0-observer-client](authorities/stove0-observer-client/index.md) — Narrow HTTP client for Stove0 content observers.
  - [Python](authorities/stove0-observer-client/python/index.md) (6) — Defines extension: [stove0.observer-semantic-validators](extensions/extension-point-stove0-observer-semantic-validators.md).

- [stove0-observer-protocol](authorities/stove0-observer-protocol/index.md) — Dependency-light public contracts for external stove0 content observers.
  - [Python](authorities/stove0-observer-protocol/python/index.md) (81) — Defines protocol: [stove0-observer](extensions/process-protocol-stove0-observer.md).

- [stove0-observer-support](authorities/stove0-observer-support/index.md) — External-author protocol, runtime, and conformance support for stove0 content observers.
  - [Process Protocol](authorities/stove0-observer-support/process-protocol/index.md) (1)
  - [Process Protocol Operations](authorities/stove0-observer-support/process-protocol-operations/index.md) (2)
  - [Process Protocol Schemas](authorities/stove0-observer-support/process-protocol-schemas/index.md) (7)
  - [CLI](authorities/stove0-observer-support/cli/index.md) (2)
  - [Python](authorities/stove0-observer-support/python/index.md) (37)

- [stove0-operator-contracts](authorities/stove0-operator-contracts/index.md) — Canonical public state contracts for the Stove0 v1 operator surface.
  - [Configuration Documents](authorities/stove0-operator-contracts/configuration/index.md) (1)
  - [Python](authorities/stove0-operator-contracts/python/index.md) (124)

- [stove0-opus-review-sampler](authorities/stove0-opus-review-sampler/index.md) — Optional nonnormative Opus review-sampler reference for Stove0. Implements protocol: [stove0-review-sampler](extensions/process-protocol-stove0-review-sampler.md).
  - [CLI](authorities/stove0-opus-review-sampler/cli/index.md) (1)
  - [Configuration Environment](authorities/stove0-opus-review-sampler/configuration-environment/index.md) (8)
  - [Python](authorities/stove0-opus-review-sampler/python/index.md) (3)

- [stove0-opus-target](authorities/stove0-opus-target/index.md) — Optional nonnormative Opus target reference for Stove0. Implements protocol: [stove0-target](extensions/process-protocol-stove0-target.md).
  - [CLI](authorities/stove0-opus-target/cli/index.md) (1)
  - [Configuration Environment](authorities/stove0-opus-target/configuration-environment/index.md) (9)
  - [Python](authorities/stove0-opus-target/python/index.md) (8)

- [stove0-protocol](authorities/stove0-protocol/index.md) — Canonical content-opaque collection orchestration contracts for stove0.
  - [Python](authorities/stove0-protocol/python/index.md) (205)

- [stove0-recipe-config](authorities/stove0-recipe-config/index.md) — Portable deployment-owned Stove0 recipe catalog contracts and validation.
  - [Configuration Documents](authorities/stove0-recipe-config/configuration/index.md) (1)
  - [Python](authorities/stove0-recipe-config/python/index.md) (30)

- [stove0-review-materialize-target](authorities/stove0-review-materialize-target/index.md) — Optional nonnormative review materialization target reference for Stove0. Implements protocol: [stove0-target](extensions/process-protocol-stove0-target.md).
  - [CLI](authorities/stove0-review-materialize-target/cli/index.md) (1)
  - [Configuration Environment](authorities/stove0-review-materialize-target/configuration-environment/index.md) (10)
  - [Python](authorities/stove0-review-materialize-target/python/index.md) (9)

- [stove0-review-planning](authorities/stove0-review-planning/index.md) — Optional nonnormative planning bridge for maintained Stove0 review references.
  - [CLI](authorities/stove0-review-planning/cli/index.md) (1)
  - [Python](authorities/stove0-review-planning/python/index.md) (4)

- [stove0-review-rclone-effect-target](authorities/stove0-review-rclone-effect-target/index.md) — Optional nonnormative rclone review-effect target reference for Stove0. Implements protocol: [stove0-target](extensions/process-protocol-stove0-target.md).
  - [CLI](authorities/stove0-review-rclone-effect-target/cli/index.md) (1)
  - [Configuration Environment](authorities/stove0-review-rclone-effect-target/configuration-environment/index.md) (15)
  - [Python](authorities/stove0-review-rclone-effect-target/python/index.md) (11)

- [stove0-review-sampler-client](authorities/stove0-review-sampler-client/index.md) — Optional nonnormative sampler-client reference for the Stove0 review target.
  - [Python](authorities/stove0-review-sampler-client/python/index.md) (7)

- [stove0-review-sampler-protocol](authorities/stove0-review-sampler-protocol/index.md) — Optional nonnormative sampler-protocol reference for the Stove0 review target.
  - [Python](authorities/stove0-review-sampler-protocol/python/index.md) (36) — Defines protocol: [stove0-review-sampler](extensions/process-protocol-stove0-review-sampler.md).

- [stove0-review-sampler-support](authorities/stove0-review-sampler-support/index.md) — Optional nonnormative sampler support for Stove0 review references.
  - [Process Protocol](authorities/stove0-review-sampler-support/process-protocol/index.md) (1)
  - [Process Protocol Operations](authorities/stove0-review-sampler-support/process-protocol-operations/index.md) (2)
  - [Process Protocol Schemas](authorities/stove0-review-sampler-support/process-protocol-schemas/index.md) (5)
  - [CLI](authorities/stove0-review-sampler-support/cli/index.md) (2)
  - [Python](authorities/stove0-review-sampler-support/python/index.md) (21)

- [stove0-review-target-contracts](authorities/stove0-review-target-contracts/index.md) — Optional nonnormative review contract reference for Stove0 targets.
  - [Python](authorities/stove0-review-target-contracts/python/index.md) (26)

- [stove0-review-target-support](authorities/stove0-review-target-support/index.md) — Optional nonnormative shared review-target support reference for Stove0.
  - [Configuration Documents](authorities/stove0-review-target-support/configuration/index.md) (1)
  - [Python](authorities/stove0-review-target-support/python/index.md) (20)

- [stove0-server](authorities/stove0-server/index.md) — Optional nonnormative content-opaque transformation reference application for Riverhog.
  - [CLI](authorities/stove0-server/cli/index.md) (7)
  - [Configuration Environment](authorities/stove0-server/configuration-environment/index.md) (25)
  - [Python](authorities/stove0-server/python/index.md) (324)

- [stove0-target-client](authorities/stove0-target-client/index.md) — Narrow HTTP client for Stove0 transform targets.
  - [Python](authorities/stove0-target-client/python/index.md) (15)

- [stove0-target-jobs](authorities/stove0-target-jobs/index.md) — Hardware-neutral target protocol, runtime, and conformance support for stove0.
  - [Durable State](authorities/stove0-target-jobs/durable-state/index.md) (3)

- [stove0-target-protocol](authorities/stove0-target-protocol/index.md) — Dependency-light public contracts for external stove0 targets.
  - [Python](authorities/stove0-target-protocol/python/index.md) (137) — Defines protocol: [stove0-target](extensions/process-protocol-stove0-target.md).

- [stove0-target-support](authorities/stove0-target-support/index.md) — Hardware-neutral target protocol, runtime, and conformance support for stove0.
  - [Process Protocol](authorities/stove0-target-support/process-protocol/index.md) (1)
  - [Process Protocol Operations](authorities/stove0-target-support/process-protocol-operations/index.md) (5)
  - [Process Protocol Schemas](authorities/stove0-target-support/process-protocol-schemas/index.md) (8)
  - [CLI](authorities/stove0-target-support/cli/index.md) (2)
  - [Configuration Environment](authorities/stove0-target-support/configuration-environment/index.md) (1)
  - [Python](authorities/stove0-target-support/python/index.md) (152)
