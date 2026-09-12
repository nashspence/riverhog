# Complete component relationship inventory

[Atlas](../../index.md) · [Relationships](../index.md)

Every release component appears exactly once below. Purpose text is the maintained project description; role, path, and relationships are executable release metadata.

## Deployed Implementation

Components: **1**

| Component | Purpose | Owned contract elements |
|---|---|---:|
| [riverhog-server](riverhog-server.md) | Encrypted archive management, catalog, and retrieval. | 1 |

## Internal Build Unit

Components: **3**

| Component | Purpose | Owned contract elements |
|---|---|---:|
| [config-validation](config-validation.md) | Strict YAML and JSON Schema configuration validation. | 1 |
| [state-schema](state-schema.md) | Forward-only relational state schema and migration contracts. | 1 |
| [time-formats](time-formats.md) | UTC timestamp and operator duration formats. | 1 |

## Reference Application

Components: **6**

| Component | Purpose | Owned contract elements |
|---|---|---:|
| [gogurt](gogurt.md) | Optional nonnormative mounted-volume ingestion reference application for Riverhog. | 22 |
| [mango-fish](mango-fish.md) | Optional nonnormative CloudEvents reference application for Riverhog. | 7 |
| [piggity](piggity.md) | Optional nonnormative Piggity reference client for Riverhog. | 87 |
| [riverhog-recover](riverhog-recover.md) | Optional nonnormative independent recovery reference application for Riverhog archives. | 2 |
| [stove0-client](stove0-client.md) | Optional nonnormative command-line client for the Stove0 reference application. | 1 |
| [stove0-server](stove0-server.md) | Optional nonnormative content-opaque transformation reference application for Riverhog. | 1 |

## Reference Component

Components: **37**

| Component | Purpose | Owned contract elements |
|---|---|---:|
| [gogurt-linux-listener-host](gogurt-linux-listener-host.md) | Optional nonnormative Linux systemd-user listener-host reference for Gogurt. | 1 |
| [gogurt-linux-mounted-volume](gogurt-linux-mounted-volume.md) | Optional nonnormative Linux mounted-volume reference for Gogurt. | 1 |
| [gogurt-macos-listener-host](gogurt-macos-listener-host.md) | Optional nonnormative macOS launchd listener-host reference for Gogurt. | 1 |
| [gogurt-macos-mounted-volume](gogurt-macos-mounted-volume.md) | Optional nonnormative macOS mounted-volume reference for Gogurt. | 1 |
| [gogurt-path-volume-support](gogurt-path-volume-support.md) | Optional nonnormative path-mounted-volume support for Gogurt reference providers. | 1 |
| [gogurt-windows-listener-host](gogurt-windows-listener-host.md) | Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt. | 1 |
| [gogurt-windows-mounted-volume](gogurt-windows-mounted-volume.md) | Optional nonnormative Windows mounted-volume reference for Gogurt. | 1 |
| [riverhog-ftp-adapter](riverhog-ftp-adapter.md) | Optional nonnormative FTP ingress reference for Riverhog. | 26 |
| [riverhog-ftp-adapter-api-client](riverhog-ftp-adapter-api-client.md) | Optional nonnormative client for the Riverhog FTP ingress reference. | 1 |
| [riverhog-provenance-linux-contracts](riverhog-provenance-linux-contracts.md) | Optional nonnormative Linux observation-contract reference for Riverhog provenance. | 6 |
| [riverhog-provenance-linux-observer](riverhog-provenance-linux-observer.md) | Optional nonnormative Linux filesystem-observer reference for Riverhog provenance. | 1 |
| [riverhog-provenance-macos-contracts](riverhog-provenance-macos-contracts.md) | Optional nonnormative macOS observation-contract reference for Riverhog provenance. | 5 |
| [riverhog-provenance-macos-observer](riverhog-provenance-macos-observer.md) | Optional nonnormative macOS filesystem-observer reference for Riverhog provenance. | 1 |
| [riverhog-provenance-windows-contracts](riverhog-provenance-windows-contracts.md) | Optional nonnormative Windows observation-contract reference for Riverhog provenance. | 11 |
| [riverhog-provenance-windows-observer](riverhog-provenance-windows-observer.md) | Optional nonnormative Windows filesystem-observer reference for Riverhog provenance. | 1 |
| [riverhog-storage-adapter-aws](riverhog-storage-adapter-aws.md) | Optional nonnormative AWS storage reference for Riverhog. | 1 |
| [riverhog-storage-adapter-backblaze](riverhog-storage-adapter-backblaze.md) | Optional nonnormative Backblaze B2 storage reference for Riverhog. | 1 |
| [riverhog-storage-adapter-filesystem](riverhog-storage-adapter-filesystem.md) | Optional nonnormative Linux filesystem storage reference for Riverhog. | 1 |
| [riverhog-storage-adapter-s3-support](riverhog-storage-adapter-s3-support.md) | Optional nonnormative S3 support for Riverhog storage references. | 1 |
| [stove0-exiftool-observer](stove0-exiftool-observer.md) | Optional nonnormative ExifTool observer reference for Stove0. | 1 |
| [stove0-ffprobe-sampling-observer](stove0-ffprobe-sampling-observer.md) | Optional nonnormative FFprobe sampling-observer reference for Stove0. | 1 |
| [stove0-media-archive-target-contracts](stove0-media-archive-target-contracts.md) | Optional nonnormative media-archive contract reference for Stove0 targets. | 1 |
| [stove0-media-archive-target-support](stove0-media-archive-target-support.md) | Optional nonnormative projection support for Stove0 media-archive references. | 1 |
| [stove0-media-metadata-observer-contracts](stove0-media-metadata-observer-contracts.md) | Optional nonnormative media-metadata contract reference for Stove0 observers. | 1 |
| [stove0-media-sampling-observer-contracts](stove0-media-sampling-observer-contracts.md) | Optional nonnormative media-sampling contract reference for Stove0 observers. | 1 |
| [stove0-nvenc-av1-opus-review-sampler](stove0-nvenc-av1-opus-review-sampler.md) | Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0. | 1 |
| [stove0-nvenc-av1-opus-target](stove0-nvenc-av1-opus-target.md) | Optional nonnormative NVENC AV1 and Opus target reference for Stove0. | 1 |
| [stove0-opus-review-sampler](stove0-opus-review-sampler.md) | Optional nonnormative Opus review-sampler reference for Stove0. | 1 |
| [stove0-opus-target](stove0-opus-target.md) | Optional nonnormative Opus target reference for Stove0. | 1 |
| [stove0-review-materialize-target](stove0-review-materialize-target.md) | Optional nonnormative review materialization target reference for Stove0. | 1 |
| [stove0-review-planning](stove0-review-planning.md) | Optional nonnormative planning bridge for maintained Stove0 review references. | 2 |
| [stove0-review-rclone-effect-target](stove0-review-rclone-effect-target.md) | Optional nonnormative rclone review-effect target reference for Stove0. | 1 |
| [stove0-review-sampler-client](stove0-review-sampler-client.md) | Optional nonnormative sampler-client reference for the Stove0 review target. | 1 |
| [stove0-review-sampler-protocol](stove0-review-sampler-protocol.md) | Optional nonnormative sampler-protocol reference for the Stove0 review target. | 2 |
| [stove0-review-sampler-support](stove0-review-sampler-support.md) | Optional nonnormative sampler support for Stove0 review references. | 7 |
| [stove0-review-target-contracts](stove0-review-target-contracts.md) | Optional nonnormative review contract reference for Stove0 targets. | 1 |
| [stove0-review-target-support](stove0-review-target-support.md) | Optional nonnormative shared review-target support reference for Stove0. | 1 |

## Reusable Library

Components: **24**

| Component | Purpose | Owned contract elements |
|---|---|---:|
| [gogurt-core](gogurt-core.md) | Portable Gogurt marker, routing, action, and watch semantics. | 3 |
| [gogurt-listener-runtime](gogurt-listener-runtime.md) | Portable durable listener runtime and native-platform port for Gogurt. | 3 |
| [http-api-contracts](http-api-contracts.md) | Public typed HTTP error, health, client, and operation contracts. | 2 |
| [lifecycle-events](lifecycle-events.md) | Durable CloudEvents lifecycle log and client primitives. | 2 |
| [riverhog-age](riverhog-age.md) | Resumable age encryption used by the Riverhog protocol. | 2 |
| [riverhog-application-access](riverhog-application-access.md) | Public Riverhog application-access contracts and canonical grant grammar. | 2 |
| [riverhog-archive-contracts](riverhog-archive-contracts.md) | Dependency-light immutable Riverhog archive recovery contracts. | 6 |
| [riverhog-client](riverhog-client.md) | Typed generic Riverhog client and capability-scoped collection-processing runtime. | 3 |
| [riverhog-protocol](riverhog-protocol.md) | Canonical Riverhog wire and identity contracts. | 3 |
| [riverhog-provenance](riverhog-provenance.md) | Portable Riverhog v1 per-file provenance journals and validation. | 10 |
| [riverhog-provenance-contracts](riverhog-provenance-contracts.md) | Canonical Riverhog provenance identity and reference contracts. | 3 |
| [riverhog-storage-adapter-asgi-support](riverhog-storage-adapter-asgi-support.md) | Authenticated ASGI shell for independently scoped Riverhog storage adapters. | 2 |
| [riverhog-storage-adapter-protocol](riverhog-storage-adapter-protocol.md) | Provider-neutral opaque-object capability contracts for Riverhog storage adapters. | 3 |
| [riverhog-storage-adapter-support](riverhog-storage-adapter-support.md) | HTTP binding and conformance support for Riverhog storage adapters. | 26 |
| [stove0-api-client](stove0-api-client.md) | Official Python client for the stove0 v1 workflow API. | 2 |
| [stove0-observer-client](stove0-observer-client.md) | Narrow HTTP client for Stove0 content observers. | 3 |
| [stove0-observer-protocol](stove0-observer-protocol.md) | Dependency-light public contracts for external stove0 content observers. | 3 |
| [stove0-observer-support](stove0-observer-support.md) | External-author protocol, runtime, and conformance support for stove0 content observers. | 10 |
| [stove0-operator-contracts](stove0-operator-contracts.md) | Canonical public state contracts for the Stove0 v1 operator surface. | 2 |
| [stove0-protocol](stove0-protocol.md) | Canonical content-opaque collection orchestration contracts for stove0. | 2 |
| [stove0-recipe-config](stove0-recipe-config.md) | Portable deployment-owned Stove0 recipe catalog contracts and validation. | 2 |
| [stove0-target-client](stove0-target-client.md) | Narrow HTTP client for Stove0 transform targets. | 2 |
| [stove0-target-protocol](stove0-target-protocol.md) | Dependency-light public contracts for external stove0 targets. | 3 |
| [stove0-target-support](stove0-target-support.md) | Hardware-neutral target protocol, runtime, and conformance support for stove0. | 11 |
