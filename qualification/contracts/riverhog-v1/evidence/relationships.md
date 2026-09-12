# Relationship-edge inventory

[Atlas](../index.md) · [Freeze evidence](index.md)

This is the exact generated node and edge set behind the human contract map. It is evidence, not a second navigation hierarchy.

## Relationship shape

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

## Exact nodes

| Identity | Kind | Name | Role or owner | Maintained purpose |
|---|---|---|---|---|
| `component:config-validation` | `component` | `config-validation` | `internal_build_unit` | Strict YAML and JSON Schema configuration validation. |
| `component:gogurt` | `component` | `gogurt` | `reference_application` | Optional nonnormative mounted-volume ingestion reference application for Riverhog. |
| `component:gogurt-core` | `component` | `gogurt-core` | `reusable_library` | Portable Gogurt marker, routing, action, and watch semantics. |
| `component:gogurt-linux-listener-host` | `component` | `gogurt-linux-listener-host` | `reference_component` | Optional nonnormative Linux systemd-user listener-host reference for Gogurt. |
| `component:gogurt-linux-mounted-volume` | `component` | `gogurt-linux-mounted-volume` | `reference_component` | Optional nonnormative Linux mounted-volume reference for Gogurt. |
| `component:gogurt-listener-runtime` | `component` | `gogurt-listener-runtime` | `reusable_library` | Portable durable listener runtime and native-platform port for Gogurt. |
| `component:gogurt-macos-listener-host` | `component` | `gogurt-macos-listener-host` | `reference_component` | Optional nonnormative macOS launchd listener-host reference for Gogurt. |
| `component:gogurt-macos-mounted-volume` | `component` | `gogurt-macos-mounted-volume` | `reference_component` | Optional nonnormative macOS mounted-volume reference for Gogurt. |
| `component:gogurt-path-volume-support` | `component` | `gogurt-path-volume-support` | `reference_component` | Optional nonnormative path-mounted-volume support for Gogurt reference providers. |
| `component:gogurt-windows-listener-host` | `component` | `gogurt-windows-listener-host` | `reference_component` | Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt. |
| `component:gogurt-windows-mounted-volume` | `component` | `gogurt-windows-mounted-volume` | `reference_component` | Optional nonnormative Windows mounted-volume reference for Gogurt. |
| `component:http-api-contracts` | `component` | `http-api-contracts` | `reusable_library` | Public typed HTTP error, health, client, and operation contracts. |
| `component:lifecycle-events` | `component` | `lifecycle-events` | `reusable_library` | Durable CloudEvents lifecycle log and client primitives. |
| `component:mango-fish` | `component` | `mango-fish` | `reference_application` | Optional nonnormative CloudEvents reference application for Riverhog. |
| `component:piggity` | `component` | `piggity` | `reference_application` | Optional nonnormative Piggity reference client for Riverhog. |
| `component:riverhog-age` | `component` | `riverhog-age` | `reusable_library` | Resumable age encryption used by the Riverhog protocol. |
| `component:riverhog-application-access` | `component` | `riverhog-application-access` | `reusable_library` | Public Riverhog application-access contracts and canonical grant grammar. |
| `component:riverhog-archive-contracts` | `component` | `riverhog-archive-contracts` | `reusable_library` | Dependency-light immutable Riverhog archive recovery contracts. |
| `component:riverhog-client` | `component` | `riverhog-client` | `reusable_library` | Typed generic Riverhog client and capability-scoped collection-processing runtime. |
| `component:riverhog-ftp-adapter` | `component` | `riverhog-ftp-adapter` | `reference_component` | Optional nonnormative FTP ingress reference for Riverhog. |
| `component:riverhog-ftp-adapter-api-client` | `component` | `riverhog-ftp-adapter-api-client` | `reference_component` | Optional nonnormative client for the Riverhog FTP ingress reference. |
| `component:riverhog-protocol` | `component` | `riverhog-protocol` | `reusable_library` | Canonical Riverhog wire and identity contracts. |
| `component:riverhog-provenance` | `component` | `riverhog-provenance` | `reusable_library` | Portable Riverhog v1 per-file provenance journals and validation. |
| `component:riverhog-provenance-contracts` | `component` | `riverhog-provenance-contracts` | `reusable_library` | Canonical Riverhog provenance identity and reference contracts. |
| `component:riverhog-provenance-linux-contracts` | `component` | `riverhog-provenance-linux-contracts` | `reference_component` | Optional nonnormative Linux observation-contract reference for Riverhog provenance. |
| `component:riverhog-provenance-linux-observer` | `component` | `riverhog-provenance-linux-observer` | `reference_component` | Optional nonnormative Linux filesystem-observer reference for Riverhog provenance. |
| `component:riverhog-provenance-macos-contracts` | `component` | `riverhog-provenance-macos-contracts` | `reference_component` | Optional nonnormative macOS observation-contract reference for Riverhog provenance. |
| `component:riverhog-provenance-macos-observer` | `component` | `riverhog-provenance-macos-observer` | `reference_component` | Optional nonnormative macOS filesystem-observer reference for Riverhog provenance. |
| `component:riverhog-provenance-windows-contracts` | `component` | `riverhog-provenance-windows-contracts` | `reference_component` | Optional nonnormative Windows observation-contract reference for Riverhog provenance. |
| `component:riverhog-provenance-windows-observer` | `component` | `riverhog-provenance-windows-observer` | `reference_component` | Optional nonnormative Windows filesystem-observer reference for Riverhog provenance. |
| `component:riverhog-recover` | `component` | `riverhog-recover` | `reference_application` | Optional nonnormative independent recovery reference application for Riverhog archives. |
| `component:riverhog-server` | `component` | `riverhog-server` | `deployed_implementation` | Encrypted archive management, catalog, and retrieval. |
| `component:riverhog-storage-adapter-asgi-support` | `component` | `riverhog-storage-adapter-asgi-support` | `reusable_library` | Authenticated ASGI shell for independently scoped Riverhog storage adapters. |
| `component:riverhog-storage-adapter-aws` | `component` | `riverhog-storage-adapter-aws` | `reference_component` | Optional nonnormative AWS storage reference for Riverhog. |
| `component:riverhog-storage-adapter-backblaze` | `component` | `riverhog-storage-adapter-backblaze` | `reference_component` | Optional nonnormative Backblaze B2 storage reference for Riverhog. |
| `component:riverhog-storage-adapter-filesystem` | `component` | `riverhog-storage-adapter-filesystem` | `reference_component` | Optional nonnormative Linux filesystem storage reference for Riverhog. |
| `component:riverhog-storage-adapter-protocol` | `component` | `riverhog-storage-adapter-protocol` | `reusable_library` | Provider-neutral opaque-object capability contracts for Riverhog storage adapters. |
| `component:riverhog-storage-adapter-s3-support` | `component` | `riverhog-storage-adapter-s3-support` | `reference_component` | Optional nonnormative S3 support for Riverhog storage references. |
| `component:riverhog-storage-adapter-support` | `component` | `riverhog-storage-adapter-support` | `reusable_library` | HTTP binding and conformance support for Riverhog storage adapters. |
| `component:state-schema` | `component` | `state-schema` | `internal_build_unit` | Forward-only relational state schema and migration contracts. |
| `component:stove0-api-client` | `component` | `stove0-api-client` | `reusable_library` | Official Python client for the stove0 v1 workflow API. |
| `component:stove0-client` | `component` | `stove0-client` | `reference_application` | Optional nonnormative command-line client for the Stove0 reference application. |
| `component:stove0-exiftool-observer` | `component` | `stove0-exiftool-observer` | `reference_component` | Optional nonnormative ExifTool observer reference for Stove0. |
| `component:stove0-ffprobe-sampling-observer` | `component` | `stove0-ffprobe-sampling-observer` | `reference_component` | Optional nonnormative FFprobe sampling-observer reference for Stove0. |
| `component:stove0-media-archive-target-contracts` | `component` | `stove0-media-archive-target-contracts` | `reference_component` | Optional nonnormative media-archive contract reference for Stove0 targets. |
| `component:stove0-media-archive-target-support` | `component` | `stove0-media-archive-target-support` | `reference_component` | Optional nonnormative projection support for Stove0 media-archive references. |
| `component:stove0-media-metadata-observer-contracts` | `component` | `stove0-media-metadata-observer-contracts` | `reference_component` | Optional nonnormative media-metadata contract reference for Stove0 observers. |
| `component:stove0-media-sampling-observer-contracts` | `component` | `stove0-media-sampling-observer-contracts` | `reference_component` | Optional nonnormative media-sampling contract reference for Stove0 observers. |
| `component:stove0-nvenc-av1-opus-review-sampler` | `component` | `stove0-nvenc-av1-opus-review-sampler` | `reference_component` | Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0. |
| `component:stove0-nvenc-av1-opus-target` | `component` | `stove0-nvenc-av1-opus-target` | `reference_component` | Optional nonnormative NVENC AV1 and Opus target reference for Stove0. |
| `component:stove0-observer-client` | `component` | `stove0-observer-client` | `reusable_library` | Narrow HTTP client for Stove0 content observers. |
| `component:stove0-observer-protocol` | `component` | `stove0-observer-protocol` | `reusable_library` | Dependency-light public contracts for external stove0 content observers. |
| `component:stove0-observer-support` | `component` | `stove0-observer-support` | `reusable_library` | External-author protocol, runtime, and conformance support for stove0 content observers. |
| `component:stove0-operator-contracts` | `component` | `stove0-operator-contracts` | `reusable_library` | Canonical public state contracts for the Stove0 v1 operator surface. |
| `component:stove0-opus-review-sampler` | `component` | `stove0-opus-review-sampler` | `reference_component` | Optional nonnormative Opus review-sampler reference for Stove0. |
| `component:stove0-opus-target` | `component` | `stove0-opus-target` | `reference_component` | Optional nonnormative Opus target reference for Stove0. |
| `component:stove0-protocol` | `component` | `stove0-protocol` | `reusable_library` | Canonical content-opaque collection orchestration contracts for stove0. |
| `component:stove0-recipe-config` | `component` | `stove0-recipe-config` | `reusable_library` | Portable deployment-owned Stove0 recipe catalog contracts and validation. |
| `component:stove0-review-materialize-target` | `component` | `stove0-review-materialize-target` | `reference_component` | Optional nonnormative review materialization target reference for Stove0. |
| `component:stove0-review-planning` | `component` | `stove0-review-planning` | `reference_component` | Optional nonnormative planning bridge for maintained Stove0 review references. |
| `component:stove0-review-rclone-effect-target` | `component` | `stove0-review-rclone-effect-target` | `reference_component` | Optional nonnormative rclone review-effect target reference for Stove0. |
| `component:stove0-review-sampler-client` | `component` | `stove0-review-sampler-client` | `reference_component` | Optional nonnormative sampler-client reference for the Stove0 review target. |
| `component:stove0-review-sampler-protocol` | `component` | `stove0-review-sampler-protocol` | `reference_component` | Optional nonnormative sampler-protocol reference for the Stove0 review target. |
| `component:stove0-review-sampler-support` | `component` | `stove0-review-sampler-support` | `reference_component` | Optional nonnormative sampler support for Stove0 review references. |
| `component:stove0-review-target-contracts` | `component` | `stove0-review-target-contracts` | `reference_component` | Optional nonnormative review contract reference for Stove0 targets. |
| `component:stove0-review-target-support` | `component` | `stove0-review-target-support` | `reference_component` | Optional nonnormative shared review-target support reference for Stove0. |
| `component:stove0-server` | `component` | `stove0-server` | `reference_application` | Optional nonnormative content-opaque transformation reference application for Riverhog. |
| `component:stove0-target-client` | `component` | `stove0-target-client` | `reusable_library` | Narrow HTTP client for Stove0 transform targets. |
| `component:stove0-target-protocol` | `component` | `stove0-target-protocol` | `reusable_library` | Dependency-light public contracts for external stove0 targets. |
| `component:stove0-target-support` | `component` | `stove0-target-support` | `reusable_library` | Hardware-neutral target protocol, runtime, and conformance support for stove0. |
| `component:time-formats` | `component` | `time-formats` | `internal_build_unit` | UTC timestamp and operator duration formats. |
| `extension-point:gogurt.listener-host-providers` | `extension-point` | `gogurt.listener-host-providers` | `gogurt-listener-runtime` | Entry-point extension boundary owned by gogurt-listener-runtime. |
| `extension-point:gogurt.mounted-volume-providers` | `extension-point` | `gogurt.mounted-volume-providers` | `gogurt-core` | Entry-point extension boundary owned by gogurt-core. |
| `extension-point:riverhog.provenance-contracts` | `extension-point` | `riverhog.provenance-contracts` | `riverhog-provenance-contracts` | Entry-point extension boundary owned by riverhog-provenance-contracts. |
| `extension-point:riverhog.provenance-observers` | `extension-point` | `riverhog.provenance-observers` | `riverhog-provenance` | Entry-point extension boundary owned by riverhog-provenance. |
| `extension-point:stove0.observer-semantic-validators` | `extension-point` | `stove0.observer-semantic-validators` | `stove0-observer-client` | Entry-point extension boundary owned by stove0-observer-client. |
| `image:runtime:mango-fish` | `runtime-image` | `mango-fish` | `reference` | Optional nonnormative CloudEvents reference application for Riverhog. |
| `image:runtime:riverhog` | `runtime-image` | `riverhog` | `product` | Riverhog archive service. |
| `image:runtime:riverhog-ftp-adapter` | `runtime-image` | `riverhog-ftp-adapter` | `reference` | Optional nonnormative Riverhog FTP ingress reference. |
| `image:runtime:riverhog-storage-adapter-aws` | `runtime-image` | `riverhog-storage-adapter-aws` | `reference` | Optional nonnormative AWS storage reference for Riverhog. |
| `image:runtime:riverhog-storage-adapter-backblaze` | `runtime-image` | `riverhog-storage-adapter-backblaze` | `reference` | Optional nonnormative Backblaze B2 storage reference for Riverhog. |
| `image:runtime:riverhog-storage-adapter-filesystem` | `runtime-image` | `riverhog-storage-adapter-filesystem` | `reference` | Optional nonnormative Linux filesystem storage reference for Riverhog. |
| `image:runtime:stove0` | `runtime-image` | `stove0` | `reference` | Optional nonnormative transformation reference application for Riverhog. |
| `image:runtime:stove0-exiftool-observer` | `runtime-image` | `stove0-exiftool-observer` | `reference` | Optional nonnormative ExifTool observer reference for Stove0. |
| `image:runtime:stove0-ffprobe-sampling-observer` | `runtime-image` | `stove0-ffprobe-sampling-observer` | `reference` | Optional nonnormative FFprobe sampling-observer reference for Stove0. |
| `image:runtime:stove0-nvenc-av1-opus-target` | `runtime-image` | `stove0-nvenc-av1-opus-target` | `reference` | Optional nonnormative NVENC AV1 and Opus target reference for Stove0. |
| `image:runtime:stove0-opus-target` | `runtime-image` | `stove0-opus-target` | `reference` | Optional nonnormative Opus target reference for Stove0. |
| `image:runtime:stove0-review-materialize-target` | `runtime-image` | `stove0-review-materialize-target` | `reference` | Optional nonnormative review materialization target reference for Stove0. |
| `image:runtime:stove0-review-rclone-effect-target` | `runtime-image` | `stove0-review-rclone-effect-target` | `reference` | Optional nonnormative rclone review-effect target reference for Stove0. |
| `installation:uv-tool` | `installation` | `uv-tool` | `—` | Coordinated end-user installation roots declared by the release contract. |
| `process-protocol:riverhog-storage-adapter` | `process-protocol` | `riverhog-storage-adapter` | `riverhog-storage-adapter-protocol` | Independently deployed process protocol owned by riverhog-storage-adapter-protocol. |
| `process-protocol:stove0-observer` | `process-protocol` | `stove0-observer` | `stove0-observer-protocol` | Independently deployed process protocol owned by stove0-observer-protocol. |
| `process-protocol:stove0-review-sampler` | `process-protocol` | `stove0-review-sampler` | `stove0-review-sampler-protocol` | Independently deployed process protocol owned by stove0-review-sampler-protocol. |
| `process-protocol:stove0-target` | `process-protocol` | `stove0-target` | `stove0-target-protocol` | Independently deployed process protocol owned by stove0-target-protocol. |

## Exact edges

| From | Relationship | To | Scope or binding |
|---|---|---|---|
| `riverhog-storage-adapter-support` | `binds-protocol` | `riverhog-storage-adapter` | `http` |
| `stove0-observer-support` | `binds-protocol` | `stove0-observer` | `http` |
| `stove0-review-sampler-support` | `binds-protocol` | `stove0-review-sampler` | `http` |
| `stove0-target-support` | `binds-protocol` | `stove0-target` | `http` |
| `gogurt` | `depends-on` | `config-validation` | `required` |
| `gogurt` | `depends-on` | `gogurt-core` | `required` |
| `gogurt` | `depends-on` | `gogurt-listener-runtime` | `required` |
| `gogurt-core` | `depends-on` | `config-validation` | `required` |
| `gogurt-linux-listener-host` | `depends-on` | `gogurt-listener-runtime` | `required` |
| `gogurt-linux-mounted-volume` | `depends-on` | `gogurt-core` | `required` |
| `gogurt-linux-mounted-volume` | `depends-on` | `gogurt-path-volume-support` | `required` |
| `gogurt-listener-runtime` | `depends-on` | `config-validation` | `required` |
| `gogurt-listener-runtime` | `depends-on` | `gogurt-core` | `required` |
| `gogurt-macos-listener-host` | `depends-on` | `gogurt-listener-runtime` | `required` |
| `gogurt-macos-mounted-volume` | `depends-on` | `gogurt-core` | `required` |
| `gogurt-macos-mounted-volume` | `depends-on` | `gogurt-path-volume-support` | `required` |
| `gogurt-path-volume-support` | `depends-on` | `config-validation` | `required` |
| `gogurt-path-volume-support` | `depends-on` | `gogurt-core` | `required` |
| `gogurt-windows-listener-host` | `depends-on` | `gogurt-listener-runtime` | `required` |
| `gogurt-windows-mounted-volume` | `depends-on` | `gogurt-core` | `required` |
| `gogurt-windows-mounted-volume` | `depends-on` | `gogurt-path-volume-support` | `required` |
| `lifecycle-events` | `depends-on` | `time-formats` | `required` |
| `mango-fish` | `depends-on` | `lifecycle-events` | `required` |
| `mango-fish` | `depends-on` | `state-schema` | `required` |
| `piggity` | `depends-on` | `http-api-contracts` | `required` |
| `piggity` | `depends-on` | `riverhog-application-access` | `required` |
| `piggity` | `depends-on` | `riverhog-client` | `required` |
| `piggity` | `depends-on` | `riverhog-protocol` | `required` |
| `piggity` | `depends-on` | `riverhog-provenance` | `required` |
| `piggity` | `depends-on` | `state-schema` | `required` |
| `piggity` | `depends-on` | `time-formats` | `required` |
| `riverhog-application-access` | `depends-on` | `riverhog-protocol` | `required` |
| `riverhog-client` | `depends-on` | `http-api-contracts` | `required` |
| `riverhog-client` | `depends-on` | `riverhog-application-access` | `required` |
| `riverhog-client` | `depends-on` | `riverhog-protocol` | `required` |
| `riverhog-client` | `depends-on` | `riverhog-provenance-contracts` | `required` |
| `riverhog-ftp-adapter` | `depends-on` | `http-api-contracts` | `required` |
| `riverhog-ftp-adapter` | `depends-on` | `riverhog-client` | `required` |
| `riverhog-ftp-adapter` | `depends-on` | `riverhog-ftp-adapter-api-client` | `required` |
| `riverhog-ftp-adapter` | `depends-on` | `riverhog-protocol` | `required` |
| `riverhog-ftp-adapter` | `depends-on` | `riverhog-provenance` | `required` |
| `riverhog-ftp-adapter-api-client` | `depends-on` | `http-api-contracts` | `required` |
| `riverhog-protocol` | `depends-on` | `http-api-contracts` | `required` |
| `riverhog-protocol` | `depends-on` | `lifecycle-events` | `required` |
| `riverhog-protocol` | `depends-on` | `riverhog-provenance-contracts` | `required` |
| `riverhog-provenance` | `depends-on` | `riverhog-provenance-contracts` | `required` |
| `riverhog-provenance-linux-contracts` | `depends-on` | `riverhog-provenance-contracts` | `required` |
| `riverhog-provenance-linux-observer` | `depends-on` | `riverhog-provenance` | `required` |
| `riverhog-provenance-linux-observer` | `depends-on` | `riverhog-provenance-linux-contracts` | `required` |
| `riverhog-provenance-macos-contracts` | `depends-on` | `riverhog-provenance-contracts` | `required` |
| `riverhog-provenance-macos-observer` | `depends-on` | `riverhog-provenance` | `required` |
| `riverhog-provenance-macos-observer` | `depends-on` | `riverhog-provenance-macos-contracts` | `required` |
| `riverhog-provenance-windows-contracts` | `depends-on` | `riverhog-provenance-contracts` | `required` |
| `riverhog-provenance-windows-observer` | `depends-on` | `riverhog-provenance` | `required` |
| `riverhog-provenance-windows-observer` | `depends-on` | `riverhog-provenance-windows-contracts` | `required` |
| `riverhog-recover` | `depends-on` | `riverhog-archive-contracts` | `required` |
| `riverhog-recover` | `depends-on` | `riverhog-protocol` | `required` |
| `riverhog-recover` | `depends-on` | `riverhog-provenance` | `required` |
| `riverhog-server` | `depends-on` | `http-api-contracts` | `required` |
| `riverhog-server` | `depends-on` | `lifecycle-events` | `required` |
| `riverhog-server` | `depends-on` | `riverhog-age` | `required` |
| `riverhog-server` | `depends-on` | `riverhog-application-access` | `required` |
| `riverhog-server` | `depends-on` | `riverhog-archive-contracts` | `required` |
| `riverhog-server` | `depends-on` | `riverhog-protocol` | `required` |
| `riverhog-server` | `depends-on` | `riverhog-provenance` | `required` |
| `riverhog-server` | `depends-on` | `riverhog-provenance-contracts` | `required` |
| `riverhog-server` | `depends-on` | `riverhog-storage-adapter-protocol` | `required` |
| `riverhog-server` | `depends-on` | `riverhog-storage-adapter-support` | `required` |
| `riverhog-server` | `depends-on` | `state-schema` | `required` |
| `riverhog-server` | `depends-on` | `time-formats` | `required` |
| `riverhog-storage-adapter-asgi-support` | `depends-on` | `http-api-contracts` | `required` |
| `riverhog-storage-adapter-asgi-support` | `depends-on` | `riverhog-storage-adapter-protocol` | `required` |
| `riverhog-storage-adapter-asgi-support` | `depends-on` | `riverhog-storage-adapter-support` | `required` |
| `riverhog-storage-adapter-aws` | `depends-on` | `riverhog-storage-adapter-asgi-support` | `required` |
| `riverhog-storage-adapter-aws` | `depends-on` | `riverhog-storage-adapter-protocol` | `required` |
| `riverhog-storage-adapter-aws` | `depends-on` | `riverhog-storage-adapter-s3-support` | `required` |
| `riverhog-storage-adapter-aws` | `depends-on` | `time-formats` | `required` |
| `riverhog-storage-adapter-backblaze` | `depends-on` | `riverhog-storage-adapter-asgi-support` | `required` |
| `riverhog-storage-adapter-backblaze` | `depends-on` | `riverhog-storage-adapter-s3-support` | `required` |
| `riverhog-storage-adapter-filesystem` | `depends-on` | `riverhog-storage-adapter-asgi-support` | `required` |
| `riverhog-storage-adapter-filesystem` | `depends-on` | `riverhog-storage-adapter-protocol` | `required` |
| `riverhog-storage-adapter-filesystem` | `depends-on` | `time-formats` | `required` |
| `riverhog-storage-adapter-protocol` | `depends-on` | `time-formats` | `required` |
| `riverhog-storage-adapter-s3-support` | `depends-on` | `riverhog-storage-adapter-protocol` | `required` |
| `riverhog-storage-adapter-s3-support` | `depends-on` | `time-formats` | `required` |
| `riverhog-storage-adapter-support` | `depends-on` | `http-api-contracts` | `required` |
| `riverhog-storage-adapter-support` | `depends-on` | `riverhog-storage-adapter-protocol` | `required` |
| `stove0-api-client` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-api-client` | `depends-on` | `stove0-operator-contracts` | `required` |
| `stove0-api-client` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-client` | `depends-on` | `stove0-api-client` | `required` |
| `stove0-client` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-client` | `depends-on` | `stove0-recipe-config` | `required` |
| `stove0-exiftool-observer` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-exiftool-observer` | `depends-on` | `stove0-media-metadata-observer-contracts` | `required` |
| `stove0-exiftool-observer` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-exiftool-observer` | `depends-on` | `stove0-observer-support` | `required` |
| `stove0-ffprobe-sampling-observer` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-ffprobe-sampling-observer` | `depends-on` | `stove0-media-sampling-observer-contracts` | `required` |
| `stove0-ffprobe-sampling-observer` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-ffprobe-sampling-observer` | `depends-on` | `stove0-observer-support` | `required` |
| `stove0-media-archive-target-contracts` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-media-archive-target-contracts` | `depends-on` | `stove0-target-protocol` | `required` |
| `stove0-media-archive-target-support` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-media-archive-target-support` | `depends-on` | `stove0-media-archive-target-contracts` | `required` |
| `stove0-media-archive-target-support` | `depends-on` | `stove0-media-metadata-observer-contracts` | `required` |
| `stove0-media-archive-target-support` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-media-archive-target-support` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-media-archive-target-support` | `depends-on` | `stove0-target-protocol` | `required` |
| `stove0-media-metadata-observer-contracts` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-media-sampling-observer-contracts` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-nvenc-av1-opus-review-sampler` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-nvenc-av1-opus-review-sampler` | `depends-on` | `stove0-media-archive-target-contracts` | `required` |
| `stove0-nvenc-av1-opus-review-sampler` | `depends-on` | `stove0-review-sampler-protocol` | `required` |
| `stove0-nvenc-av1-opus-review-sampler` | `depends-on` | `stove0-review-sampler-support` | `required` |
| `stove0-nvenc-av1-opus-review-sampler` | `depends-on` | `stove0-review-target-contracts` | `required` |
| `stove0-nvenc-av1-opus-target` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-nvenc-av1-opus-target` | `depends-on` | `riverhog-client` | `required` |
| `stove0-nvenc-av1-opus-target` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-nvenc-av1-opus-target` | `depends-on` | `stove0-media-archive-target-contracts` | `required` |
| `stove0-nvenc-av1-opus-target` | `depends-on` | `stove0-media-archive-target-support` | `required` |
| `stove0-nvenc-av1-opus-target` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-nvenc-av1-opus-target` | `depends-on` | `stove0-target-support` | `required` |
| `stove0-observer-client` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-observer-client` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-observer-protocol` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-observer-protocol` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-observer-support` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-observer-support` | `depends-on` | `riverhog-client` | `required` |
| `stove0-observer-support` | `depends-on` | `stove0-observer-client` | `required` |
| `stove0-observer-support` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-operator-contracts` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-operator-contracts` | `depends-on` | `lifecycle-events` | `required` |
| `stove0-operator-contracts` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-operator-contracts` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-operator-contracts` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-operator-contracts` | `depends-on` | `stove0-recipe-config` | `required` |
| `stove0-operator-contracts` | `depends-on` | `stove0-target-protocol` | `required` |
| `stove0-opus-review-sampler` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-opus-review-sampler` | `depends-on` | `stove0-media-archive-target-contracts` | `required` |
| `stove0-opus-review-sampler` | `depends-on` | `stove0-review-sampler-protocol` | `required` |
| `stove0-opus-review-sampler` | `depends-on` | `stove0-review-sampler-support` | `required` |
| `stove0-opus-review-sampler` | `depends-on` | `stove0-review-target-contracts` | `required` |
| `stove0-opus-target` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-opus-target` | `depends-on` | `riverhog-client` | `required` |
| `stove0-opus-target` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-opus-target` | `depends-on` | `stove0-media-archive-target-contracts` | `required` |
| `stove0-opus-target` | `depends-on` | `stove0-media-archive-target-support` | `required` |
| `stove0-opus-target` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-opus-target` | `depends-on` | `stove0-target-support` | `required` |
| `stove0-protocol` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-recipe-config` | `depends-on` | `config-validation` | `required` |
| `stove0-recipe-config` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-recipe-config` | `depends-on` | `stove0-target-protocol` | `required` |
| `stove0-review-materialize-target` | `depends-on` | `riverhog-client` | `required` |
| `stove0-review-materialize-target` | `depends-on` | `stove0-review-target-contracts` | `required` |
| `stove0-review-materialize-target` | `depends-on` | `stove0-review-target-support` | `required` |
| `stove0-review-materialize-target` | `depends-on` | `stove0-target-support` | `required` |
| `stove0-review-planning` | `depends-on` | `stove0-media-sampling-observer-contracts` | `required` |
| `stove0-review-planning` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-review-planning` | `depends-on` | `stove0-review-target-contracts` | `required` |
| `stove0-review-rclone-effect-target` | `depends-on` | `riverhog-client` | `required` |
| `stove0-review-rclone-effect-target` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-review-rclone-effect-target` | `depends-on` | `stove0-review-target-contracts` | `required` |
| `stove0-review-rclone-effect-target` | `depends-on` | `stove0-review-target-support` | `required` |
| `stove0-review-rclone-effect-target` | `depends-on` | `stove0-target-support` | `required` |
| `stove0-review-sampler-client` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-review-sampler-client` | `depends-on` | `stove0-review-sampler-protocol` | `required` |
| `stove0-review-sampler-protocol` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-review-sampler-protocol` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-review-sampler-protocol` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-review-sampler-support` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-review-sampler-support` | `depends-on` | `stove0-review-sampler-client` | `required` |
| `stove0-review-sampler-support` | `depends-on` | `stove0-review-sampler-protocol` | `required` |
| `stove0-review-target-contracts` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-review-target-contracts` | `depends-on` | `stove0-target-protocol` | `required` |
| `stove0-review-target-support` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-review-target-support` | `depends-on` | `riverhog-client` | `required` |
| `stove0-review-target-support` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-review-target-support` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-review-target-support` | `depends-on` | `stove0-review-sampler-client` | `required` |
| `stove0-review-target-support` | `depends-on` | `stove0-review-sampler-protocol` | `required` |
| `stove0-review-target-support` | `depends-on` | `stove0-review-target-contracts` | `required` |
| `stove0-review-target-support` | `depends-on` | `stove0-target-support` | `required` |
| `stove0-server` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-server` | `depends-on` | `riverhog-client` | `required` |
| `stove0-server` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-server` | `depends-on` | `state-schema` | `required` |
| `stove0-server` | `depends-on` | `stove0-observer-client` | `required` |
| `stove0-server` | `depends-on` | `stove0-observer-protocol` | `required` |
| `stove0-server` | `depends-on` | `stove0-operator-contracts` | `required` |
| `stove0-server` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-server` | `depends-on` | `stove0-recipe-config` | `required` |
| `stove0-server` | `depends-on` | `stove0-target-client` | `required` |
| `stove0-server` | `depends-on` | `stove0-target-protocol` | `required` |
| `stove0-server` | `depends-on` | `time-formats` | `required` |
| `stove0-target-client` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-target-client` | `depends-on` | `stove0-target-protocol` | `required` |
| `stove0-target-protocol` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-target-protocol` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-target-protocol` | `depends-on` | `stove0-protocol` | `required` |
| `stove0-target-support` | `depends-on` | `http-api-contracts` | `required` |
| `stove0-target-support` | `depends-on` | `riverhog-client` | `required` |
| `stove0-target-support` | `depends-on` | `riverhog-protocol` | `required` |
| `stove0-target-support` | `depends-on` | `stove0-target-client` | `required` |
| `stove0-target-support` | `depends-on` | `stove0-target-protocol` | `required` |
| `gogurt-linux-listener-host` | `implements-extension-point` | `gogurt.listener-host-providers` | `gogurt-linux-listener-host` |
| `gogurt-linux-mounted-volume` | `implements-extension-point` | `gogurt.mounted-volume-providers` | `gogurt-linux-mounted-volume` |
| `gogurt-macos-listener-host` | `implements-extension-point` | `gogurt.listener-host-providers` | `gogurt-macos-listener-host` |
| `gogurt-macos-mounted-volume` | `implements-extension-point` | `gogurt.mounted-volume-providers` | `gogurt-macos-mounted-volume` |
| `gogurt-windows-listener-host` | `implements-extension-point` | `gogurt.listener-host-providers` | `gogurt-windows-listener-host` |
| `gogurt-windows-mounted-volume` | `implements-extension-point` | `gogurt.mounted-volume-providers` | `gogurt-windows-mounted-volume` |
| `riverhog-provenance-linux-contracts` | `implements-extension-point` | `riverhog.provenance-contracts` | `riverhog-linux` |
| `riverhog-provenance-linux-observer` | `implements-extension-point` | `riverhog.provenance-observers` | `riverhog-linux` |
| `riverhog-provenance-macos-contracts` | `implements-extension-point` | `riverhog.provenance-contracts` | `riverhog-macos` |
| `riverhog-provenance-macos-observer` | `implements-extension-point` | `riverhog.provenance-observers` | `riverhog-macos` |
| `riverhog-provenance-windows-contracts` | `implements-extension-point` | `riverhog.provenance-contracts` | `riverhog-windows` |
| `riverhog-provenance-windows-observer` | `implements-extension-point` | `riverhog.provenance-observers` | `riverhog-windows` |
| `stove0-media-metadata-observer-contracts` | `implements-extension-point` | `stove0.observer-semantic-validators` | `media-metadata` |
| `stove0-media-sampling-observer-contracts` | `implements-extension-point` | `stove0.observer-semantic-validators` | `media-sampling` |
| `riverhog-storage-adapter-aws` | `implements-protocol` | `riverhog-storage-adapter` | `` |
| `riverhog-storage-adapter-backblaze` | `implements-protocol` | `riverhog-storage-adapter` | `` |
| `riverhog-storage-adapter-filesystem` | `implements-protocol` | `riverhog-storage-adapter` | `` |
| `stove0-exiftool-observer` | `implements-protocol` | `stove0-observer` | `` |
| `stove0-ffprobe-sampling-observer` | `implements-protocol` | `stove0-observer` | `` |
| `stove0-nvenc-av1-opus-review-sampler` | `implements-protocol` | `stove0-review-sampler` | `` |
| `stove0-nvenc-av1-opus-target` | `implements-protocol` | `stove0-target` | `` |
| `stove0-opus-review-sampler` | `implements-protocol` | `stove0-review-sampler` | `` |
| `stove0-opus-target` | `implements-protocol` | `stove0-target` | `` |
| `stove0-review-materialize-target` | `implements-protocol` | `stove0-target` | `` |
| `stove0-review-rclone-effect-target` | `implements-protocol` | `stove0-target` | `` |
| `gogurt` | `installed-as` | `uv-tool` | `` |
| `piggity` | `installed-as` | `uv-tool` | `` |
| `riverhog-recover` | `installed-as` | `uv-tool` | `` |
| `stove0-client` | `installed-as` | `uv-tool` | `` |
| `gogurt-core` | `owns-extension-point` | `gogurt.mounted-volume-providers` | `` |
| `gogurt-listener-runtime` | `owns-extension-point` | `gogurt.listener-host-providers` | `` |
| `riverhog-provenance` | `owns-extension-point` | `riverhog.provenance-observers` | `` |
| `riverhog-provenance-contracts` | `owns-extension-point` | `riverhog.provenance-contracts` | `` |
| `stove0-observer-client` | `owns-extension-point` | `stove0.observer-semantic-validators` | `` |
| `riverhog-storage-adapter-protocol` | `owns-protocol` | `riverhog-storage-adapter` | `` |
| `stove0-observer-protocol` | `owns-protocol` | `stove0-observer` | `` |
| `stove0-review-sampler-protocol` | `owns-protocol` | `stove0-review-sampler` | `` |
| `stove0-target-protocol` | `owns-protocol` | `stove0-target` | `` |
| `mango-fish` | `packaged-in` | `mango-fish` | `` |
| `riverhog-ftp-adapter` | `packaged-in` | `riverhog-ftp-adapter` | `` |
| `riverhog-provenance-linux-observer` | `packaged-in` | `riverhog-ftp-adapter` | `` |
| `riverhog-server` | `packaged-in` | `riverhog` | `` |
| `riverhog-storage-adapter-aws` | `packaged-in` | `riverhog-storage-adapter-aws` | `` |
| `riverhog-storage-adapter-backblaze` | `packaged-in` | `riverhog-storage-adapter-backblaze` | `` |
| `riverhog-storage-adapter-filesystem` | `packaged-in` | `riverhog-storage-adapter-filesystem` | `` |
| `stove0-exiftool-observer` | `packaged-in` | `stove0-exiftool-observer` | `` |
| `stove0-ffprobe-sampling-observer` | `packaged-in` | `stove0-ffprobe-sampling-observer` | `` |
| `stove0-nvenc-av1-opus-review-sampler` | `packaged-in` | `stove0-nvenc-av1-opus-target` | `` |
| `stove0-nvenc-av1-opus-target` | `packaged-in` | `stove0-nvenc-av1-opus-target` | `` |
| `stove0-opus-review-sampler` | `packaged-in` | `stove0-opus-target` | `` |
| `stove0-opus-target` | `packaged-in` | `stove0-opus-target` | `` |
| `stove0-review-materialize-target` | `packaged-in` | `stove0-review-materialize-target` | `` |
| `stove0-review-rclone-effect-target` | `packaged-in` | `stove0-review-rclone-effect-target` | `` |
| `stove0-server` | `packaged-in` | `stove0` | `` |
