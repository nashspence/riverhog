# Exact authority inventory

[Atlas](../index.md) · [Freeze evidence](index.md)

This page is intentionally an alphabetical reconciliation inventory, not another contract map.

## Aggregate ownership

| Interface | Count |
|---|---:|
| `boundary` | 84 |
| `cli` | 167 |
| `configuration` | 6 |
| `configuration-environment` | 128 |
| `durable-state` | 8 |
| `extent` | 12 |
| `http` | 544 |
| `operation` | 147 |
| `protocol` | 78 |
| `python` | 25 |
| `release` | 12 |

## Declared aggregate authorities

These cross-component authorities are explicit repository decisions. Component and durable-state authorities come directly from their frozen registries.

| Authority | Normative scope |
|---|---|
| `extent-contract` | Repository-wide v1 external extent principles and rules. |
| `release` | Coordinated v1 compatibility and publication promises. |
| `repository` | Repository-owned v1 boundary and packaging promises. |
| `riverhog` | The Riverhog service API and its maintained cross-interface operation parity. |
| `stove0` | The Stove0 reference application API and its maintained cross-interface operation parity. |

## Non-contractual projection machinery

These values remain in the exact machine projection for validation, but do not own external product promises.

| Projection record | Machine authority | Reason |
|---|---|---|
| `contract-projection-envelope` | `/schema, /series` | Machine projection identity, not an external product promise. |
| `durable-state-registry-envelope` | `/external_contract/durable_state/schema` | Registry format identity; each durable-state promise belongs to its named owner. |
| `extent-projection-envelope` | `/external_contract/extents/coverage, /external_contract/extents/schema, /external_contract/extents/sha256` | Generated coverage and identity metadata, not external extent semantics. |

## Authorities

| Authority | Contract elements | Interfaces |
|---|---:|---|
| [config-validation](../authorities/config-validation/index.md) | 1 | boundary |
| [extent-contract](../authorities/extent-contract/index.md) | 12 | extent |
| [gogurt](../authorities/gogurt/index.md) | 24 | boundary, cli, configuration-environment |
| [gogurt-core](../authorities/gogurt-core/index.md) | 4 | boundary, configuration, python |
| [gogurt-linux-listener-host](../authorities/gogurt-linux-listener-host/index.md) | 1 | boundary |
| [gogurt-linux-mounted-volume](../authorities/gogurt-linux-mounted-volume/index.md) | 1 | boundary |
| [gogurt-listener](../authorities/gogurt-listener/index.md) | 1 | durable-state |
| [gogurt-listener-runtime](../authorities/gogurt-listener-runtime/index.md) | 3 | boundary, python |
| [gogurt-macos-listener-host](../authorities/gogurt-macos-listener-host/index.md) | 1 | boundary |
| [gogurt-macos-mounted-volume](../authorities/gogurt-macos-mounted-volume/index.md) | 1 | boundary |
| [gogurt-path-volume-support](../authorities/gogurt-path-volume-support/index.md) | 1 | boundary |
| [gogurt-windows-listener-host](../authorities/gogurt-windows-listener-host/index.md) | 1 | boundary |
| [gogurt-windows-mounted-volume](../authorities/gogurt-windows-mounted-volume/index.md) | 1 | boundary |
| [http-api-contracts](../authorities/http-api-contracts/index.md) | 2 | boundary, python |
| [lifecycle-events](../authorities/lifecycle-events/index.md) | 2 | boundary, python |
| [mango-fish](../authorities/mango-fish/index.md) | 7 | boundary, cli, configuration |
| [mango-fish-cursor](../authorities/mango-fish-cursor/index.md) | 1 | durable-state |
| [piggity](../authorities/piggity/index.md) | 94 | boundary, cli, configuration-environment |
| [piggity-local](../authorities/piggity-local/index.md) | 1 | durable-state |
| [release](../authorities/release/index.md) | 12 | release |
| [repository](../authorities/repository/index.md) | 4 | boundary |
| [riverhog](../authorities/riverhog/index.md) | 471 | http, operation |
| [riverhog-age](../authorities/riverhog-age/index.md) | 2 | boundary, python |
| [riverhog-application-access](../authorities/riverhog-application-access/index.md) | 2 | boundary, python |
| [riverhog-archive-contracts](../authorities/riverhog-archive-contracts/index.md) | 6 | boundary, protocol, python |
| [riverhog-catalog](../authorities/riverhog-catalog/index.md) | 1 | durable-state |
| [riverhog-client](../authorities/riverhog-client/index.md) | 15 | boundary, configuration-environment, python |
| [riverhog-ftp-adapter](../authorities/riverhog-ftp-adapter/index.md) | 29 | boundary, cli, configuration, configuration-environment, http, operation |
| [riverhog-ftp-adapter-api-client](../authorities/riverhog-ftp-adapter-api-client/index.md) | 6 | boundary, configuration-environment |
| [riverhog-ftp-custody](../authorities/riverhog-ftp-custody/index.md) | 1 | durable-state |
| [riverhog-protocol](../authorities/riverhog-protocol/index.md) | 3 | boundary, protocol, python |
| [riverhog-provenance](../authorities/riverhog-provenance/index.md) | 11 | boundary, configuration-environment, protocol, python |
| [riverhog-provenance-contracts](../authorities/riverhog-provenance-contracts/index.md) | 3 | boundary, python |
| [riverhog-provenance-installation](../authorities/riverhog-provenance-installation/index.md) | 1 | durable-state |
| [riverhog-provenance-linux-contracts](../authorities/riverhog-provenance-linux-contracts/index.md) | 6 | boundary, protocol |
| [riverhog-provenance-linux-observer](../authorities/riverhog-provenance-linux-observer/index.md) | 1 | boundary |
| [riverhog-provenance-macos-contracts](../authorities/riverhog-provenance-macos-contracts/index.md) | 5 | boundary, protocol |
| [riverhog-provenance-macos-observer](../authorities/riverhog-provenance-macos-observer/index.md) | 1 | boundary |
| [riverhog-provenance-windows-contracts](../authorities/riverhog-provenance-windows-contracts/index.md) | 11 | boundary, protocol |
| [riverhog-provenance-windows-observer](../authorities/riverhog-provenance-windows-observer/index.md) | 1 | boundary |
| [riverhog-recover](../authorities/riverhog-recover/index.md) | 2 | boundary, cli |
| [riverhog-server](../authorities/riverhog-server/index.md) | 52 | boundary, configuration-environment |
| [riverhog-storage-adapter-asgi-support](../authorities/riverhog-storage-adapter-asgi-support/index.md) | 2 | boundary, python |
| [riverhog-storage-adapter-aws](../authorities/riverhog-storage-adapter-aws/index.md) | 1 | boundary |
| [riverhog-storage-adapter-backblaze](../authorities/riverhog-storage-adapter-backblaze/index.md) | 1 | boundary |
| [riverhog-storage-adapter-filesystem](../authorities/riverhog-storage-adapter-filesystem/index.md) | 2 | boundary, cli |
| [riverhog-storage-adapter-protocol](../authorities/riverhog-storage-adapter-protocol/index.md) | 3 | boundary, python |
| [riverhog-storage-adapter-s3-support](../authorities/riverhog-storage-adapter-s3-support/index.md) | 1 | boundary |
| [riverhog-storage-adapter-support](../authorities/riverhog-storage-adapter-support/index.md) | 28 | boundary, cli, protocol, python |
| [state-schema](../authorities/state-schema/index.md) | 1 | boundary |
| [stove0](../authorities/stove0/index.md) | 203 | http, operation |
| [stove0-api-client](../authorities/stove0-api-client/index.md) | 7 | boundary, configuration-environment, python |
| [stove0-client](../authorities/stove0-client/index.md) | 38 | boundary, cli |
| [stove0-control](../authorities/stove0-control/index.md) | 1 | durable-state |
| [stove0-exiftool-observer](../authorities/stove0-exiftool-observer/index.md) | 9 | boundary, configuration-environment |
| [stove0-ffprobe-sampling-observer](../authorities/stove0-ffprobe-sampling-observer/index.md) | 9 | boundary, configuration-environment |
| [stove0-media-archive-target-contracts](../authorities/stove0-media-archive-target-contracts/index.md) | 1 | boundary |
| [stove0-media-archive-target-support](../authorities/stove0-media-archive-target-support/index.md) | 1 | boundary |
| [stove0-media-metadata-observer-contracts](../authorities/stove0-media-metadata-observer-contracts/index.md) | 1 | boundary |
| [stove0-media-sampling-observer-contracts](../authorities/stove0-media-sampling-observer-contracts/index.md) | 1 | boundary |
| [stove0-nvenc-av1-opus-review-sampler](../authorities/stove0-nvenc-av1-opus-review-sampler/index.md) | 2 | boundary, configuration-environment |
| [stove0-nvenc-av1-opus-target](../authorities/stove0-nvenc-av1-opus-target/index.md) | 3 | boundary, configuration-environment |
| [stove0-observer-client](../authorities/stove0-observer-client/index.md) | 3 | boundary, python |
| [stove0-observer-protocol](../authorities/stove0-observer-protocol/index.md) | 3 | boundary, python |
| [stove0-observer-support](../authorities/stove0-observer-support/index.md) | 12 | boundary, cli, protocol, python |
| [stove0-operator-contracts](../authorities/stove0-operator-contracts/index.md) | 2 | boundary, python |
| [stove0-opus-review-sampler](../authorities/stove0-opus-review-sampler/index.md) | 2 | boundary, configuration-environment |
| [stove0-opus-target](../authorities/stove0-opus-target/index.md) | 2 | boundary, configuration-environment |
| [stove0-protocol](../authorities/stove0-protocol/index.md) | 2 | boundary, python |
| [stove0-recipe-config](../authorities/stove0-recipe-config/index.md) | 3 | boundary, configuration, python |
| [stove0-review-materialize-target](../authorities/stove0-review-materialize-target/index.md) | 1 | boundary |
| [stove0-review-planning](../authorities/stove0-review-planning/index.md) | 2 | boundary, cli |
| [stove0-review-rclone-effect-target](../authorities/stove0-review-rclone-effect-target/index.md) | 1 | boundary |
| [stove0-review-sampler-client](../authorities/stove0-review-sampler-client/index.md) | 1 | boundary |
| [stove0-review-sampler-protocol](../authorities/stove0-review-sampler-protocol/index.md) | 2 | boundary |
| [stove0-review-sampler-support](../authorities/stove0-review-sampler-support/index.md) | 9 | boundary, cli, protocol |
| [stove0-review-target-contracts](../authorities/stove0-review-target-contracts/index.md) | 1 | boundary |
| [stove0-review-target-support](../authorities/stove0-review-target-support/index.md) | 3 | boundary, configuration |
| [stove0-server](../authorities/stove0-server/index.md) | 21 | boundary, configuration-environment |
| [stove0-target-client](../authorities/stove0-target-client/index.md) | 2 | boundary, python |
| [stove0-target-jobs](../authorities/stove0-target-jobs/index.md) | 1 | durable-state |
| [stove0-target-protocol](../authorities/stove0-target-protocol/index.md) | 3 | boundary, python |
| [stove0-target-support](../authorities/stove0-target-support/index.md) | 14 | boundary, cli, configuration-environment, protocol, python |
| [time-formats](../authorities/time-formats/index.md) | 1 | boundary |
