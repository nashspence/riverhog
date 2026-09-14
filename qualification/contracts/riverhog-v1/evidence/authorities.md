# Exact authority inventory

[Atlas](../index.md) · [Freeze evidence](index.md)

This page is intentionally an alphabetical reconciliation inventory, not another contract map.

## Aggregate ownership

| Interface | Count |
|---|---:|
| `artifact-verification` | 3 |
| `cli` | 167 |
| `compatibility-guarantees` | 8 |
| `configuration` | 7 |
| `configuration-environment` | 252 |
| `durable-state` | 137 |
| `extent` | 12 |
| `http-operations` | 147 |
| `http-schemas` | 394 |
| `http-security-schemes` | 2 |
| `http-service-declaration` | 3 |
| `installation-roots` | 4 |
| `process-protocol` | 4 |
| `process-protocol-operations` | 24 |
| `process-protocol-schemas` | 43 |
| `publication-locations` | 2 |
| `python` | 2640 |
| `python-distributions` | 71 |
| `release-artifacts` | 12 |
| `runtime-images` | 13 |
| `schema` | 31 |
| `versioning-tags` | 5 |

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
| `boundary-projection` | `/boundaries` | Frozen authority and extension topology used to attribute and navigate semantic contracts; component existence is not itself an external semantic promise. |
| `contract-projection-envelope` | `/schema, /series` | Machine projection identity, not an external product promise. |
| `durable-state-registry-envelope` | `/external_contract/durable_state/schema` | Registry format identity; each durable-state promise belongs to its named owner. |
| `extent-projection-envelope` | `/external_contract/extents/coverage, /external_contract/extents/schema, /external_contract/extents/sha256` | Generated coverage and identity metadata, not external extent semantics. |
| `release-publication-envelope` | `/external_contract/release/publication/schema` | Release generator and evidence format metadata; the exact publication promises belong to the meaningful release interfaces. |

## Authorities

| Authority | Contract elements | Interfaces |
|---|---:|---|
| [extent-contract](../authorities/extent-contract/index.md) | 12 | extent |
| [gogurt](../authorities/gogurt/index.md) | 21 | cli |
| [gogurt-core](../authorities/gogurt-core/index.md) | 41 | configuration, python |
| [gogurt-linux-listener-host](../authorities/gogurt-linux-listener-host/index.md) | 14 | configuration-environment, python |
| [gogurt-linux-mounted-volume](../authorities/gogurt-linux-mounted-volume/index.md) | 3 | python |
| [gogurt-listener](../authorities/gogurt-listener/index.md) | 4 | durable-state |
| [gogurt-listener-runtime](../authorities/gogurt-listener-runtime/index.md) | 53 | python |
| [gogurt-macos-listener-host](../authorities/gogurt-macos-listener-host/index.md) | 12 | python |
| [gogurt-macos-mounted-volume](../authorities/gogurt-macos-mounted-volume/index.md) | 3 | python |
| [gogurt-path-volume-support](../authorities/gogurt-path-volume-support/index.md) | 7 | python |
| [gogurt-windows-listener-host](../authorities/gogurt-windows-listener-host/index.md) | 19 | configuration-environment, python |
| [gogurt-windows-mounted-volume](../authorities/gogurt-windows-mounted-volume/index.md) | 3 | python |
| [http-api-contracts](../authorities/http-api-contracts/index.md) | 63 | python |
| [lifecycle-events](../authorities/lifecycle-events/index.md) | 26 | python |
| [mango-fish](../authorities/mango-fish/index.md) | 6 | cli, configuration |
| [mango-fish-cursor](../authorities/mango-fish-cursor/index.md) | 2 | durable-state |
| [piggity](../authorities/piggity/index.md) | 93 | cli, configuration-environment |
| [piggity-local](../authorities/piggity-local/index.md) | 7 | durable-state |
| [release](../authorities/release/index.md) | 118 | artifact-verification, compatibility-guarantees, installation-roots, publication-locations, python-distributions, release-artifacts, runtime-images, versioning-tags |
| [riverhog](../authorities/riverhog/index.md) | 364 | http-operations, http-schemas, http-security-schemes, http-service-declaration |
| [riverhog-age](../authorities/riverhog-age/index.md) | 34 | python |
| [riverhog-application-access](../authorities/riverhog-application-access/index.md) | 47 | python |
| [riverhog-archive-contracts](../authorities/riverhog-archive-contracts/index.md) | 76 | python, schema |
| [riverhog-catalog](../authorities/riverhog-catalog/index.md) | 91 | durable-state |
| [riverhog-client](../authorities/riverhog-client/index.md) | 250 | configuration-environment, python |
| [riverhog-ftp-adapter](../authorities/riverhog-ftp-adapter/index.md) | 37 | cli, configuration, configuration-environment, http-operations, http-schemas, http-security-schemes, http-service-declaration, python |
| [riverhog-ftp-adapter-api-client](../authorities/riverhog-ftp-adapter-api-client/index.md) | 16 | configuration-environment, python |
| [riverhog-ftp-custody](../authorities/riverhog-ftp-custody/index.md) | 6 | durable-state |
| [riverhog-protocol](../authorities/riverhog-protocol/index.md) | 279 | python, schema |
| [riverhog-provenance](../authorities/riverhog-provenance/index.md) | 115 | configuration-environment, python, schema |
| [riverhog-provenance-contracts](../authorities/riverhog-provenance-contracts/index.md) | 16 | python |
| [riverhog-provenance-installation](../authorities/riverhog-provenance-installation/index.md) | 1 | durable-state |
| [riverhog-provenance-linux-contracts](../authorities/riverhog-provenance-linux-contracts/index.md) | 9 | python, schema |
| [riverhog-provenance-macos-contracts](../authorities/riverhog-provenance-macos-contracts/index.md) | 8 | python, schema |
| [riverhog-provenance-windows-contracts](../authorities/riverhog-provenance-windows-contracts/index.md) | 14 | python, schema |
| [riverhog-recover](../authorities/riverhog-recover/index.md) | 8 | cli, python |
| [riverhog-server](../authorities/riverhog-server/index.md) | 52 | configuration-environment |
| [riverhog-storage-adapter-asgi-support](../authorities/riverhog-storage-adapter-asgi-support/index.md) | 1 | python |
| [riverhog-storage-adapter-aws](../authorities/riverhog-storage-adapter-aws/index.md) | 37 | configuration-environment, python |
| [riverhog-storage-adapter-backblaze](../authorities/riverhog-storage-adapter-backblaze/index.md) | 20 | configuration-environment |
| [riverhog-storage-adapter-filesystem](../authorities/riverhog-storage-adapter-filesystem/index.md) | 30 | cli, configuration-environment, python |
| [riverhog-storage-adapter-protocol](../authorities/riverhog-storage-adapter-protocol/index.md) | 121 | python |
| [riverhog-storage-adapter-s3-support](../authorities/riverhog-storage-adapter-s3-support/index.md) | 24 | python |
| [riverhog-storage-adapter-support](../authorities/riverhog-storage-adapter-support/index.md) | 86 | cli, process-protocol, process-protocol-operations, process-protocol-schemas, python |
| [state-schema](../authorities/state-schema/index.md) | 58 | python |
| [stove0](../authorities/stove0/index.md) | 170 | http-operations, http-schemas, http-service-declaration |
| [stove0-api-client](../authorities/stove0-api-client/index.md) | 39 | configuration-environment, python |
| [stove0-client](../authorities/stove0-client/index.md) | 37 | cli |
| [stove0-control](../authorities/stove0-control/index.md) | 23 | durable-state |
| [stove0-exiftool-observer](../authorities/stove0-exiftool-observer/index.md) | 12 | configuration-environment, python |
| [stove0-ffprobe-sampling-observer](../authorities/stove0-ffprobe-sampling-observer/index.md) | 12 | configuration-environment, python |
| [stove0-media-archive-target-contracts](../authorities/stove0-media-archive-target-contracts/index.md) | 30 | python |
| [stove0-media-archive-target-support](../authorities/stove0-media-archive-target-support/index.md) | 23 | python |
| [stove0-media-metadata-observer-contracts](../authorities/stove0-media-metadata-observer-contracts/index.md) | 19 | python |
| [stove0-media-sampling-observer-contracts](../authorities/stove0-media-sampling-observer-contracts/index.md) | 16 | python |
| [stove0-nvenc-av1-opus-review-sampler](../authorities/stove0-nvenc-av1-opus-review-sampler/index.md) | 11 | configuration-environment, python |
| [stove0-nvenc-av1-opus-target](../authorities/stove0-nvenc-av1-opus-target/index.md) | 12 | configuration-environment, python |
| [stove0-observer-client](../authorities/stove0-observer-client/index.md) | 6 | python |
| [stove0-observer-protocol](../authorities/stove0-observer-protocol/index.md) | 75 | python |
| [stove0-observer-support](../authorities/stove0-observer-support/index.md) | 49 | cli, process-protocol, process-protocol-operations, process-protocol-schemas, python |
| [stove0-operator-contracts](../authorities/stove0-operator-contracts/index.md) | 99 | configuration, python |
| [stove0-opus-review-sampler](../authorities/stove0-opus-review-sampler/index.md) | 11 | configuration-environment, python |
| [stove0-opus-target](../authorities/stove0-opus-target/index.md) | 11 | configuration-environment, python |
| [stove0-protocol](../authorities/stove0-protocol/index.md) | 190 | python |
| [stove0-recipe-config](../authorities/stove0-recipe-config/index.md) | 29 | configuration, python |
| [stove0-review-materialize-target](../authorities/stove0-review-materialize-target/index.md) | 11 | configuration-environment, python |
| [stove0-review-planning](../authorities/stove0-review-planning/index.md) | 5 | cli, python |
| [stove0-review-rclone-effect-target](../authorities/stove0-review-rclone-effect-target/index.md) | 18 | configuration-environment, python |
| [stove0-review-sampler-client](../authorities/stove0-review-sampler-client/index.md) | 7 | python |
| [stove0-review-sampler-protocol](../authorities/stove0-review-sampler-protocol/index.md) | 30 | python |
| [stove0-review-sampler-support](../authorities/stove0-review-sampler-support/index.md) | 31 | cli, process-protocol, process-protocol-operations, process-protocol-schemas, python |
| [stove0-review-target-contracts](../authorities/stove0-review-target-contracts/index.md) | 24 | python |
| [stove0-review-target-support](../authorities/stove0-review-target-support/index.md) | 16 | configuration, python |
| [stove0-server](../authorities/stove0-server/index.md) | 349 | configuration-environment, python |
| [stove0-target-client](../authorities/stove0-target-client/index.md) | 15 | python |
| [stove0-target-jobs](../authorities/stove0-target-jobs/index.md) | 3 | durable-state |
| [stove0-target-protocol](../authorities/stove0-target-protocol/index.md) | 129 | python |
| [stove0-target-support](../authorities/stove0-target-support/index.md) | 162 | cli, configuration-environment, process-protocol, process-protocol-operations, process-protocol-schemas, python |
