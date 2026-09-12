# Riverhog v1 contract atlas

This generated atlas is the human navigation of the exact monolithic machine closure in `../riverhog-v1.json`. It is organized by authority, interface, and native semantic dossier; no page boundary changes contract identity.

## Closure status

Status: **complete** — every discovered candidate has exactly one disposition and every contractual fact has exactly one human owner.

Contract elements: **1208** · Extent decisions: **1962** · Excluded candidates: **13** · Source authorities: **217** · Atlas documents: **1620**

### Closure anomalies

| Anomaly | Count |
|---|---:|
| `missing` | 0 |
| `duplicate` | 0 |
| `stale` | 0 |
| `undecided` | 0 |
| `multiply_disposed` | 0 |
| `multiply_represented` | 0 |

### Independent evidence identities

| Identity domain | SHA-256 |
|---|---|
| `boundary_canonical_sha256` | `5af7171d406deec927ce70092a60eb033f51c53f2da16b7f1cea794e03e807be` |
| `boundary_legacy_sha256` | `5af7171d406deec927ce70092a60eb033f51c53f2da16b7f1cea794e03e807be` |
| `external_contract_sha256` | `c7e0f637b07409e63ddef600a4700ec6f213f5562c3c3b92f5bbc210694c61db` |
| `semantic_contract_sha256` | `f0fccc8ee3b98bf6f719e9a4bd31649cd5f22251d6bff884e455a6ba88be4bcb` |
| `coverage_sha256` | `d5a3846787aab654c12ccc98dabe41b3c1128957b95554868978b4ec21960f76` |
| `trace_sha256` | `1ebb9e0edcd9d0ba7ee7ad617bf47177632f7f725a38667570074e7c312948f7` |

The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the machine closure. It cannot be embedded inside the document bytes that it identifies.

## Audit navigation

- [Relationship-aware boundary map](relationships/index.md)
- [Contract-policy registry](policies/index.md)
- [Explicit exclusions](exclusions/index.md)
- [Executable sources and qualification routes](evidence/index.md)

## Aggregate contract shape

| Interface | Count |
|---|---:|
| `boundary` | 85 |
| `cli` | 167 |
| `configuration` | 6 |
| `configuration-environment` | 120 |
| `durable-state` | 9 |
| `extent` | 15 |
| `http` | 544 |
| `operation` | 147 |
| `protocol` | 78 |
| `python` | 25 |
| `release` | 12 |

| Detector | Count |
|---|---:|
| `boundary` | 85 |
| `cli-tree` | 167 |
| `configuration-document` | 6 |
| `configuration-environment` | 120 |
| `durable-state` | 9 |
| `extent` | 15 |
| `http-openapi` | 544 |
| `operation-matrix` | 147 |
| `protocol-schema` | 78 |
| `python-export` | 25 |
| `release-metadata` | 12 |

| Qualification route | Count |
|---|---:|
| `make build` | 200 |
| `make compose-smoke` | 670 |
| `make contract-freeze` | 15 |
| `make database-qualification` | 9 |
| `make dist-smoke` | 270 |
| `make operation-qualification` | 873 |
| `make release-check` | 106 |
| `make unit` | 126 |

## Complete authority inventory

The relationship map explains how these authorities interact. This flat inventory remains the exact completeness view.

| Authority | Contract elements | Interfaces |
|---|---:|---|
| [config-validation](authorities/config-validation/index.md) | 1 | `boundary` |
| [configuration](authorities/configuration/index.md) | 120 | `configuration-environment` |
| [durable-state](authorities/durable-state/index.md) | 1 | `durable-state` |
| [extent-contract](authorities/extent-contract/index.md) | 15 | `extent` |
| [gogurt](authorities/gogurt/index.md) | 22 | `boundary`, `cli` |
| [gogurt-core](authorities/gogurt-core/index.md) | 3 | `boundary`, `python` |
| [gogurt-linux-listener-host](authorities/gogurt-linux-listener-host/index.md) | 1 | `boundary` |
| [gogurt-linux-mounted-volume](authorities/gogurt-linux-mounted-volume/index.md) | 1 | `boundary` |
| [gogurt-listener](authorities/gogurt-listener/index.md) | 1 | `durable-state` |
| [gogurt-listener-runtime](authorities/gogurt-listener-runtime/index.md) | 3 | `boundary`, `python` |
| [gogurt-macos-listener-host](authorities/gogurt-macos-listener-host/index.md) | 1 | `boundary` |
| [gogurt-macos-mounted-volume](authorities/gogurt-macos-mounted-volume/index.md) | 1 | `boundary` |
| [gogurt-path-volume-support](authorities/gogurt-path-volume-support/index.md) | 1 | `boundary` |
| [gogurt-routes](authorities/gogurt-routes/index.md) | 1 | `configuration` |
| [gogurt-windows-listener-host](authorities/gogurt-windows-listener-host/index.md) | 1 | `boundary` |
| [gogurt-windows-mounted-volume](authorities/gogurt-windows-mounted-volume/index.md) | 1 | `boundary` |
| [http-api-contracts](authorities/http-api-contracts/index.md) | 2 | `boundary`, `python` |
| [lifecycle-events](authorities/lifecycle-events/index.md) | 2 | `boundary`, `python` |
| [mango-fish](authorities/mango-fish/index.md) | 7 | `boundary`, `cli`, `configuration` |
| [mango-fish-cursor](authorities/mango-fish-cursor/index.md) | 1 | `durable-state` |
| [piggity](authorities/piggity/index.md) | 87 | `boundary`, `cli` |
| [piggity-local](authorities/piggity-local/index.md) | 1 | `durable-state` |
| [release](authorities/release/index.md) | 12 | `release` |
| [repository](authorities/repository/index.md) | 5 | `boundary` |
| [riverhog](authorities/riverhog/index.md) | 471 | `http`, `operation` |
| [riverhog-age](authorities/riverhog-age/index.md) | 2 | `boundary`, `python` |
| [riverhog-application-access](authorities/riverhog-application-access/index.md) | 2 | `boundary`, `python` |
| [riverhog-archive-contracts](authorities/riverhog-archive-contracts/index.md) | 6 | `boundary`, `protocol`, `python` |
| [riverhog-catalog](authorities/riverhog-catalog/index.md) | 1 | `durable-state` |
| [riverhog-client](authorities/riverhog-client/index.md) | 3 | `boundary`, `python` |
| [riverhog-ftp-adapter](authorities/riverhog-ftp-adapter/index.md) | 26 | `boundary`, `cli`, `configuration`, `http`, `operation` |
| [riverhog-ftp-adapter-api-client](authorities/riverhog-ftp-adapter-api-client/index.md) | 1 | `boundary` |
| [riverhog-ftp-custody](authorities/riverhog-ftp-custody/index.md) | 1 | `durable-state` |
| [riverhog-protocol](authorities/riverhog-protocol/index.md) | 3 | `boundary`, `protocol`, `python` |
| [riverhog-provenance](authorities/riverhog-provenance/index.md) | 10 | `boundary`, `protocol`, `python` |
| [riverhog-provenance-contracts](authorities/riverhog-provenance-contracts/index.md) | 3 | `boundary`, `python` |
| [riverhog-provenance-installation](authorities/riverhog-provenance-installation/index.md) | 1 | `durable-state` |
| [riverhog-provenance-linux-contracts](authorities/riverhog-provenance-linux-contracts/index.md) | 6 | `boundary`, `protocol` |
| [riverhog-provenance-linux-observer](authorities/riverhog-provenance-linux-observer/index.md) | 1 | `boundary` |
| [riverhog-provenance-macos-contracts](authorities/riverhog-provenance-macos-contracts/index.md) | 5 | `boundary`, `protocol` |
| [riverhog-provenance-macos-observer](authorities/riverhog-provenance-macos-observer/index.md) | 1 | `boundary` |
| [riverhog-provenance-windows-contracts](authorities/riverhog-provenance-windows-contracts/index.md) | 11 | `boundary`, `protocol` |
| [riverhog-provenance-windows-observer](authorities/riverhog-provenance-windows-observer/index.md) | 1 | `boundary` |
| [riverhog-recover](authorities/riverhog-recover/index.md) | 2 | `boundary`, `cli` |
| [riverhog-server](authorities/riverhog-server/index.md) | 1 | `boundary` |
| [riverhog-storage-adapter-asgi-support](authorities/riverhog-storage-adapter-asgi-support/index.md) | 2 | `boundary`, `python` |
| [riverhog-storage-adapter-aws](authorities/riverhog-storage-adapter-aws/index.md) | 1 | `boundary` |
| [riverhog-storage-adapter-backblaze](authorities/riverhog-storage-adapter-backblaze/index.md) | 1 | `boundary` |
| [riverhog-storage-adapter-conformance](authorities/riverhog-storage-adapter-conformance/index.md) | 1 | `cli` |
| [riverhog-storage-adapter-filesystem](authorities/riverhog-storage-adapter-filesystem/index.md) | 1 | `boundary` |
| [riverhog-storage-adapter-filesystem-materialize](authorities/riverhog-storage-adapter-filesystem-materialize/index.md) | 1 | `cli` |
| [riverhog-storage-adapter-protocol](authorities/riverhog-storage-adapter-protocol/index.md) | 3 | `boundary`, `python` |
| [riverhog-storage-adapter-s3-support](authorities/riverhog-storage-adapter-s3-support/index.md) | 1 | `boundary` |
| [riverhog-storage-adapter-schemas](authorities/riverhog-storage-adapter-schemas/index.md) | 1 | `cli` |
| [riverhog-storage-adapter-support](authorities/riverhog-storage-adapter-support/index.md) | 26 | `boundary`, `protocol`, `python` |
| [state-schema](authorities/state-schema/index.md) | 1 | `boundary` |
| [stove0](authorities/stove0/index.md) | 240 | `cli`, `http`, `operation` |
| [stove0-api-client](authorities/stove0-api-client/index.md) | 2 | `boundary`, `python` |
| [stove0-client](authorities/stove0-client/index.md) | 1 | `boundary` |
| [stove0-control](authorities/stove0-control/index.md) | 1 | `durable-state` |
| [stove0-exiftool-observer](authorities/stove0-exiftool-observer/index.md) | 1 | `boundary` |
| [stove0-ffprobe-sampling-observer](authorities/stove0-ffprobe-sampling-observer/index.md) | 1 | `boundary` |
| [stove0-media-archive-target-contracts](authorities/stove0-media-archive-target-contracts/index.md) | 1 | `boundary` |
| [stove0-media-archive-target-support](authorities/stove0-media-archive-target-support/index.md) | 1 | `boundary` |
| [stove0-media-metadata-observer-contracts](authorities/stove0-media-metadata-observer-contracts/index.md) | 1 | `boundary` |
| [stove0-media-sampling-observer-contracts](authorities/stove0-media-sampling-observer-contracts/index.md) | 1 | `boundary` |
| [stove0-nvenc-av1-opus-review-sampler](authorities/stove0-nvenc-av1-opus-review-sampler/index.md) | 1 | `boundary` |
| [stove0-nvenc-av1-opus-target](authorities/stove0-nvenc-av1-opus-target/index.md) | 1 | `boundary` |
| [stove0-observer-client](authorities/stove0-observer-client/index.md) | 3 | `boundary`, `python` |
| [stove0-observer-conformance](authorities/stove0-observer-conformance/index.md) | 1 | `cli` |
| [stove0-observer-protocol](authorities/stove0-observer-protocol/index.md) | 3 | `boundary`, `python` |
| [stove0-observer-schemas](authorities/stove0-observer-schemas/index.md) | 1 | `cli` |
| [stove0-observer-support](authorities/stove0-observer-support/index.md) | 10 | `boundary`, `protocol`, `python` |
| [stove0-operator-contracts](authorities/stove0-operator-contracts/index.md) | 2 | `boundary`, `python` |
| [stove0-opus-review-sampler](authorities/stove0-opus-review-sampler/index.md) | 1 | `boundary` |
| [stove0-opus-target](authorities/stove0-opus-target/index.md) | 1 | `boundary` |
| [stove0-protocol](authorities/stove0-protocol/index.md) | 2 | `boundary`, `python` |
| [stove0-recipe-config](authorities/stove0-recipe-config/index.md) | 2 | `boundary`, `python` |
| [stove0-recipes](authorities/stove0-recipes/index.md) | 1 | `configuration` |
| [stove0-review-materialize-target](authorities/stove0-review-materialize-target/index.md) | 1 | `boundary` |
| [stove0-review-planning](authorities/stove0-review-planning/index.md) | 2 | `boundary`, `cli` |
| [stove0-review-rclone-effect-target](authorities/stove0-review-rclone-effect-target/index.md) | 1 | `boundary` |
| [stove0-review-sampler-client](authorities/stove0-review-sampler-client/index.md) | 1 | `boundary` |
| [stove0-review-sampler-conformance](authorities/stove0-review-sampler-conformance/index.md) | 1 | `cli` |
| [stove0-review-sampler-protocol](authorities/stove0-review-sampler-protocol/index.md) | 2 | `boundary` |
| [stove0-review-sampler-schemas](authorities/stove0-review-sampler-schemas/index.md) | 1 | `cli` |
| [stove0-review-sampler-support](authorities/stove0-review-sampler-support/index.md) | 7 | `boundary`, `protocol` |
| [stove0-review-target](authorities/stove0-review-target/index.md) | 1 | `configuration` |
| [stove0-review-target-contracts](authorities/stove0-review-target-contracts/index.md) | 1 | `boundary` |
| [stove0-review-target-sampler](authorities/stove0-review-target-sampler/index.md) | 1 | `configuration` |
| [stove0-review-target-support](authorities/stove0-review-target-support/index.md) | 1 | `boundary` |
| [stove0-server](authorities/stove0-server/index.md) | 1 | `boundary` |
| [stove0-target-client](authorities/stove0-target-client/index.md) | 2 | `boundary`, `python` |
| [stove0-target-conformance](authorities/stove0-target-conformance/index.md) | 1 | `cli` |
| [stove0-target-jobs](authorities/stove0-target-jobs/index.md) | 1 | `durable-state` |
| [stove0-target-protocol](authorities/stove0-target-protocol/index.md) | 3 | `boundary`, `python` |
| [stove0-target-schemas](authorities/stove0-target-schemas/index.md) | 1 | `cli` |
| [stove0-target-support](authorities/stove0-target-support/index.md) | 11 | `boundary`, `protocol`, `python` |
| [time-formats](authorities/time-formats/index.md) | 1 | `boundary` |
