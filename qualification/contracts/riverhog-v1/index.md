# Riverhog v1 contract atlas

This generated atlas is the human navigation of the exact monolithic machine closure in `../riverhog-v1.json`. It is organized by authority, interface, and native semantic dossier; no page boundary changes contract identity.

**Closure: complete; anomalies: 0.** Every discovered candidate has exactly one disposition and every contractual fact has exactly one human owner. **1208** contract elements and **1962** extent decisions are represented.

## Guided contract map

Start with the Riverhog authority, then follow only the branch relevant to the audit question. Linked pages provide maintained descriptions and exact contract detail.

### Public Riverhog service and API

- [riverhog](authorities/riverhog/index.md) — 471 public contract elements
  - Interfaces: [http](authorities/riverhog/http/index.md) · [operation](authorities/riverhog/operation/index.md)
  - Packaged implementation: [riverhog-server](relationships/components/riverhog-server.md)

### Archive custody and recovery

- [Riverhog archive authority](authorities/riverhog/index.md)
  - [Required implementation relationships](relationships/components/riverhog-server.md)
  - [Installed end-user and recovery surfaces](relationships/installation/index.md)

### Reusable contract and library authorities

- [Reusable contract and library authorities](relationships/authorities/index.md#route-reusable-contracts) — 15 authorities
  - [Component dependency view](relationships/riverhog-libraries/index.md)

### Independently implementable extension boundaries

- [Extension ownership and reference bindings](relationships/extensions/index.md) — 9 boundaries
  - [gogurt.listener-host-providers](relationships/extensions/index.md#node-extension-point-gogurt-listener-host-providers) · [gogurt.mounted-volume-providers](relationships/extensions/index.md#node-extension-point-gogurt-mounted-volume-providers) · [riverhog-storage-adapter](relationships/extensions/index.md#node-process-protocol-riverhog-storage-adapter) · [riverhog.provenance-contracts](relationships/extensions/index.md#node-extension-point-riverhog-provenance-contracts) · [riverhog.provenance-observers](relationships/extensions/index.md#node-extension-point-riverhog-provenance-observers) · [stove0-observer](relationships/extensions/index.md#node-process-protocol-stove0-observer) · [stove0-review-sampler](relationships/extensions/index.md#node-process-protocol-stove0-review-sampler) · [stove0-target](relationships/extensions/index.md#node-process-protocol-stove0-target) · [stove0.observer-semantic-validators](relationships/extensions/index.md#node-extension-point-stove0-observer-semantic-validators)

### Installed nonnormative references

- [Nonnormative reference authorities](relationships/authorities/index.md#route-nonnormative-references) — 73 authorities
- [Exact installation roots](relationships/installation/index.md)
  - [gogurt](relationships/components/gogurt.md) · [piggity](relationships/components/piggity.md) · [riverhog-recover](relationships/components/riverhog-recover.md) · [stove0-client](relationships/components/stove0-client.md)
- [Complete nonnormative reference ecosystem](relationships/references/index.md) — 55 components and 12 runtime images

### Supporting exact authorities

- [Packaged implementation and build authorities](relationships/authorities/index.md#route-implementation-build) — 5 authorities
- [Cross-cutting release, configuration, and state authorities](relationships/authorities/index.md#route-cross-cutting) — 5 authorities

The guided authority routes above account for every exact authority exactly once.

[Open the complete typed relationship graph](relationships/index.md).

## Completeness and evidence reference

Use these exhaustive layers after selecting the relevant contract branch above:

- [Complete typed relationship graph](relationships/index.md)
- [Contract-policy registry](policies/index.md)
- [Explicit exclusions](exclusions/index.md)
- [Executable sources and qualification routes](evidence/index.md)
- [Aggregate contract shape](#aggregate-contract-shape)
- [Exact authority ownership map](relationships/authorities/index.md)
- [Closure anomalies and evidence identities](#closure-and-identity-accounting)

## Closure and identity accounting

Contract elements: **1208** · Extent decisions: **1962** · Excluded candidates: **13** · Source authorities: **217** · Atlas documents: **1621**

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
