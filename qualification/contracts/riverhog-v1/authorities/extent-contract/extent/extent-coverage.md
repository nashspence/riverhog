# Extent coverage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-coverage:ff25c7c922 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `policy` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `classified` | 1962 |
| `discovered` | 1962 |
| `duplicate` | 0 |
| `missing` | 0 |
| `owners` | additional keys=`gogurt`, `gogurt-routes`, `https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json`, `https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json`, `https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json`, `https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json`, `https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json`, `https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json`, `https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json`, `https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json`, `https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json`, `https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json`, `https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json`, `mango-fish`, `piggity`, `riverhog`, `riverhog-client`, `riverhog-ftp-adapter`, `riverhog-ftp-adapter-api-client`, `riverhog-recover`, `riverhog-server`, `riverhog-storage-adapter-conformance`, `riverhog-storage-adapter-filesystem-materialize`, `riverhog-storage-adapter-protocol`, `riverhog-storage-adapter-schemas`, `stove0`, `stove0-api-client`, `stove0-observer-protocol`, `stove0-observer-schemas`, `stove0-recipes`, `stove0-review-sampler-conformance`, `stove0-review-sampler-protocol`, `stove0-review-sampler-schemas`, `stove0-review-target`, `stove0-review-target-sampler`, `stove0-server`, `stove0-target-protocol`, `stove0-target-schemas`, `stove0-target-support` |
| `policies` | additional keys=`contract_max`, `extension_owned`, `fixed`, `operational_policy`, `segmented_no_total_max` |
| `stale` | 0 |
| `undecided` | 0 |

## Governing policies

- `extent-principle/logical-totals/v1`

## Evidence

### Qualification

- `make contract-freeze`
- `make operation-qualification`

### Executable sources

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/coverage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23e6446882410f742735186485ed5759430778bca07054ac9d16b076386aae57 -->

```json
{
  "classified": 1962,
  "discovered": 1962,
  "duplicate": 0,
  "missing": 0,
  "owners": {
    "gogurt": 55,
    "gogurt-routes": 2,
    "https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json": 79,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json": 8,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json": 1,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json": 3,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json": 6,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json": 1,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json": 3,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json": 3,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json": 2,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json": 7,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json": 5,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json": 3,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json": 2,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json": 1,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json": 1,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json": 11,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json": 2,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json": 1,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json": 2,
    "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json": 2,
    "https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json": 5,
    "https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json": 2,
    "https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json": 14,
    "https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json": 5,
    "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json": 4,
    "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json": 1,
    "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json": 2,
    "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json": 7,
    "https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json": 2,
    "mango-fish": 8,
    "piggity": 311,
    "riverhog": 493,
    "riverhog-client": 7,
    "riverhog-ftp-adapter": 27,
    "riverhog-ftp-adapter-api-client": 1,
    "riverhog-recover": 3,
    "riverhog-server": 34,
    "riverhog-storage-adapter-conformance": 1,
    "riverhog-storage-adapter-filesystem-materialize": 3,
    "riverhog-storage-adapter-protocol": 66,
    "riverhog-storage-adapter-schemas": 1,
    "stove0": 413,
    "stove0-api-client": 1,
    "stove0-observer-protocol": 77,
    "stove0-observer-schemas": 1,
    "stove0-recipes": 39,
    "stove0-review-sampler-conformance": 2,
    "stove0-review-sampler-protocol": 54,
    "stove0-review-sampler-schemas": 1,
    "stove0-review-target": 4,
    "stove0-review-target-sampler": 3,
    "stove0-server": 6,
    "stove0-target-protocol": 162,
    "stove0-target-schemas": 1,
    "stove0-target-support": 1
  },
  "policies": {
    "contract_max": 432,
    "extension_owned": 55,
    "fixed": 914,
    "operational_policy": 480,
    "segmented_no_total_max": 81
  },
  "stale": 0,
  "undecided": 0
}
```
