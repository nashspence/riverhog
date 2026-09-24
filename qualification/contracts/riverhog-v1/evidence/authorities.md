# Authority reconciliation

[Atlas](../index.md) · [Reference navigation](index.md)

The ordinary atlas map owns the authority/interface inventory and its scope descriptions. This reference reconciles aggregate ownership and the projection's internal bookkeeping.

## Aggregate ownership

| Interface | Count |
|---|---:|
| `artifact-verification` | 3 |
| `cli` | 190 |
| `compatibility-guarantees` | 9 |
| `configuration` | 7 |
| `configuration-environment` | 252 |
| `durable-state` | 137 |
| `extent` | 12 |
| `http-operations` | 147 |
| `http-schemas` | 401 |
| `http-security-schemes` | 2 |
| `http-service-declaration` | 3 |
| `installation-roots` | 4 |
| `process-protocol` | 4 |
| `process-protocol-operations` | 24 |
| `process-protocol-schemas` | 43 |
| `publication-locations` | 2 |
| `python` | 2843 |
| `python-distributions` | 72 |
| `release-artifacts` | 12 |
| `runtime-images` | 13 |
| `schema` | 31 |
| `versioning-tags` | 5 |

## Declared aggregate scopes

| Declared authority | Scope description |
|---|---|
| `extent-contract` | [extent-contract](../authorities/extent-contract/index.md) |
| `release` | [release](../authorities/release/index.md) |
| `repository` | Repository-owned v1 boundary and packaging promises. No contract elements use this aggregate owner. |
| `riverhog` | [riverhog](../authorities/riverhog/index.md) |
| `stove0` | [stove0](../authorities/stove0/index.md) |

## Non-contractual projection machinery

These internal projection records are not exclusions of discovered external contracts.

| Projection record | Machine location | Reason |
|---|---|---|
| `boundary-projection` | `/boundaries` | Frozen authority and extension topology used to attribute and navigate semantic contracts; component existence is not itself an external semantic promise. |
| `contract-projection-envelope` | `/format, /series` | Machine projection identity, not an external product promise. |
| `durable-state-registry-envelope` | `/external_contract/durable_state/format` | Registry format identity; each durable-state promise belongs to its named owner. |
| `extent-projection-envelope` | `/external_contract/extents/coverage, /external_contract/extents/format, /external_contract/extents/sha256` | Generated coverage and identity metadata, not external extent semantics. |
| `release-publication-envelope` | `/external_contract/release/publication/format` | Release generator and evidence format metadata; the exact publication promises belong to the meaningful release interfaces. |
