# Authority reconciliation

[Atlas](../index.md) · [Freeze evidence](index.md)

Aggregate ownership and projection bookkeeping support reconciliation. The atlas opening is the canonical authority/interface inventory.

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
| `http-schemas` | 394 |
| `http-security-schemes` | 2 |
| `http-service-declaration` | 3 |
| `installation-roots` | 4 |
| `process-protocol` | 4 |
| `process-protocol-operations` | 24 |
| `process-protocol-schemas` | 43 |
| `publication-locations` | 2 |
| `python` | 2827 |
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
