# Riverhog v1 bounded immutable archive-volume metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-bounded-immutable-archive-vol-a08fdea82e:4b65368595 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-archive-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 14 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-volume-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json` — `packages/riverhog-archive-contracts/schemas/collection-archive-volume-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=50000, minimum=1, reason=schema-maximum |
| cardinality | items | `segmented_no_total_max` | maximum=1024, minimum=1, reason=bounded-archive-volume-parts |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `segmented_no_total_max` | maximum=1024, minimum=1, reason=bounded-archive-volume-parts |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json
- `title`: Riverhog v1 bounded immutable archive-volume metadata
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_generation` | yes | #/$defs/sha256 |  |
| `archive_tree_sha256` | yes | #/$defs/sha256 |  |
| `schema` | yes | object (1 fields) |  |
| `volume` | yes | object (1 fields) |  |

### Definitions

| Definition | Shape |
|---|---|
| `age_state` | object |
| `pack` | object |
| `part` | object |
| `segment` | object |
| `segment_file` | object |
| `sequence` | string |
| `sha256` | string |
