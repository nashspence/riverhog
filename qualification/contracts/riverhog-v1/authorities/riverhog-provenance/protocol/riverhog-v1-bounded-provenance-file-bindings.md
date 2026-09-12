# Riverhog v1 bounded provenance file bindings

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-v1-bounded-provenance-file-bindings:00ab173f22 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-bindings-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json` — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-bindings-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | maximum=512, minimum=1, reason=bounded-provenance-binding-volume |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json
- `title`: Riverhog v1 bounded provenance file bindings
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `files` | yes | array |  |
| `first_file_order` | yes | integer |  |
| `schema` | yes | object (1 fields) |  |

### Definitions

| Definition | Shape |
|---|---|
| `file` | object |
| `sha256` | string |
