# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:https-nashspence-github-io-riverhog-v1-pr-15ee50803f:cf9b0ef47a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1sparse-map.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json` — `packages/riverhog-provenance/src/riverhog_provenance/schemas/sparse-map.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `complete` | yes | boolean |  |
| `extents` | yes | array |  |
