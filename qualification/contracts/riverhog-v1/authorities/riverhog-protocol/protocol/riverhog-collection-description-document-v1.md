# Riverhog collection description document v1

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-protocol:riverhog-collection-description-document-v1:6266f2d39a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-protocol` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-collection-description-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json` — `packages/riverhog-protocol/schemas/riverhog-collection-description-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| encoded-size | bytes | `contract_max` | maximum=32768, reason=bounded-human-authored-catalog-description |
| length | characters | `contract_max` | maximum=32768, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json
- `title`: Riverhog collection description document v1
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_root_sha256` | yes | string |  |
| `description` | yes | object (1 fields) |  |
| `description_identity` | yes | string |  |
| `format` | yes | object (1 fields) |  |
| `revision` | yes | integer |  |
