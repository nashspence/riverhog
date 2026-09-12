# generated:stove0-observer: ObserverContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-observercontract:8c3081b52b -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObserverContract`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-observer` — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=67108864, minimum=1, reason=schema-maximum |

## Contract

- `title`: ObserverContract
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `contract_sha256` | yes | string |  |
| `facts_schema` | yes | #/$defs/JsonSchemaDocument |  |
| `facts_semantics` | yes | #/$defs/SemanticValidationProfile |  |
| `id` | yes | string |  |
| `maximum_result_bytes` | no | integer |  |
| `options_schema` | yes | #/$defs/JsonSchemaDocument |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `SemanticValidationProfile` | object |
