# generated:stove0-observer: ObserverDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-observerdescriptor:dda498f06c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObserverDescriptor`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-observer` — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |

## Contract

- `title`: ObserverDescriptor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `contracts` | yes | array |  |
| `descriptor_sha256` | yes | string |  |
| `image_digest` | yes | string |  |
| `implementation_id` | yes | string |  |
| `implementation_version` | yes | string |  |
| `protocol` | no | string |  |
| `source_revision` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `ObserverContractSupport` | object |
| `SemanticValidationProfile` | object |
