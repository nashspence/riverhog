# generated:stove0-observer: ObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-observationrequest:47be8ff95c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObservationRequest`

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
| value | schema-value | `contract_max` | maximum=67108864, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=86400, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: ObservationRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `format` | no | string |  |
| `maximum_result_bytes` | no | integer |  |
| `observer_contract_id` | yes | string |  |
| `observer_contract_sha256` | yes | string |  |
| `observer_descriptor_sha256` | yes | string |  |
| `observer_registration_id` | yes | string |  |
| `options` | no | object |  |
| `request_id` | yes | string |  |
| `retrieval_policy` | no | string |  |
| `subjects` | yes | array |  |
| `timeout_seconds` | no | integer |  |
| `work_id` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `ArtifactSubject` | object |
| `CollectionId` | integer |
| `CollectionRootRef` | object |
| `JsonValue` | object (0 fields) |
