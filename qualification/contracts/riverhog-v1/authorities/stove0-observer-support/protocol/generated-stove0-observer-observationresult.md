# generated:stove0-observer: ObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-observationresult:033cccc2b7 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 14 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObservationResult`

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
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |

## Contract

- `title`: ObservationResult
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `execution_evidence` | no | object |  |
| `facts` | no | object (3 fields) |  |
| `facts_schema` | no | object (2 fields) |  |
| `facts_sha256` | no | object (3 fields) |  |
| `failure` | no | object (2 fields) |  |
| `format` | no | string |  |
| `inapplicable` | no | object (2 fields) |  |
| `observer` | yes | #/$defs/ObserverImplementation |  |
| `observer_contract_id` | yes | string |  |
| `observer_contract_sha256` | yes | string |  |
| `request_id` | yes | string |  |
| `result_sha256` | yes | string |  |
| `state` | yes | string |  |
| `subjects` | yes | array |  |

### Definitions

| Definition | Shape |
|---|---|
| `ArtifactSubject` | object |
| `CollectionId` | integer |
| `CollectionRootRef` | object |
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `ObservationFailure` | object |
| `ObservationInapplicable` | object |
| `ObserverImplementation` | object |
