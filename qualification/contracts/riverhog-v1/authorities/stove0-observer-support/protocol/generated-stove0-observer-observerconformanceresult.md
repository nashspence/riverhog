# generated:stove0-observer: ObserverConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-observerconformanceresult:bab9f16cd8 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 30 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObserverConformanceResult`

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
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=67108864, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: ObserverConformanceResult
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `contracts` | yes | array |  |
| `coverage` | yes | #/$defs/ObserverConformanceCoverage |  |
| `descriptor` | yes | #/$defs/ObserverDescriptor |  |
| `format` | no | string |  |
| `status` | yes | string |  |

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
| `ObservationRequest` | object |
| `ObservationResult` | object |
| `ObserverConformanceCoverage` | object |
| `ObserverContractConformance` | object |
| `ObserverContractConformanceEvidence` | object |
| `ObserverContractSupport` | object |
| `ObserverDescriptor` | object |
| `ObserverImplementation` | object |
| `ObserverSemanticAcceptance` | object |
| `ObserverSemanticVectorEvidence` | object |
| `SemanticFactsConformanceVector` | object |
| `SemanticFactsConformanceVectors` | object |
| `SemanticValidationProfile` | object |
