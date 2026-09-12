# generated:stove0-target: OperationContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-operationcontract:85bdd342ae -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 10 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/OperationContract`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-target` — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: OperationContract
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `contract_sha256` | yes | string |  |
| `effect_receipt_schema` | no | object (2 fields) |  |
| `id` | yes | string |  |
| `inputs` | yes | array |  |
| `intent_schema` | yes | #/$defs/JsonSchemaDocument |  |
| `intent_semantics` | yes | #/$defs/SemanticValidationProfile |  |
| `outputs` | no | array |  |
| `result_kind` | no | string |  |
| `source_retirement_permitted` | no | boolean |  |

### Definitions

| Definition | Shape |
|---|---|
| `InputArtifactContract` | object |
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `OutputArtifactContract` | object |
| `SemanticValidationProfile` | object |
