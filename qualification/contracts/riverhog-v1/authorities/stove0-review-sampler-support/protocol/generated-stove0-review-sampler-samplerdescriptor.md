# generated:stove0-review-sampler: SamplerDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-samplerdescriptor:e7f5e3e5e8 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerDescriptor`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-review-sampler` — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |

## Contract

- `title`: SamplerDescriptor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `descriptor_sha256` | yes | string |  |
| `image_digest` | yes | string |  |
| `implementation_id` | yes | string |  |
| `implementation_version` | yes | string |  |
| `output_role` | yes | string |  |
| `portable_intent_schema` | yes | #/$defs/JsonSchemaDocument |  |
| `primary_operation_contract_sha256` | yes | string |  |
| `primary_operation_id` | yes | string |  |
| `protocol` | no | string |  |
| `source_revision` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
