# generated:stove0-review-sampler: SamplerRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-samplerrequest:afb432726c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 9 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerRequest`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-review-sampler` — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=1099511627776, minimum=1, reason=schema-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=86400, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: SamplerRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `cancellation_path` | yes | string |  |
| `format` | no | string |  |
| `inputs` | yes | array |  |
| `maximum_output_bytes` | yes | integer |  |
| `portable_intent` | yes | object |  |
| `request_sha256` | yes | string |  |
| `sampler_descriptor_sha256` | yes | string |  |
| `timeout_seconds` | yes | integer |  |
| `windows` | yes | array |  |
| `workspace_id` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonValue` | object (0 fields) |
| `SamplerInput` | object |
| `SamplerWindow` | object |
