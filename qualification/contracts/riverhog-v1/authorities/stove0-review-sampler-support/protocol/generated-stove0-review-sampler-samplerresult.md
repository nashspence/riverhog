# generated:stove0-review-sampler: SamplerResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-samplerresult:84b113bb2c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerResult`

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
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: SamplerResult
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `execution_evidence` | no | object |  |
| `failure` | no | object (2 fields) |  |
| `format` | no | string |  |
| `inapplicable` | no | object (2 fields) |  |
| `outputs` | no | array |  |
| `request_sha256` | yes | string |  |
| `result_sha256` | yes | string |  |
| `sampler_descriptor_sha256` | yes | string |  |
| `state` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonValue` | object (0 fields) |
| `SamplerFailure` | object |
| `SamplerInapplicable` | object |
| `SamplerOutput` | object |
