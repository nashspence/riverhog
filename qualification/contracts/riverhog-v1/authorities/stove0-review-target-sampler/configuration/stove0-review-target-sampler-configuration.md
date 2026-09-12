# stove0-review-target-sampler configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-review-target-sampler:stove0-review-target-sampler-configuration:f34c0cad9c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-target-sampler` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/configuration_documents/stove0-review-target-sampler`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `configuration:stove0-review-target-sampler` — `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::SamplerConfig`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=2048, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: SamplerConfig
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allow_insecure_http` | no | boolean |  |
| `base_url` | yes | string |  |
| `descriptor_sha256` | yes | string |  |
| `id` | yes | string |  |
| `image_digest` | yes | string |  |
| `token_file` | yes | string |  |
