# stove0-review-sampler-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-conformance:stove0-review-sampler-conformance:c57fa0f5ef -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-conformance` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/cli/stove0-review-sampler-conformance/name`
- `/external_contract/cli/stove0-review-sampler-conformance/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0-review-sampler-conformance` — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/conformance.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract

- Parser name: `stove0-review-sampler-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  |  |
| `` | _StoreAction | yes | Path | --token-file |
| `` | _StoreAction | no | Path | --request |
| `` | _StoreTrueAction | no |  | --allow-insecure-http |
| `` | _VersionAction | no |  | --version |
