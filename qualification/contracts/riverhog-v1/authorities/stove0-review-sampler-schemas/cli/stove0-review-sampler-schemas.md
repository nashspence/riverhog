# stove0-review-sampler-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-sampler-schemas:stove0-review-sampler-schemas:e0c130478c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-schemas` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0-review-sampler-schemas/name`
- `/external_contract/cli/stove0-review-sampler-schemas/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0-review-sampler-schemas` — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `stove0-review-sampler-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _VersionAction | no |  | --version |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-sampler-schemas/name`

<!-- exact-contract-value: 162a3ef3dc87aa1b3b1b2b34823d4702440ce3754ea9e31d325df3f59ec24a0c -->

```json
"stove0-review-sampler-schemas"
```

### `/external_contract/cli/stove0-review-sampler-schemas/parameters`

<!-- exact-contract-value: 280890e414521dae90aaec62414808f8ed59735cf99b00ca647a8e328fd86c02 -->

```json
[
  {
    "dest": "version",
    "kind": "_VersionAction",
    "nargs": 0,
    "options": [
      "--version"
    ],
    "required": false
  }
]
```
