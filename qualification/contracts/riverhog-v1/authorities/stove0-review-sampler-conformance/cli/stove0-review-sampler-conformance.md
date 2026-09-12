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

## Contract summary

- Parser name: `stove0-review-sampler-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  |  |
| `` | _StoreAction | yes | Path | --token-file |
| `` | _StoreAction | no | Path | --request |
| `` | _StoreTrueAction | no |  | --allow-insecure-http |
| `` | _VersionAction | no |  | --version |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-sampler-conformance/name`

<!-- exact-contract-value: 2be934e3e5c6d2b8852132daa56e7eacc7a61415aa715a8e2ecb0373352b3905 -->

```json
"stove0-review-sampler-conformance"
```

### `/external_contract/cli/stove0-review-sampler-conformance/parameters`

<!-- exact-contract-value: 49cca455fdf894c8d637d0b2169d59ee01d635b1ff796a41e3b609b0e826eb3e -->

```json
[
  {
    "dest": "base_url",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  },
  {
    "dest": "token_file",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--token-file"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "request",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--request"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "allow_insecure_http",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--allow-insecure-http"
    ],
    "required": false
  },
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
