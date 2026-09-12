# stove0-observer-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-conformance:stove0-observer-conformance:2b03b92040 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-conformance` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/cli/stove0-observer-conformance/name`
- `/external_contract/cli/stove0-observer-conformance/parameters`

## Effective policies

- `compatibility/cli/v1`

## Executable sources and proof

- `cli:stove0-observer-conformance` — `reference/stove0/packages/observer-support/src/stove0_observer_support/conformance.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Contract summary

- Parser name: `stove0-observer-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  |  |
| `` | _AppendAction | no | Path | --invocation |
| `` | _AppendAction | no | Path | --semantic-vectors |
| `` | _AppendAction | no |  | --semantic-validator-provider |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-observer-conformance/name`

<!-- exact-contract-value: e0b72013170271e2f13e91746ae3838974b2346286ccc7288e52e35f176d8677 -->

```json
"stove0-observer-conformance"
```

### `/external_contract/cli/stove0-observer-conformance/parameters`

<!-- exact-contract-value: 081d1ac083ce7b64aa00e86ad69c85cc777077719787c8513002aaf88d4360fe -->

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
    "default": [],
    "dest": "invocation",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--invocation"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": [],
    "dest": "semantic_vectors",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--semantic-vectors"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": [],
    "dest": "semantic_validator_provider",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--semantic-validator-provider"
    ],
    "required": false
  }
]
```
