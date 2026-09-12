# stove0-observer-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-conformance:stove0-observer-conformance:2b03b92040 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-conformance](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](index.md#f-943ef934d4) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- <a id="s-72fdebec40"></a>Parser name: `stove0-observer-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-6504128d72"></a>`` | _StoreAction | yes |  |  |
| <a id="s-3ce127cbc3"></a>`` | _AppendAction | no | Path | --invocation |
| <a id="s-38ac0c67ea"></a>`` | _AppendAction | no | Path | --semantic-vectors |
| <a id="s-c322de089c"></a>`` | _AppendAction | no |  | --semantic-validator-provider |

## Governing policies

- <a id="pa-e1bf1b3690"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-observer-conformance](../../../evidence/sources.md#src-5719d140a8) — `reference/stove0/packages/observer-support/src/stove0_observer_support/conformance.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-observer-conformance/name`
- `/external_contract/cli/stove0-observer-conformance/parameters`

### Exact owned JSON

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
