# stove0-observer-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-support:stove0-observer-conformance:8a7b697a8d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-72fdebec40"></a>Parser name: `stove0-observer-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-6504128d72"></a>`base_url` | _StoreAction | yes |  |  |
| <a id="s-3ce127cbc3"></a>`invocation` | _AppendAction | no | Path | --invocation |
| <a id="s-38ac0c67ea"></a>`semantic_vectors` | _AppendAction | no | Path | --semantic-vectors |
| <a id="s-c322de089c"></a>`semantic_validator_provider` | _AppendAction | no |  | --semantic-validator-provider |

### Result and failure contract

- <a id="s-2afc9660ff"></a>Result identity: `stove0-observer-conformance-cli-result/root/v1`
- <a id="s-c25f2c3b51"></a>Profile: `stove0-observer-conformance-cli/v1`
- <a id="s-e46691ab2c"></a>Structured output: `always-json`
- <a id="s-0fa93f9723"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-1327368945"></a>`conformant` | <a id="s-c66cfdc7f4"></a>`0` | <a id="s-87e367e201"></a>`{"json":"stove0-observer-conformance-result/v1"}` | <a id="s-529648d0d7"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-b34855a653"></a>`usage` | <a id="s-c5f5947482"></a>`2` | <a id="s-2442499427"></a>`{"all":"empty"}` | <a id="s-95cc494653"></a>`{"all":"noncontractual-usage-diagnostic"}` |

## Governing policies

- <a id="pa-4953436fd0"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

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
- `/external_contract/cli/stove0-observer-conformance/result_contract`

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

### `/external_contract/cli/stove0-observer-conformance/result_contract`

<!-- exact-contract-value: 55117924f5863846a07f09ee0c7b28b5af236a42945f7240122c64c95288bc57 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "stderr": {
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "stove0-observer-conformance-cli-result/root/v1",
  "profile_id": "stove0-observer-conformance-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "conformant",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": "stove0-observer-conformance-result/v1"
      }
    }
  ]
}
```
