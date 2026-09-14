# stove0-observer-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-support:stove0-observer-conformance:13c54f4871 -->

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

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-3f7b8569a5"></a>`help` | <a id="s-d96f74d625"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-46223dbefe"></a>`0` | <a id="s-a225e25c21"></a>`"noncontractual-framework-help"` | <a id="s-838f158800"></a>`"empty"` |

### Result and failure contract

- <a id="s-2afc9660ff"></a>Result identity: `stove0-observer-conformance-cli-result/root/v1`
- <a id="s-c25f2c3b51"></a>Profile: `stove0-observer-conformance-cli/v1`
- <a id="s-e46691ab2c"></a>Structured output: `always-json`
- <a id="s-0fa93f9723"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1327368945"></a>`conformant` | <a id="s-5e5db3361b"></a>`{"kind":"conformance-completed"}` | <a id="s-c66cfdc7f4"></a>`0` | <a id="s-87e367e201"></a>`json: stove0-observer-conformance-result/v1` | <a id="s-529648d0d7"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b34855a653"></a>`usage` | <a id="s-fe2ac3466e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c5f5947482"></a>`2` | <a id="s-2442499427"></a>`all: empty` | <a id="s-95cc494653"></a>`all: noncontractual-usage-diagnostic` |

## Governing policies

- <a id="pa-a94d050db1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

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
- `/external_contract/cli/stove0-observer-conformance/terminating_controls`

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

<!-- exact-contract-value: c0efa382a08bf95928ca6fc8e550b7c78529bfc0f90ee145aa80eb132c46f42e -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "selected_by": {
        "kind": "parser-rejected-invocation"
      },
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
      "selected_by": {
        "kind": "conformance-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "identity": "stove0-observer-conformance-result/v1",
          "kind": "semantic-format"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-observer-conformance/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "-h",
        "--help"
      ]
    }
  }
]
```
