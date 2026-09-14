# stove0-target-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-support:stove0-target-conformance:d841c51477 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fde6abe43b"></a>Parser name: `stove0-target-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-e562e2016c"></a>`base_url` | _StoreAction | yes |  |  |
| <a id="s-e11b46d794"></a>`case` | _AppendAction | no | Path | --case |

### Result and failure contract

- <a id="s-6249f0cc01"></a>Result identity: `stove0-target-conformance-cli-result/root/v1`
- <a id="s-1350f1c503"></a>Profile: `stove0-target-conformance-cli/v1`
- <a id="s-bc3f5fd291"></a>Structured output: `always-json`
- <a id="s-657d814bea"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-0b0fd48900"></a>`conformant` | <a id="s-c13ae47cfe"></a>`0` | <a id="s-4ef2ecd06f"></a>`{"json":"stove0-target-conformance-result/v1"}` | <a id="s-dac23286c0"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-e8837d64c5"></a>`usage` | <a id="s-93ddbb5b98"></a>`2` | <a id="s-b09afd9389"></a>`{"all":"empty"}` | <a id="s-abd1bb4830"></a>`{"all":"noncontractual-usage-diagnostic"}` |

## Governing policies

- <a id="pa-0099487305"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-target-conformance](../../../evidence/sources.md#src-7a44eec01b) — `reference/stove0/packages/target-support/src/stove0_target_support/conformance.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-target-conformance/name`
- `/external_contract/cli/stove0-target-conformance/parameters`
- `/external_contract/cli/stove0-target-conformance/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-target-conformance/name`

<!-- exact-contract-value: 7d2f3eb433195f93988d5c7dcbf7137865b0f1e5c179664a7c5c265cb50162aa -->

```json
"stove0-target-conformance"
```

### `/external_contract/cli/stove0-target-conformance/parameters`

<!-- exact-contract-value: ea7099ca96775fc4d87b2bb319c3f57a9777a35ab2c40592b9644eb27f9ab03b -->

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
    "dest": "case",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--case"
    ],
    "required": false,
    "type": "Path"
  }
]
```

### `/external_contract/cli/stove0-target-conformance/result_contract`

<!-- exact-contract-value: 1e9e36978a593fbcf13b2359cc7d168a9d49c7d0bf605de866c28c705540e4e9 -->

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
  "identity": "stove0-target-conformance-cli-result/root/v1",
  "profile_id": "stove0-target-conformance-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "conformant",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": "stove0-target-conformance-result/v1"
      }
    }
  ]
}
```
