# stove0-target-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-support:stove0-target-conformance:3ebf652daa -->

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

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-aa3d50f436"></a>`help` | <a id="s-20d511eefa"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-6f8e2d7c3f"></a>`0` | <a id="s-5238b20ecc"></a>`"noncontractual-framework-help"` | <a id="s-4c8621e9c6"></a>`"empty"` |

### Result and failure contract

- <a id="s-6249f0cc01"></a>Result identity: `stove0-target-conformance-cli-result/root/v1`
- <a id="s-1350f1c503"></a>Profile: `stove0-target-conformance-cli/v1`
- <a id="s-bc3f5fd291"></a>Structured output: `always-json`
- <a id="s-657d814bea"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0b0fd48900"></a>`conformant` | <a id="s-93ffbe4e74"></a>`{"kind":"conformance-completed"}` | <a id="s-c13ae47cfe"></a>`0` | <a id="s-4ef2ecd06f"></a>json: [generated:stove0-target: TargetConformanceResult](../process-protocol-schemas/generated-stove0-target-targetconformanceresult.md) | <a id="s-dac23286c0"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e8837d64c5"></a>`usage` | <a id="s-d202e41e20"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-93ddbb5b98"></a>`2` | <a id="s-b09afd9389"></a>all: `empty` | <a id="s-abd1bb4830"></a>all: `noncontractual-usage-diagnostic` |

## Governing policies

- <a id="pa-c80f2bfdda"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

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
- `/external_contract/cli/stove0-target-conformance/terminating_controls`

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

<!-- exact-contract-value: ab483de6d91fd840041cc49ee219d32f434b1f524a51587cbbea2e9e2ac28156 -->

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
  "identity": "stove0-target-conformance-cli-result/root/v1",
  "profile_id": "stove0-target-conformance-cli/v1",
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
          "authority": "generated:stove0-target",
          "definition": "TargetConformanceResult",
          "kind": "schema-authority"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-target-conformance/terminating_controls`

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
