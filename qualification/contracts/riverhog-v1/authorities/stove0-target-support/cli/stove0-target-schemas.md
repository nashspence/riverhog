# stove0-target-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-support:stove0-target-schemas:23cfbd1fa3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-bf1e05e507"></a>Parser name: `stove0-target-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-2ed9764c8b"></a>`output` | _StoreAction | no | Path | --output |
| <a id="s-e30f06cf81"></a>`compact` | _StoreTrueAction | no |  | --compact |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4f3d4aceb0"></a>`help` | <a id="s-0bd8e84067"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-383669ae67"></a>`0` | <a id="s-e5179d89c4"></a>`"noncontractual-framework-help"` | <a id="s-fcf4c931c1"></a>`"empty"` |

### Result and failure contract

- <a id="s-f28bef60d8"></a>Result identity: `stove0-target-schemas-cli-result/root/v1`
- <a id="s-512f160535"></a>Profile: `stove0-target-schemas-cli/v1`
- <a id="s-61ebe19865"></a>Structured output: `always-json`
- <a id="s-468df1d76e"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d2d10ff9e4"></a>`emitted` | <a id="s-64c1848d52"></a>`{"kind":"option-absent","parameter":"output"}` | <a id="s-9b9753b7e5"></a>`0` | <a id="s-a76f53c364"></a>json: [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md) | <a id="s-64a9479089"></a>all: `empty` |
| <a id="s-58777f49d4"></a>`written` | <a id="s-3f753cac11"></a>`{"kind":"option-present","parameter":"output"}` | <a id="s-e6dc9fa4fc"></a>`0` | <a id="s-5183a46fc8"></a>all: `empty` | <a id="s-442860db44"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-49d7facace"></a>`usage` | <a id="s-b16257bdc6"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d4bb46238f"></a>`2` | <a id="s-e7f36b4908"></a>all: `empty` | <a id="s-b9965c0f7e"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --compact](#s-e30f06cf81) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-b9062b2394"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-851f99f45d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-target-schemas](../../../evidence/sources.md#src-71d64b87b5) — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-target-schemas/name`
- `/external_contract/cli/stove0-target-schemas/parameters`
- `/external_contract/cli/stove0-target-schemas/result_contract`
- `/external_contract/cli/stove0-target-schemas/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-target-schemas/name`

<!-- exact-contract-value: 061cab384f391f332f2ed0807ef95bc47f0507c010da1026b316ff00f4fab52b -->

```json
"stove0-target-schemas"
```

### `/external_contract/cli/stove0-target-schemas/parameters`

<!-- exact-contract-value: d09dd1e324eea493304f34cc58b3f5fe965bfdb99ef580493ab2779a9afcca7d -->

```json
[
  {
    "dest": "output",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--output"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "compact",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--compact"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/stove0-target-schemas/result_contract`

<!-- exact-contract-value: ef5cfabe6b011a2b4f241996cf1d603cebe66ffe52cae25399d14e8ac8a024da -->

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
  "identity": "stove0-target-schemas-cli-result/root/v1",
  "profile_id": "stove0-target-schemas-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "emitted",
      "selected_by": {
        "kind": "option-absent",
        "parameter": "output"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "authority": "generated:stove0-target",
          "kind": "document-authority"
        }
      }
    },
    {
      "exit_status": 0,
      "id": "written",
      "selected_by": {
        "kind": "option-present",
        "parameter": "output"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-target-schemas/terminating_controls`

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
