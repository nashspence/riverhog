# stove0-target-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-support:stove0-target-schemas:1cd479914e -->

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

### Result and failure contract

- <a id="s-f28bef60d8"></a>Result identity: `stove0-target-schemas-cli-result/root/v1`
- <a id="s-512f160535"></a>Profile: `stove0-target-schemas-cli/v1`
- <a id="s-61ebe19865"></a>Structured output: `always-json`
- <a id="s-468df1d76e"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-d2d10ff9e4"></a>`emitted` | <a id="s-9b9753b7e5"></a>`0` | <a id="s-a76f53c364"></a>`{"json":"stove0-target-schema-bundle/v1"}` | <a id="s-64a9479089"></a>`{"all":"empty"}` |
| <a id="s-58777f49d4"></a>`written` | <a id="s-e6dc9fa4fc"></a>`0` | <a id="s-5183a46fc8"></a>`{"all":"empty"}` | <a id="s-442860db44"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-49d7facace"></a>`usage` | <a id="s-d4bb46238f"></a>`2` | <a id="s-e7f36b4908"></a>`{"all":"empty"}` | <a id="s-b9965c0f7e"></a>`{"all":"noncontractual-usage-diagnostic"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --compact](#s-e30f06cf81) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-f377f02552"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-b0bbe2de3d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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

<!-- exact-contract-value: 918419868e6a5035215e85286495c049caf8dd86d6819af76be1dcf036152e67 -->

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
  "identity": "stove0-target-schemas-cli-result/root/v1",
  "profile_id": "stove0-target-schemas-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "emitted",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": "stove0-target-schema-bundle/v1"
      }
    },
    {
      "exit_status": 0,
      "id": "written",
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
