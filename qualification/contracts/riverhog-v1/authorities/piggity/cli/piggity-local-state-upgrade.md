# piggity local state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-state-upgrade:6c063ee851 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7d7e1764bc"></a>Parser name: `upgrade`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-69cb4766a3"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-a8626cedd1"></a>Result identity: `piggity-cli-result/local/state/upgrade/v1`
- <a id="s-aa92509f2f"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-e905bf6142"></a>Structured output: `optional-json`
- <a id="s-e53a7e2c24"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-bfa86c8e3d"></a>`completed` | <a id="s-7cd3cfb0ef"></a>`0` | <a id="s-ecd561968b"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-643eb44d07"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-909b7682b5"></a>`usage` | <a id="s-f28452bff2"></a>`2` | <a id="s-65268670d3"></a>`{"all":"empty"}` | <a id="s-b8b0ebed46"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-ed5d0c67ce"></a>`operational` | <a id="s-74a21eda29"></a>`1` | <a id="s-ee326a54ea"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-cd01b6d2f3"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-69cb4766a3) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-33547ce9f2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ba5d9438bb"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/name`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/parameters`

<!-- exact-contract-value: f2cf9ed04ac608b58219dbcf22fc63be2fdf35901bc058f443df21b229aefd32 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_mode",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 5ec87d5b715fbc8de50c616216b7a04e8cd06273bf3887c30f2b7fe657c0e1a5 -->

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
    },
    {
      "exit_status": 1,
      "id": "operational",
      "stderr": {
        "human": "noncontractual-diagnostic",
        "json": "empty"
      },
      "stdout": {
        "human": "empty",
        "json": "http-api-contracts.ErrorResponse"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/local/state/upgrade/v1",
  "profile_id": "piggity-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ]
}
```
