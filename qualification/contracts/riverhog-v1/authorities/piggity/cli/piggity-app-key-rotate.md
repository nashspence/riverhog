# piggity app key rotate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-rotate:ab9c94f847 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-383824b169"></a>Parser name: `rotate`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-667bdb79a0"></a>`app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| <a id="s-fa35dffec2"></a>`key_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | key_id |
| <a id="s-6529995c44"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-a6b465fea1"></a>Result identity: `piggity-cli-result/app/key/rotate/v1`
- <a id="s-5888ab4ed9"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-875de54892"></a>Structured output: `optional-json`
- <a id="s-56f297383f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-fb5f9c867f"></a>`completed` | <a id="s-c2d6dfd70a"></a>`0` | <a id="s-641a48ee38"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-79e7c04085"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-a0d9f03ecd"></a>`usage` | <a id="s-e126783be0"></a>`2` | <a id="s-9d3973c593"></a>`{"all":"empty"}` | <a id="s-fafdf928b0"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-4478e2c0c0"></a>`operational` | <a id="s-08ba5101a1"></a>`1` | <a id="s-c2fee5a1ca"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-13e3b07f3c"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-667bdb79a0) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-6529995c44) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter key_id](#s-fa35dffec2) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/rotate](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-rotate.md)

## Governing policies

- <a id="pa-660a7abc34"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e07d6105a5"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/name`

<!-- exact-contract-value: f66ab9323564942157c358fa5caee94868c1433e5c3e672e8456f747813b9a59 -->

```json
"rotate"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/parameters`

<!-- exact-contract-value: 1749f130449a17b41843bc114da325f40e7d6ade60854043985e5bcec11d2cad -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "app_name",
    "nargs": 1,
    "options": [
      "app_name"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "key_id",
    "nargs": 1,
    "options": [
      "key_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/result_contract`

<!-- exact-contract-value: 619034e28030576ed20c381d5271df96a6141a4d2beb7a56297836f41051cf05 -->

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
  "identity": "piggity-cli-result/app/key/rotate/v1",
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
