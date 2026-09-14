# piggity app key quota set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-quota-set:9cdb4d35bb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e9e1b2971e"></a>Parser name: `set`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-9e2a6fbded"></a>`app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| <a id="s-1fdbbe9e74"></a>`key_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | key_id |
| <a id="s-face82c0e9"></a>`limit` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | limit |
| <a id="s-d1f18c8492"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-718da5acec"></a>Result identity: `piggity-cli-result/app/key/quota/set/v1`
- <a id="s-b91024870b"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-189fa5b204"></a>Structured output: `optional-json`
- <a id="s-424d30aabf"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-05c9985b25"></a>`completed` | <a id="s-0fe22078fa"></a>`0` | <a id="s-d34ef277e8"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-cb81e8ec76"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-78a41990d9"></a>`usage` | <a id="s-43d10b6021"></a>`2` | <a id="s-3d7093daa0"></a>`{"all":"empty"}` | <a id="s-cc718b83de"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-b376aa8695"></a>`operational` | <a id="s-e671af018d"></a>`1` | <a id="s-a4744c4a43"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-babba70233"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-9e2a6fbded) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-d1f18c8492) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter key_id](#s-1fdbbe9e74) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter limit](#s-face82c0e9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/download-quota](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-download-quota.md)

## Governing policies

- <a id="pa-f5ca244575"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-fe5cd57f67"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/name`

<!-- exact-contract-value: c7f5814b92ec9430648136406d844823df66e1af010dfa9456b5ec7ff5017f8b -->

```json
"set"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/parameters`

<!-- exact-contract-value: 638090c9b4c8b867084196d21d13a4c6d162923fdd5cb85b06f606947f84e108 -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "limit",
    "nargs": 1,
    "options": [
      "limit"
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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/result_contract`

<!-- exact-contract-value: 73216329c70dbc9b643ee9f488695843e7f734908c2955126137294906766d66 -->

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
  "identity": "piggity-cli-result/app/key/quota/set/v1",
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
