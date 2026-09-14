# piggity app key access remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-access-remove:3878362c42 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c3016d500e"></a>Parser name: `remove`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-ca1077ee91"></a>`selector` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selector |
| <a id="s-12ddc39a1c"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-f5c26d8f85"></a>Result identity: `piggity-cli-result/app/key/access/remove/v1`
- <a id="s-7f574629a9"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-9a0a199ad7"></a>Structured output: `optional-json`
- <a id="s-91f4ca3d6b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-f15a4473ed"></a>`completed` | <a id="s-23a1b59ce2"></a>`0` | <a id="s-0d1afb37c6"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-0a647059db"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-d20a95d470"></a>`usage` | <a id="s-b5301b3d6e"></a>`2` | <a id="s-8b7f749142"></a>`{"all":"empty"}` | <a id="s-40bbae1fb7"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-03f863eef0"></a>`operational` | <a id="s-152e1ea9f4"></a>`1` | <a id="s-98c24f59a3"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-6eca98b4f0"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-12ddc39a1c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter selector](#s-ca1077ee91) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [DELETE /v1/apps/{app}/keys/{key_id}/access](../../riverhog/http-operations/delete-v1-apps-app-keys-key-id-access.md)

## Governing policies

- <a id="pa-4c163050f8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-3c3cb29be5"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/remove/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/remove/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/remove/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/remove/name`

<!-- exact-contract-value: 66f3d68b7627c9f6965029d01923daefa869b3f33123c145ddb25464b41a529a -->

```json
"remove"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/remove/parameters`

<!-- exact-contract-value: 6b32bc56c11c2cdafb747d7a7aa740468c2f614b5b1d0e2588fb031d746e52f9 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selector",
    "nargs": 1,
    "options": [
      "selector"
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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/remove/result_contract`

<!-- exact-contract-value: 658e9748c78c987c5a70c88ac09a84e62a8265ca84a853ad3441dad9403b9d1c -->

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
  "identity": "piggity-cli-result/app/key/access/remove/v1",
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
