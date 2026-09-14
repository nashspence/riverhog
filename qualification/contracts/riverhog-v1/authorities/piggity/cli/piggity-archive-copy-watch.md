# piggity archive copy watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-copy-watch:be665996d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-887c110d92"></a>Parser name: `watch`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-078e3f681f"></a>`selector` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selector |
| <a id="s-d555ad43d2"></a>`interval` | TyperOption | no | {'class': 'typer._click.types.FloatRange', 'minimum': 0.1, 'name': 'float range'} | --interval |
| <a id="s-d51d9a083a"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-3bb659ae93"></a>Result identity: `piggity-cli-result/archive/copy/watch/v1`
- <a id="s-bcad18689d"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-6af3e893d4"></a>Structured output: `optional-json`
- <a id="s-ed376ea7a6"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-ad41e45553"></a>`completed` | <a id="s-6778b37f32"></a>`0` | <a id="s-264091cc9f"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-6e0e616a5a"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-5964ecb969"></a>`usage` | <a id="s-8d34e74b97"></a>`2` | <a id="s-d7e4ddcaa7"></a>`{"all":"empty"}` | <a id="s-d2a97b1ef8"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-a21748acac"></a>`operational` | <a id="s-9e31f9a4b3"></a>`1` | <a id="s-bb537a5b68"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-32dc682159"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |
| <a id="s-4272e15497"></a>`terminal-job-failure` | <a id="s-6bde8974f5"></a>`1` | <a id="s-d2e75d2ac8"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-d5dd66039c"></a>`{"all":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --interval](#s-d555ad43d2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-d51d9a083a) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter selector](#s-078e3f681f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/copies/{collection_id}/{destination_store}](../../riverhog/http-operations/get-v1-archive-copies-collection-id-destination-store.md)

## Governing policies

- <a id="pa-e9b102d643"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5d768180ba"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/name`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/name`

<!-- exact-contract-value: 73da76bff71a604995ddd94e223ffa8b7c171b54e0a953c0fb794ac85a61534b -->

```json
"watch"
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/parameters`

<!-- exact-contract-value: eb6e1b2a1271291e3a99a3c15afd19951a252010905621030980f9b269a6d741 -->

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
    "default": 1,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "interval",
    "nargs": 1,
    "options": [
      "--interval"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.FloatRange",
      "minimum": 0.1,
      "name": "float range"
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

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/result_contract`

<!-- exact-contract-value: 21f758c66346a02131289489b2c09891ae68eb65b6f4c05bd0452b24c10610a6 -->

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
    },
    {
      "exit_status": 1,
      "id": "terminal-job-failure",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/archive/copy/watch/v1",
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
