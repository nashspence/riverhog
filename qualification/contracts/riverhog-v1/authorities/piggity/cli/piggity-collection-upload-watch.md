# piggity collection upload watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-watch:33affbaaaf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3f50299b41"></a>Parser name: `watch`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-2d6558a87d"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-514e76b029"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-41db34eacc"></a>Result identity: `piggity-cli-result/collection/upload/watch/v1`
- <a id="s-9a0709aeae"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-fee7be7e1b"></a>Structured output: `optional-json`
- <a id="s-9e22d744f0"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-acd3c70be4"></a>`completed` | <a id="s-8272705a42"></a>`0` | <a id="s-6d0344c8cd"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-dea94d91e8"></a>`{"all":"noncontractual-progress"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-ed4f6ab955"></a>`usage` | <a id="s-cfd35862a8"></a>`2` | <a id="s-ce76b53ad7"></a>`{"all":"empty"}` | <a id="s-d08182b49a"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-aa66567ab9"></a>`operational` | <a id="s-600ca67dc8"></a>`1` | <a id="s-961fb3d8e1"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-f3cdf852d2"></a>`{"human":"noncontractual-diagnostic-or-progress","json":"noncontractual-progress"}` |
| <a id="s-5de1ee9645"></a>`custody-timeout` | <a id="s-7e83c1f628"></a>`124` | <a id="s-44154f6b2e"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-fbe773a64f"></a>`{"all":"noncontractual-progress"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-2d6558a87d) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-514e76b029) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md)

## Governing policies

- <a id="pa-40e83f1199"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-78672d05c0"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/name`

<!-- exact-contract-value: 73da76bff71a604995ddd94e223ffa8b7c171b54e0a953c0fb794ac85a61534b -->

```json
"watch"
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/parameters`

<!-- exact-contract-value: 69121b7dd4df39852c314f302ca34fb358e3d4472f9e5565bdec50242e30ee3a -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection_id",
    "nargs": 1,
    "options": [
      "collection_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
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

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/result_contract`

<!-- exact-contract-value: 453f7a481b1300eb436215345a9459e40c9c3f4763a445d08832ff6b089e347d -->

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
        "human": "noncontractual-diagnostic-or-progress",
        "json": "noncontractual-progress"
      },
      "stdout": {
        "human": "empty",
        "json": "http-api-contracts.ErrorResponse"
      }
    },
    {
      "exit_status": 124,
      "id": "custody-timeout",
      "stderr": {
        "all": "noncontractual-progress"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/collection/upload/watch/v1",
  "profile_id": "piggity-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "stderr": {
        "all": "noncontractual-progress"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ]
}
```
