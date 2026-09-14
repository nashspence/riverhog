# piggity collection delete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-delete:43b34221b4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-accbc5cbe6"></a>Parser name: `delete`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-d6934e56de"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-105e737284"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run, --plan |
| <a id="s-0b1ae3ab19"></a>`confirm` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --confirm |
| <a id="s-b0369c7ed6"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-c3225b5395"></a>Result identity: `piggity-cli-result/collection/delete/v1`
- <a id="s-3ae6133e67"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-85170dacda"></a>Structured output: `optional-json`
- <a id="s-efa45087ad"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-a3853a44a8"></a>`completed` | <a id="s-88a93d4afa"></a>`0` | <a id="s-ccf18d3d82"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-c2db8e784e"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-f5da864260"></a>`usage` | <a id="s-82187dd7f7"></a>`2` | <a id="s-33e0e19560"></a>`{"all":"empty"}` | <a id="s-50bc038faf"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-ded7ed275f"></a>`operational` | <a id="s-d4e64b1c68"></a>`1` | <a id="s-47e6b6df91"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-651a9a011d"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |
| <a id="s-3e686b6e9b"></a>`blocked` | <a id="s-e9870664eb"></a>`1` | <a id="s-12f27115d1"></a>`{"human":"noncontractual-presentation-of-command-result"}` | <a id="s-4e8913613a"></a>`{"human":"empty"}` |
| <a id="s-7f6714b433"></a>`confirmation-declined` | <a id="s-c5a8fbb6f5"></a>`1` | <a id="s-affc944b60"></a>`{"human":"noncontractual-presentation-of-command-result"}` | <a id="s-d10a6cff56"></a>`{"human":"noncontractual-diagnostic"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-d6934e56de) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --confirm](#s-0b1ae3ab19) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-105e737284) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-b0369c7ed6) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/delete](../../riverhog/http-operations/post-v1-collections-collection-id-delete.md)
- [POST /v1/collections/{collection_id}/deletion-plan](../../riverhog/http-operations/post-v1-collections-collection-id-deletion-plan.md)

## Governing policies

- <a id="pa-0654b0c5c5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-68aaeaa1d0"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/delete/name`
- `/external_contract/cli/piggity/commands/collection/commands/delete/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/delete/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/delete/name`

<!-- exact-contract-value: b05a18f448a1d2ebeb4812c35e8978201e645044185f821f7511a5c3b62c7e14 -->

```json
"delete"
```

### `/external_contract/cli/piggity/commands/collection/commands/delete/parameters`

<!-- exact-contract-value: e6bd272eae2fa7e8f7abcba6ea104a2fada51f1e99c3ea07bb15c66d70403834 -->

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
    "name": "dry_run",
    "nargs": 1,
    "options": [
      "--dry-run",
      "--plan"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "confirm",
    "nargs": 1,
    "options": [
      "--confirm"
    ],
    "required": false,
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

### `/external_contract/cli/piggity/commands/collection/commands/delete/result_contract`

<!-- exact-contract-value: bfd17bc7954666d703f529b4cd360ed5ff8e2181f619f70bf2e50232a0600d74 -->

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
      "id": "blocked",
      "stderr": {
        "human": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result"
      }
    },
    {
      "exit_status": 1,
      "id": "confirmation-declined",
      "stderr": {
        "human": "noncontractual-diagnostic"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/collection/delete/v1",
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
