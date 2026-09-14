# piggity collection upload discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-discard:8e90d83ab4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-4b01da3732"></a>Parser name: `discard`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-63bbf5885e"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-f375035e7e"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run, --plan |
| <a id="s-9a049cc7bb"></a>`confirm` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --confirm |
| <a id="s-38abeb20f1"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-3a6aab496c"></a>Result identity: `piggity-cli-result/collection/upload/discard/v1`
- <a id="s-f3624faaea"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-0091570fcf"></a>Structured output: `optional-json`
- <a id="s-1f0aef5c08"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-8e890f9515"></a>`completed` | <a id="s-ba00c57c4d"></a>`0` | <a id="s-1efaef072b"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-cf43b55869"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-13699ddc9d"></a>`usage` | <a id="s-e991784d56"></a>`2` | <a id="s-48f1160a92"></a>`{"all":"empty"}` | <a id="s-e94088cf0a"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-df72a4715c"></a>`operational` | <a id="s-8eff56bb8d"></a>`1` | <a id="s-d38fcc4ce7"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-029d5235e1"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |
| <a id="s-8e6fdb3179"></a>`blocked` | <a id="s-64e3bd7a99"></a>`1` | <a id="s-5404debe72"></a>`{"human":"noncontractual-presentation-of-command-result"}` | <a id="s-e12b23c3b3"></a>`{"human":"empty"}` |
| <a id="s-4e3f96da41"></a>`confirmation-declined` | <a id="s-6cdc9343f1"></a>`1` | <a id="s-46e349a5db"></a>`{"human":"noncontractual-presentation-of-command-result"}` | <a id="s-c77ebaac47"></a>`{"human":"noncontractual-diagnostic"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-63bbf5885e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --confirm](#s-9a049cc7bb) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-f375035e7e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-38abeb20f1) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/discard-plan](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard-plan.md)
- [POST /v1/collection-upload-sessions/{collection_id}/discard](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard.md)

## Governing policies

- <a id="pa-669c7ef6b8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e0a57a5dee"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/name`

<!-- exact-contract-value: 27e5d8fcb7e7c0c194453fff8dcce54dbd2cec00c0d18c995023ce62b981b7da -->

```json
"discard"
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/parameters`

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

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/result_contract`

<!-- exact-contract-value: 1923085fd9b3459f5658c4528faaba248fa7fa4fa1ec1bf4c360f75ffd3c78da -->

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
  "identity": "piggity-cli-result/collection/upload/discard/v1",
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
