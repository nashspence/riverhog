# piggity local evict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-evict:f9ca2206a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-799165ab40"></a>Parser name: `evict`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-b5a778db57"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-80d4a4a715"></a>`confirm` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --confirm |
| <a id="s-f823e282c9"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-eb73f10a25"></a>Result identity: `piggity-cli-result/local/evict/v1`
- <a id="s-e86469f93a"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-0fb0b7fd7a"></a>Structured output: `optional-json`
- <a id="s-40bdcb513f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-0ffe86b917"></a>`completed` | <a id="s-8cf164c617"></a>`0` | <a id="s-72ed83cf52"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-041d8bdc42"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-49515a8026"></a>`usage` | <a id="s-42cfa40fe3"></a>`2` | <a id="s-73db70dd4e"></a>`{"all":"empty"}` | <a id="s-dd6c35f28e"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-6158fc1c90"></a>`operational` | <a id="s-001d3e29c9"></a>`1` | <a id="s-9e8fba785e"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-892b6e4ba8"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-b5a778db57) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --confirm](#s-80d4a4a715) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-f823e282c9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [DELETE /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/delete-v1-retrieval-jobs-job-id.md)
- [GET /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id.md)

## Governing policies

- <a id="pa-87f8fc07fb"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4813a6ea03"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/evict/name`
- `/external_contract/cli/piggity/commands/local/commands/evict/parameters`
- `/external_contract/cli/piggity/commands/local/commands/evict/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/evict/name`

<!-- exact-contract-value: 90a4b686887bd1a4444a5b25700727402328babe6e5a215000cb5712dad96460 -->

```json
"evict"
```

### `/external_contract/cli/piggity/commands/local/commands/evict/parameters`

<!-- exact-contract-value: 4efee97b1e0b5efebedc88669c125863abe67f9cc6fca582ec13c75c2c67664c -->

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
    "name": "confirm",
    "nargs": 1,
    "options": [
      "--confirm"
    ],
    "required": false,
    "secondary_options": [
      "--no-confirm"
    ],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
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

### `/external_contract/cli/piggity/commands/local/commands/evict/result_contract`

<!-- exact-contract-value: 4f1f29afaf1e387fcefe46212e7f99ecff5bc8397fc59df455e1ddffd70e078a -->

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
  "identity": "piggity-cli-result/local/evict/v1",
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
