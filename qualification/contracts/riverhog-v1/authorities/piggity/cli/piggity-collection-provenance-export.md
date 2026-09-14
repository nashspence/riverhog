# piggity collection provenance export

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-export:83decd640d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-002c4f842a"></a>Parser name: `export`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-546fc23aad"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-918685ae19"></a>`journal_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | journal_id |
| <a id="s-23253560e6"></a>`output` | TyperOption | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | --output, -o |
| <a id="s-4168b25082"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-2bbaaf4c9c"></a>Result identity: `piggity-cli-result/collection/provenance/export/v1`
- <a id="s-6c9a98688f"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-8bd4c297d8"></a>Structured output: `optional-json`
- <a id="s-3008008bb9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-54c365a877"></a>`completed` | <a id="s-3c77526821"></a>`0` | <a id="s-a3a0a70fd5"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-e9d03a6eb5"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-d77f6a8f47"></a>`usage` | <a id="s-9dc652abf3"></a>`2` | <a id="s-b2d941374b"></a>`{"all":"empty"}` | <a id="s-d0e0ed8df9"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-cb2d3355cc"></a>`operational` | <a id="s-4bbf5dc3b2"></a>`1` | <a id="s-c9f2314d7e"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-b55e01adc5"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-546fc23aad) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter journal_id](#s-918685ae19) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-4168b25082) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --output](#s-23253560e6) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-journals-journal-id.md)

## Governing policies

- <a id="pa-ef2a7c2d23"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4ca167b170"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/name`

<!-- exact-contract-value: 3491337573000235960ad0689fabc5fc85f50d5718f2a62c9f46fbe3047f2a55 -->

```json
"export"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/parameters`

<!-- exact-contract-value: 88c297cf98f356fa3084298eea3abd882f0387518e55775f6171b1512cec06df -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "journal_id",
    "nargs": 1,
    "options": [
      "journal_id"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "output",
    "nargs": 1,
    "options": [
      "--output",
      "-o"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/result_contract`

<!-- exact-contract-value: e4d5ec6b305581aff3066017f4efbc15e2f32aa40dcf4731118234fe10ddb28a -->

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
  "identity": "piggity-cli-result/collection/provenance/export/v1",
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
