# piggity collection provenance export

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-export:b117f20d76 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [collection](families/collection/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- <a id="s-002c4f842a4c"></a>Parser name: `export`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-546fc23aad3b"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-918685ae19d0"></a>`journal_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | journal_id |
| <a id="s-23253560e63c"></a>`output` | TyperOption | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | --output, -o |
| <a id="s-4168b2508250"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-546fc23aad3b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter journal_id](#s-918685ae19d0) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-4168b2508250) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --output](#s-23253560e63c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: stream_collection_provenance_journal](../../riverhog/operation/operation-parity-stream-collection-provenance-journal.md)

## Governing policies

- <a id="pa-165ef9fc61e1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-6508455e3c2d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f2c) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/parameters`

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
