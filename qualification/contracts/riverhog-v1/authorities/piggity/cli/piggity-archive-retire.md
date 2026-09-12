# piggity archive retire

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-retire:282a6aa991 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

- <a id="s-e5a72fb08f"></a>Parser name: `retire`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-74ff7f0ecb"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-1cad34944b"></a>`store` | TyperOption | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --store |
| <a id="s-6334a27c26"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run, --plan |
| <a id="s-bc3027a32b"></a>`confirm` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --confirm |
| <a id="s-bb4c28ef4b"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-74ff7f0ecb) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --confirm](#s-bc3027a32b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-6334a27c26) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-bb4c28ef4b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --store](#s-1cad34944b) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: retire_archive_copy](../../riverhog/operation/operation-parity-retire-archive-copy.md)
- [Operation parity: plan_archive_copy_retirement](../../riverhog/operation/operation-parity-plan-archive-copy-retirement.md)

## Governing policies

- <a id="pa-3cd8124378"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-3818910dbe"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/retire/name`
- `/external_contract/cli/piggity/commands/archive/commands/retire/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/retire/name`

<!-- exact-contract-value: a2a9d4ffbc4c361801bc55cbc97e993b34974d1cb94ba86138a02d7b5630ad91 -->

```json
"retire"
```

### `/external_contract/cli/piggity/commands/archive/commands/retire/parameters`

<!-- exact-contract-value: 116b926952db53e8cab7ae68dd815774c23383fb5dd194c74c89118b771e12fa -->

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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "store",
    "nargs": 1,
    "options": [
      "--store"
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
