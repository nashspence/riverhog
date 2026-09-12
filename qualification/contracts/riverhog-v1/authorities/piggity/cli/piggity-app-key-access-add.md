# piggity app key access add

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-access-add:71af315342 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [app](families/app/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- <a id="s-6ecebac263"></a>Parser name: `add`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-b6cd176a80"></a>`app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| <a id="s-1cfd6e12b6"></a>`key_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | key_id |
| <a id="s-7ed433ab01"></a>`allow` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | allow |
| <a id="s-834577b6de"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter allow](#s-7ed433ab01) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter app_name](#s-b6cd176a80) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-834577b6de) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter key_id](#s-1cfd6e12b6) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: add_app_key_access](../../riverhog/operation/operation-parity-add-app-key-access.md)

## Governing policies

- <a id="pa-6cd6db98e3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-cfc760f90d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/add/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/add/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/add/name`

<!-- exact-contract-value: 7b8a6f33b43ca26a3f2aa73e408748f9ceb391ac21dfe746c94563016ab72f85 -->

```json
"add"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/add/parameters`

<!-- exact-contract-value: f5e3dd9d1b01d343a526cfe2ef1b0146c4123c114321a56ec7da1a3143daace5 -->

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
    "name": "allow",
    "nargs": 1,
    "options": [
      "allow"
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
