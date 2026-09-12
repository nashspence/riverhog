# piggity app key revoke

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-revoke:ad3b52c2aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [app](families/app/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-9c780f52a5"></a>Parser name: `revoke`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-6c6eb4f988"></a>`app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| <a id="s-15b43c36fc"></a>`key_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | key_id |
| <a id="s-96ecefa133"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-6c6eb4f988) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-96ecefa133) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter key_id](#s-15b43c36fc) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: revoke_app_key](../../riverhog/operation/operation-parity-revoke-app-key.md)

## Governing policies

- <a id="pa-8719b3fe86"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-87e78730ff"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/name`

<!-- exact-contract-value: 4dbb5115705a0de2a6127443c67e751a0baa58326bad39f890b1efc796cc6a3a -->

```json
"revoke"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/parameters`

<!-- exact-contract-value: 1749f130449a17b41843bc114da325f40e7d6ade60854043985e5bcec11d2cad -->

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
