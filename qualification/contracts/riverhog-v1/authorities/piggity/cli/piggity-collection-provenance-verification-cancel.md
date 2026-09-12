# piggity collection provenance verification-cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-verification-cancel:689b800c6c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [collection](families/collection/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- <a id="s-9e7a27617849"></a>Parser name: `verification-cancel`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-d966bbd28c14"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-7eca69dae12c"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-d966bbd28c14) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-7eca69dae12c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: cancel_collection_provenance_verification](../../riverhog/operation/operation-parity-cancel-collection-provenance-verification.md)

## Governing policies

- <a id="pa-f42ebacdffa8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-6b74396ebdf6"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f2c) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/name`

<!-- exact-contract-value: 910bb6e80e3b26665adde72400f34b7925959fd6853bfbaa1e64f8cc2e34be5f -->

```json
"verification-cancel"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/parameters`

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
