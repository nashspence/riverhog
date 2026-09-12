# stove0-client selection show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-client-selection-show:aafd3d196b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [cli](index.md) |
| Family | [selection](families/selection/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- <a id="s-631ffef902"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-3a8e06a180"></a>`selection_sha256` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selection_sha256 |
| <a id="s-5f3ed31399"></a>`continuation` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --continuation |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --continuation](#s-5f3ed31399) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter selection_sha256](#s-3a8e06a180) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: get_artifact_selection](../../stove0/operation/operation-parity-get-artifact-selection.md)

## Governing policies

- <a id="pa-e0eac956ce"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-fbd32694ba"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/selection/commands/show/name`
- `/external_contract/cli/stove0/commands/selection/commands/show/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/selection/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/selection/commands/show/parameters`

<!-- exact-contract-value: 0b10d00386773bfc4b66ab848fefb0e9ce0dfa31cd1793be7c11acef3238bc00 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selection_sha256",
    "nargs": 1,
    "options": [
      "selection_sha256"
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
    "name": "continuation",
    "nargs": 1,
    "options": [
      "--continuation"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```
