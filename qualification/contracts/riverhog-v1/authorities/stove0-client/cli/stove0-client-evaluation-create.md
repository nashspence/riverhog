# stove0-client evaluation create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-client-evaluation-create:40938e6cfa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [cli](index.md) |
| Family | [evaluation](families/evaluation/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-0639b8f049"></a>Parser name: `create`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-64aae09091"></a>`definition` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | definition |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter definition](#s-64aae09091) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: create_evaluation](../../stove0/operation/operation-parity-create-evaluation.md)

## Governing policies

- <a id="pa-c02d19ea8a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-104799db12"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/create/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/create/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/create/name`

<!-- exact-contract-value: 5498a731a187f424a5800943afcba027f3a6cd684e38fe6e40c02bee1753152d -->

```json
"create"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/create/parameters`

<!-- exact-contract-value: 4fb330ee8d838285ee3bb2f2f8c99dccc40350eb1951a541aaaddd3d0f750f22 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "definition",
    "nargs": 1,
    "options": [
      "definition"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  }
]
```
