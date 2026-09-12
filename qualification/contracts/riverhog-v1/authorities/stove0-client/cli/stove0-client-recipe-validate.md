# stove0-client recipe validate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-client-recipe-validate:336f2e6575 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [cli](index.md) |
| Family | [recipe](families/recipe/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-c2446b375b"></a>Parser name: `validate`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-b8713f6067"></a>`path` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'file'} | path |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter path](#s-b8713f6067) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-bfcdf71458"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-cca55a7712"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/validate/name`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/recipe/commands/validate/name`

<!-- exact-contract-value: 2c9877104bf173f3ecf6b4d44be3e77da3b1b3c87e448be5f6cec01b12ccf804 -->

```json
"validate"
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/parameters`

<!-- exact-contract-value: 16bb2841bd48031955d2caf741ae9c4ff040273016852cfe532eb80ee09dbe8b -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "path",
    "nargs": 1,
    "options": [
      "path"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "file"
    }
  }
]
```
