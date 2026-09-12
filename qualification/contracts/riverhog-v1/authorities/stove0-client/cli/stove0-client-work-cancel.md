# stove0-client work cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-client-work-cancel:b59d8845fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [cli](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-33d656b25b"></a>Parser name: `cancel`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-c5c44334bc"></a>`work_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | work_id |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter work_id](#s-c5c44334bc) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: cancel_work](../../stove0/operation/operation-parity-cancel-work.md)

## Governing policies

- <a id="pa-62242c1c63"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-aa106d1571"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/cancel/name`
- `/external_contract/cli/stove0/commands/work/commands/cancel/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/cancel/name`

<!-- exact-contract-value: 5a83111e44703c9dd7431bae2754317bb495994fbf736cf7739842ded4dcbc20 -->

```json
"cancel"
```

### `/external_contract/cli/stove0/commands/work/commands/cancel/parameters`

<!-- exact-contract-value: 817c2e603d886c184e6cc1469f89564372168f34c065c797f0d455ecaece1909 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "work_id",
    "nargs": 1,
    "options": [
      "work_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```
