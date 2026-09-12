# stove0 health

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-health:df4c9c7dfa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [cli](index.md) |
| Family | [health](families/health/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-dd0f6f8ecb"></a>Parser name: `health`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-0148aceb45"></a>`ready` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --ready |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ready](#s-0148aceb45) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: health_live](../operation/operation-parity-health-live.md)
- [Operation parity: health_ready](../operation/operation-parity-health-ready.md)

## Governing policies

- <a id="pa-4e3e86c757"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-096d7662a2"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/health/name`
- `/external_contract/cli/stove0/commands/health/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/health/name`

<!-- exact-contract-value: 69b7c75b4a0260f2e018aae0172314b00e80b3899c79ba4d71e070ba74cd8db4 -->

```json
"health"
```

### `/external_contract/cli/stove0/commands/health/parameters`

<!-- exact-contract-value: bb7aa89b48a0fb797521cb9cfad5a62b55dce8cb3a19bcda2acbd80306355cb4 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "ready",
    "nargs": 1,
    "options": [
      "--ready"
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
