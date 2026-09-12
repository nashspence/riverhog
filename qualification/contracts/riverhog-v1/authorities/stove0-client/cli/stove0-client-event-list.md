# stove0-client event list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-client-event-list:baa03b3071 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [cli](index.md) |
| Family | [event](families/event/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-be551f1555"></a>Parser name: `list`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-43a6ce6b6f"></a>`after` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --after |
| <a id="s-247945017e"></a>`limit` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --limit |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --after](#s-43a6ce6b6f) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --limit](#s-247945017e) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --limit](#s-247945017e) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [Operation parity: list_events](../../stove0/operation/operation-parity-list-events.md)

## Governing policies

- <a id="pa-de10a66c05"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8c62b86eea"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/event/commands/list/name`
- `/external_contract/cli/stove0/commands/event/commands/list/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/event/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/event/commands/list/parameters`

<!-- exact-contract-value: c5a15fad184dce9004bf6a5d73b682313fdbe44cfb6323c6bd53c3cb57f198e6 -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "after",
    "nargs": 1,
    "options": [
      "--after"
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
    "default": 100,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "limit",
    "nargs": 1,
    "options": [
      "--limit"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntRange",
      "maximum": 100,
      "minimum": 1,
      "name": "integer range"
    }
  }
]
```
