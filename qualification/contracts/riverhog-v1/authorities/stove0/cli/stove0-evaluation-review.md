# stove0 evaluation review

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-evaluation-review:24dcbb9695 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [cli](index.md) |
| Family | [evaluation](families/evaluation/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

- <a id="s-dac3c3238217"></a>Parser name: `review`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-5d4571c4fb8d"></a>`evaluation_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | evaluation_id |
| <a id="s-988f27c0186a"></a>`variant_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | variant_id |
| <a id="s-9c3cdf9820a3"></a>`rating` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 5, 'minimum': 1, 'name': 'integer range'} | --rating |
| <a id="s-7944ac36f505"></a>`note` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --note |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter evaluation_id](#s-5d4571c4fb8d) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --note](#s-7944ac36f505) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --rating](#s-9c3cdf9820a3) | `value · cli-value · contract_max` | maximum=5; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --rating](#s-9c3cdf9820a3) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter variant_id](#s-988f27c0186a) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [Operation parity: review_evaluation_variant](../operation/operation-parity-review-evaluation-variant.md)

## Governing policies

- <a id="pa-e896f660f3c4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-1d8e51c1a301"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d8812) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/review/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/review/name`

<!-- exact-contract-value: fe82cb229e29dfc87a309c8d1679ff9f58be239c8e6f3ff52bc431efb3709b6b -->

```json
"review"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/review/parameters`

<!-- exact-contract-value: 79adb3a588446ba8deca3a85ad0ce63ed2f49b2c0ee0678bea72fe7a7373fa00 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "evaluation_id",
    "nargs": 1,
    "options": [
      "evaluation_id"
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
    "name": "variant_id",
    "nargs": 1,
    "options": [
      "variant_id"
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
    "name": "rating",
    "nargs": 1,
    "options": [
      "--rating"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntRange",
      "maximum": 5,
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "note",
    "nargs": 1,
    "options": [
      "--note"
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
