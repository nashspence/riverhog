# stove0 evaluation review

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-review:650d1dc774 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dac3c32382"></a>Parser name: `review`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-5d4571c4fb"></a>`evaluation_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | evaluation_id |
| <a id="s-988f27c018"></a>`variant_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | variant_id |
| <a id="s-9c3cdf9820"></a>`rating` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 5, 'minimum': 1, 'name': 'integer range'} | --rating |
| <a id="s-7944ac36f5"></a>`note` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --note |

### Result and failure contract

- <a id="s-d0c428a900"></a>Result identity: `stove0-cli-result/evaluation/review/v1`
- <a id="s-561263e206"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-388d72e33b"></a>Structured output: `optional-json`
- <a id="s-850341bf66"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-53a29984fb"></a>`completed` | <a id="s-36afc45e35"></a>`0` | <a id="s-9f1ae67281"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-04329a4ad8"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-42e1fbfaa0"></a>`usage` | <a id="s-0e1e570e05"></a>`2` | <a id="s-e7c98e3419"></a>`{"all":"empty"}` | <a id="s-b76f8da6e1"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-3a8c36178d"></a>`operational` | <a id="s-c6e37e069c"></a>`1` | <a id="s-0bdf213c00"></a>`{"all":"empty"}` | <a id="s-ebb2d3185a"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter evaluation_id](#s-5d4571c4fb) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --note](#s-7944ac36f5) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --rating](#s-9c3cdf9820) | `value · cli-value · contract_max` | maximum=5; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --rating](#s-9c3cdf9820) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter variant_id](#s-988f27c018) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](../../stove0/http-operations/put-v1-evaluations-evaluation-id-variants-variant-id-review.md)

## Governing policies

- <a id="pa-c7cbbef852"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8ea6ba92df"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/review/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/result_contract`

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

### `/external_contract/cli/stove0/commands/evaluation/commands/review/result_contract`

<!-- exact-contract-value: bffb59643d679e6df9a149cf58a294f590ef8575b17a415902157c0c987c3820 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "stderr": {
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    },
    {
      "exit_status": 1,
      "id": "operational",
      "stderr": {
        "all": "stove0-cli-diagnostic/v1"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "stove0-cli-result/evaluation/review/v1",
  "profile_id": "stove0-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ]
}
```
