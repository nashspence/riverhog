# stove0 admission show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission-show:da5de1fd42 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ada52463dd"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-d195271b1a"></a>`admission_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | admission_id |

### Result and failure contract

- <a id="s-b4c3a3f0de"></a>Result identity: `stove0-cli-result/admission/show/v1`
- <a id="s-ee2465604a"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-d6a0db16a6"></a>Structured output: `optional-json`
- <a id="s-a74e167dd5"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-954407e9c5"></a>`completed` | <a id="s-08abd8d8b3"></a>`0` | <a id="s-bc98471949"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-526439aaf8"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-29b3e7fc43"></a>`usage` | <a id="s-96af832097"></a>`2` | <a id="s-ea5474628d"></a>`{"all":"empty"}` | <a id="s-25747f5e1c"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-2aad4b2f9f"></a>`operational` | <a id="s-d930265dac"></a>`1` | <a id="s-342a25eafd"></a>`{"all":"empty"}` | <a id="s-c32a63f0f4"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter admission_id](#s-d195271b1a) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/admissions/{admission_id}](../../stove0/http-operations/get-v1-admissions-admission-id.md)

## Governing policies

- <a id="pa-99d733058c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-642479d7dd"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/show/name`
- `/external_contract/cli/stove0/commands/admission/commands/show/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/show/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/admission/commands/show/parameters`

<!-- exact-contract-value: f0147f7c85fb6352af335636f4d2091373b3e3691b02d05f9bf96424681ec398 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "admission_id",
    "nargs": 1,
    "options": [
      "admission_id"
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

### `/external_contract/cli/stove0/commands/admission/commands/show/result_contract`

<!-- exact-contract-value: ab41dce68ce4dcb1c8652006068f06cb107e40d70ff6ef9d0e9aa442223a523a -->

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
  "identity": "stove0-cli-result/admission/show/v1",
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
