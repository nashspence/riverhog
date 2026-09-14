# stove0 admission policy rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission-policy-rebaseline:b17f813d37 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e4838b9759"></a>Parser name: `rebaseline`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-46ad26d07c"></a>`policy_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | policy_id |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f8c52fdee7"></a>`help` | <a id="s-5af45a62ad"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-fb358da13d"></a>`0` | <a id="s-26a1a9ddc7"></a>`"noncontractual-framework-help"` | <a id="s-973ac0dfc2"></a>`"empty"` |

### Result and failure contract

- <a id="s-6550570e83"></a>Result identity: `stove0-cli-result/admission/policy/rebaseline/v1`
- <a id="s-6f62d9ecfb"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-0ae9ee4ceb"></a>Structured output: `optional-json`
- <a id="s-eccf1749de"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-39d3cf55b8"></a>`completed` | <a id="s-075a93f45d"></a>`{"kind":"command-completed"}` | <a id="s-34d65467cc"></a>`0` | <a id="s-fbf468177e"></a>`human: noncontractual-presentation-of-command-result; json: HTTP rebaseline_admission_policy — #/components/schemas/AdmissionPolicyStatus` | <a id="s-240f6201c5"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3ddb60cbfb"></a>`usage` | <a id="s-86eb16801b"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7721721103"></a>`2` | <a id="s-e761eb56ef"></a>`all: empty` | <a id="s-266218da5c"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-425e286e20"></a>`operational` | <a id="s-47fd6c0279"></a>`{"kind":"application-error"}` | <a id="s-0cac3c4322"></a>`1` | <a id="s-99edbb81bf"></a>`all: empty` | <a id="s-548bbf8ea5"></a>`all: stove0-cli-diagnostic/v1` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter policy_id](#s-46ad26d07c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/admission-policies/{policy_id}:rebaseline](../../stove0/http-operations/post-v1-admission-policies-policy-id-rebaseline.md)

## Governing policies

- <a id="pa-21b6afb412"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-524c8291ec"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/rebaseline/name`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/rebaseline/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/rebaseline/result_contract`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/rebaseline/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/rebaseline/name`

<!-- exact-contract-value: 6d6afe02995e3b8523f798f248dce3c58f9efae832d68eb5f8626c5ebdbf8423 -->

```json
"rebaseline"
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/rebaseline/parameters`

<!-- exact-contract-value: c2ba15d022e17d87c4fef6b4fc54fd47ee4a58f77e072e2fddfb95ae64e3222a -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "policy_id",
    "nargs": 1,
    "options": [
      "policy_id"
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

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/rebaseline/result_contract`

<!-- exact-contract-value: f04cdc5c488cd8207a4aee9f2b1b2acf3f464743c3f7ae4e504e3640e9a28176 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "selected_by": {
        "kind": "parser-rejected-invocation"
      },
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
      "selected_by": {
        "kind": "application-error"
      },
      "stderr": {
        "all": "stove0-cli-diagnostic/v1"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "stove0-cli-result/admission/policy/rebaseline/v1",
  "profile_id": "stove0-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "selected_by": {
        "kind": "command-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "stove0",
          "kind": "http-operation-response",
          "method": "POST",
          "operation_id": "rebaseline_admission_policy",
          "path": "/v1/admission-policies/{policy_id}:rebaseline",
          "schema": {
            "$ref": "#/components/schemas/AdmissionPolicyStatus"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/rebaseline/terminating_controls`

<!-- exact-contract-value: 654ffd6937a42b17b4204e0750fd74bd42751d2241f4a4632015efb38811a79c -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "--help"
      ]
    }
  }
]
```
