# stove0 admission policy backfill

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission-policy-backfill:53faf9baf1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d32a9028d2"></a>Parser name: `backfill`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-ad33cb5cc6"></a>`policy_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | policy_id |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-71352a0aa4"></a>`help` | <a id="s-3e154a5472"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e953da3c9a"></a>`0` | <a id="s-b7d0a7592d"></a>`"noncontractual-framework-help"` | <a id="s-dddd725cef"></a>`"empty"` |

### Result and failure contract

- <a id="s-3fec756552"></a>Result identity: `stove0-cli-result/admission/policy/backfill/v1`
- <a id="s-7dca277bc8"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-c22b42e0aa"></a>Structured output: `optional-json`
- <a id="s-37b37d11dc"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fbd649bc61"></a>`completed` | <a id="s-2e09a20525"></a>`{"kind":"command-completed"}` | <a id="s-06ad8deb3a"></a>`0` | <a id="s-65ffb96b56"></a>`human: noncontractual-presentation-of-command-result; json: HTTP backfill_admission_policy — #/components/schemas/AdmissionPolicyStatus` | <a id="s-d0c965fa9f"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-671b7d4b84"></a>`usage` | <a id="s-dfcb2eac2c"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-e5789cefa3"></a>`2` | <a id="s-a0676e2ffa"></a>`all: empty` | <a id="s-56ab28d8aa"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-6df1d99d97"></a>`operational` | <a id="s-7f814027c2"></a>`{"kind":"application-error"}` | <a id="s-2d3fddd7e0"></a>`1` | <a id="s-46bcfaa255"></a>`all: empty` | <a id="s-e12796c423"></a>`all: stove0-cli-diagnostic/v1` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter policy_id](#s-ad33cb5cc6) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/admission-policies/{policy_id}:backfill](../../stove0/http-operations/post-v1-admission-policies-policy-id-backfill.md)

## Governing policies

- <a id="pa-e3941a3dae"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5e69caeec4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/name`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/result_contract`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/name`

<!-- exact-contract-value: 598bbc355057414041935e7063530c6581ced46576ae4f50eb9a26c33752e354 -->

```json
"backfill"
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/parameters`

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

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/result_contract`

<!-- exact-contract-value: 51024760ffa61c20256e4296c82948cea83658053323d192ba2edbca93429578 -->

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
  "identity": "stove0-cli-result/admission/policy/backfill/v1",
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
          "operation_id": "backfill_admission_policy",
          "path": "/v1/admission-policies/{policy_id}:backfill",
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

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/terminating_controls`

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
