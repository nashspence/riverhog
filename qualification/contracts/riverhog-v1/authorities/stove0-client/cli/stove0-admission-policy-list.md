# stove0 admission policy list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission-policy-list:db22ba4d55 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-080a18bf21"></a>Parser name: `list`
- <a id="s-1d9b703f2d"></a>Extra arguments at this parser: rejected.
- <a id="s-5ac8ae2be3"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-d614bfe950"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-116a6d3e38"></a>`help` | <a id="s-bb1395c480"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-918c92764d"></a>`0` | <a id="s-66fcd43c21"></a>`"noncontractual-framework-help"` | <a id="s-62b799fc07"></a>`"empty"` |

### Result and failure contract

- <a id="s-9d2b532663"></a>Result identity: `stove0-cli-result/admission/policy/list/v1`
- <a id="s-bbcd02bafe"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-a74b5eadc6"></a>Structured output: `optional-json`
- <a id="s-de82f139b2"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-dba6a47208"></a>`completed` | <a id="s-97c546086e"></a>`{"kind":"command-completed"}` | <a id="s-05a30182b7"></a>`0` | <a id="s-6ef08ab781"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP list_admission_policies response 200](../../stove0/http-operations/get-v1-admission-policies.md#s-7e95b10728) | <a id="s-9bfab50558"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f0cc8d88b4"></a>`usage` | <a id="s-a4520b13bb"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-055c90a286"></a>`2` | <a id="s-587607ac34"></a>all: `empty` | <a id="s-373b7275c7"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-b84c872ee7"></a>`operational` | <a id="s-19121d5672"></a>`{"kind":"application-error"}` | <a id="s-b6075ad180"></a>`1` | <a id="s-49b59dd950"></a>all: `empty` | <a id="s-483b75cbe3"></a>all: `noncontractual-diagnostic` |

## Maintained corroboration

### Related interface records

- [GET /v1/admission-policies](../../stove0/http-operations/get-v1-admission-policies.md)
- [stove0_api_client.Stove0ApiClient.list_admission_policies](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-admission-policies.md)

## Governing policies

- <a id="pa-fdb1ffea78"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::list_admission_policies](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L185)

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/allow_extra_args`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/name`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/result_contract`

<!-- exact-contract-value: bb06a54271df453782c92728490cd092aad69d0a3a30f816231491018767ab61 -->

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
        "all": "noncontractual-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "stove0-cli-result/admission/policy/list/v1",
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
          "method": "GET",
          "operation_id": "list_admission_policies",
          "path": "/v1/admission-policies",
          "schema": {
            "$ref": "#/components/schemas/AdmissionPolicyCatalogView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/terminating_controls`

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
