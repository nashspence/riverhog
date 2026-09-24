# stove0 departure policy list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-departure-policy-list:8ddef0d486 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fe5fd44a87"></a>Parser name: `list`

| Field | Value |
|---|---|
| <a id="s-6cd5a450b0"></a>`parameters` | `[]` |
- <a id="s-85cc69c7f8"></a>Extra arguments at this parser: rejected.
- <a id="s-e5bb674c64"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-9cfd8eff66"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-8e94233f20"></a>`help` | <a id="s-88e5db06e6"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-71f05799e0"></a>`0` | <a id="s-259a27a85e"></a>`"noncontractual-framework-help"` | <a id="s-af00efa694"></a>`"empty"` |

### Result and failure contract

- <a id="s-141a69d8a2"></a>Result identity: `stove0-cli-result/departure/policy/list/v1`
- <a id="s-e98c8f13a9"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-07cfa7f6dc"></a>Structured output: `optional-json`
- <a id="s-3d2584de34"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b4f602ec79"></a>`completed` | <a id="s-84bc3e37f8"></a>`{"kind":"command-completed"}` | <a id="s-5b15e0dc40"></a>`0` | <a id="s-badb7c25d9"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_departure_policies response 200](../../stove0/http-operations/get-v1-departure-policies.md#s-7006873953) | <a id="s-75d6ac7265"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-37dbaf43fe"></a>`usage` | <a id="s-e36d135837"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a62828f603"></a>`2` | <a id="s-ecab6d6f40"></a>all: `"empty"` | <a id="s-1e6f3ab021"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-85073b86b8"></a>`operational` | <a id="s-bbbc7573ff"></a>`{"kind":"application-error"}` | <a id="s-6a0ebe0ab7"></a>`1` | <a id="s-5ff4e95553"></a>all: `"empty"` | <a id="s-39b554f4bb"></a>all: `"noncontractual-diagnostic"` |

## Maintained corroboration

### Related interface records

- [GET /v1/departure-policies](../../stove0/http-operations/get-v1-departure-policies.md)
- [stove0_api_client.Stove0ApiClient.list_departure_policies](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-departure-policies.md)

## Governing policies

- <a id="pa-a38adce34d"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::list\_departure\_policies](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L256)

### Machine authority

- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/allow_extra_args`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/name`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/parameters`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/result_contract`

<!-- exact-contract-value: 359e35ee5372d07ba4b1b9f250f9f176ff989fb94f0976c84994882e67c1c8fd -->

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
  "identity": "stove0-cli-result/departure/policy/list/v1",
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
          "operation_id": "list_departure_policies",
          "path": "/v1/departure-policies",
          "schema": {
            "$ref": "#/components/schemas/DeparturePolicyCatalogView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/departure/commands/policy/commands/list/terminating_controls`

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

</details>
