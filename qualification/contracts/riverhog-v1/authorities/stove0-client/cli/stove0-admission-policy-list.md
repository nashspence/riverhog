# stove0 admission policy list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission-policy-list:b10363508b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-080a18bf21"></a>Parser name: `list`

### Result and failure contract

- <a id="s-9d2b532663"></a>Result identity: `stove0-cli-result/admission/policy/list/v1`
- <a id="s-bbcd02bafe"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-a74b5eadc6"></a>Structured output: `optional-json`
- <a id="s-de82f139b2"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-dba6a47208"></a>`completed` | <a id="s-05a30182b7"></a>`0` | <a id="s-6ef08ab781"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-9bfab50558"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-f0cc8d88b4"></a>`usage` | <a id="s-055c90a286"></a>`2` | <a id="s-587607ac34"></a>`{"all":"empty"}` | <a id="s-373b7275c7"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-b84c872ee7"></a>`operational` | <a id="s-b6075ad180"></a>`1` | <a id="s-49b59dd950"></a>`{"all":"empty"}` | <a id="s-483b75cbe3"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

## Maintained corroboration

### Related interface records

- [GET /v1/admission-policies](../../stove0/http-operations/get-v1-admission-policies.md)

## Governing policies

- <a id="pa-f18121ce7e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/name`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/list/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

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

<!-- exact-contract-value: 80759c9e316f1e54bd2f3bfc72ed9d5b813bf7559e082b2cfd1c54f697215aa9 -->

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
  "identity": "stove0-cli-result/admission/policy/list/v1",
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
