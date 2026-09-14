# riverhog-ftp-adapter run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-run:08fb1985b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cda0da140d"></a>Parser name: `run`

### Result and failure contract

- <a id="s-18dd7d100c"></a>Result identity: `riverhog-ftp-adapter-cli-result/run/v1`
- <a id="s-a014ef2abb"></a>Profile: `riverhog-ftp-adapter-cli-human-json/v1`
- <a id="s-e67d68b853"></a>Structured output: `optional-json`
- <a id="s-2738420124"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-18348219fb"></a>`completed` | <a id="s-8b0dd7cef3"></a>`0` | <a id="s-152848db02"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-cd12160af1"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-32bf03befa"></a>`usage` | <a id="s-4963ea4d40"></a>`2` | <a id="s-a7fbe00920"></a>`{"all":"empty"}` | <a id="s-ac4efa109c"></a>`{"all":"noncontractual-usage-diagnostic"}` |

## Maintained corroboration

### Related interface records

- [POST /v1/run](../http-operations/post-v1-run.md)

## Governing policies

- <a id="pa-e1232754f4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/run/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/run/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/run/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/run/name`

<!-- exact-contract-value: 5e87f618bd8837e87070ae7f83753c6a23ff095f43de6ededcd38ae535031c29 -->

```json
"run"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/run/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/run/result_contract`

<!-- exact-contract-value: bca46f373cde1a1056f5309a95c84d7b03e40726ec4f8397465e5e1daeefb31f -->

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
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "riverhog-ftp-adapter-cli-result/run/v1",
  "profile_id": "riverhog-ftp-adapter-cli-human-json/v1",
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
