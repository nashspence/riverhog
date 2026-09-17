# riverhog-ftp-adapter run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-run:956d196af0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cda0da140d"></a>Parser name: `run`

| Field | Value |
|---|---|
| <a id="s-10c8365655"></a>`parameters` | `[]` |
- <a id="s-65db5da7df"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-720e043902"></a>`help` | <a id="s-027152ce5e"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-a17339249a"></a>`0` | <a id="s-d04bdb6398"></a>`"noncontractual-framework-help"` | <a id="s-b792ea3b4f"></a>`"empty"` |

### Result and failure contract

- <a id="s-18dd7d100c"></a>Result identity: `riverhog-ftp-adapter-cli-result/run/v1`
- <a id="s-a014ef2abb"></a>Profile: `riverhog-ftp-adapter-cli-human-json/v1`
- <a id="s-e67d68b853"></a>Structured output: `optional-json`
- <a id="s-2738420124"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-18348219fb"></a>`completed` | <a id="s-e3df997f51"></a>`{"kind":"command-completed"}` | <a id="s-8b0dd7cef3"></a>`0` | <a id="s-152848db02"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP run_ftp_adapter_pass response 200](../http-operations/post-v1-run.md#s-52ad3c0e19) | <a id="s-cd12160af1"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-32bf03befa"></a>`usage` | <a id="s-31e6f9d970"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4963ea4d40"></a>`2` | <a id="s-a7fbe00920"></a>all: `"empty"` | <a id="s-ac4efa109c"></a>all: `"noncontractual-usage-diagnostic"` |

## Maintained corroboration

### Related interface records

- [POST /v1/run](../http-operations/post-v1-run.md)
- [riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.run_ftp_adapter_pass](../../riverhog-ftp-adapter-api-client/python/riverhog-ftp-adapter-api-client-riverhogftpadapterclient-run-ftp-adapter-pass.md)

## Governing policies

- <a id="pa-5bb50b3c4a"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources/authorities.md#src-303f765bca) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/app.py::&lt;module&gt;](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/app.py::\_run\_command](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py#L389)

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/run/allow_abbrev`
- `/external_contract/cli/riverhog-ftp-adapter/commands/run/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/run/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/run/result_contract`
- `/external_contract/cli/riverhog-ftp-adapter/commands/run/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/run/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

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

<!-- exact-contract-value: 5afe3016ce2176d97c16e537dddfe5767608e675430431047afb22132c0c72bf -->

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
      "selected_by": {
        "kind": "command-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "riverhog-ftp-adapter",
          "kind": "http-operation-response",
          "method": "POST",
          "operation_id": "run_ftp_adapter_pass",
          "path": "/v1/run",
          "schema": {
            "additionalProperties": true,
            "title": "Response Run Ftp Adapter Pass",
            "type": "object"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/run/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

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
        "-h",
        "--help"
      ]
    }
  }
]
```

</details>
