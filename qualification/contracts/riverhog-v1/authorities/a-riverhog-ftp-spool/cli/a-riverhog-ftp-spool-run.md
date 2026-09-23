# a-riverhog-ftp-spool run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-ftp-spool:a-riverhog-ftp-spool-run:637fc654b1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f9741cd877"></a>Parser name: `run`

| Field | Value |
|---|---|
| <a id="s-9ba41622e0"></a>`parameters` | `[]` |
- <a id="s-3210ee659a"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-db5eefc768"></a>`help` | <a id="s-66fad36f90"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-651fdfb601"></a>`0` | <a id="s-209b8ed7cd"></a>`"noncontractual-framework-help"` | <a id="s-08809afde1"></a>`"empty"` |

### Result and failure contract

- <a id="s-349a9865ab"></a>Result identity: `a-riverhog-ftp-spool-cli-result/run/v1`
- <a id="s-d042a4d31a"></a>Profile: `a-riverhog-ftp-spool-cli-human-json/v1`
- <a id="s-7124099aa8"></a>Structured output: `optional-json`
- <a id="s-8f0ef6d485"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b4decc1db8"></a>`completed` | <a id="s-b3cc63ec09"></a>`{"kind":"command-completed"}` | <a id="s-4ae1ccbb64"></a>`0` | <a id="s-b6ed97b362"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP run_ftp_spool_pass response 200](../http-operations/post-v1-run.md#s-93c65270ba) | <a id="s-59abe5e5ac"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6763fd3fb4"></a>`usage` | <a id="s-925916ac62"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-8e771b071f"></a>`2` | <a id="s-dc91182346"></a>all: `"empty"` | <a id="s-e5c64c7fd6"></a>all: `"noncontractual-usage-diagnostic"` |

## Maintained corroboration

### Related interface records

- [POST /v1/run](../http-operations/post-v1-run.md)
- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.run_ftp_spool_pass](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-run-ftp-spool-pass.md)

## Governing policies

- <a id="pa-bba5ad7683"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-4f9bc5584a) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::\_run\_command](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L389)

### Machine authority

- `/external_contract/cli/a-riverhog-ftp-spool/commands/run/allow_abbrev`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/run/name`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/run/parameters`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/run/result_contract`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/run/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-ftp-spool/commands/run/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/run/name`

<!-- exact-contract-value: 5e87f618bd8837e87070ae7f83753c6a23ff095f43de6ededcd38ae535031c29 -->

```json
"run"
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/run/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/run/result_contract`

<!-- exact-contract-value: 52380b1dc77a81d634fd2872f7507b8481bf0b6d484763c844a68a830af89b9a -->

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
  "identity": "a-riverhog-ftp-spool-cli-result/run/v1",
  "profile_id": "a-riverhog-ftp-spool-cli-human-json/v1",
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
          "application": "a-riverhog-ftp-spool",
          "kind": "http-operation-response",
          "method": "POST",
          "operation_id": "run_ftp_spool_pass",
          "path": "/v1/run",
          "schema": {
            "additionalProperties": true,
            "title": "Response Run Ftp Spool Pass",
            "type": "object"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/run/terminating_controls`

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
