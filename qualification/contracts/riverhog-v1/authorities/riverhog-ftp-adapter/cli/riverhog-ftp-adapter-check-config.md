# riverhog-ftp-adapter check-config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-check-config:a6a531c811 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8d9a1412db"></a>Parser name: `check-config`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-7d3e879bdf"></a>`help` | <a id="s-cfc2a809fa"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-381409e56e"></a>`0` | <a id="s-2a4b065847"></a>`"noncontractual-framework-help"` | <a id="s-83335ee5df"></a>`"empty"` |

### Result and failure contract

- <a id="s-d6f3a24c23"></a>Result identity: `riverhog-ftp-adapter-cli-result/check-config/v1`
- <a id="s-297458da24"></a>Profile: `riverhog-ftp-adapter-cli-human-json/v1`
- <a id="s-b6430d75e3"></a>Structured output: `optional-json`
- <a id="s-7a61b370ba"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-99a6e5ad36"></a>`completed` | <a id="s-5bd8803a71"></a>`{"kind":"command-completed"}` | <a id="s-081524bb41"></a>`0` | <a id="s-cb8aa96d78"></a>`human: noncontractual-presentation-of-command-result; json: riverhog-ftp-adapter-config-check/v1` | <a id="s-c556170d4f"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fd0313879a"></a>`usage` | <a id="s-fc1bc234a3"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9b6f49f57d"></a>`2` | <a id="s-a9fddab3b9"></a>`all: empty` | <a id="s-e5ab4560d5"></a>`all: noncontractual-usage-diagnostic` |

## Governing policies

- <a id="pa-63b576e08b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/check-config/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/check-config/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/check-config/result_contract`
- `/external_contract/cli/riverhog-ftp-adapter/commands/check-config/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/check-config/name`

<!-- exact-contract-value: 7d1e6f2a5e6adc41b7eecc20dba2cd88e0570cddf2fd660b1f6f4ddbcd7d1e61 -->

```json
"check-config"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/check-config/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/check-config/result_contract`

<!-- exact-contract-value: 12abcdc5a1cba8c85b26b962fcedd4b29fa0ffd21366eac36c8366da9541a0ad -->

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
  "identity": "riverhog-ftp-adapter-cli-result/check-config/v1",
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
          "identity": "riverhog-ftp-adapter-config-check/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "format": {
                "const": "riverhog-ftp-adapter-config-check/v1"
              },
              "sources": {
                "minimum": 1,
                "type": "integer"
              },
              "status": {
                "const": "ok"
              }
            },
            "required": [
              "format",
              "status",
              "sources"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/check-config/terminating_controls`

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
