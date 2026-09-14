# riverhog-ftp-adapter check-config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-check-config:9e29284e57 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8d9a1412db"></a>Parser name: `check-config`

### Result and failure contract

- <a id="s-d6f3a24c23"></a>Result identity: `riverhog-ftp-adapter-cli-result/check-config/v1`
- <a id="s-297458da24"></a>Profile: `riverhog-ftp-adapter-cli-human-json/v1`
- <a id="s-b6430d75e3"></a>Structured output: `optional-json`
- <a id="s-7a61b370ba"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-99a6e5ad36"></a>`completed` | <a id="s-081524bb41"></a>`0` | <a id="s-cb8aa96d78"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-c556170d4f"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-fd0313879a"></a>`usage` | <a id="s-9b6f49f57d"></a>`2` | <a id="s-a9fddab3b9"></a>`{"all":"empty"}` | <a id="s-e5ab4560d5"></a>`{"all":"noncontractual-usage-diagnostic"}` |

## Governing policies

- <a id="pa-1b838d3944"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

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

<!-- exact-contract-value: be8387e1b38fcc820329c8f7be515654660b5419ef057aaeecfaeaa6844a3bf7 -->

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
  "identity": "riverhog-ftp-adapter-cli-result/check-config/v1",
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
