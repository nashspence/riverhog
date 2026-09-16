# riverhog-ftp-adapter flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-flush:848e165940 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2ec7852124"></a>Parser name: `flush`
- <a id="s-18b878fc66"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-206a3aa721"></a>`source` | required positional; 1 value | not recorded | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c7b79279f4"></a>`help` | <a id="s-dc3ee42c58"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-928f9816fd"></a>`0` | <a id="s-e624db01e9"></a>`"noncontractual-framework-help"` | <a id="s-2c1d9b87d9"></a>`"empty"` |

### Result and failure contract

- <a id="s-6169445427"></a>Result identity: `riverhog-ftp-adapter-cli-result/flush/v1`
- <a id="s-5c16aa336f"></a>Profile: `riverhog-ftp-adapter-cli-human-json/v1`
- <a id="s-faa3721373"></a>Structured output: `optional-json`
- <a id="s-c7b8c9f3d9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-edfcdca2eb"></a>`completed` | <a id="s-c71b82840b"></a>`{"kind":"command-completed"}` | <a id="s-f72133d8af"></a>`0` | <a id="s-3470129f43"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP flush_ftp_adapter_source response 200](../http-operations/post-v1-sources-source-id-flush.md#s-010c427b4f) | <a id="s-4efd12c773"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bcc74c7169"></a>`usage` | <a id="s-61b75a97ff"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-892dc37171"></a>`2` | <a id="s-72f6faf0e3"></a>all: `empty` | <a id="s-3b2ab6a91b"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-206a3aa721) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/sources/{source_id}/flush](../http-operations/post-v1-sources-source-id-flush.md)
- [riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.flush_ftp_adapter_source](../../riverhog-ftp-adapter-api-client/python/riverhog-ftp-adapter-api-client-riverhogftpadapterclient-flush-ftp-adapter-source.md)

## Governing policies

- <a id="pa-6fb4224b0e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-3117a0d211"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources.md#src-303f765bca) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::_flush_command](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py#L404)

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/flush/allow_abbrev`
- `/external_contract/cli/riverhog-ftp-adapter/commands/flush/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/flush/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/flush/result_contract`
- `/external_contract/cli/riverhog-ftp-adapter/commands/flush/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/flush/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/flush/name`

<!-- exact-contract-value: d030d1ec72ab826b31bde3f98775d1488886825eda49653ab46c31787454b2b6 -->

```json
"flush"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/flush/parameters`

<!-- exact-contract-value: 92534cadceaf71260a6bb9cc94a449a3c15a506af0b41425543fdbd26c2c1d32 -->

```json
[
  {
    "dest": "source",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  }
]
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/flush/result_contract`

<!-- exact-contract-value: 04981bd74804726908b811a02782843a5a647795ef801e5c727f0bb7ec569653 -->

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
  "identity": "riverhog-ftp-adapter-cli-result/flush/v1",
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
          "operation_id": "flush_ftp_adapter_source",
          "path": "/v1/sources/{source_id}/flush",
          "schema": {
            "additionalProperties": true,
            "title": "Response Flush Ftp Adapter Source",
            "type": "object"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/flush/terminating_controls`

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
