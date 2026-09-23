# a-riverhog-ftp-spool flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-ftp-spool:a-riverhog-ftp-spool-flush:a859c6462a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3c2f2bc39a"></a>Parser name: `flush`
- <a id="s-b9ad601d26"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-9d5e11d41c"></a>`source` | required positional; 1 value | not recorded | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f128210342"></a>`help` | <a id="s-0d70a71fea"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-022e268c9f"></a>`0` | <a id="s-57f9a3c16c"></a>`"noncontractual-framework-help"` | <a id="s-cd9f7a2c7e"></a>`"empty"` |

### Result and failure contract

- <a id="s-274f1b0d84"></a>Result identity: `a-riverhog-ftp-spool-cli-result/flush/v1`
- <a id="s-72029f7a7f"></a>Profile: `a-riverhog-ftp-spool-cli-human-json/v1`
- <a id="s-961ceae9c2"></a>Structured output: `optional-json`
- <a id="s-5f196b7e57"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4782c13636"></a>`completed` | <a id="s-acc384144b"></a>`{"kind":"command-completed"}` | <a id="s-22871a163e"></a>`0` | <a id="s-80900fdd53"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP flush_ftp_spool_source response 200](../http-operations/post-v1-sources-source-id-flush.md#s-9794a3c643) | <a id="s-00da84f98b"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-837f8d2070"></a>`usage` | <a id="s-2a6efdc44d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-288fefdbf1"></a>`2` | <a id="s-39675c2fb7"></a>all: `"empty"` | <a id="s-41511c6f52"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-9d5e11d41c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/sources/{source_id}/flush](../http-operations/post-v1-sources-source-id-flush.md)
- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.flush_ftp_spool_source](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-flush-ftp-spool-source.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c6186dbe54"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-e6c41ebd7f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-4f9bc5584a) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::\_flush\_command](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L404)

### Machine authority

- `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/allow_abbrev`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/name`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/parameters`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/result_contract`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/name`

<!-- exact-contract-value: d030d1ec72ab826b31bde3f98775d1488886825eda49653ab46c31787454b2b6 -->

```json
"flush"
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/parameters`

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

### `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/result_contract`

<!-- exact-contract-value: d3aeb6ab9decfe5cbbeb4ed187e0816bca2cfa1c7142b7cf2b82e751ced9bee6 -->

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
  "identity": "a-riverhog-ftp-spool-cli-result/flush/v1",
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
          "operation_id": "flush_ftp_spool_source",
          "path": "/v1/sources/{source_id}/flush",
          "schema": {
            "additionalProperties": true,
            "title": "Response Flush Ftp Spool Source",
            "type": "object"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/flush/terminating_controls`

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
