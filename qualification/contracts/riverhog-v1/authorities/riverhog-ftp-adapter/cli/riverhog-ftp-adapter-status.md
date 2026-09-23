# riverhog-ftp-adapter status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-ftp-adapter:riverhog-ftp-adapter-status:a1e0ee88f1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f81d8fd65a"></a>Parser name: `status`
- <a id="s-d5a60a1915"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f54fd55b02"></a>`page_size`<br>`--page-size` | optional option; 1 value | int | `25` |
| <a id="s-2a51aae1b2"></a>`page_token`<br>`--page-token` | optional option; 1 value | not recorded | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bdfbd6150d"></a>`help` | <a id="s-cae040a388"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-34248bcad3"></a>`0` | <a id="s-1d35b4abea"></a>`"noncontractual-framework-help"` | <a id="s-52896f5fd3"></a>`"empty"` |

### Result and failure contract

- <a id="s-e4422efdd8"></a>Result identity: `riverhog-ftp-adapter-cli-result/status/v1`
- <a id="s-1bb35bd7c8"></a>Profile: `riverhog-ftp-adapter-cli-human-json/v1`
- <a id="s-0b068eb1e0"></a>Structured output: `optional-json`
- <a id="s-3222b25064"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c0c37f0681"></a>`completed` | <a id="s-8d5654142e"></a>`{"kind":"command-completed"}` | <a id="s-8c1fb79da4"></a>`0` | <a id="s-054c956010"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_ftp_adapter_status response 200](../http-operations/get-v1-status.md#s-de7ccd665a) | <a id="s-791b0f86eb"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-04d532acf3"></a>`usage` | <a id="s-795120a6b4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-71989a06f8"></a>`2` | <a id="s-53ee405f4e"></a>all: `"empty"` | <a id="s-fccc131e6b"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --page-size](#s-f54fd55b02) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --page-token](#s-2a51aae1b2) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/status](../http-operations/get-v1-status.md)
- [riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.get_ftp_adapter_status](../../riverhog-ftp-adapter-api-client/python/riverhog-ftp-adapter-api-client-riverhogftpadapterclient-get-ftp-adapter-status.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8898c3a011"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-fcc65c1820"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-ftp-adapter](../../../evidence/sources/authorities.md#src-303f765bca) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/app.py::&lt;module&gt;](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/app.py::\_status\_command](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py#L395)

### Machine authority

- `/external_contract/cli/riverhog-ftp-adapter/commands/status/allow_abbrev`
- `/external_contract/cli/riverhog-ftp-adapter/commands/status/name`
- `/external_contract/cli/riverhog-ftp-adapter/commands/status/parameters`
- `/external_contract/cli/riverhog-ftp-adapter/commands/status/result_contract`
- `/external_contract/cli/riverhog-ftp-adapter/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/parameters`

<!-- exact-contract-value: 9c78a4249746cc77ff1ad2537e381ac766e9b1e5866cda7e926f55bfb3973828 -->

```json
[
  {
    "default": 25,
    "dest": "page_size",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--page-size"
    ],
    "required": false,
    "type": "int"
  },
  {
    "dest": "page_token",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--page-token"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/result_contract`

<!-- exact-contract-value: 58aacb96bb8a853543d6e37e651147fb4e98dc01b2779e0da7bb9f5f0d587998 -->

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
  "identity": "riverhog-ftp-adapter-cli-result/status/v1",
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
          "method": "GET",
          "operation_id": "get_ftp_adapter_status",
          "path": "/v1/status",
          "schema": {
            "additionalProperties": true,
            "title": "Response Get Ftp Adapter Status",
            "type": "object"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/riverhog-ftp-adapter/commands/status/terminating_controls`

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
