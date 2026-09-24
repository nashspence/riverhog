# a-riverhog-ftp-spool status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-ftp-spool:a-riverhog-ftp-spool-status:faa3d60dbf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fb750c2a8e"></a>Parser name: `status`
- <a id="s-bcea3e3d6b"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-83d1f6a25b"></a>`page_size`<br>`--page-size` | optional option; 1 value | int | `25` |
| <a id="s-0a791e06b8"></a>`page_token`<br>`--page-token` | optional option; 1 value | not recorded | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-963a0b7fb0"></a>`help` | <a id="s-5831938385"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-a2569e16a2"></a>`0` | <a id="s-5f3f6378bd"></a>`"noncontractual-framework-help"` | <a id="s-4a21c456e9"></a>`"empty"` |

### Result and failure contract

- <a id="s-86bf4eb462"></a>Result identity: `a-riverhog-ftp-spool-cli-result/status/v1`
- <a id="s-875c2020c3"></a>Profile: `a-riverhog-ftp-spool-cli-human-json/v1`
- <a id="s-851042ee21"></a>Structured output: `optional-json`
- <a id="s-06c4050be8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-20be1a0d18"></a>`completed` | <a id="s-ae78885343"></a>`{"kind":"command-completed"}` | <a id="s-29f6348b67"></a>`0` | <a id="s-f37e0889ef"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_ftp_spool_status response 200](../http-operations/get-v1-status.md#s-9461dfcf79) | <a id="s-d36c1c3fa6"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a951b97aa8"></a>`usage` | <a id="s-d57b4501ab"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-ac2f73283c"></a>`2` | <a id="s-bb72f5de22"></a>all: `"empty"` | <a id="s-613cf807ca"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --page-size](#s-83d1f6a25b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --page-token](#s-0a791e06b8) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/status](../http-operations/get-v1-status.md)
- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.get_ftp_spool_status](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-get-ftp-spool-status.md)

### Referenced contract elements

- [schemas: FtpSpoolStatus](../http-schemas/schemas-ftpspoolstatus.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e5f197d21a"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-71bd161250"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-4f9bc5584a) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::\_status\_command](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L446)

### Machine authority

- `/external_contract/cli/a-riverhog-ftp-spool/commands/status/allow_abbrev`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/status/name`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/status/parameters`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/status/result_contract`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-ftp-spool/commands/status/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/status/parameters`

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

### `/external_contract/cli/a-riverhog-ftp-spool/commands/status/result_contract`

<!-- exact-contract-value: 9b629e4fb34dfb54f87179981985dd5f61a3ed4c420bb0f37bec0172cf0c5cd1 -->

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
  "identity": "a-riverhog-ftp-spool-cli-result/status/v1",
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
          "method": "GET",
          "operation_id": "get_ftp_spool_status",
          "path": "/v1/status",
          "schema": {
            "$ref": "#/components/schemas/FtpSpoolStatus"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/status/terminating_controls`

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
