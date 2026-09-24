# a-riverhog-ftp-spool events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-ftp-spool:a-riverhog-ftp-spool-events:d02c98202a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-81415b2658"></a>Parser name: `events`
- <a id="s-8a0e53e079"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-69e3146f40"></a>`source` | required positional; 1 value | not recorded | not recorded |
| <a id="s-efa1ab5120"></a>`after`<br>`--after` | optional option; 1 value | not recorded | not recorded |
| <a id="s-33e697fc97"></a>`limit`<br>`--limit` | optional option; 1 value | int | `100` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-7de2ae6f7f"></a>`help` | <a id="s-9c9c716586"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-93b53acf38"></a>`0` | <a id="s-b70c91a75e"></a>`"noncontractual-framework-help"` | <a id="s-dd079998a1"></a>`"empty"` |

### Result and failure contract

- <a id="s-7cffa12ad7"></a>Result identity: `a-riverhog-ftp-spool-cli-result/events/v1`
- <a id="s-301ab35308"></a>Profile: `a-riverhog-ftp-spool-cli-human-json/v1`
- <a id="s-eb466cb0f4"></a>Structured output: `optional-json`
- <a id="s-a7965600b5"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9b973eee08"></a>`completed` | <a id="s-2d2d78d8ac"></a>`{"kind":"command-completed"}` | <a id="s-284d37c62a"></a>`0` | <a id="s-bcd1ddfcfa"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_ftp_spool_events response 200](../http-operations/get-v1-sources-source-id-events.md#s-b7e636c45c) | <a id="s-a1b710db60"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4ddb5fabc8"></a>`usage` | <a id="s-55f7732e0e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bea851b6d8"></a>`2` | <a id="s-080a05a261"></a>all: `"empty"` | <a id="s-c0b3fe435b"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-69e3146f40) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --after](#s-efa1ab5120) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --limit](#s-33e697fc97) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/sources/{source_id}/events](../http-operations/get-v1-sources-source-id-events.md)
- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.list_ftp_spool_events](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-list-ftp-spool-events.md)

### Referenced contract elements

- [schemas: FtpEventPage](../http-schemas/schemas-ftpeventpage.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9d04a77257"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-172aa041c3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-4f9bc5584a) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::\_events\_command](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L455)

### Machine authority

- `/external_contract/cli/a-riverhog-ftp-spool/commands/events/allow_abbrev`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/events/name`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/events/parameters`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/events/result_contract`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/events/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-ftp-spool/commands/events/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/events/name`

<!-- exact-contract-value: 5482c9818e6159bc4df8afc87619d87f7ccdb8d9bbc75beb63cc04f66efc93a4 -->

```json
"events"
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/events/parameters`

<!-- exact-contract-value: 0f9a6953e7a0d237f536165327c346ea9e4a0f8bb06e650d379b4867da0824f1 -->

```json
[
  {
    "dest": "source",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  },
  {
    "dest": "after",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--after"
    ],
    "required": false
  },
  {
    "default": 100,
    "dest": "limit",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--limit"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/events/result_contract`

<!-- exact-contract-value: 9eeb6632ef3254728cae3e572c6e8c4822ea2bd256d91ba5f17b32a2226b42b4 -->

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
  "identity": "a-riverhog-ftp-spool-cli-result/events/v1",
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
          "operation_id": "list_ftp_spool_events",
          "path": "/v1/sources/{source_id}/events",
          "schema": {
            "$ref": "#/components/schemas/FtpEventPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/events/terminating_controls`

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
