# riverhog-api state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-server:riverhog-api-state-status:025bd759a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7e7de7f9f5"></a>Parser name: `status`
- <a id="s-f157cad814"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f95091bdf2"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b9aae90e7c"></a>`help` | <a id="s-eff9103092"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-a7222e6d9a"></a>`0` | <a id="s-de33f43c30"></a>`"noncontractual-framework-help"` | <a id="s-27fa08dd39"></a>`"empty"` |

### Result and failure contract

- <a id="s-86afb4d609"></a>Result identity: `riverhog-api-cli-result/state/status/v1`
- <a id="s-3746db9c7f"></a>Profile: `riverhog-api-cli-state/v1`
- <a id="s-6f769434bb"></a>Structured output: `optional-json`
- <a id="s-391724d169"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b4d8e29365"></a>`completed` | <a id="s-42d82e28d8"></a>`{"kind":"command-completed"}` | <a id="s-6aa0bc58c3"></a>`0` | <a id="s-4a6c09e422"></a>human: `"noncontractual-presentation-of-command-result"`; json: [riverhog-api-state-status/v1](#s-23a467fb0e) | <a id="s-ad4db1246f"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9249c155de"></a>`usage` | <a id="s-1caf89c127"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6146d74626"></a>`2` | <a id="s-528a738c2b"></a>all: `"empty"` | <a id="s-23ffb16c77"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-afcdf121e7"></a>`state-error` | <a id="s-a778fa59d6"></a>`{"kind":"application-error"}` | <a id="s-117461caee"></a>`1` | <a id="s-6e0d2062da"></a>all: `"empty"` | <a id="s-55537fe0ce"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-23a467fb0e"></a>`riverhog-api-state-status/v1`

Applies to: completed · stdout (json).

<a id="s-650a988395"></a>

- <a id="s-38e05d1b87"></a>`type`: `"object"`
- <a id="s-999ca5432d"></a>`required`: `["name","condition","current_revision","head_revision"]`
- <a id="s-5745ab947b"></a>`title`: `"StateStatus"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e38e3c8c2"></a>`condition` | yes | type="string"; enum=["empty","current","upgrade_required","unversioned","incompatible"]; title="Condition" |  |
| <a id="s-ce9cb00d0b"></a>`current_revision` | yes | anyOf=[(type="string"); (type="null")]; title="Current Revision" |  |
| <a id="s-f8f6cd06bb"></a>`head_revision` | yes | type="string"; title="Head Revision" |  |
| <a id="s-7aa048ef76"></a>`name` | yes | type="string"; title="Name" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-f95091bdf2) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c492602d9b"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-86b6c5cd9f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-api](../../../evidence/sources/authorities.md#src-18139c42dd) — [riverhog/src/riverhog\_api/app.py::&lt;module&gt;](../../../../../../riverhog/src/riverhog_api/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/riverhog-api/commands/state/commands/status/allow_abbrev`
- `/external_contract/cli/riverhog-api/commands/state/commands/status/name`
- `/external_contract/cli/riverhog-api/commands/state/commands/status/parameters`
- `/external_contract/cli/riverhog-api/commands/state/commands/status/result_contract`
- `/external_contract/cli/riverhog-api/commands/state/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-api/commands/state/commands/status/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-api/commands/state/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/riverhog-api/commands/state/commands/status/parameters`

<!-- exact-contract-value: a4eee56605df36f2261ef19e35bdc3c16631d89a610d870dece837b2b2866441 -->

```json
[
  {
    "default": false,
    "dest": "json",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--json"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/riverhog-api/commands/state/commands/status/result_contract`

<!-- exact-contract-value: ccae01798551e97e81ac2e5c1e5f01b05de9cfc346b71416a1a6cbab37773661 -->

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
    },
    {
      "exit_status": 1,
      "id": "state-error",
      "selected_by": {
        "kind": "application-error"
      },
      "stderr": {
        "all": "noncontractual-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "riverhog-api-cli-result/state/status/v1",
  "profile_id": "riverhog-api-cli-state/v1",
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
          "identity": "riverhog-api-state-status/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "properties": {
              "condition": {
                "enum": [
                  "empty",
                  "current",
                  "upgrade_required",
                  "unversioned",
                  "incompatible"
                ],
                "title": "Condition",
                "type": "string"
              },
              "current_revision": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "title": "Current Revision"
              },
              "head_revision": {
                "title": "Head Revision",
                "type": "string"
              },
              "name": {
                "title": "Name",
                "type": "string"
              }
            },
            "required": [
              "name",
              "condition",
              "current_revision",
              "head_revision"
            ],
            "title": "StateStatus",
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/riverhog-api/commands/state/commands/status/terminating_controls`

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
