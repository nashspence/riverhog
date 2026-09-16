# riverhog-api state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-server:riverhog-api-state-upgrade:f848e2d741 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-48d638f49d"></a>Parser name: `upgrade`
- <a id="s-d76d1bf4ce"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-648ad95ee1"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-09a0654bdb"></a>`help` | <a id="s-a571419e4a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-e9846f4af5"></a>`0` | <a id="s-83da217ee3"></a>`"noncontractual-framework-help"` | <a id="s-867a1d21a1"></a>`"empty"` |

### Result and failure contract

- <a id="s-c26b3a7eaf"></a>Result identity: `riverhog-api-cli-result/state/upgrade/v1`
- <a id="s-bee36368c6"></a>Profile: `riverhog-api-cli-state/v1`
- <a id="s-dae227e134"></a>Structured output: `optional-json`
- <a id="s-271eb2f7f5"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3f2e340b76"></a>`completed` | <a id="s-b2e660b682"></a>`{"kind":"command-completed"}` | <a id="s-3ff90c06bc"></a>`0` | <a id="s-5a04c8a2ed"></a>human: `"noncontractual-presentation-of-command-result"`; json: [riverhog-api-state-status/v1](#s-d8e9bd16fc) | <a id="s-1ccdcdcccc"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2fd85f8ed4"></a>`usage` | <a id="s-aab3d3b480"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a6fb8f2b24"></a>`2` | <a id="s-0977bed548"></a>all: `"empty"` | <a id="s-ed2bdfdbb3"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-95b1279200"></a>`state-error` | <a id="s-31ac6e3d1c"></a>`{"kind":"application-error"}` | <a id="s-7d929084dd"></a>`1` | <a id="s-cc2085a2b0"></a>all: `"empty"` | <a id="s-9462eb0aaa"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-d8e9bd16fc"></a>`riverhog-api-state-status/v1`

Applies to: completed · stdout (json).

<a id="s-0c71f2741e"></a>

- <a id="s-634e35a33e"></a>`type`: `"object"`
- <a id="s-f5ef8b1475"></a>`required`: `["name","condition","current_revision","head_revision"]`
- <a id="s-4b86dc8806"></a>`title`: `"StateStatus"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-68b86b625f"></a>`condition` | yes | type="string"; enum=["empty","current","upgrade_required","unversioned","incompatible"]; title="Condition" |  |
| <a id="s-3a7c1edd7a"></a>`current_revision` | yes | anyOf=[(type="string"); (type="null")]; title="Current Revision" |  |
| <a id="s-3970899db2"></a>`head_revision` | yes | type="string"; title="Head Revision" |  |
| <a id="s-bd07704e29"></a>`name` | yes | type="string"; title="Name" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-648ad95ee1) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-d424ac54f9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c8a8236ef6"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-api](../../../evidence/sources.md#src-18139c42dd) — `riverhog/src/riverhog_api/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/allow_abbrev`
- `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/name`
- `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/result_contract`
- `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/parameters`

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

### `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 5a7b7053d2e0a290c9db54b042b35a6fbb99598e6a73a08d1d71f457ca2e5a9f -->

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
  "identity": "riverhog-api-cli-result/state/upgrade/v1",
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

### `/external_contract/cli/riverhog-api/commands/state/commands/upgrade/terminating_controls`

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
