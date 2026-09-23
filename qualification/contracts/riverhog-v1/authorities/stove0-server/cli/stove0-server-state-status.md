# stove0-server state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-server:stove0-server-state-status:eaea128afe -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3940dd993a"></a>Parser name: `status`
- <a id="s-09e144684f"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-a3d0ae9c3f"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b70c6d8c57"></a>`help` | <a id="s-695d8bf73e"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-7d2aa2012f"></a>`0` | <a id="s-7e01c62a29"></a>`"noncontractual-framework-help"` | <a id="s-29157f0975"></a>`"empty"` |

### Result and failure contract

- <a id="s-5ec442346a"></a>Result identity: `stove0-server-cli-result/state/status/v1`
- <a id="s-94f6279623"></a>Profile: `stove0-server-cli-state/v1`
- <a id="s-bc4cad3d17"></a>Structured output: `optional-json`
- <a id="s-0d7350204b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b63d2df545"></a>`completed` | <a id="s-73ac0b5afe"></a>`{"kind":"command-completed"}` | <a id="s-48f24190ca"></a>`0` | <a id="s-a8040563d9"></a>human: `"noncontractual-presentation-of-command-result"`; json: [stove0-server-state-status/v1](#s-1eb8de4f52) | <a id="s-45d960a6f6"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-59b48f38a5"></a>`usage` | <a id="s-71b1be5304"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-027dbf0bc7"></a>`2` | <a id="s-19391ac607"></a>all: `"empty"` | <a id="s-f3cfd93bf0"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-e2184cdaf3"></a>`state-error` | <a id="s-93adaac3f1"></a>`{"kind":"application-error"}` | <a id="s-c68199b2b2"></a>`1` | <a id="s-adb7efe9aa"></a>all: `"empty"` | <a id="s-64494038c8"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-1eb8de4f52"></a>`stove0-server-state-status/v1`

Applies to: completed · stdout (json).

<a id="s-7e69ae609d"></a>

- <a id="s-8d8f86c60a"></a>`type`: `"object"`
- <a id="s-1b14195881"></a>`required`: `["name","condition","current_revision","head_revision"]`
- <a id="s-6e4b9e20ce"></a>`title`: `"StateStatus"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1a0fbcd19a"></a>`condition` | yes | type="string"; enum=["empty","current","upgrade_required","unversioned","incompatible"]; title="Condition" |  |
| <a id="s-61a644ab69"></a>`current_revision` | yes | anyOf=[(type="string"); (type="null")]; title="Current Revision" |  |
| <a id="s-d2ba21e644"></a>`head_revision` | yes | type="string"; title="Head Revision" |  |
| <a id="s-2d51ea6367"></a>`name` | yes | type="string"; title="Name" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-a3d0ae9c3f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dddb96bfe3"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-e13e7e1231"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-server](../../../evidence/sources/authorities.md#src-6f5bc9f6db) — [some-implementations/stove0/application/server/src/stove0\_api/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-server/commands/state/commands/status/allow_abbrev`
- `/external_contract/cli/stove0-server/commands/state/commands/status/name`
- `/external_contract/cli/stove0-server/commands/state/commands/status/parameters`
- `/external_contract/cli/stove0-server/commands/state/commands/status/result_contract`
- `/external_contract/cli/stove0-server/commands/state/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-server/commands/state/commands/status/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/commands/state/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/stove0-server/commands/state/commands/status/parameters`

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

### `/external_contract/cli/stove0-server/commands/state/commands/status/result_contract`

<!-- exact-contract-value: 9f123e35a329d2f7f7e441e7107678768725f95b3b5689dd7c0f80954e7958c2 -->

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
  "identity": "stove0-server-cli-result/state/status/v1",
  "profile_id": "stove0-server-cli-state/v1",
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
          "identity": "stove0-server-state-status/v1",
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

### `/external_contract/cli/stove0-server/commands/state/commands/status/terminating_controls`

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
