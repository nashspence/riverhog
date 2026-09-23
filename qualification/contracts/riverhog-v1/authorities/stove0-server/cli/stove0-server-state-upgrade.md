# stove0-server state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-server:stove0-server-state-upgrade:dc864b8f33 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cbef36f027"></a>Parser name: `upgrade`
- <a id="s-e78d6e9d78"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-ac5aebf23f"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-e0aab5ba95"></a>`help` | <a id="s-7b21c60cc5"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-498745eafd"></a>`0` | <a id="s-1d0132fb75"></a>`"noncontractual-framework-help"` | <a id="s-96414a62cf"></a>`"empty"` |

### Result and failure contract

- <a id="s-ab6a80fc24"></a>Result identity: `stove0-server-cli-result/state/upgrade/v1`
- <a id="s-9710a5d7f4"></a>Profile: `stove0-server-cli-state/v1`
- <a id="s-4f1d91505d"></a>Structured output: `optional-json`
- <a id="s-179c40b456"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6490ce65c8"></a>`completed` | <a id="s-c3cb629cc4"></a>`{"kind":"command-completed"}` | <a id="s-f476405bf4"></a>`0` | <a id="s-fe36d0be07"></a>human: `"noncontractual-presentation-of-command-result"`; json: [stove0-server-state-status/v1](#s-356fa8d7ec) | <a id="s-ab6ad77984"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7018875163"></a>`usage` | <a id="s-60bd510567"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1f46970160"></a>`2` | <a id="s-1acbef8b46"></a>all: `"empty"` | <a id="s-c1138f0407"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-7ce93d55f7"></a>`state-error` | <a id="s-96ed90cb50"></a>`{"kind":"application-error"}` | <a id="s-5e2db5d803"></a>`1` | <a id="s-17c14e7476"></a>all: `"empty"` | <a id="s-709cec2fc6"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-356fa8d7ec"></a>`stove0-server-state-status/v1`

Applies to: completed · stdout (json).

<a id="s-edae4c60c3"></a>

- <a id="s-5296bc5116"></a>`type`: `"object"`
- <a id="s-f131616e40"></a>`required`: `["name","condition","current_revision","head_revision"]`
- <a id="s-9bd950d139"></a>`title`: `"StateStatus"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-503b932173"></a>`condition` | yes | type="string"; enum=["empty","current","upgrade_required","unversioned","incompatible"]; title="Condition" |  |
| <a id="s-13a4bca283"></a>`current_revision` | yes | anyOf=[(type="string"); (type="null")]; title="Current Revision" |  |
| <a id="s-f91aa823c2"></a>`head_revision` | yes | type="string"; title="Head Revision" |  |
| <a id="s-befa1025cb"></a>`name` | yes | type="string"; title="Name" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-ac5aebf23f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f43ceeaee8"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-caed35ff5e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-server](../../../evidence/sources/authorities.md#src-6f5bc9f6db) — [some-implementations/stove0/application/server/src/stove0\_api/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-server/commands/state/commands/upgrade/allow_abbrev`
- `/external_contract/cli/stove0-server/commands/state/commands/upgrade/name`
- `/external_contract/cli/stove0-server/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/stove0-server/commands/state/commands/upgrade/result_contract`
- `/external_contract/cli/stove0-server/commands/state/commands/upgrade/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-server/commands/state/commands/upgrade/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/stove0-server/commands/state/commands/upgrade/parameters`

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

### `/external_contract/cli/stove0-server/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 3f90d7cca2a9ca57ba4544dda4cd2245457a4c90a2ee01e7f3961fce05328164 -->

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
  "identity": "stove0-server-cli-result/state/upgrade/v1",
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

### `/external_contract/cli/stove0-server/commands/state/commands/upgrade/terminating_controls`

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
