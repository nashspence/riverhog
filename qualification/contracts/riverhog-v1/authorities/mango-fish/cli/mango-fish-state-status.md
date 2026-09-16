# mango-fish state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-state-status:44ee056490 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-23c16a2129"></a>Parser name: `status`
- <a id="s-da26689bb9"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f1aea638f4"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6fddeb07ef"></a>`help` | <a id="s-f11b00cbe6"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-484c49791a"></a>`0` | <a id="s-e1ddc154fb"></a>`"noncontractual-framework-help"` | <a id="s-1db75308e5"></a>`"empty"` |

### Result and failure contract

- <a id="s-a7ffa71a3e"></a>Result identity: `mango-fish-cli-result/state/status/v1`
- <a id="s-8163a11e67"></a>Profile: `mango-fish-cli-state-human-json/v1`
- <a id="s-fef5b3d8f3"></a>Structured output: `optional-json`
- <a id="s-09784fc8e9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1c9d261761"></a>`completed` | <a id="s-e088b7b87a"></a>`{"kind":"state-schema-operation-completed"}` | <a id="s-3576ac6723"></a>`0` | <a id="s-a41133c2a5"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-564fb3163a) | <a id="s-47792e58f1"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-83d4e893ff"></a>`usage` | <a id="s-bb8605b1ca"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0ed756ac5e"></a>`2` | <a id="s-bde8a0e7e0"></a>all: `"empty"` | <a id="s-ce98abc8ed"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-261943abb1"></a>`state-schema` | <a id="s-6c44ee637d"></a>`{"kind":"state-schema-error"}` | <a id="s-ff1a92103f"></a>`1` | <a id="s-f09a63bb89"></a>all: `"empty"` | <a id="s-afd2ae98a4"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-564fb3163a"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-b65b49ce91"></a>

- <a id="s-b4cffceed0"></a>`type`: `"object"`
- <a id="s-2ead9809f1"></a>`additionalProperties`: `false`
- <a id="s-f2fe1f53f5"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61ca57022d"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-3d0027b518"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-df4161e7c9"></a>`head_revision` | yes | type="string" |  |
| <a id="s-ca12f26b79"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-f1aea638f4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-182241e1a2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d16b31ad94"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/status/allow_abbrev`
- `/external_contract/cli/mango-fish/commands/state/commands/status/name`
- `/external_contract/cli/mango-fish/commands/state/commands/status/parameters`
- `/external_contract/cli/mango-fish/commands/state/commands/status/result_contract`
- `/external_contract/cli/mango-fish/commands/state/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/status/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/mango-fish/commands/state/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/mango-fish/commands/state/commands/status/parameters`

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

### `/external_contract/cli/mango-fish/commands/state/commands/status/result_contract`

<!-- exact-contract-value: 72c55e258ae11ecdf131c37494b3efd30f53fd83696cd25d58c222ca3b99395e -->

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
      "id": "state-schema",
      "selected_by": {
        "kind": "state-schema-error"
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
  "identity": "mango-fish-cli-result/state/status/v1",
  "profile_id": "mango-fish-cli-state-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "selected_by": {
        "kind": "state-schema-operation-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "identity": "state-schema-status/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "condition": {
                "enum": [
                  "empty",
                  "current",
                  "upgrade_required",
                  "unversioned",
                  "incompatible"
                ]
              },
              "current_revision": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "head_revision": {
                "type": "string"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "condition",
              "current_revision",
              "head_revision"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/mango-fish/commands/state/commands/status/terminating_controls`

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
