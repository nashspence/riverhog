# mango-fish state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-state-verify:384b9ac81b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9527b6167f"></a>Parser name: `verify`
- <a id="s-46bb6d815a"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-7505345121"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-96c21387d9"></a>`help` | <a id="s-661efc97a4"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-c1aac36992"></a>`0` | <a id="s-d9032240ef"></a>`"noncontractual-framework-help"` | <a id="s-6b4df12664"></a>`"empty"` |

### Result and failure contract

- <a id="s-6327dd9720"></a>Result identity: `mango-fish-cli-result/state/verify/v1`
- <a id="s-4b384d33d3"></a>Profile: `mango-fish-cli-state-human-json/v1`
- <a id="s-4994b906c5"></a>Structured output: `optional-json`
- <a id="s-f90c4c742b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cf072c036d"></a>`completed` | <a id="s-5ae8897388"></a>`{"kind":"state-schema-operation-completed"}` | <a id="s-71b34aab03"></a>`0` | <a id="s-114723fad1"></a>human: `noncontractual-presentation-of-command-result`; json: [state-schema-status/v1](#s-8d5852510b) | <a id="s-fd16c3d55b"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cfb5d4af33"></a>`usage` | <a id="s-c8a5a3e692"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-081ff352f1"></a>`2` | <a id="s-0845615c84"></a>all: `empty` | <a id="s-9d889bd126"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-103859e7e6"></a>`state-schema` | <a id="s-339b8275e1"></a>`{"kind":"state-schema-error"}` | <a id="s-f74b41fde2"></a>`1` | <a id="s-d87cb320b7"></a>all: `empty` | <a id="s-51dbf4baca"></a>all: `noncontractual-diagnostic` |

### Local structured outputs


#### <a id="s-8d5852510b"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-89175d01a7"></a>

- <a id="s-03c6899307"></a>`type`: `"object"`
- <a id="s-5c205dd177"></a>`additionalProperties`: `false`
- <a id="s-e67a5d9484"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa90cbe8d8"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-a3de378e88"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-3d5e090338"></a>`head_revision` | yes | type="string" |  |
| <a id="s-651904d0a2"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-7505345121) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-ffc58149b1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c0331b6971"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/verify/allow_abbrev`
- `/external_contract/cli/mango-fish/commands/state/commands/verify/name`
- `/external_contract/cli/mango-fish/commands/state/commands/verify/parameters`
- `/external_contract/cli/mango-fish/commands/state/commands/verify/result_contract`
- `/external_contract/cli/mango-fish/commands/state/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/verify/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/mango-fish/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/mango-fish/commands/state/commands/verify/parameters`

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

### `/external_contract/cli/mango-fish/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: 3019aef47467d0155e2780ab82250287c4f3178e9b313cbc877ec46b7b8498fd -->

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
  "identity": "mango-fish-cli-result/state/verify/v1",
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

### `/external_contract/cli/mango-fish/commands/state/commands/verify/terminating_controls`

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
