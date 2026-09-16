# riverhog-api state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-server:riverhog-api-state-verify:ece4af962f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-27e2aa4c46"></a>Parser name: `verify`
- <a id="s-5170169332"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-cfa7b4ca34"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-7c8fb8f37d"></a>`help` | <a id="s-00b9c1e2e1"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-5bb35cacb4"></a>`0` | <a id="s-177b8286b9"></a>`"noncontractual-framework-help"` | <a id="s-2f968aeed6"></a>`"empty"` |

### Result and failure contract

- <a id="s-944ede0252"></a>Result identity: `riverhog-api-cli-result/state/verify/v1`
- <a id="s-fe5c14a2d2"></a>Profile: `riverhog-api-cli-state/v1`
- <a id="s-7323a9e4b3"></a>Structured output: `optional-json`
- <a id="s-4e98ad2afd"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-dda385fa9c"></a>`completed` | <a id="s-abec21c4de"></a>`{"kind":"command-completed"}` | <a id="s-c7ab610028"></a>`0` | <a id="s-57932cfc57"></a>human: `noncontractual-presentation-of-command-result`; json: [riverhog-api-state-status/v1](#s-37dff76549) | <a id="s-c27cf877e9"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bf3e433e69"></a>`usage` | <a id="s-4daeaec6f3"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f442531c8b"></a>`2` | <a id="s-a0849c241a"></a>all: `empty` | <a id="s-d08786d2bb"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-c8cdf046bd"></a>`state-error` | <a id="s-a7bceef75b"></a>`{"kind":"application-error"}` | <a id="s-7656a4102c"></a>`1` | <a id="s-0550a4c7e3"></a>all: `empty` | <a id="s-feff1165a9"></a>all: `noncontractual-diagnostic` |

### Local structured outputs


#### <a id="s-37dff76549"></a>`riverhog-api-state-status/v1`

Applies to: completed · stdout (json).

<a id="s-b12bd8b108"></a>

- <a id="s-22f5788de7"></a>`type`: `"object"`
- <a id="s-3777135891"></a>`required`: `["name","condition","current_revision","head_revision"]`
- <a id="s-f2fa6957c8"></a>`title`: `"StateStatus"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dee53e1809"></a>`condition` | yes | type="string"; enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-af82c621d8"></a>`current_revision` | yes | anyOf=(type="string") \| (type="null") |  |
| <a id="s-68eafec6c2"></a>`head_revision` | yes | type="string" |  |
| <a id="s-29b15a7446"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-cfa7b4ca34) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-9464da49d9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8c2c2abc90"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-api](../../../evidence/sources.md#src-18139c42dd) — `riverhog/src/riverhog_api/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-api/commands/state/commands/verify/allow_abbrev`
- `/external_contract/cli/riverhog-api/commands/state/commands/verify/name`
- `/external_contract/cli/riverhog-api/commands/state/commands/verify/parameters`
- `/external_contract/cli/riverhog-api/commands/state/commands/verify/result_contract`
- `/external_contract/cli/riverhog-api/commands/state/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-api/commands/state/commands/verify/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-api/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/riverhog-api/commands/state/commands/verify/parameters`

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

### `/external_contract/cli/riverhog-api/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: 948bc13f4a5905bf2701c6c64305fa1d40c713cab804e7f7279df66e48ef713f -->

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
  "identity": "riverhog-api-cli-result/state/verify/v1",
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

### `/external_contract/cli/riverhog-api/commands/state/commands/verify/terminating_controls`

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
