# stove0-server state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-server:stove0-server-state-verify:f09b5acf29 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e133cd60d0"></a>Parser name: `verify`
- <a id="s-ad3cdc799c"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-2585e3bfe8"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0afbb86413"></a>`help` | <a id="s-0bef0895b6"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-d1f1ee9cef"></a>`0` | <a id="s-285167c646"></a>`"noncontractual-framework-help"` | <a id="s-1c982b1a2c"></a>`"empty"` |

### Result and failure contract

- <a id="s-f121faf4f9"></a>Result identity: `stove0-server-cli-result/state/verify/v1`
- <a id="s-2e4f5b8caf"></a>Profile: `stove0-server-cli-state/v1`
- <a id="s-1da39c62aa"></a>Structured output: `optional-json`
- <a id="s-c81d38b649"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fcb29a0aca"></a>`completed` | <a id="s-20d1a7354f"></a>`{"kind":"command-completed"}` | <a id="s-3d1accc53b"></a>`0` | <a id="s-a021163408"></a>human: `noncontractual-presentation-of-command-result`; json: [stove0-server-state-status/v1](#s-343c17f088) | <a id="s-52ddd296f1"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-82940e40cc"></a>`usage` | <a id="s-3a49b36051"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-929ce8cab3"></a>`2` | <a id="s-968b0088f4"></a>all: `empty` | <a id="s-f9fe990ce5"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-4d4aa6e9ae"></a>`state-error` | <a id="s-ae175e2be6"></a>`{"kind":"application-error"}` | <a id="s-e285418831"></a>`1` | <a id="s-9de75d8471"></a>all: `empty` | <a id="s-8b4d7a724e"></a>all: `noncontractual-diagnostic` |

### Local structured outputs


#### <a id="s-343c17f088"></a>`stove0-server-state-status/v1`

Applies to: completed · stdout (json).

<a id="s-bf8bb3ba10"></a>

- <a id="s-061b65ba68"></a>`type`: `"object"`
- <a id="s-2273ef710c"></a>`required`: `["name","condition","current_revision","head_revision"]`
- <a id="s-c34a9ebedd"></a>`title`: `"StateStatus"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d8c48f97d8"></a>`condition` | yes | type="string"; enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-aad4b2dfe7"></a>`current_revision` | yes | anyOf=(type="string") \| (type="null") |  |
| <a id="s-e7a5082bad"></a>`head_revision` | yes | type="string" |  |
| <a id="s-99114c5912"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-2585e3bfe8) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-cbe9a2e84d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ad16511921"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-server](../../../evidence/sources.md#src-6f5bc9f6db) — `reference/stove0/application/server/src/stove0_api/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-server/commands/state/commands/verify/allow_abbrev`
- `/external_contract/cli/stove0-server/commands/state/commands/verify/name`
- `/external_contract/cli/stove0-server/commands/state/commands/verify/parameters`
- `/external_contract/cli/stove0-server/commands/state/commands/verify/result_contract`
- `/external_contract/cli/stove0-server/commands/state/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-server/commands/state/commands/verify/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/stove0-server/commands/state/commands/verify/parameters`

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

### `/external_contract/cli/stove0-server/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: 085b80699f268ae09f3451ace14ed7e0b7c1c057418c6caf52d473cd7791ee36 -->

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
  "identity": "stove0-server-cli-result/state/verify/v1",
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

### `/external_contract/cli/stove0-server/commands/state/commands/verify/terminating_controls`

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
