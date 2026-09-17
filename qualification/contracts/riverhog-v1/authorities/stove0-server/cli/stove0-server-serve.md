# stove0-server serve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-server:stove0-server-serve:6a56a6773b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-5d3ee28d64"></a>Parser name: `serve`
- <a id="s-6d19740d82"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-660eaa2b15"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"0.0.0.0"` |
| <a id="s-24e564e8f4"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d960d326c2"></a>`help` | <a id="s-7098a62ba4"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-f97403f5b7"></a>`0` | <a id="s-864d1330a9"></a>`"noncontractual-framework-help"` | <a id="s-b9f8286889"></a>`"empty"` |

### Result and failure contract

- <a id="s-bb00a0a874"></a>Result identity: `stove0-server-cli-result/serve/v1`
- <a id="s-57f21114a4"></a>Profile: `stove0-server-cli-runtime/v1`
- <a id="s-ec416be388"></a>Structured output: `none`
- <a id="s-11c6694c4b"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b9a370bb2b"></a>`stopped` | <a id="s-5cfe7f3e4b"></a>`{"kind":"service-runtime-returned"}` | <a id="s-76e54472e1"></a>`0` | <a id="s-a75e7204f8"></a>all: `"no-command-result"` | <a id="s-e782d5c933"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5824785927"></a>`usage` | <a id="s-81134d91aa"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2427a0314d"></a>`2` | <a id="s-57a207f94b"></a>all: `"empty"` | <a id="s-47f2b46ed2"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-660eaa2b15) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-24e564e8f4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-72feee4c74"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5c148b52cd"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-server](../../../evidence/sources.md#src-6f5bc9f6db) — [reference/stove0/application/server/src/stove0\_api/app.py::&lt;module&gt;](../../../../../../reference/stove0/application/server/src/stove0_api/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-server/commands/serve/allow_abbrev`
- `/external_contract/cli/stove0-server/commands/serve/name`
- `/external_contract/cli/stove0-server/commands/serve/parameters`
- `/external_contract/cli/stove0-server/commands/serve/result_contract`
- `/external_contract/cli/stove0-server/commands/serve/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-server/commands/serve/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/commands/serve/name`

<!-- exact-contract-value: f9cb5b4cc5c002946a5ca44426d4db8fbdf0ba02d0850cccb953338686f2de7c -->

```json
"serve"
```

### `/external_contract/cli/stove0-server/commands/serve/parameters`

<!-- exact-contract-value: 37f09f6ec1d10894e488d0329d9b49fe76ff6e22f99eca7c2e8d5f0660163ecc -->

```json
[
  {
    "default": "0.0.0.0",
    "dest": "host",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--host"
    ],
    "required": false
  },
  {
    "default": 8080,
    "dest": "port",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--port"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/stove0-server/commands/serve/result_contract`

<!-- exact-contract-value: 09dbade175e0e998d5324ba8507f358c4e1377f2df4fb43b8f69a988dbbcddda -->

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
  "human_json_relationship": "not-applicable",
  "identity": "stove0-server-cli-result/serve/v1",
  "profile_id": "stove0-server-cli-runtime/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "stopped",
      "selected_by": {
        "kind": "service-runtime-returned"
      },
      "stderr": {
        "all": "noncontractual-runtime-log"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-server/commands/serve/terminating_controls`

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
