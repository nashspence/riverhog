# stove0-server scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-server:stove0-server-scheduler:214121ca44 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8357693941"></a>Parser name: `scheduler`
- <a id="s-83e8b98c66"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-a539a6a1df"></a>`role`<br>`--role` | optional option; 1 value | not recorded; choices=`["controller","worker","combined"]` | `"combined"` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-7a5901ca9c"></a>`help` | <a id="s-c514a5810a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-462582402d"></a>`0` | <a id="s-562e893ae5"></a>`"noncontractual-framework-help"` | <a id="s-b4989d2fc5"></a>`"empty"` |

### Result and failure contract

- <a id="s-ce7a1715cf"></a>Result identity: `stove0-server-cli-result/scheduler/v1`
- <a id="s-23c0b03d87"></a>Profile: `stove0-server-cli-runtime/v1`
- <a id="s-52632257fe"></a>Structured output: `none`
- <a id="s-92458bc6ab"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-41193471de"></a>`stopped` | <a id="s-f63ed2e5ae"></a>`{"kind":"service-runtime-returned"}` | <a id="s-b98f7bdd3c"></a>`0` | <a id="s-3bcdb86595"></a>all: `no-command-result` | <a id="s-8c7fc6d87f"></a>all: `noncontractual-runtime-log` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-628ab273be"></a>`usage` | <a id="s-5adabd4e42"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-e7a4376602"></a>`2` | <a id="s-219d9f27ed"></a>all: `empty` | <a id="s-026dd4a5a8"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --role](#s-a539a6a1df) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-95efd85860"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e77fd772cd"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-server](../../../evidence/sources.md#src-6f5bc9f6db) — `reference/stove0/application/server/src/stove0_api/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-server/commands/scheduler/allow_abbrev`
- `/external_contract/cli/stove0-server/commands/scheduler/name`
- `/external_contract/cli/stove0-server/commands/scheduler/parameters`
- `/external_contract/cli/stove0-server/commands/scheduler/result_contract`
- `/external_contract/cli/stove0-server/commands/scheduler/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-server/commands/scheduler/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/commands/scheduler/name`

<!-- exact-contract-value: 877fc37669cb9960b62ee3b97e9e47bf8732dc50cc93fb34e447fa6318d23920 -->

```json
"scheduler"
```

### `/external_contract/cli/stove0-server/commands/scheduler/parameters`

<!-- exact-contract-value: 765f5be891cfe67f345979ffb6e2c6ed2819e062736cbd02a47db7dc46342b1e -->

```json
[
  {
    "choices": [
      "controller",
      "worker",
      "combined"
    ],
    "default": "combined",
    "dest": "role",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--role"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/stove0-server/commands/scheduler/result_contract`

<!-- exact-contract-value: 0ebebafd0f217eb757d497ec73fc2ba27a868ec97b9415487a6bc6d4e4baf231 -->

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
  "identity": "stove0-server-cli-result/scheduler/v1",
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

### `/external_contract/cli/stove0-server/commands/scheduler/terminating_controls`

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
